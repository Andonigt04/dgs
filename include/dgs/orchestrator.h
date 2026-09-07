#ifndef DGS_ORCHESTRATOR_H
#define DGS_ORCHESTRATOR_H

#include "include/dgs/types.h"
#include "include/dgs/network.h"
// ⚠️ This header USES `DGS::Packet` and did not include it: it only compiled if whoever included it
// had already pulled in `packet.h`. The nodes do, so it went unnoticed; the moment a test includes it
// on its own it fails with `'Packet' is not a member of 'DGS'`. A header must compile by itself.
#include "include/dgs/packet.h"

#include <httplib.h>

#include <vector>
#include <map>
#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdlib>
#include <chrono>
#include <algorithm>
#include <set>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <cstdio>

namespace DGS
{
    // CONFIGURABLE evaluation thresholds/constants (§4.2). Read from the environment, with defaults.
    // Resolved once (not per call) so the metrics hot path never pays for getenv/atof.
    static float evalCfg(const char* name, float def)
    {
        const char* v = std::getenv(name);
        return v ? (float)std::atof(v) : def;
    }

    struct MetricsRate
    {
        uint64_t lastBytesRx = 0;
        uint64_t lastBytesTx = 0;
        uint64_t lastStartTimeS = 0;
        bool     haveBaseline = false;   // primeras muestras: establecen baseline (EWMA)
        bool     haveEwma = false;
        double   rxEWMA = 0.0;           // bytes/s
        double   txEWMA = 0.0;
        // ⚠️ THE TICK TIME WAS THE ONLY SIGNAL WITH NO SMOOTHING, and it is the one the split decision
        // now leans on. A single slow tick — a persistence flush, a restore, the OS — would have been
        // enough to order a scale-up on a healthy zone. Same EWMA as the byte rates.
        double   tickEWMA = 0.0;         // ms
    };

    // §3.9 (P9b): lifecycle operations with a formal PRIORITY. Higher value = executed first.
    // The triggers in evaluateServer/sweep do NOT mutate the cluster directly: they enqueue the
    // operation and `processLifecycleQueue()` runs ONE per tick by global priority. Order: CRASH/lease
    // (most urgent) > REASSIGN on failure (P6/F1: validator down / high failedTransfers) > MERGE >
    // SPLIT. That way a dead pod is evicted before any merge/scale, and a split never runs in the same
    // tick as a merge pending on the same zone.
    enum class LifecycleOp : uint8_t
    {
        LIFECYCLE_SPLIT     = 0,   // bajo carga (load/net)
        LIFECYCLE_MERGE     = 1,   // idle zone (window + hysteresis)
        LIFECYCLE_REASSIGN  = 2,   // P6: failing zone (P2+O5, F1) → handoff to a healthy neighbour
        LIFECYCLE_EVICT     = 3    // crash / lease vencido
    };

    // §3.8 (P8): abstract SPAWN backend. The orchestrator does NOT know the infrastructure: every
    // backend implements create/destroy/resize with the SAME lifecycle semantics (§3.9), so standalone
    // and cluster behave identically (F14). The mode only changes how a node is materialised:
    //   - LOCAL:     fork/exec of the zone_node binary on this same machine (dev/demo/portable, 1 node).
    //   - K8S:       the kubernetes API from inside the cluster (ServiceAccount) — the real cluster mode.
    //   - TERRAFORM: infrastructure already provisioned by `dgs up --terraform`; spawn applies the SAME
    //                manifest via `kubectl apply` (parity with K8S, but terraform raised the cluster).
    enum class SpawnBackend : uint8_t
    {
        SPAWN_LOCAL     = 0,
        SPAWN_K8S       = 1,
        SPAWN_TERRAFORM = 2
    };

    // Resolves the backend exactly once. Precedence: env DGS_SPAWN_BACKEND (local/k8s/terraform);
    // failing that, K8S if running in-cluster (a ServiceAccount exists), otherwise LOCAL (portable).
    static SpawnBackend resolveSpawnBackend()
    {
        const char* v = std::getenv("DGS_SPAWN_BACKEND");
        if (v)
        {
            std::string s(v);
            if (s == "local" || s == "LOCAL")     return SpawnBackend::SPAWN_LOCAL;
            if (s == "terraform" || s == "TERRAFORM") return SpawnBackend::SPAWN_TERRAFORM;
            return SpawnBackend::SPAWN_K8S;
        }
        std::ifstream f("/var/run/secrets/kubernetes.io/serviceaccount/token");
        return f.good() ? SpawnBackend::SPAWN_K8S : SpawnBackend::SPAWN_LOCAL;
    }

    class Orchestrator
    {
        public:
            Orchestrator(DGS::TCPSocket& s)
                : socket(s), currentReplicas(1), backend(resolveSpawnBackend()) {}
            std::vector<ZoneInfo> activeZones;

            // §3.8 (P8): access to the active backend (the head exposes it for the `dgs status` CLI).
            SpawnBackend spawnBackend() const { return backend; }
            void setSpawnBackend(SpawnBackend b) { backend = b; }

            void updateNodeTopology(int fd, const ServerMetrics& m)
            {
                lastSeenMs[fd] = steadyMs();   // §3.9: lease/eviction on staleness

                PopProfile& pp = popProfiles[fd];
                std::memcpy(pp.x, m.popX, sizeof(pp.x));
                std::memcpy(pp.y, m.popY, sizeof(pp.y));
                std::memcpy(pp.z, m.popZ, sizeof(pp.z));

                for (auto& zone : activeZones)
                {
                    if (zone.fd == fd)
                    {
                        zone.chunkXMin = m.node.chunkXMin; zone.chunkXMax = m.node.chunkXMax;
                        zone.chunkYMin = m.node.chunkYMin; zone.chunkYMax = m.node.chunkYMax;
                        zone.chunkZMin = m.node.chunkZMin; zone.chunkZMax = m.node.chunkZMax;
                        std::strncpy(zone.addr, m.node.addr, sizeof(zone.addr) - 1);
                        zone.port = m.node.port;
                        return;
                    }
                }

                if (!isRoutable(fd)) return;   // §3.9: a retiring/dead zone is not re-registered

                ZoneInfo info{};
                info.fd        = fd;
                info.chunkXMin = m.node.chunkXMin; info.chunkXMax = m.node.chunkXMax;
                info.chunkYMin = m.node.chunkYMin; info.chunkYMax = m.node.chunkYMax;
                info.chunkZMin = m.node.chunkZMin; info.chunkZMax = m.node.chunkZMax;
                std::strncpy(info.addr, m.node.addr, sizeof(info.addr) - 1);
                info.port = m.node.port;
                activeZones.push_back(info);

                // §3.9 gap 2: the BASE zone-node never goes through spawnZoneNode, so `portToName`
                // does not know its deployment (port 42425 → "zone-node"). Register it here so lease
                // eviction and draining can delete ITS pod too (no leaked replicas).
                if (!portToName.count(info.port))
                {
                    const int basePort = (int)evalCfg("ZONE_BASE_PORT", 42425);
                    portToName[info.port] = (info.port == basePort) ? "zone-node" : "zone-node-" + std::to_string(info.port);
                }
            }

            int findTargetNode(int32_t chunkX, int32_t chunkY, int32_t chunkZ)
            {
                for (const auto& zone : activeZones)
                {
                    if (!isRoutable(zone.fd)) continue;   // §3.9: DRAINING/DEAD receives nothing
                    if (chunkX >= zone.chunkXMin && chunkX <= zone.chunkXMax &&
                        chunkY >= zone.chunkYMin && chunkY <= zone.chunkYMax &&
                        chunkZ >= zone.chunkZMin && chunkZ <= zone.chunkZMax) return zone.fd;
                }

                return -1;
            }

            ZoneResponse findZoneResponse(int32_t chunkX, int32_t chunkY, int32_t chunkZ)
            {
                for (const auto& zone : activeZones)
                {
                    if (!isRoutable(zone.fd)) continue;   // §3.9
                    if (chunkX >= zone.chunkXMin && chunkX <= zone.chunkXMax &&
                        chunkY >= zone.chunkYMin && chunkY <= zone.chunkYMax &&
                        chunkZ >= zone.chunkZMin && chunkZ <= zone.chunkZMax)
                    {
                        ZoneResponse r{};
                        std::strncpy(r.addr, zone.addr, sizeof(r.addr) - 1);
                        r.port = zone.port;
                        return r;
                    }
                }
                return ZoneResponse{};
            }

            std::vector<int> findNeighbors(int fd, NeighborMode mode = NeighborMode::FACE)
            {
                const ZoneInfo* origin = nullptr;
                for (const auto& z : activeZones)
                    if (z.fd == fd) { origin = &z; break; }

                std::vector<int> neighbors;
                if (!origin) return neighbors;

                for (const auto& z : activeZones)
                {
                    if (z.fd == fd) continue;
                    if (!isRoutable(z.fd)) continue;   // §3.9: do not count drained/expired zones as neighbours

                    bool adjX = origin->chunkXMin <= z.chunkXMax + 1 && z.chunkXMin <= origin->chunkXMax + 1;
                    bool adjY = origin->chunkYMin <= z.chunkYMax + 1 && z.chunkYMin <= origin->chunkYMax + 1;
                    bool adjZ = origin->chunkZMin <= z.chunkZMax + 1 && z.chunkZMin <= origin->chunkZMax + 1;

                    if (!adjX || !adjY || !adjZ) continue;

                    int touching = 0;
                    if (origin->chunkXMax + 1 == z.chunkXMin || z.chunkXMax + 1 == origin->chunkXMin) touching++;
                    if (origin->chunkYMax + 1 == z.chunkYMin || z.chunkYMax + 1 == origin->chunkYMin) touching++;
                    if (origin->chunkZMax + 1 == z.chunkZMin || z.chunkZMax + 1 == origin->chunkZMin) touching++;

                    bool include = false;
                    switch (mode)
                    {
                        case NeighborMode::FACE:             include = touching == 1; break;
                        case NeighborMode::FACE_EDGE:        include = touching <= 2; break;
                        case NeighborMode::FACE_EDGE_CORNER: include = touching <= 3; break;
                    }

                    if (include) neighbors.push_back(z.fd);
                }
                return neighbors;
            }

            void evaluateServer(const ServerMetrics& m, int nodeFD)
            {
                // Constantes configurables, resueltas UNA vez (env o default) — §4.2.
                static const double alpha    = evalCfg("EVAL_EWMA_ALPHA", 0.2);
                static const double dtEst    = evalCfg("EVAL_DT_S",       0.1);
                static const float  loadTh   = evalCfg("EVAL_LOAD_RAM",   0.80f);

                // ⚠️ THE LOAD SIGNAL WAS BACKWARDS, and it was backwards in a way that made it fire on
                // the wrong zones. It read:
                //
                //     load = ramUsage > EVAL_LOAD_RAM && performance < EVAL_LOAD_PERF   (default 0.36)
                //
                // `ServerMetrics::performance` is documented "0..1", but what `zone_node` actually puts
                // in it is THE TICK TIME IN MILLISECONDS (see the end of its main loop). So the second
                // half of that AND said "and its tick took less than 0.36 ms" — it demanded the zone be
                // FAST to call it overloaded, and a zone that had genuinely fallen behind, ticking at
                // 40 ms of its 100 ms budget, was disqualified from splitting by the very fact that it
                // was struggling.
                //
                // It is now the tick time against its own budget (`ZONE_TICK_US`, 100 ms), which is the
                // one number that already accounts for entities, interest management, validation and
                // broadcast together. RAM stays as a second, independent signal — a zone can run out of
                // memory without missing a tick — and they are OR'd, not AND'd: either one is enough.
                //
                // THE DEFAULT IS MEASURED, not guessed. One zone, one chunk of 1000 m,
                // INTEREST_RADIUS_M=500 (so everybody is inside everybody's radius — a crowd, which is
                // the case that makes a zone split), players injected at 20 Hz, tick time read back out
                // of its own metrics:
                //
                //     players    25     50     60     70     80     100    150    200
                //     tick p50  3.2   21.3   16.0   55.7  111.3   103.7  308.2  692.7   ms
                //
                // Quadratic, and the knee is sharp: 60 players cost 16 ms, 80 cost 111. The first draft
                // of this used 60 ms, which fires at ~72 players — but a split takes ~30 s to land
                // (cooldown + settle + spawn) and on that curve a zone at 72 keeps climbing while it
                // waits. 25 ms fires at ~63 and buys that reaction time. Below ~20 ms the numbers are
                // in the noise of an idle zone, so there is nothing to gain by going lower.
                //
                // ⚠️ Y ES UNA FRACCION DEL PRESUPUESTO, NO UN NUMERO SUELTO. Los 25 ms de la primera
                // version eran el 25 % de un periodo de 100 ms. Con el tick a 60 Hz el periodo son
                // 16,6 ms, y 25 ms ya no es "una zona ocupada": es una zona que NO LLEGA — habria
                // que esperar a que perdiera el ritmo para empezar a partirla, que es justo tarde.
                // Atado al mismo `ZONE_TICK_US` que usa la zona, el umbral se mueve con el reloj en
                // vez de quedarse anclado a un periodo que ya no existe. El 70 % deja sitio para los
                // ~30 s que tarda un split en aterrizar (cooldown + settle + arranque).
                static const float  tickBudgetMs = (float)(std::getenv("ZONE_TICK_US")
                                        ? std::atoi(std::getenv("ZONE_TICK_US")) : 16666) / 1000.0f;
                static const float  tickMsTh = evalCfg("EVAL_LOAD_TICK_MS", tickBudgetMs * 0.70f);

                // A third signal, off by default: split at a flat entity count. The tick time above is
                // self-calibrating and should be preferred; this exists because "no more than N players
                // per zone" is sometimes a product decision rather than a measurement, and there was no
                // way to express it — `activeEntities` was only ever read for the MERGE side.
                // 0 = disabled, and it stays disabled by default even now that the number IS measured
                // (~64 players in one chunk on this hardware, from the curve above). A flat cap cannot
                // tell a crowd from a spread-out population: the same 200 players scattered over twenty
                // chunks cost a fraction of what they cost standing together, and a flat cap would split
                // zones that are not struggling. The tick time already knows the difference. Set this
                // only when "no more than N players per zone" is a product rule rather than a capacity
                // one.
                static const uint32_t entTh = (uint32_t)evalCfg("EVAL_LOAD_ENTITIES", 0.0f);
                static const double asymmTh  = evalCfg("EVAL_NET_ASYMM",   4.0);
                static const float  failTh   = evalCfg("EVAL_FAIL_THRESH", 40.0f);
                static const float  mergeLoad = evalCfg("EVAL_MERGE_LOAD_RAM", 0.22f);
                static const int    sweepEveryMs = (int)evalCfg("EVAL_SWEEP_MS", 10000);
                static const uint32_t mergeMinEntities = (uint32_t)evalCfg("EVAL_MERGE_ENTITIES", 1);

                // §3.9 drain fail-safe: if the node does not ack within the timeout, go back to READY
                // (never leave the region empty).
                if (zoneState(nodeFD) == ZoneState::DRAINING)
                {
                    auto dl = drainDeadlineMs.find(nodeFD);
                    if (dl != drainDeadlineMs.end() && steadyMs() > dl->second)
                    {
                        std::cout << "[Orchestrator] Drain timeout fd=" << nodeFD
                                  << " -> READY (fail-safe)" << std::endl;
                        markZoneState(nodeFD, ZoneState::READY);
                        drainDeadlineMs.erase(nodeFD);
                        drainRequestId.erase(nodeFD);
                        drainTarget.erase(nodeFD);
                    }
                }

                // Eviction of zombie pods (crash / expired lease) every ~sweepEveryMs.
                uint64_t nowMs = steadyMs();
                if (nowMs - lastSweepMs > (uint64_t)sweepEveryMs)
                {
                    lastSweepMs = nowMs;
                    sweepStaleZones();
                }

                // Tracks the "under load" window for merging (hysteresis §3.9). A merge is not only
                // triggered by low RAM: a zone with FEW active entities (visiting players, no
                // neighbourhood) is a candidate to give up its range even at medium RAM — this is the
                // TODO's "remove zone_nodes based on players count". Threshold configurable
                // (EVAL_MERGE_ENTITIES, default 1).
                auto lowIt = lowSinceMs.find(nodeFD);
                bool idleEntities = m.activeEntities <= mergeMinEntities;
                if (m.ramUsage < mergeLoad || idleEntities)
                {
                    if (lowIt == lowSinceMs.end()) lowSinceMs[nodeFD] = nowMs;
                }
                else
                    lowSinceMs.erase(nodeFD);

                // --- Per-node EWMA state (bandwidth rates). Accumulated counters → window-free Δ.
                MetricsRate& r = metricRates[nodeFD];

                if (!r.haveBaseline)
                {
                    r.lastBytesRx = m.bytesRx;
                    r.lastBytesTx = m.bytesTx;
                    r.lastStartTimeS = m.startTimeS;
                    r.haveBaseline = true;
                    processLifecycleQueue();   // P9b: drain pending ops even though this is a baseline
                    return;   // first sample: baseline only, decides nothing
                }

                // RESTARTED node (startTimeS changed): drop the stale counter baseline.
                if (r.lastStartTimeS != 0 && m.startTimeS != 0 && m.startTimeS != r.lastStartTimeS)
                {
                    r.lastBytesRx = m.bytesRx;
                    r.lastBytesTx = m.bytesTx;
                    r.lastStartTimeS = m.startTimeS;
                    processLifecycleQueue();   // P9b: keep draining the queue
                    return;
                }

                // Unsigned Δ (modular subtraction, immune to a uint64 counter wrapping).
                uint64_t dRx = m.bytesRx - r.lastBytesRx;
                uint64_t dTx = m.bytesTx - r.lastBytesTx;
                r.lastBytesRx = m.bytesRx;
                r.lastBytesTx = m.bytesTx;

                // EWMA of the rate in bytes/s (real inter-sample time is the zone's ~100 ms tick).
                double rxRate = (double)dRx / dtEst;
                double txRate = (double)dTx / dtEst;
                if (!r.haveEwma)
                {
                    r.rxEWMA = rxRate;
                    r.txEWMA = txRate;
                    r.tickEWMA = (double)m.performance;
                    r.haveEwma = true;
                }
                else
                {
                    r.rxEWMA = alpha * rxRate + (1.0 - alpha) * r.rxEWMA;
                    r.txEWMA = alpha * txRate + (1.0 - alpha) * r.txEWMA;
                    r.tickEWMA = alpha * (double)m.performance + (1.0 - alpha) * r.tickEWMA;
                }

                // --- Decision: three independent signals, ONE lifecycle queue (§4.2, P6/P9b) ---
                // No signal mutates the cluster here: each enqueues its operation and
                // `processLifecycleQueue()` (below) runs ONE per tick by priority. The split cooldown,
                // the merge window and neighbour availability for a handoff are checked at execution.
                const bool ramHot  = m.ramUsage      > loadTh;
                const bool tickHot = r.tickEWMA      > tickMsTh;              // ms of its 100 ms budget
                const bool crowded = entTh > 0 && m.activeEntities > entTh;
                bool load          = ramHot || tickHot || crowded;
                // ⚠️ THIS SIGNAL FIRED ON EVERY IDLE ZONE, and it is what actually caused the splits
                // that looked like the load signal working. It was `txEWMA > 0 && txEWMA/(rxEWMA+1) >
                // 4`, and a ratio of egress to ingress is disproportionate BY DESIGN in a zone: it
                // receives one transform per player and broadcasts the world to all of them. An idle
                // zone sending nothing but metrics — a few hundred bytes a second — against an rx of
                // zero scores a ratio in the hundreds. Measured: a zone with one player and a 0.02 ms
                // tick was being told to split, repeatedly, until it hit the one-chunk floor.
                //
                // The ratio only means anything once the egress is large in absolute terms. The floor
                // comes from a measurement already in this repo: 64 players cost 2.67 MB/s of egress
                // before interest management and 0.17 MB/s after. 1 MB/s sits above a healthy busy zone
                // and below the pathological one.
                static const double asymmMinTx = evalCfg("EVAL_NET_MIN_TX_BPS", 1000000.0f);
                bool netSaturated  = r.txEWMA > asymmMinTx &&
                                     r.txEWMA / (r.rxEWMA + 1.0) > asymmTh;   // sends far more than it receives
                bool failureProne  = m.failedTransfers > (uint32_t)failTh;    // validador/traspaso va mal

                if (failureProne)
                {
                    // P6 (P2+O5, F1): the node is failing (validation timeouts / broken handoffs).
                    // It is not scaled up: its region is REASSIGNED to a healthy neighbour (handoff on a
                    // failed metric). Priority 2 (after crash, before merge/split).
                    std::cout << "[Orchestrator] fallo fd=" << nodeFD
                              << " failedTransfers=" << m.failedTransfers
                              << " -> encolado REASSIGN" << std::endl;
                    enqueueLifecycle(nodeFD, LifecycleOp::LIFECYCLE_REASSIGN);
                }
                else if (load || netSaturated)
                {
                    // A zone already at the one-chunk floor cannot be split, so queueing one is not a
                    // retry — it is a spin. It used to happen on every metrics sample, ten times a
                    // second, for as long as the zone stayed overloaded. Report the real condition on a
                    // throttle instead, because THIS is the state an operator has to see: a zone over
                    // its budget that the cluster cannot help.
                    if (atSplitFloor.count(nodeFD))
                    {
                        static const uint64_t sayEveryMs =
                            (uint64_t)evalCfg("EVAL_FLOOR_REPORT_S", 30.0f) * 1000;
                        auto& last = floorReportedMs[nodeFD];
                        if (steadyMs() - last >= sayEveryMs)
                        {
                            last = steadyMs();
                            std::cout << "[Orchestrator] ⚠ zone fd=" << nodeFD
                                      << " OVERLOADED AND UNSPLITTABLE: tick="
                                      << r.tickEWMA << "ms entities=" << m.activeEntities
                                      << " ram=" << m.ramUsage
                                      << " — it owns one chunk. Nothing the orchestrator can do."
                                      << std::endl;
                        }
                    }
                    else
                    {
                        std::cout << "[Orchestrator] Umbral alcanzado fd=" << nodeFD
                                  << " (ram=" << ramHot << " tick=" << tickHot
                                  << " crowd=" << crowded << " net=" << netSaturated
                                  << ") -> encolado SPLIT" << std::endl;
                        enqueueLifecycle(nodeFD, LifecycleOp::LIFECYCLE_SPLIT);
                    }
                }
                // §3.9 scaling DOWN (merge): only if the node is consistently under load (window +
                // hysteresis) and there is a smaller neighbour to drain. Enqueuing MERGE puts it behind
                // any pending crash/lease and never in the same tick as a split.
                else if (lowSinceMs.count(nodeFD))
                    enqueueLifecycle(nodeFD, LifecycleOp::LIFECYCLE_MERGE);

                // P9b: drain the lifecycle queue (at most ONE operation per evaluation).
                processLifecycleQueue();
            }

            // ---------------------------------------------------------------------------------------
            // §3.9 zone LIFECYCLE: merge/scale-down, orderly destruction, zombie eviction and the drain
            // fail-safe. States: PROVISIONING → READY → DRAINING → DESTROYED (+ DEAD).
            // ---------------------------------------------------------------------------------------

            void markZoneState(int fd, ZoneState s) { zoneStates[fd] = s; }
            ZoneState zoneState(int fd) const
            {
                auto it = zoneStates.find(fd);
                return it == zoneStates.end() ? ZoneState::READY : it->second;
            }
            int replicas() const { return currentReplicas; }

            bool isRoutable(int fd) const
            {
                ZoneState s = zoneState(fd);
                return s != ZoneState::DRAINING && s != ZoneState::DEAD && s != ZoneState::DESTROYED;
            }

            /// How many zones are actually SERVING right now — derived from the topology, so it cannot
            /// drift.
            ///
            /// ⚠️ THIS EXISTS BECAUSE `currentReplicas` LIES, AND LYING ONE WAY IS PERMANENT.
            /// `currentReplicas` is only incremented by zones the orchestrator SPAWNED itself, but it is
            /// decremented for EVERY zone that leaves — and a zone can join on its own (the base
            /// zone-node, or a replica deployed by hand: they simply connect and register through
            /// `updateNodeTopology`). So the counter drifts downwards and never recovers. Measured with
            /// a probe: three self-registered zones evicted took it from 1 to **-2**, and it stayed at
            /// -2 while two healthy zones rejoined. Since `tryMergeDown` and `tryReassign` both guard on
            /// `currentReplicas <= EVAL_MIN_REPLICAS` (default 1), that silently disabled merging AND
            /// the handoff-on-failure for the whole cluster, for the life of the process — the exact
            /// moment a failing zone most needs to be handed over.
            ///
            /// The guards want to know "how many zones would still be serving if I gave this one up".
            /// That is this number, and it is computed from `activeZones`, so no bookkeeping can rot.
            int routableZoneCount() const
            {
                int n = 0;
                for (const auto& z : activeZones) if (isRoutable(z.fd)) ++n;
                return n;
            }

            // Processes the ACK of a PKT_DRAIN (accepted by the node) → confirms destruction.
            void handleZoneLifecycle(int fd, const ZoneLifecycle& lc)
            {
                if (zoneState(fd) != ZoneState::DRAINING)
                {
                    std::cout << "[Orchestrator] Ack " << (int)lc.ack << " de fd=" << fd
                              << " ignorado (no estaba DRAINING)" << std::endl;
                    return;
                }
                auto rq = drainRequestId.find(fd);
                if (rq != drainRequestId.end() && rq->second != lc.requestId)
                {
                    std::cout << "[Orchestrator] Ack stale requestId=" << lc.requestId << std::endl;
                    return;
                }

                std::cout << "[Orchestrator] Drain OK fd=" << fd << " -> destroying the zone" << std::endl;

                // 1) The survivor already absorbed the range in the topology (absorbRegion when the
                //    drain was requested). 2) Delete the pod (deployment+service) so no replicas leak.
                deleteZoneNode(fd);

                // 3) Remove from activeZones, decrement replicas, mark DESTROYED.
                removeFromActiveZones(fd);
                if (currentReplicas > 0) --currentReplicas;   // never report a negative replica count
                markZoneState(fd, ZoneState::DESTROYED);
                drainDeadlineMs.erase(fd);
                drainRequestId.erase(fd);
                drainTarget.erase(fd);
                lastLifecycleMs[fd] = steadyMs();   // anti-flappy: asentamiento

                // 4) Confirm its exit to the node (it may not have seen the pod deletion).
                DGS::ZoneLifecycle del{ lc.requestId, 0 };
                DGS::Packet pDel; pDel.packDelete(del);
                socket.send(fd, pDel.getRawData(), pDel.getSize());
            }

            // Eviction on staleness (expired lease): the zombie pod (crashed without deletion) is
            // removed so replicas do not leak (§3.9 unplanned death, F3/F4). It detects and ENQUEUES the
            // eviction (P9b): it does not delete inline — priority orders it against merge/split.
            void sweepStaleZones()
            {
                uint64_t now = steadyMs();
                uint64_t lease = (uint64_t)evalCfg("EVAL_ZONE_LEASE_S", 30) * 1000;
                for (auto it = activeZones.begin(); it != activeZones.end();)
                {
                    int fd = it->fd;
                    auto ls = lastSeenMs.find(fd);
                    if (ls == lastSeenMs.end()) { ++it; continue; }
                    if (now - ls->second > lease)
                    {
                        std::cout << "[Orchestrator] Lease vencido fd=" << fd
                                  << " (ultima metrica hace " << (now - ls->second) / 1000
                                  << "s) -> encolado EVICT" << std::endl;
                        enqueueLifecycle(fd, LifecycleOp::LIFECYCLE_EVICT);
                    }
                    ++it;
                }
            }

            // P9b: enqueues a lifecycle operation for `fd`. If another is already pending for the same
            // zone, the HIGHER priority one wins (crash>merge>split).
            void enqueueLifecycle(int fd, LifecycleOp op)
            {
                auto it = pendingLifecycle.find(fd);
                if (it == pendingLifecycle.end()) { pendingLifecycle[fd] = op; return; }

                // ⚠️ MERGE AND SPLIT ON THE SAME ZONE ARE NOT A PRIORITY QUESTION, they are a
                // CONTRADICTION, and treating them as a priority question meant a zone could never
                // scale up once it had been idle. `pendingLifecycle` holds one op per fd and MERGE
                // outranks SPLIT, so a MERGE queued while the zone was empty sat in that slot and every
                // later SPLIT was silently discarded by the `>` below.
                //
                // Measured: 80 players in one zone, shipped defaults, 25 s — the head printed
                // "Umbral alcanzado ... -> encolado SPLIT" 217 times and spawned ZERO children, while
                // the queue kept postponing op 1 (merge) inside its settle window.
                //
                // The newest of the two is the true one: they come from the same evaluation of the same
                // metrics sample, which chooses one or the other. Crash and reassign still outrank both.
                const bool bothScaling =
                    (op == LifecycleOp::LIFECYCLE_MERGE || op == LifecycleOp::LIFECYCLE_SPLIT) &&
                    (it->second == LifecycleOp::LIFECYCLE_MERGE || it->second == LifecycleOp::LIFECYCLE_SPLIT);

                if (bothScaling || (int)op > (int)it->second)
                    pendingLifecycle[fd] = op;
            }

            // P6 (F1): the head calls this on receiving PKT_VALIDATOR_STATUS with state=DOWN/OPEN (the
            // validator's breaker open). Node `fd` is serving without verdicts → the master reassigns its
            // region to a healthy neighbour (handoff on a failed metric). The status's accumulated
            // `failedTransfers` also feeds evaluateServer's trigger; here the trip is EXPLICIT and
            // prioritised above merge/split (LIFECYCLE_REASSIGN).
            void notifyValidatorDown(int fd, const DGS::ValidatorStatus& st)
            {
                std::cout << "[Orchestrator] Validador DOWN/OPEN en fd=" << fd
                          << " (reqTimeout=" << st.reqTimeout
                          << " failed=" << st.failedTransfers
                          << ") -> encolado REASSIGN" << std::endl;
                enqueueLifecycle(fd, LifecycleOp::LIFECYCLE_REASSIGN);
                processLifecycleQueue();
            }

            // P9b: processes the lifecycle queue. Runs ONE operation per call — the highest priority in
            // the whole system that is NOT inside its zone's anti-flapping settle window
            // (EVAL_LIFECYCLE_SETTLE_S). Returns true if an operation ran (so two never run in one tick).
            bool processLifecycleQueue()
            {
                if (pendingLifecycle.empty()) return false;

                static const uint64_t settleMs = (uint64_t)evalCfg("EVAL_LIFECYCLE_SETTLE_S", 30.0f) * 1000;

                // Iterate in descending priority (crash>merge>split); run the first whose zone has
                // already settled. If the highest priority one is settling, skip to the next — a settling
                // zone does NOT block the other zones' operations.
                int         bestFd  = -1;
                LifecycleOp bestOp  = LifecycleOp::LIFECYCLE_SPLIT;
                int         bestPri = -1;
                for (const auto& kv : pendingLifecycle)
                {
                    if ((int)kv.second < bestPri) continue;   // priority does not beat the best candidate
                    if ((int)kv.second == bestPri && bestFd >= 0) continue;   // igual prioridad: cualquiera

                    auto lc = lastLifecycleMs.find(kv.first);
                    if (lc != lastLifecycleMs.end() && steadyMs() - lc->second < settleMs)
                    {
                        std::cout << "[Orchestrator] anti-flapping settle fd=" << kv.first
                                  << " (" << (settleMs - (steadyMs() - lc->second)) / 1000
                                  << "s left) -> postponing op " << (int)kv.second << std::endl;
                        continue;   // not this zone; try the next by priority
                    }

                    bestFd = kv.first; bestOp = kv.second; bestPri = (int)kv.second;
                }
                if (bestFd < 0) return false;   // all settling: retry on the next tick

                pendingLifecycle.erase(bestFd);

                // ⚠️ THE SETTLE IS FOR SOMETHING THAT HAPPENED. It used to be stamped unconditionally,
                // so an operation that decided to do NOTHING — `tryMergeDown` returning false because
                // there is only one zone, which is the common case in a small cluster — still froze that
                // zone out of the lifecycle for 30 s. A cluster of one zone re-queued that no-op merge
                // forever and spent its whole life inside a settle window it had earned by doing nothing.
                bool acted = false;
                switch (bestOp)
                {
                    case LifecycleOp::LIFECYCLE_EVICT:     evictStaleZone(bestFd); acted = true; break;
                    case LifecycleOp::LIFECYCLE_REASSIGN:  tryReassign(bestFd);    acted = true; break;
                    case LifecycleOp::LIFECYCLE_MERGE:     acted = tryMergeDown(bestFd);         break;
                    case LifecycleOp::LIFECYCLE_SPLIT:     acted = trySplitDown(bestFd);         break;
                }
                if (acted) lastLifecycleMs[bestFd] = steadyMs();   // asentamiento tras operar
                return true;
            }

        private:
            std::map<int, MetricsRate> metricRates;
            int currentReplicas;
            int nextNodePort { 30426 };
            SpawnBackend backend;   // §3.8 (P8): active spawn backend (LOCAL/K8S/TERRAFORM)
            std::map<std::string, pid_t> localPids;   // LOCAL: zone-node name → pid (for SIGTERM)
            // Zones that own a single chunk and therefore cannot be split at all. Kept so the doomed
            // SPLIT is not re-queued ten times a second, and so the condition can be reported as what
            // it is rather than as a skipped operation.
            std::set<int>           atSplitFloor;
            std::map<int, uint64_t> floorReportedMs;
            // The last population profile each zone reported, so a SPLIT can cut where the people are.
            // Kept beside `activeZones` rather than inside `ZoneInfo` because ZoneInfo is the wire shape
            // of a zone and this is local knowledge.
            struct PopProfile { uint16_t x[MAX_SPLIT_BUCKETS], y[MAX_SPLIT_BUCKETS], z[MAX_SPLIT_BUCKETS]; };
            std::map<int, PopProfile> popProfiles;
            std::map<int, std::chrono::steady_clock::time_point> lastScaleTime;

            // --- Per-zone lifecycle state (§3.9) ---
            std::map<int, ZoneState>       zoneStates;
            std::map<int, uint64_t>        lastSeenMs;      // lease/eviction on staleness
            std::map<int, uint64_t>        lowSinceMs;      // since when it has been below the merge threshold
            std::map<int, uint64_t>        lastLifecycleMs; // anti-flapping: settle time between ops
            std::map<int, uint64_t>        drainDeadlineMs; // drain fail-safe
            std::map<int, uint32_t>        drainRequestId;
            std::map<int, int>             drainTarget;     // the survivor absorbing it
            std::map<int, std::string>     portToName;      // UDP NodePort → deployment name
            std::map<int, LifecycleOp>     pendingLifecycle;// P9b: priority lifecycle queue
            uint64_t                       lastSweepMs = 0;
            uint32_t                       lcSeq = 1;

            static uint64_t steadyMs()
            {
                return (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count();
            }

            void removeFromActiveZones(int fd)
            {
                atSplitFloor.erase(fd);
                floorReportedMs.erase(fd);
                popProfiles.erase(fd);
                for (auto it = activeZones.begin(); it != activeZones.end(); ++it)
                    if (it->fd == fd) { activeZones.erase(it); return; }
            }

            const ZoneInfo* findZoneInfo(int fd) const
            {
                for (const auto& z : activeZones)
                    if (z.fd == fd) return &z;
                return nullptr;
            }

            // Expands the survivor's topology to cover the union (so findTargetNode routes to the
            // survivor while the draining node gives up). The node applies the new range through its env
            // (the zone re-reads CHUNK_* every tick) — resizing the deployment is a backend job (§3.8).
            /// @return el eje por el que se pueden fusionar, o -1 si su union NO es una caja.
            ///
            /// ⚠️ LA ENVOLVENTE NO ES LA UNION. Esto calculaba min/max en los tres ejes y se quedaba
            /// con la caja que los contiene a los dos — que es MAS GRANDE que su union en cuanto no
            /// estan alineados. Con A = X[0..10] Y[0..100] y B = X[11..20] Y[0..50], la envolvente
            /// incluye X[11..20] Y[51..100]: territorio que era de un TERCERO, o de nadie. El
            /// superviviente se lo quedaba, y como `findTargetNode` devuelve la primera coincidencia,
            /// le robaba los chunks a su vecino sin que nada lo dijera.
            ///
            /// Dos cajas solo se pueden fusionar si coinciden EXACTAMENTE en dos ejes y se tocan en el
            /// tercero. Cualquier otra cosa se rechaza: mejor no fusionar que inventarse una region.
            static int mergeableAxis(const ZoneInfo& a, const ZoneInfo& b)
            {
                const bool sameX = a.chunkXMin == b.chunkXMin && a.chunkXMax == b.chunkXMax;
                const bool sameY = a.chunkYMin == b.chunkYMin && a.chunkYMax == b.chunkYMax;
                const bool sameZ = a.chunkZMin == b.chunkZMin && a.chunkZMax == b.chunkZMax;
                auto touch = [](int32_t aMin, int32_t aMax, int32_t bMin, int32_t bMax) {
                    return aMax + 1 == bMin || bMax + 1 == aMin;
                };
                if (sameY && sameZ && touch(a.chunkXMin, a.chunkXMax, b.chunkXMin, b.chunkXMax)) return AXIS_X;
                if (sameX && sameZ && touch(a.chunkYMin, a.chunkYMax, b.chunkYMin, b.chunkYMax)) return AXIS_Y;
                if (sameX && sameY && touch(a.chunkZMin, a.chunkZMax, b.chunkZMin, b.chunkZMax)) return AXIS_Z;
                return -1;
            }

            void absorbRegion(int survivorFD, int victimFD)
            {
                const ZoneInfo* s = findZoneInfo(survivorFD);
                const ZoneInfo* v = findZoneInfo(victimFD);
                if (!s || !v) { std::cout << "[Orchestrator] absorbRegion: zone not in the topology" << std::endl; return; }

                const int axis = mergeableAxis(*s, *v);
                if (axis < 0)
                {
                    std::cout << "[Orchestrator] absorbRegion: fd=" << survivorFD << " y fd=" << victimFD
                              << " no forman una caja -> NO se fusionan" << std::endl;
                    return;
                }

                const int32_t nMin = (axis == AXIS_X) ? std::min(s->chunkXMin, v->chunkXMin)
                                   : (axis == AXIS_Y) ? std::min(s->chunkYMin, v->chunkYMin)
                                                      : std::min(s->chunkZMin, v->chunkZMin);
                const int32_t nMax = (axis == AXIS_X) ? std::max(s->chunkXMax, v->chunkXMax)
                                   : (axis == AXIS_Y) ? std::max(s->chunkYMax, v->chunkYMax)
                                                      : std::max(s->chunkZMax, v->chunkZMax);

                for (auto& z : activeZones)
                {
                    if (z.fd != survivorFD) continue;
                    if (axis == AXIS_X) { z.chunkXMin = nMin; z.chunkXMax = nMax; }
                    if (axis == AXIS_Y) { z.chunkYMin = nMin; z.chunkYMax = nMax; }
                    if (axis == AXIS_Z) { z.chunkZMin = nMin; z.chunkZMax = nMax; }
                }

                // ⚠️ Y SE LO DECIMOS A LA ZONA. Anotarlo solo aqui duraba hasta la siguiente muestra
                // de metricas: `updateNodeTopology` reescribe la caja del head con la que reporta la
                // zona. Sin este comando el mundo de la victima quedaba huerfano a los 100 ms.
                sendResizeCommand(survivorFD, (ResizeAxis)axis, nMin, nMax);

                std::cout << "[Orchestrator] Absorbiendo fd=" << victimFD << " en fd=" << survivorFD
                          << " eje=" << (axis == AXIS_X ? 'X' : axis == AXIS_Y ? 'Y' : 'Z')
                          << " [" << nMin << "-" << nMax << "]" << std::endl;
            }

            // Number of chunks in a zone (the "which is smallest" heuristic).
            static int64_t zoneVolume(const ZoneInfo& z)
            {
                return (int64_t)(z.chunkXMax - z.chunkXMin + 1) *
                       (z.chunkYMax - z.chunkYMin + 1) *
                       (z.chunkZMax - z.chunkZMin + 1);
            }

            // §3.9 (P9b): executes the lease/crash eviction (top priority: crash>merge>split).
            // The zombie pod is deleted and the zone removed from the topology (the same body as the old
            // inline path in sweepStaleZones, now serialised through the lifecycle queue).
            void evictStaleZone(int fd)
            {
                std::cout << "[Orchestrator] EVICT fd=" << fd << " (pod zombie)" << std::endl;
                if (zoneState(fd) != ZoneState::DESTROYED) deleteZoneNode(fd);
                markZoneState(fd, ZoneState::DEAD);
                drainDeadlineMs.erase(fd);
                drainRequestId.erase(fd);
                drainTarget.erase(fd);
                if (currentReplicas > 0) --currentReplicas;   // never report a negative replica count
                removeFromActiveZones(fd);
            }

            // §3.9 (P9b): executes the SPLIT (scale up), serialised through the lifecycle queue. It used
            // to live inline in evaluateServer; it is now a queue operation with SPLIT priority (the
            // lowest, after crash and merge) and the cooldown still applies per zone (lastScaleTime).
            bool trySplitDown(int fd)
            {
                static const int cooldown = (int)evalCfg("EVAL_COOLDOWN_S", 30.0f);

                const ZoneInfo* me = findZoneInfo(fd);
                if (!me) return false;
                if (zoneState(fd) != ZoneState::READY) return false;

                auto now = std::chrono::steady_clock::now();
                auto it  = lastScaleTime.find(fd);
                if (it != lastScaleTime.end() &&
                    std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() < cooldown)
                    return false;

                // ⚠️ IT ONLY EVER CUT ON X, and that is not a smaller version of splitting — it is a
                // different, weaker thing. A zone cut on X over and over becomes a thinner and thinner
                // SLAB: the crowd that made it split stays together inside one band, and every extra
                // node buys a strip of empty world next to them. Cutting the LONGEST axis instead is
                // what makes repeated splits converge on a grid, so the second cut separates what the
                // first one could not.
                const int32_t wX = me->chunkXMax - me->chunkXMin;
                const int32_t wY = me->chunkYMax - me->chunkYMin;
                const int32_t wZ = me->chunkZMax - me->chunkZMin;

                ResizeAxis axis = AXIS_X;
                int32_t    width = wX;
                if (wY > width) { axis = AXIS_Y; width = wY; }
                if (wZ > width) { axis = AXIS_Z; width = wZ; }

                // ⚠️ THE FLOOR. A zone's box is expressed in WHOLE CHUNKS, and the routing key is a
                // chunk: `ZoneQuery` carries `chunkX/Y/Z` and nothing finer, so two zones cannot share
                // one chunk — the head would have no way to tell them apart. A zone down to a single
                // chunk therefore CANNOT be split, on any axis, and no amount of hardware helps.
                //
                // What that costs is a measurement, not an opinion. On this machine, one zone with a
                // 1000 m chunk and INTEREST_RADIUS_M=500 crosses its tick budget at ~70 players
                // standing together (60 -> 16 ms, 70 -> 56 ms, 80 -> 111 ms). So the floor reads:
                // ANY 1000 m OF WORLD HOLDING MORE THAN ~70 PLAYERS IS BEYOND WHAT SPLITTING CAN FIX.
                // A town square of 500 people needs chunks of about 350 m, and that is a deploy-time
                // decision (`CHUNK_SIZE_*`) taken before anybody logs in.
                //
                // Lifting it for real means making the routing key finer than a chunk — position in
                // `ZoneQuery`, sub-chunk bounds, and every chunk-keyed query along with it. That is a
                // protocol change and it is NOT done here.
                //
                // What IS done here: stop pretending. This used to log one line and return, while
                // `evaluateServer` re-enqueued the same doomed SPLIT on every metrics sample — a zone
                // drowning at 700 ms a tick spun the lifecycle queue forever and the only trace was a
                // line that read like an ordinary skip. Now the zone is marked, the condition is
                // reported once with what to actually do about it, and no further SPLIT is queued for
                // it (see `atSplitFloor` in evaluateServer).
                if (width < 1)
                {
                    if (!atSplitFloor.count(fd))
                    {
                        atSplitFloor.insert(fd);
                        std::cout << "[Orchestrator] ⚠ zone fd=" << fd
                                  << " is AT THE SPLIT FLOOR: it owns a single chunk"
                                  << " (X=" << wX << " Y=" << wY << " Z=" << wZ << ")"
                                  << " and CANNOT be divided further — the routing key is a chunk."
                                  << " If it is overloaded, splitting will not fix it:"
                                  << " reduce CHUNK_SIZE_* at deploy time or cut INTEREST_RADIUS_M."
                                  << std::endl;
                    }
                    return false;   // nothing happened: do not earn a settle window
                }
                atSplitFloor.erase(fd);   // it has room again (a merge gave it back some range)

                // ⚠️ AND NOW WHERE TO CUT IT. Everything above picks an axis from the GEOMETRY. That is
                // the right fallback and the wrong default: a zone splits because of the people in it,
                // and the people are not spread evenly across its box. With the whole crowd in chunk 3
                // of a 0..100 zone, the geometric cut lands at 50 — the child gets 51..100, empty, the
                // parent keeps every last player, and it takes about five generations (~2.5 min at one
                // split per 30 s) to walk the cut down to them. A population cut lands on the third
                // chunk the first time.
                //
                // So if this zone has reported a population profile, the axis AND the cut both come
                // from it: for each axis, find the bucket boundary where the entities divide most
                // evenly, and take the axis whose best boundary is the most even of the three. Zero
                // population, no profile at all (a zone older than this field), or a crowd that no
                // boundary can divide — everyone inside one bucket — all fall back to the geometric
                // choice already computed above, which is what `chosen` starts as.
                bool     popCut = false;
                int32_t  cutLow = 0;

                auto pit = popProfiles.find(fd);
                if (pit != popProfiles.end())
                {
                    double bestImbalance = 1.0;   // 1.0 = every entity on one side: no better than none

                    for (int a = 0; a < 3; ++a)
                    {
                        const uint16_t* h = (a == 0) ? pit->second.x : (a == 1) ? pit->second.y : pit->second.z;
                        const int32_t aLo = (a == 0) ? me->chunkXMin : (a == 1) ? me->chunkYMin : me->chunkZMin;
                        const int32_t aHi = (a == 0) ? me->chunkXMax : (a == 1) ? me->chunkYMax : me->chunkZMax;
                        const int64_t span = (int64_t)aHi - (int64_t)aLo + 1;
                        if (span < 2) continue;   // one chunk on this axis: nothing to divide

                        int64_t total = 0;
                        for (uint32_t b = 0; b < MAX_SPLIT_BUCKETS; ++b) total += h[b];
                        if (total == 0) continue;

                        // Walk the bucket boundaries, keeping the one that halves the population best.
                        int64_t cum = 0;
                        for (uint32_t b = 0; b + 1 < MAX_SPLIT_BUCKETS; ++b)
                        {
                            cum += h[b];

                            // ⚠️ THE LAST CHUNK OF BUCKET b, and it has to be the EXACT inverse of the
                            // zone's `(v - lo) * B / span`. A first version used
                            // `lo + (b+1)*span/B - 1`, which is off by one chunk: with span=101 and
                            // B=32, chunk 3 lands in bucket 0 but that formula ends bucket 0 at chunk
                            // 2 — so part of the bucket's own population fell on the far side of the
                            // cut it was supposed to be counted for. Measured: a town in chunks 0..10
                            // was cut at 2 (22/58) instead of 6 (50/30).
                            const int64_t endOfBucket =
                                (int64_t)aLo + ((int64_t)(b + 1) * span - 1) / (int64_t)MAX_SPLIT_BUCKETS;
                            if (endOfBucket < aLo || endOfBucket >= aHi) continue;

                            const double imbalance =
                                (double)std::llabs(cum - (total - cum)) / (double)total;
                            if (imbalance < bestImbalance)
                            {
                                bestImbalance = imbalance;
                                axis   = (a == 0) ? AXIS_X : (a == 1) ? AXIS_Y : AXIS_Z;
                                cutLow = (int32_t)endOfBucket;
                                popCut = true;
                            }
                        }
                    }
                }

                // Nothing divided them: every entity sits inside one bucket, on all three axes. That is
                // the case a cut cannot fix — the routing key is a chunk — but the geometric middle is
                // still the wrong answer to give. Cutting AROUND the crowd instead narrows the box down
                // to the chunk they are standing in within a couple of generations, so the cluster
                // reaches the honest "unsplittable" verdict (and frees the rest of the world) in about
                // a minute instead of walking a binary search down to it over seven splits and three
                // and a half minutes.
                if (!popCut && pit != popProfiles.end())
                {
                    for (int a = 0; a < 3 && !popCut; ++a)
                    {
                        const uint16_t* h = (a == 0) ? pit->second.x : (a == 1) ? pit->second.y : pit->second.z;
                        const int32_t aLo = (a == 0) ? me->chunkXMin : (a == 1) ? me->chunkYMin : me->chunkZMin;
                        const int32_t aHi = (a == 0) ? me->chunkXMax : (a == 1) ? me->chunkYMax : me->chunkZMax;
                        const int64_t span = (int64_t)aHi - (int64_t)aLo + 1;
                        if (span < 2) continue;

                        uint32_t heaviest = 0; uint16_t most = 0;
                        for (uint32_t b = 0; b < MAX_SPLIT_BUCKETS; ++b)
                            if (h[b] > most) { most = h[b]; heaviest = b; }
                        if (most == 0) continue;

                        auto endOf = [&](uint32_t b) {
                            return (int64_t)aLo + ((int64_t)(b + 1) * span - 1) / (int64_t)MAX_SPLIT_BUCKETS;
                        };
                        // Prefer cutting BELOW the crowd (giving away the empty space under it); if it
                        // is already in the first bucket there is nothing below, so cut above instead.
                        const int64_t below = (heaviest > 0) ? endOf(heaviest - 1) : (int64_t)aLo - 1;
                        const int64_t above = endOf(heaviest);
                        const int64_t pick  = (below >= aLo && below < aHi) ? below
                                            : ((above >= aLo && above < aHi) ? above : (int64_t)aLo - 1);
                        if (pick < aLo || pick >= aHi) continue;

                        axis   = (a == 0) ? AXIS_X : (a == 1) ? AXIS_Y : AXIS_Z;
                        cutLow = (int32_t)pick;
                        popCut = true;
                    }
                }

                const int32_t lo = (axis == AXIS_X) ? me->chunkXMin : (axis == AXIS_Y) ? me->chunkYMin : me->chunkZMin;
                const int32_t hi = (axis == AXIS_X) ? me->chunkXMax : (axis == AXIS_Y) ? me->chunkYMax : me->chunkZMax;

                const int32_t midLow  = popCut ? cutLow      : ( (lo + hi)      / 2);
                const int32_t midHigh = popCut ? (cutLow + 1) : (((lo + hi) + 1) / 2);

                std::cout << "[Orchestrator] SPLIT fd=" << fd << " (cola de vida) axis="
                          << (axis == AXIS_X ? 'X' : axis == AXIS_Y ? 'Y' : 'Z')
                          << " cut=" << (popCut ? "POPULATION" : "geometric")
                          << " " << lo << ".." << hi << " -> " << lo << ".." << midLow
                          << " + " << midHigh << ".." << hi << std::endl;

                // The child takes the upper half OF THE CHOSEN AXIS and inherits the other two whole.
                const int32_t cxMin = (axis == AXIS_X) ? midHigh : me->chunkXMin;
                const int32_t cyMin = (axis == AXIS_Y) ? midHigh : me->chunkYMin;
                const int32_t czMin = (axis == AXIS_Z) ? midHigh : me->chunkZMin;

                if (spawnZoneNode(cxMin, me->chunkXMax,
                                  cyMin, me->chunkYMax,
                                  czMin, me->chunkZMax))
                {
                    lastScaleTime[fd] = now;
                    sendResizeCommand(fd, axis, lo, midLow);
                    return true;
                }
                // The spawn failed (no fork, kubectl refused, the API said no). No child exists, so the
                // parent must NOT be resized — that would hand its upper half to nobody — and no settle
                // is earned: the next evaluation should be free to try again.
                return false;
            }

            // Merge (scale down): picks the smallest neighbour as the victim, requests a drain, and lets
            // the survivor absorb it in the topology. Needs hysteresis + window + anti-flapping.
            bool tryMergeDown(int fd)
            {
                static const int    mergeWindowS  = (int)evalCfg("EVAL_MERGE_WINDOW_S", 120);
                static const int    drainTimeoutS = (int)evalCfg("EVAL_DRAIN_TIMEOUT_S", 15);
                static const int    minReplicas   = (int)evalCfg("EVAL_MIN_REPLICAS", 1);

                if (routableZoneCount() <= minReplicas) return false;
                if (zoneState(fd) != ZoneState::READY) return false;

                const ZoneInfo* me = findZoneInfo(fd);
                if (!me) return false;

                // Anti-flapping: start nothing if we have just run a lifecycle operation on this zone.
                auto lc = lastLifecycleMs.find(fd);
                if (lc != lastLifecycleMs.end() &&
                    steadyMs() - lc->second < (uint64_t)mergeWindowS * 1000) return false;

                // Pick the smallest (face) neighbour as the victim.
                auto neighs = findNeighbors(fd, NeighborMode::FACE);
                const ZoneInfo* victim = nullptr;
                for (int nfd : neighs)
                {
                    const ZoneInfo* n = findZoneInfo(nfd);
                    if (!n || zoneState(nfd) != ZoneState::READY) continue;
                    if (!victim || zoneVolume(*n) < zoneVolume(*victim)) victim = n;
                }
                if (!victim)
                {
                    lowSinceMs.erase(fd);   // no viable partner → restart the window
                    return false;
                }

                // a consistent low-load window (hysteresis: never off a single metric).
                auto lowIt = lowSinceMs.find(fd);
                if (lowIt == lowSinceMs.end()) { lowSinceMs[fd] = steadyMs(); return false; }
                if (steadyMs() - lowIt->second < (uint64_t)mergeWindowS * 1000) return false;

                // START of the drain.
                int victimFD = victim->fd;
                uint32_t req = lcSeq++;
                drainRequestId[victimFD] = req;
                drainDeadlineMs[victimFD] = steadyMs() + (uint64_t)drainTimeoutS * 1000;
                drainTarget[victimFD] = fd;
                markZoneState(victimFD, ZoneState::DRAINING);
                markZoneState(fd, ZoneState::READY);
                lastLifecycleMs[victimFD] = steadyMs();
                lastLifecycleMs[fd]       = steadyMs();   // the survivor does not scale until it settles
                lowSinceMs.erase(fd);

                absorbRegion(fd, victimFD);

                DGS::ZoneLifecycle dr{ req, 0 };
                DGS::Packet pDr; pDr.pack(dr);
                socket.send(victimFD, pDr.getRawData(), pDr.getSize());

                std::cout << "[Orchestrator] FUSION: fd=" << victimFD
                          << " (drenado) -> fd=" << fd
                          << " req=" << req << " deadline=" << drainTimeoutS << "s" << std::endl;
                return true;
            }

            // P6 (P2+O5, F1): HANDOFF ON A FAILED METRIC. Node `fd` is failing (validation timeouts /
            // broken handoffs / validator down) → it cedes ITS region to a healthy neighbour and enters
            // DRAINING; the neighbour absorbs the range in the topology (absorbRegion) and the fd is
            // drained (through the same path as a merge: PKT_DRAIN → ack → DELETE_ZONE). It mirrors
            // tryMergeDown: there the idle fd is the survivor; here the failing fd is the VICTIM.
            bool tryReassign(int fd)
            {
                static const int    drainTimeoutS = (int)evalCfg("EVAL_DRAIN_TIMEOUT_S", 15);
                static const int    minReplicas   = (int)evalCfg("EVAL_MIN_REPLICAS", 1);

                if (routableZoneCount() <= minReplicas) return false;
                if (zoneState(fd) != ZoneState::READY) return false;

                const ZoneInfo* me = findZoneInfo(fd);
                if (!me) return false;

                // A healthy (face) neighbour to absorb the region; prefer the largest volume (most capacity).
                auto neighs = findNeighbors(fd, NeighborMode::FACE);
                const ZoneInfo* survivor = nullptr;
                for (int nfd : neighs)
                {
                    const ZoneInfo* n = findZoneInfo(nfd);
                    if (!n || zoneState(nfd) != ZoneState::READY) continue;
                    if (!survivor || zoneVolume(*n) > zoneVolume(*survivor)) survivor = n;
                }
                if (!survivor)
                {
                    std::cout << "[Orchestrator] REASSIGN fd=" << fd
                              << " no healthy neighbour -> keeping the zone" << std::endl;
                    return false;
                }

                // START of the handoff: fd cedes, the neighbour absorbs.
                int survivorFD = survivor->fd;
                uint32_t req = lcSeq++;
                drainRequestId[fd] = req;
                drainDeadlineMs[fd] = steadyMs() + (uint64_t)drainTimeoutS * 1000;
                drainTarget[fd] = survivorFD;
                markZoneState(fd, ZoneState::DRAINING);
                markZoneState(survivorFD, ZoneState::READY);
                lastLifecycleMs[fd]          = steadyMs();
                lastLifecycleMs[survivorFD]  = steadyMs();

                absorbRegion(survivorFD, fd);

                DGS::ZoneLifecycle dr{ req, 0 };
                DGS::Packet pDr; pDr.pack(dr);
                socket.send(fd, pDr.getRawData(), pDr.getSize());

                std::cout << "[Orchestrator] REASSIGN por fallo: fd=" << fd
                          << " (drenado) -> fd=" << survivorFD
                          << " req=" << req << " deadline=" << drainTimeoutS << "s" << std::endl;
                return true;
            }

            void deleteZoneNode(int fd)
            {
                const ZoneInfo* z = findZoneInfo(fd);
                if (!z) return;
                auto it = portToName.find(z->port);
                if (it == portToName.end())
                {
                    std::cout << "[Orchestrator] No deployment name for fd=" << fd
                              << " (port " << z->port << ") -> topology only" << std::endl;
                    return;
                }
                const std::string name = it->second;

                switch (backend)
                {
                    case SpawnBackend::SPAWN_LOCAL:     deleteLocalNode(name); break;
                    case SpawnBackend::SPAWN_TERRAFORM: deleteK8sNode(name, true); break;
                    default:                            deleteK8sNode(name, false); break;
                }
                portToName.erase(z->port);
            }

            // §3.8 (P8) LOCAL: the pod is a forked process on this machine → terminated with SIGTERM.
            void deleteLocalNode(const std::string& name)
            {
                auto it = localPids.find(name);
                if (it == localPids.end())
                {
                    std::cout << "[Orchestrator] Sin pid local para " << name << std::endl;
                    return;
                }
                if (kill(it->second, SIGTERM) == 0)
                {
                    int st = 0;
                    waitpid(it->second, &st, 0);
                    std::cout << "[Orchestrator] Processo local terminado: " << name
                              << " (pid " << it->second << ")" << std::endl;
                }
                localPids.erase(it);
            }

            // §3.8 (P8) K8S/TERRAFORM: borra Deployment+Service. Con TERRAFORM usa `kubectl delete -f`
            // (terraform provisioned the cluster); with K8S it calls the API from the ServiceAccount.
            void deleteK8sNode(const std::string& name, bool viaKubectl)
            {
                if (viaKubectl)
                {
                    const std::string file = k8sManifestPath(name);
                    std::string cmd = "kubectl delete -f " + file + " --ignore-not-found 2>/dev/null";
                    std::cout << "[Orchestrator] (terraform) " << cmd << std::endl;
                    if (system(cmd.c_str()) == 0)
                        std::cout << "[Orchestrator] Pod eliminado via kubectl: " << name << std::endl;
                    else
                        std::cerr << "[Orchestrator] kubectl delete fallo para " << name << std::endl;
                    return;
                }

                const std::string tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token";
                const std::string caPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
                const std::string ns        = "dgs";
                std::string token = readFile(tokenPath);
                if (token.empty())
                {
                    std::cerr << "[Orchestrator] No token de ServiceAccount para borrar pod" << std::endl;
                    return;
                }
                httplib::SSLClient k8s("kubernetes.default.svc", 443);
                k8s.set_ca_cert_path(caPath.c_str());
                k8s.set_default_headers({{"Authorization", "Bearer " + token}});
                auto dep = k8s.Delete("/apis/apps/v1/namespaces/" + ns + "/deployments/" + name);
                auto svc = k8s.Delete("/api/v1/namespaces/" + ns + "/services/" + name);
                std::cout << "[Orchestrator] Pod zombie eliminado: " << name
                          << " (dep=" << (dep ? dep->status : -1)
                          << " svc=" << (svc ? svc->status : -1) << ")" << std::endl;
            }

            static std::string readFile(const std::string& path)
            {
                std::ifstream f(path);
                return std::string(std::istreambuf_iterator<char>(f),
                                   std::istreambuf_iterator<char>());
            }

            // §3.8 (P8): manifest directory for the TERRAFORM backend (kubectl apply -f).
            static std::string k8sManifestPath(const std::string& name)
            {
                const char* dir = std::getenv("DGS_MANIFEST_DIR");
                return std::string(dir ? dir : "terraform/manifests") + "/" + name + ".yaml";
            }

            // Returns the image name from the base zone-node deployment.
            static std::string fetchZoneImage(httplib::SSLClient& k8s, const std::string& ns)
            {
                auto res = k8s.Get("/apis/apps/v1/namespaces/" + ns + "/deployments/zone-node");
                if (res && res->status == 200)
                {
                    const auto& b = res->body;
                    auto pos = b.find("\"image\":\"");
                    if (pos != std::string::npos)
                    {
                        pos += 9;
                        auto end = b.find('"', pos);
                        if (end != std::string::npos)
                            return b.substr(pos, end - pos);
                    }
                }
                return "dgs-zone-node:latest";
            }

            // Creates a new independent zone-node Deployment + NodePort Service
            // for the given chunk range. Returns true on success.
            bool spawnZoneNode(int32_t xMin, int32_t xMax,
                               int32_t yMin, int32_t yMax,
                               int32_t zMin, int32_t zMax)
            {
                // §3.8 (P8): the spawn backend is abstract — the lifecycle logic does NOT change.
                switch (backend)
                {
                    case SpawnBackend::SPAWN_LOCAL:     return spawnLocalNode(xMin, xMax, yMin, yMax, zMin, zMax);
                    case SpawnBackend::SPAWN_TERRAFORM: return spawnK8sNode(xMin, xMax, yMin, yMax, zMin, zMax, true);
                    default:                            return spawnK8sNode(xMin, xMax, yMin, yMax, zMin, zMax, false);
                }
            }

            // §3.8 (P8): a zone's topology env list (KEY=VALUE), the SAME for every backend (LOCAL via
            // putenv, K8S/TERRAFORM as the container's env in the manifest). Behavioural parity is thus
            // by CONSTRUCTION: any difference between modes would live here and would fail the parity
            // test (tests/spawn_parity_test.cpp).
        public:
            static std::vector<std::string> zoneSpawnEnv(int32_t xMin, int32_t xMax,
                                                         int32_t yMin, int32_t yMax,
                                                         int32_t zMin, int32_t zMax,
                                                         int udpPort,
                                                         const std::string& headHost,
                                                         const std::string& headPort,
                                                         const std::string& podIP,
                                                         const std::string& csX = "1.0",
                                                         const std::string& csY = "1.0",
                                                         const std::string& csZ = "1.0")
            {
                auto i = [](int32_t v) { return std::to_string(v); };
                return {
                    "CHUNK_X_MIN=" + i(xMin), "CHUNK_X_MAX=" + i(xMax),
                    "CHUNK_Y_MIN=" + i(yMin), "CHUNK_Y_MAX=" + i(yMax),
                    "CHUNK_Z_MIN=" + i(zMin), "CHUNK_Z_MAX=" + i(zMax),
                    "CHUNK_SIZE_X=" + csX,    "CHUNK_SIZE_Y=" + csY, "CHUNK_SIZE_Z=" + csZ,
                    "ZONE_UDP_PORT=" + std::to_string(udpPort),
                    "HEAD_SERVER_HOST=" + headHost, "HEAD_SERVER_PORT=" + headPort,
                    "MY_POD_IP=" + podIP
                };
            }

            // §3.8 (P8) LOCAL: the "pod" is a forked process with the SAME topology env as the
            // manifest k8s (CHUNK_*, ZONE_UDP_PORT, HEAD_SERVER_HOST, VALIDATOR_HOST, SOCIAL_HOST).
            // The binary comes from DGS_ZONE_BIN (default "zone_node"); the pid is registered by name
            // for `deleteLocalNode` (SIGTERM + waitpid). Behavioural parity: same env, same lifecycle
            // sequence — only the materialisation differs.
            bool spawnLocalNode(int32_t xMin, int32_t xMax,
                                int32_t yMin, int32_t yMax,
                                int32_t zMin, int32_t zMax)
            {
                const char* zoneBin = std::getenv("DGS_ZONE_BIN");
                std::string bin = zoneBin ? zoneBin : "zone_node";

                const int         udpPort = nextNodePort++;
                const std::string name    = "zone-node-" + std::to_string(currentReplicas + 1);

                std::string headHost = std::getenv("HEAD_SERVER_HOST") ? std::getenv("HEAD_SERVER_HOST") : "127.0.0.1";
                std::string headPort = std::getenv("HEAD_SERVER_PORT") ? std::getenv("HEAD_SERVER_PORT") : "42424";
                std::string podIP    = std::getenv("MY_POD_IP")        ? std::getenv("MY_POD_IP")        : "127.0.0.1";

                // ⚠️ CHUNK SIZE WAS NOT INHERITED. `zoneSpawnEnv` defaults it to "1.0" and nobody
                // overrode it, so a split of a zone running 1000 m chunks produced a child running
                // ONE METRE chunks. Routing is by chunk index so it still routed, but the child's
                // world coordinates (`chunkX * csX + pos`, what goes out over UDP and to the viewer)
                // came out 1000x off. The head cannot read it off the parent — chunk size does not
                // travel in ServerMetrics — so it takes its OWN environment, which is what the k8s
                // manifest and demo_cluster.sh already set on every node of a cluster.
                auto envOr = [](const char* n, const char* d) {
                    const char* v = std::getenv(n); return std::string(v ? v : d);
                };
                const std::string csX = envOr("CHUNK_SIZE_X", "1.0");
                const std::string csY = envOr("CHUNK_SIZE_Y", "1.0");
                const std::string csZ = envOr("CHUNK_SIZE_Z", "1.0");

                pid_t pid = fork();
                if (pid < 0) { std::cerr << "[Orchestrator] fork LOCAL fallo" << std::endl; --nextNodePort; return false; }
                if (pid == 0)
                {
                    // Child: becomes the zone_node with the topology env (the same one k8s gets).
                    auto env = zoneSpawnEnv(xMin, xMax, yMin, yMax, zMin, zMax, udpPort,
                                            headHost, headPort, podIP, csX, csY, csZ);
                    for (const auto& kv : env) putenv(const_cast<char*>(kv.c_str()));

                    execl(bin.c_str(), bin.c_str(), (char*)nullptr);
                    _exit(127);   // exec fallo
                }

                ++currentReplicas;
                portToName[udpPort] = name;
                localPids[name]     = pid;
                std::cout << "[Orchestrator] ZoneNode " << name << " LOCAL (pid " << pid
                          << ")  chunks X[" << xMin << "-" << xMax << "]  UDP=" << udpPort << std::endl;
                return true;
            }

            // §3.8 (P8) K8S (in-cluster via the API) / TERRAFORM (via `kubectl apply -f` of the SAME
            // manifest). The manifest is generated identically in both: TERRAFORM only changes the
            // MECHANISM of applying it (kubectl against the terraform-provisioned cluster).
            bool spawnK8sNode(int32_t xMin, int32_t xMax,
                              int32_t yMin, int32_t yMax,
                              int32_t zMin, int32_t zMax, bool viaKubectl)
            {
                const std::string tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token";
                const std::string caPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
                const std::string ns        = "dgs";

                std::string token = readFile(tokenPath);
                if (!viaKubectl && token.empty()) { std::cerr << "[Orchestrator] No token de ServiceAccount" << std::endl; return false; }

                httplib::SSLClient k8s("kubernetes.default.svc", 443);
                k8s.set_ca_cert_path(caPath.c_str());
                k8s.set_default_headers({{"Authorization", "Bearer " + token}});

                const std::string image    = viaKubectl ? (std::getenv("DGS_ZONE_IMAGE") ? std::getenv("DGS_ZONE_IMAGE") : "dgs-zone-node:latest")
                                                        : fetchZoneImage(k8s, ns);
                const int         udpPort  = nextNodePort++;
                const std::string name     = "zone-node-" + std::to_string(currentReplicas + 1);

                // Node IP: head server passes MY_NODE_IP env var (set via kubectl set env).
                const char* nodeIP = std::getenv("MY_NODE_IP");
                const std::string podIP = nodeIP ? nodeIP : "127.0.0.1";

                auto i = [](int32_t v) { return std::to_string(v); };
                auto envOrK = [](const char* n) {
                    const char* v = std::getenv(n); return std::string(v ? v : "1.0");
                };

                // --- Service (NodePort UDP) ---
                std::string svc = R"({"apiVersion":"v1","kind":"Service","metadata":{"name":")" + name +
                    R"(","namespace":")" + ns + R"("},"spec":{"selector":{"app":")" + name +
                    R"("},"ports":[{"protocol":"UDP","port":42425,"targetPort":42425,"nodePort":)" +
                    std::to_string(udpPort) +
                    R"(}],"type":"NodePort"}})";

                // --- Deployment ---
                std::string dep =
                    R"({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":")" + name +
                    R"(","namespace":")" + ns +
                    R"("},"spec":{"replicas":1,"selector":{"matchLabels":{"app":")" + name +
                    R"("}},"template":{"metadata":{"labels":{"app":")" + name +
                    R"("}},"spec":{"containers":[{"name":"zone-node","image":")" + image +
                    R"(","imagePullPolicy":"IfNotPresent","ports":[{"containerPort":42425,"protocol":"UDP"}],)"
                    R"("env":[)"
                        R"({"name":"HEAD_SERVER_HOST","value":"head-server"},)"
                        R"({"name":"HEAD_SERVER_PORT","value":"42424"},)"
                        R"({"name":"MY_POD_IP","value":")" + podIP + R"("},)"
                        R"({"name":"ZONE_UDP_PORT","value":")" + std::to_string(udpPort) + R"("},)"
                        R"({"name":"CHUNK_X_MIN","value":")" + i(xMin) + R"("},)"
                        R"({"name":"CHUNK_X_MAX","value":")" + i(xMax) + R"("},)"
                        R"({"name":"CHUNK_Y_MIN","value":")" + i(yMin) + R"("},)"
                        R"({"name":"CHUNK_Y_MAX","value":")" + i(yMax) + R"("},)"
                        R"({"name":"CHUNK_Z_MIN","value":")" + i(zMin) + R"("},)"
                        R"({"name":"CHUNK_Z_MAX","value":")" + i(zMax) + R"("},)"
                        // Same omission as the LOCAL path had: without these the child runs 1 m chunks
                        // whatever the rest of the cluster uses, and its world coordinates come out
                        // scaled. Taken from the head's own environment (chunk size does not travel in
                        // ServerMetrics, so there is nothing per-zone to copy).
                        R"({"name":"CHUNK_SIZE_X","value":")" + envOrK("CHUNK_SIZE_X") + R"("},)"
                        R"({"name":"CHUNK_SIZE_Y","value":")" + envOrK("CHUNK_SIZE_Y") + R"("},)"
                        R"({"name":"CHUNK_SIZE_Z","value":")" + envOrK("CHUNK_SIZE_Z") + R"("})"
                    R"(]}]}}}})" ;

                if (viaKubectl)
                {
                    // TERRAFORM: same manifest, applied with kubectl (infra provisioned by terraform).
                    const std::string file = k8sManifestPath(name);
                    std::ofstream f(file);
                    if (!f) { std::cerr << "[Orchestrator] No puedo escribir " << file << std::endl; --nextNodePort; return false; }
                    f << "---\n" << svc << "\n---\n" << dep << "\n";
                    f.close();

                    std::string cmd = "kubectl apply -f " + file + " 2>/dev/null";
                    if (system(cmd.c_str()) != 0)
                    {
                        std::cerr << "[Orchestrator] kubectl apply fallo para " << name << std::endl;
                        --nextNodePort;
                        return false;
                    }
                    ++currentReplicas;
                    portToName[udpPort] = name;
                    std::cout << "[Orchestrator] ZoneNode " << name
                              << " creado (terraform/kubectl)  chunks X[" << xMin << "-" << xMax << "]"
                              << "  NodePort=" << udpPort << std::endl;
                    return true;
                }

                auto svcRes = k8s.Post("/api/v1/namespaces/" + ns + "/services", svc, "application/json");
                if (!svcRes || svcRes->status != 201)
                {
                    std::cerr << "[Orchestrator] Error creating Service " << name
                              << ": " << (svcRes ? svcRes->status : -1) << std::endl;
                    --nextNodePort;
                    return false;
                }

                auto depRes = k8s.Post("/apis/apps/v1/namespaces/" + ns + "/deployments", dep, "application/json");
                if (depRes && depRes->status == 201)
                {
                    ++currentReplicas;
                    portToName[udpPort] = name;   // §3.9: so the pod can be deleted on drain/eviction
                    std::cout << "[Orchestrator] ZoneNode " << name
                              << " creado  chunks X[" << xMin << "-" << xMax << "]"
                              << "  NodePort=" << udpPort << std::endl;
                    return true;
                }

                std::cerr << "[Orchestrator] Error creating Deployment " << name
                          << ": " << (depRes ? depRes->status : -1) << std::endl;
                // Rollback service
                k8s.Delete("/api/v1/namespaces/" + ns + "/services/" + name);
                --nextNodePort;
                return false;
            }

            void sendResizeCommand(int fd, ResizeAxis axis, int32_t newMin, int32_t newMax)
            {
                // ⚠️ ZERO-INITIALISED. It was not, so `chunkSize*` and `addr` went on the wire as
                // whatever was on the stack — and the validator, which reads its chunk size from a
                // Command, would have taken garbage for it.
                DGS::Command cmd{};
                cmd.purpose    = DGS::CMD_TRANSFER_SERVER;
                // ⚠️ EL RANGO COMPLETO, no solo el maximo. Mandar solo el maximo hacia imposible la
                // FUSION: un superviviente que se queda la region de otro tiene que CRECER, y con un
                // solo numero la zona no podia saber hasta donde. Medido: tras fusionar, el chunk de
                // la victima dejaba de tener dueno en cuanto el superviviente mandaba sus siguientes
                // metricas — `updateNodeTopology` reescribe la caja del head con la que reporta la
                // zona, y la zona seguia con la suya de siempre. `findTargetNode` devolvia -1.
                cmd.chunkX     = newMax;   // maximo nuevo en `resizeAxis`
                cmd.chunkY     = newMin;   // minimo nuevo en `resizeAxis`
                cmd.resizeAxis = axis;

                DGS::Packet p;
                p.pack(cmd);

                socket.send(fd, p.getRawData(), p.getSize());

                std::cout << "[Orchestrator] Resizing ZoneNode fd=" << fd << " axis="
                          << (axis == AXIS_X ? 'X' : axis == AXIS_Y ? 'Y' : 'Z')
                          << " -> [" << newMin << ".." << newMax << "]" << std::endl;
            }

            DGS::TCPSocket& socket;
        };
};

#endif
