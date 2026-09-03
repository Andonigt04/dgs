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
                static const float  perfTh   = evalCfg("EVAL_LOAD_PERF",  0.36f);
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
                    r.haveEwma = true;
                }
                else
                {
                    r.rxEWMA = alpha * rxRate + (1.0 - alpha) * r.rxEWMA;
                    r.txEWMA = alpha * txRate + (1.0 - alpha) * r.txEWMA;
                }

                // --- Decision: three independent signals, ONE lifecycle queue (§4.2, P6/P9b) ---
                // No signal mutates the cluster here: each enqueues its operation and
                // `processLifecycleQueue()` (below) runs ONE per tick by priority. The split cooldown,
                // the merge window and neighbour availability for a handoff are checked at execution.
                bool load          = m.ramUsage     >  loadTh && m.performance < perfTh;
                bool netSaturated  = r.txEWMA > 0 &&
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
                    std::cout << "[Orchestrator] Umbral alcanzado fd=" << nodeFD
                              << " (load=" << load << " net=" << netSaturated
                              << ") -> encolado SPLIT" << std::endl;
                    enqueueLifecycle(nodeFD, LifecycleOp::LIFECYCLE_SPLIT);
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
                if (it == pendingLifecycle.end() || (int)op > (int)it->second)
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

                switch (bestOp)
                {
                    case LifecycleOp::LIFECYCLE_EVICT:     evictStaleZone(bestFd);  break;
                    case LifecycleOp::LIFECYCLE_REASSIGN:  tryReassign(bestFd);     break;
                    case LifecycleOp::LIFECYCLE_MERGE:     tryMergeDown(bestFd);    break;
                    case LifecycleOp::LIFECYCLE_SPLIT:     trySplitDown(bestFd);    break;
                }
                lastLifecycleMs[bestFd] = steadyMs();   // asentamiento tras operar
                return true;
            }

        private:
            std::map<int, MetricsRate> metricRates;
            int currentReplicas;
            int nextNodePort { 30426 };
            SpawnBackend backend;   // §3.8 (P8): active spawn backend (LOCAL/K8S/TERRAFORM)
            std::map<std::string, pid_t> localPids;   // LOCAL: zone-node name → pid (for SIGTERM)
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
            void absorbRegion(int survivorFD, int victimFD)
            {
                const ZoneInfo* s = findZoneInfo(survivorFD);
                const ZoneInfo* v = findZoneInfo(victimFD);
                if (!s || !v) { std::cout << "[Orchestrator] absorbRegion: zone not in the topology" << std::endl; return; }
                int32_t nXMin = std::min(s->chunkXMin, v->chunkXMin), nXMax = std::max(s->chunkXMax, v->chunkXMax);
                int32_t nYMin = std::min(s->chunkYMin, v->chunkYMin), nYMax = std::max(s->chunkYMax, v->chunkYMax);
                int32_t nZMin = std::min(s->chunkZMin, v->chunkZMin), nZMax = std::max(s->chunkZMax, v->chunkZMax);
                for (auto& z : activeZones)
                {
                    if (z.fd != survivorFD) continue;
                    z.chunkXMin = nXMin; z.chunkXMax = nXMax;
                    z.chunkYMin = nYMin; z.chunkYMax = nYMax;
                    z.chunkZMin = nZMin; z.chunkZMax = nZMax;
                }
                std::cout << "[Orchestrator] Absorbiendo fd=" << victimFD << " en fd=" << survivorFD
                          << " X[" << nXMin << "-" << nXMax << "]" << std::endl;
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
            void trySplitDown(int fd)
            {
                static const int cooldown = (int)evalCfg("EVAL_COOLDOWN_S", 30.0f);

                const ZoneInfo* me = findZoneInfo(fd);
                if (!me) return;
                if (zoneState(fd) != ZoneState::READY) return;

                auto now = std::chrono::steady_clock::now();
                auto it  = lastScaleTime.find(fd);
                if (it != lastScaleTime.end() &&
                    std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() < cooldown)
                    return;

                int32_t width = me->chunkXMax - me->chunkXMin;
                if (width < 1)
                {
                    std::cout << "[Orchestrator] Zona demasiado pequena para dividir (width=" << width << ")" << std::endl;
                    return;
                }

                std::cout << "[Orchestrator] SPLIT fd=" << fd << " (cola de vida)" << std::endl;

                int32_t midLow  =  (me->chunkXMin + me->chunkXMax)      / 2;
                int32_t midHigh = ((me->chunkXMin + me->chunkXMax) + 1) / 2;

                if (spawnZoneNode(midHigh, me->chunkXMax,
                                  me->chunkYMin, me->chunkYMax,
                                  me->chunkZMin, me->chunkZMax))
                {
                    lastScaleTime[fd] = now;
                    sendResizeCommand(fd, midLow);
                }
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

                pid_t pid = fork();
                if (pid < 0) { std::cerr << "[Orchestrator] fork LOCAL fallo" << std::endl; --nextNodePort; return false; }
                if (pid == 0)
                {
                    // Child: becomes the zone_node with the topology env (the same one k8s gets).
                    auto env = zoneSpawnEnv(xMin, xMax, yMin, yMax, zMin, zMax, udpPort,
                                            headHost, headPort, podIP);
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
                        R"({"name":"CHUNK_Z_MAX","value":")" + i(zMax) + R"("})"
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
                    std::cerr << "[Orchestrator] Error creando Service " << name
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

                std::cerr << "[Orchestrator] Error creando Deployment " << name
                          << ": " << (depRes ? depRes->status : -1) << std::endl;
                // Rollback service
                k8s.Delete("/api/v1/namespaces/" + ns + "/services/" + name);
                --nextNodePort;
                return false;
            }

            void sendResizeCommand(int fd, int32_t newChunkMax)
            {
                DGS::Command cmd;
                cmd.purpose = DGS::CMD_TRANSFER_SERVER;
                cmd.chunkX  = newChunkMax;

                DGS::Packet p;
                p.pack(cmd);

                socket.send(fd, p.getRawData(), p.getSize());

                std::cout << "[Orchestrator] Actualizando contenedor ZoneNode... " << fd << std::endl;
            }

            DGS::TCPSocket& socket;
        };
};

#endif
