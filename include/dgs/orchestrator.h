#ifndef DGS_ORCHESTRATOR_H
#define DGS_ORCHESTRATOR_H

#include "include/dgs/types.h"
#include "include/dgs/network.h"

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
    // Umbrales/constantes CONFIGURABLES de evaluacion (§4.2). Se lean de env, con default.
    // Se resuelven una vez (no por llamada) para no pagar getenv/atof en el hot path de metricas.
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

    // §3.9 (P9b): operaciones de ciclo de vida con PRIORIDAD formal. Mayor valor = se ejecuta primero.
    // Los disparadores de evaluateServer/sweep NO mutan el clúster directamente: encolan la operación
    // y `processLifecycleQueue()` ejecuta UNA por tick por prioridad global. Orden: CRASH/lease (lo más
    // urgente) > REASIGN por fallo (P6/F1: validador caído / failedTransfers alto) > MERGE > SPLIT.
    // Así un pod muerto se evicta antes que cualquier fusión/escalado, y un split nunca se ejecuta en
    // el mismo tick que un merge pendiente de la misma zona.
    enum class LifecycleOp : uint8_t
    {
        LIFECYCLE_SPLIT     = 0,   // bajo carga (load/net)
        LIFECYCLE_MERGE     = 1,   // zona ociosa (ventana + histéresis)
        LIFECYCLE_REASSIGN  = 2,   // P6: zona fallando (P2+O5, F1) → traspaso a vecino sano
        LIFECYCLE_EVICT     = 3    // crash / lease vencido
    };

    // §3.8 (P8): backend de SPAWN abstracto. El orquestador NO conoce la infra: cada backend implementa
    // create/destroy/resize con la MISMA semántica de ciclo de vida (§3.9), de modo que standalone y
    // cluster se comportan igual (F14). El modo solo cambia cómo se materializa un nodo:
    //   - LOCAL:     fork/exec del binario zone_node en el mismo nodo (dev/demo/portable, 1 nodo).
    //   - K8S:       API kubernetes desde dentro del clúster (ServiceAccount) — el modo cluster real.
    //   - TERRAFORM: infra ya provisionada por `dgs up --terraform`; el spawn aplica el MISMO manifest
    //                vía `kubectl apply` (paridad con K8S, pero el clúster lo levantó terraform).
    enum class SpawnBackend : uint8_t
    {
        SPAWN_LOCAL     = 0,
        SPAWN_K8S       = 1,
        SPAWN_TERRAFORM = 2
    };

    // Resuelve el backend una sola vez. Precedencia: env DGS_SPAWN_BACKEND (local/k8s/terraform);
    // si no está, K8S si corre in-cluster (hay ServiceAccount), si no LOCAL (portable/standalone).
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

            // §3.8 (P8): acceso al backend activo (el head lo expone para el CLI `dgs status`).
            SpawnBackend spawnBackend() const { return backend; }
            void setSpawnBackend(SpawnBackend b) { backend = b; }

            void updateNodeTopology(int fd, const ServerMetrics& m)
            {
                lastSeenMs[fd] = steadyMs();   // §3.9: lease/evicción por estancamiento

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

                if (!isRoutable(fd)) return;   // §3.9: zona cesando/muerta no se re-registra

                ZoneInfo info{};
                info.fd        = fd;
                info.chunkXMin = m.node.chunkXMin; info.chunkXMax = m.node.chunkXMax;
                info.chunkYMin = m.node.chunkYMin; info.chunkYMax = m.node.chunkYMax;
                info.chunkZMin = m.node.chunkZMin; info.chunkZMax = m.node.chunkZMax;
                std::strncpy(info.addr, m.node.addr, sizeof(info.addr) - 1);
                info.port = m.node.port;
                activeZones.push_back(info);

                // §3.9 gap 2: el zone-node BASE no pasa por spawnZoneNode, así que `portToName` no
                // conoce su deployment (port 42425 → "zone-node"). Registrarlo aquí para que la
                // evicción por lease / el drain puedan borrar SU pod también (no fuga réplicas).
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
                    if (!isRoutable(zone.fd)) continue;   // §3.9: DRAINING/DEAD no recibe nada
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
                    if (!isRoutable(z.fd)) continue;   // §3.9: no contar zonas drenadas/vencidas como vecinas

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

                // §3.9 fail-safe del drain: si el nodo no ack dentro del timeout, volver a READY
                // (nunca dejar la región vacía).
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

                // Evicción de pods zombies (crash / lease vencido) cada ~sweepEveryMs.
                uint64_t nowMs = steadyMs();
                if (nowMs - lastSweepMs > (uint64_t)sweepEveryMs)
                {
                    lastSweepMs = nowMs;
                    sweepStaleZones();
                }

                // Rastrea la ventana "bajo carga" para la fusión (histéresis §3.9). La fusión no solo se
                // dispara por RAM baja: una zona con POCAS ENTIDADES activas (jugadores visitando, sin
                // vecindad) es candidata a ceder su rango aunque tenga RAM media — es el "remove zone_nodes
                // based on players count" del TODO. Umbral configurable (EVAL_MERGE_ENTITIES, def 1).
                auto lowIt = lowSinceMs.find(nodeFD);
                bool idleEntities = m.activeEntities <= mergeMinEntities;
                if (m.ramUsage < mergeLoad || idleEntities)
                {
                    if (lowIt == lowSinceMs.end()) lowSinceMs[nodeFD] = nowMs;
                }
                else
                    lowSinceMs.erase(nodeFD);

                // --- Estado EWMA por nodo (tasas de banda). Contadores acumulados → Δ libre de ventana.
                MetricsRate& r = metricRates[nodeFD];

                if (!r.haveBaseline)
                {
                    r.lastBytesRx = m.bytesRx;
                    r.lastBytesTx = m.bytesTx;
                    r.lastStartTimeS = m.startTimeS;
                    r.haveBaseline = true;
                    processLifecycleQueue();   // P9b: drenar ops ya pendientes aunque esta sea baseline
                    return;   // primera muestra: solo baseline, no decide
                }

                // Nodo REINICIADO (startTimeS cambio): descartar baseline viejo de contadores.
                if (r.lastStartTimeS != 0 && m.startTimeS != 0 && m.startTimeS != r.lastStartTimeS)
                {
                    r.lastBytesRx = m.bytesRx;
                    r.lastBytesTx = m.bytesTx;
                    r.lastStartTimeS = m.startTimeS;
                    processLifecycleQueue();   // P9b: no dejar de drenar la cola
                    return;
                }

                // Δ sin signo (resta modular, inmune a reinicio de contador uint64).
                uint64_t dRx = m.bytesRx - r.lastBytesRx;
                uint64_t dTx = m.bytesTx - r.lastBytesTx;
                r.lastBytesRx = m.bytesRx;
                r.lastBytesTx = m.bytesTx;

                // EWMA de la tasa en bytes/s (el tiempo real entre muestras es ~100ms del tick de zona).
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

                // --- Decisión: tres señales independientes, UNA cola de vida (§4.2, P6/P9b) ---
                // Ninguna señal muta el clúster aquí: encola su operación y `processLifecycleQueue()`
                // (abajo) ejecuta UNA por tick por prioridad. El cooldown del split, la ventana del
                // merge y la disponibilidad de vecino en el traspaso se comprueban al ejecutar.
                bool load          = m.ramUsage     >  loadTh && m.performance < perfTh;
                bool netSaturated  = r.txEWMA > 0 &&
                                     r.txEWMA / (r.rxEWMA + 1.0) > asymmTh;   // manda mucho mas de lo que recibe
                bool failureProne  = m.failedTransfers > (uint32_t)failTh;    // validador/traspaso va mal

                if (failureProne)
                {
                    // P6 (P2+O5, F1): el nodo está fallando (timeouts de validación / traspasos rotos).
                    // No se escala hacia arriba: se REASIGNA su región a un vecino sano (traspaso por
                    // métrica fallida). Prioridad 2 (tras crash, antes que merge/split).
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
                // §3.9 Escalado DOWN (fusión): solo si el nodo está bajo de carga de forma consistente
                // (ventana + histéresis) y hay vecina menor que drenar. Al encolar MERGE, la cola lo
                // ordena detrás de un crash/lease pendiente y nunca en el mismo tick que un split.
                else if (lowSinceMs.count(nodeFD))
                    enqueueLifecycle(nodeFD, LifecycleOp::LIFECYCLE_MERGE);

                // P9b: drenar la cola de vida (a lo sumo UNA operación por evaluación).
                processLifecycleQueue();
            }

            // ---------------------------------------------------------------------------------------
            // §3.9 CICLO DE VIDA de zonas: fusión/escalado down, destrucción ordenada, evicción de
            // zombies y fail-safe del drain. Estados: PROVISIONING → READY → DRAINING → DESTROYED (+ DEAD).
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

            // Procesa el ACK de un PKT_DRAIN (aceptado por el nodo) → confirma destrucción.
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

                std::cout << "[Orchestrator] Drain OK fd=" << fd << " -> destruyo la zona" << std::endl;

                // 1) El superviviente ya absorbió el rango en la topología (absorbRegion al pedir el
                //    drain). 2) Borrar el pod (deployment+service) para no fugar réplicas/zombies.
                deleteZoneNode(fd);

                // 3) Retirar de activeZones, decrementar réplicas, marcar DESTROYED.
                removeFromActiveZones(fd);
                --currentReplicas;
                markZoneState(fd, ZoneState::DESTROYED);
                drainDeadlineMs.erase(fd);
                drainRequestId.erase(fd);
                drainTarget.erase(fd);
                lastLifecycleMs[fd] = steadyMs();   // anti-flappy: asentamiento

                // 4) Confirmar al nodo su salida (puede no haber recibido el borrado del pod).
                DGS::ZoneLifecycle del{ lc.requestId, 0 };
                DGS::Packet pDel; pDel.packDelete(del);
                socket.send(fd, pDel.getRawData(), pDel.getSize());
            }

            // Evicción por estancamiento (lease vencido): el pod zombie (crash sin borrar) se elimina
            // para no fugar réplicas (§3.9 muerte no planificada, F3/F4). Detecta y ENCOLA la evicción
            // en la cola de vida (P9b): no borra inline — la prioridad la ordena frente a merge/split.
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

            // P9b: encola una operación de vida para `fd`. Si ya hay otra pendiente para la misma zona,
            // se queda la de MAYOR prioridad (crash>merge>split).
            void enqueueLifecycle(int fd, LifecycleOp op)
            {
                auto it = pendingLifecycle.find(fd);
                if (it == pendingLifecycle.end() || (int)op > (int)it->second)
                    pendingLifecycle[fd] = op;
            }

            // P6 (F1): el head llama esto al recibir PKT_VALIDATOR_STATUS con state=DOWN/OPEN (circuito
            // abierto del validador). El nodo `fd` está sirviendo sin veredicto → el master reasigna su
            // región a un vecino sano (traspaso por métrica fallida). El `failedTransfers` acumulado del
            // status también alimenta el trigger de evaluateServer; aquí el disparo es EXPLÍCITO y
            // priorizado por encima de merge/split (LIFECYCLE_REASSIGN).
            void notifyValidatorDown(int fd, const DGS::ValidatorStatus& st)
            {
                std::cout << "[Orchestrator] Validador DOWN/OPEN en fd=" << fd
                          << " (reqTimeout=" << st.reqTimeout
                          << " failed=" << st.failedTransfers
                          << ") -> encolado REASSIGN" << std::endl;
                enqueueLifecycle(fd, LifecycleOp::LIFECYCLE_REASSIGN);
                processLifecycleQueue();
            }

            // P9b: procesa la cola de vida. Ejecuta UNA operación por llamada —la de mayor prioridad de
            // todo el sistema que NO esté en asentamiento anti-flappy por zona (EVAL_LIFECYCLE_SETTLE_S).
            // Devuelve true si ejecutó una operación (para no ejecutar dos en el mismo tick).
            bool processLifecycleQueue()
            {
                if (pendingLifecycle.empty()) return false;

                static const uint64_t settleMs = (uint64_t)evalCfg("EVAL_LIFECYCLE_SETTLE_S", 30.0f) * 1000;

                // Iterar en orden de prioridad descendente (crash>merge>split); ejecutar la primera cuya
                // zona ya se asentó. Si la de mayor prioridad está en settle, saltar a la siguiente — una
                // zona en asentamiento NO bloquea las operaciones de las demás.
                int         bestFd  = -1;
                LifecycleOp bestOp  = LifecycleOp::LIFECYCLE_SPLIT;
                int         bestPri = -1;
                for (const auto& kv : pendingLifecycle)
                {
                    if ((int)kv.second < bestPri) continue;   // prioridad no supera la mejor candidata
                    if ((int)kv.second == bestPri && bestFd >= 0) continue;   // igual prioridad: cualquiera

                    auto lc = lastLifecycleMs.find(kv.first);
                    if (lc != lastLifecycleMs.end() && steadyMs() - lc->second < settleMs)
                    {
                        std::cout << "[Orchestrator] Asentamiento anti-flappy fd=" << kv.first
                                  << " (queda " << (settleMs - (steadyMs() - lc->second)) / 1000
                                  << "s) -> pospongo op " << (int)kv.second << std::endl;
                        continue;   // esta zona no; probar la siguiente en prioridad
                    }

                    bestFd = kv.first; bestOp = kv.second; bestPri = (int)kv.second;
                }
                if (bestFd < 0) return false;   // todas en asentamiento: reintentar en el próximo tick

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
            SpawnBackend backend;   // §3.8 (P8): backend de spawn activo (LOCAL/K8S/TERRAFORM)
            std::map<std::string, pid_t> localPids;   // LOCAL: nombre zone-node → pid (para SIGTERM)
            std::map<int, std::chrono::steady_clock::time_point> lastScaleTime;

            // --- Estado de vida por zona (§3.9) ---
            std::map<int, ZoneState>       zoneStates;
            std::map<int, uint64_t>        lastSeenMs;      // lease/evicción por estancamiento
            std::map<int, uint64_t>        lowSinceMs;      // desde cuándo está bajo el umbral de fusión
            std::map<int, uint64_t>        lastLifecycleMs; // anti-flappy: asentamiento entre ops
            std::map<int, uint64_t>        drainDeadlineMs; // fail-safe del drain
            std::map<int, uint32_t>        drainRequestId;
            std::map<int, int>             drainTarget;     // superviviente que absorbe
            std::map<int, std::string>     portToName;      // NodePort UDP → nombre del deployment
            std::map<int, LifecycleOp>     pendingLifecycle;// P9b: cola de vida con prioridad
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

            // Expande la topología del superviviente para cubrir la unión (así findTargetNode enruta al
            // superviviente mientras el drenado cede). El nodo aplica el nuevo rango vía su env (la
            // zona relee CHUNK_* cada tick) — la resección del deployment es operación del backend (§3.8).
            void absorbRegion(int survivorFD, int victimFD)
            {
                const ZoneInfo* s = findZoneInfo(survivorFD);
                const ZoneInfo* v = findZoneInfo(victimFD);
                if (!s || !v) { std::cout << "[Orchestrator] absorbRegion: zona no en topologia" << std::endl; return; }
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

            // Nº de chunks de una zona (heurística de "cuál es la más pequeña").
            static int64_t zoneVolume(const ZoneInfo& z)
            {
                return (int64_t)(z.chunkXMax - z.chunkXMin + 1) *
                       (z.chunkYMax - z.chunkYMin + 1) *
                       (z.chunkZMax - z.chunkZMin + 1);
            }

            // §3.9 (P9b): ejecución de la evicción por lease/crash (prioridad máx: crash>merge>split).
            // El pod zombie se borra y la zona se retira de la topología (mismo cuerpo que el antiguo
            // inline de sweepStaleZones, ahora serializado por la cola de vida).
            void evictStaleZone(int fd)
            {
                std::cout << "[Orchestrator] EVICT fd=" << fd << " (pod zombie)" << std::endl;
                if (zoneState(fd) != ZoneState::DESTROYED) deleteZoneNode(fd);
                markZoneState(fd, ZoneState::DEAD);
                drainDeadlineMs.erase(fd);
                drainRequestId.erase(fd);
                drainTarget.erase(fd);
                --currentReplicas;
                removeFromActiveZones(fd);
            }

            // §3.9 (P9b): ejecución del SPLIT (escalado up) serializado por la cola de vida. Antes vivía
            // inline en evaluateServer; ahora es una operación de la cola con prioridad SPLIT (la menor,
            // tras crash y merge) y el cooldown sigue aplicándose por zona (lastScaleTime).
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

            // Fusión (escalado down): elige la vecina más pequeña como víctima, pide drain, y deja que
            // el superviviente la absorba en topología. Requiere histéresis + ventana + anti-flappy.
            bool tryMergeDown(int fd)
            {
                static const int    mergeWindowS  = (int)evalCfg("EVAL_MERGE_WINDOW_S", 120);
                static const int    drainTimeoutS = (int)evalCfg("EVAL_DRAIN_TIMEOUT_S", 15);
                static const int    minReplicas   = (int)evalCfg("EVAL_MIN_REPLICAS", 1);

                if (currentReplicas <= minReplicas) return false;
                if (zoneState(fd) != ZoneState::READY) return false;

                const ZoneInfo* me = findZoneInfo(fd);
                if (!me) return false;

                // Anti-flappy: no iniciar nada si acabamos de hacer una operación de vida en esta zona.
                auto lc = lastLifecycleMs.find(fd);
                if (lc != lastLifecycleMs.end() &&
                    steadyMs() - lc->second < (uint64_t)mergeWindowS * 1000) return false;

                // Elegir la vecina (cara) más pequeña como víctima.
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
                    lowSinceMs.erase(fd);   // sin pareja viable → reiniciar la ventana
                    return false;
                }

                // ventana de carga baja consistente (histéresis: no tras una sola métrica).
                auto lowIt = lowSinceMs.find(fd);
                if (lowIt == lowSinceMs.end()) { lowSinceMs[fd] = steadyMs(); return false; }
                if (steadyMs() - lowIt->second < (uint64_t)mergeWindowS * 1000) return false;

                // INICIO del drain.
                int victimFD = victim->fd;
                uint32_t req = lcSeq++;
                drainRequestId[victimFD] = req;
                drainDeadlineMs[victimFD] = steadyMs() + (uint64_t)drainTimeoutS * 1000;
                drainTarget[victimFD] = fd;
                markZoneState(victimFD, ZoneState::DRAINING);
                markZoneState(fd, ZoneState::READY);
                lastLifecycleMs[victimFD] = steadyMs();
                lastLifecycleMs[fd]       = steadyMs();   // el superviviente tampoco escala hasta asentar
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

            // P6 (P2+O5, F1): TRASPASO POR MÉTRICA FALLIDA. El nodo `fd` está fallando (timeouts de
            // validación / traspasos rotos / validador caído) → cede SU región a un vecino sano y
            // entra en DRAINING; el vecino absorbe el rango en topología (absorbRegion) y el fd se
            // drena (vía el mismo camino que el merge: PKT_DRAIN → ack → DELETE_ZONE). Es el espejo
            // de tryMergeDown: allí el fd ocioso es superviviente; aquí el fd fallido es la VÍCTIMA.
            bool tryReassign(int fd)
            {
                static const int    drainTimeoutS = (int)evalCfg("EVAL_DRAIN_TIMEOUT_S", 15);
                static const int    minReplicas   = (int)evalCfg("EVAL_MIN_REPLICAS", 1);

                if (currentReplicas <= minReplicas) return false;
                if (zoneState(fd) != ZoneState::READY) return false;

                const ZoneInfo* me = findZoneInfo(fd);
                if (!me) return false;

                // Vecino sano (cara) que absorba la región; preferir el de mayor volumen (más capacidad).
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
                              << " sin vecino sano -> mantengo la zona" << std::endl;
                    return false;
                }

                // INICIO del traspaso: fd cede, el vecino absorbe.
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
                    std::cout << "[Orchestrator] Sin nombre de deployment para fd=" << fd
                              << " (port " << z->port << ") -> solo topologia" << std::endl;
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

            // §3.8 (P8) LOCAL: el pod es un proceso forkeado en el mismo nodo → se termina con SIGTERM.
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
            // (el clúster lo provisionó terraform); con K8S llama a la API desde el ServiceAccount.
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

            // §3.8 (P8): directorio de manifests para el backend TERRAFORM (kubectl apply -f).
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
                // §3.8 (P8): el backend de spawn es abstracto — la lógica de ciclo de vida NO cambia.
                switch (backend)
                {
                    case SpawnBackend::SPAWN_LOCAL:     return spawnLocalNode(xMin, xMax, yMin, yMax, zMin, zMax);
                    case SpawnBackend::SPAWN_TERRAFORM: return spawnK8sNode(xMin, xMax, yMin, yMax, zMin, zMax, true);
                    default:                            return spawnK8sNode(xMin, xMax, yMin, yMax, zMin, zMax, false);
                }
            }

            // §3.8 (P8): lista de env de topología de una zona (KEY=VALUE), la MISMA para todos los
            // backends (LOCAL via putenv, K8S/TERRAFORM como env del container en el manifest). Así la
            // paridad de comportamiento es por CONSTRUCCIÓN: cualquier diferencia entre modos estaría
            // aquí y fallaría el test de paridad (tests/spawn_parity_test.cpp).
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

            // §3.8 (P8) LOCAL: el "pod" es un proceso forkeado con el MISMO env de topología que el
            // manifest k8s (CHUNK_*, ZONE_UDP_PORT, HEAD_SERVER_HOST, VALIDATOR_HOST, SOCIAL_HOST).
            // El binario a ejecutar se toma de DGS_ZONE_BIN (def "zone_node"); se registra el pid por
            // nombre para `deleteLocalNode` (SIGTERM + waitpid). Paridad de comportamiento: mismo env,
            // misma secuencia de ciclo de vida — solo cambia la materialización.
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
                    // Hijo: se convierte en el zone_node con el env de topología (mismo que k8s).
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

            // §3.8 (P8) K8S (in-cluster vía API) / TERRAFORM (vía `kubectl apply -f` del MISMO manifest).
            // El manifest se genera igual en ambos: TERRAFORM solo cambia el MECANISMO de aplicar
            // (kubectl contra el clúster que provisionó terraform) — el contenido es idéntico.
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
                    // TERRAFORM: mismo manifest, aplicado con kubectl (infra provisionada por terraform).
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
                    portToName[udpPort] = name;   // §3.9: para borrar el pod al drenar/eviccionar
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
