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

    class Orchestrator
    {
        public:
            Orchestrator(DGS::TCPSocket& s) : socket(s), currentReplicas(1) {}
            std::vector<ZoneInfo> activeZones;

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
                static const int    cooldown = (int)evalCfg("EVAL_COOLDOWN_S", 30.0f);
                static const float  mergeLoad = evalCfg("EVAL_MERGE_LOAD_RAM", 0.22f);
                static const int    sweepEveryMs = (int)evalCfg("EVAL_SWEEP_MS", 10000);

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

                // Rastrea la ventana "bajo carga" para la fusión (histéresis §3.9).
                auto lowIt = lowSinceMs.find(nodeFD);
                if (m.ramUsage < mergeLoad)
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
                    return;   // primera muestra: solo baseline, no decide
                }

                // Nodo REINICIADO (startTimeS cambio): descartar baseline viejo de contadores.
                if (r.lastStartTimeS != 0 && m.startTimeS != 0 && m.startTimeS != r.lastStartTimeS)
                {
                    r.lastBytesRx = m.bytesRx;
                    r.lastBytesTx = m.bytesTx;
                    r.lastStartTimeS = m.startTimeS;
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

                // --- Decisión: tres señales independientes, UN solo camino de escalado (§4.2) ---
                bool load          = m.ramUsage     >  loadTh && m.performance < perfTh;
                bool netSaturated  = r.txEWMA > 0 &&
                                     r.txEWMA / (r.rxEWMA + 1.0) > asymmTh;   // manda mucho mas de lo que recibe
                bool failureProne  = m.failedTransfers > (uint32_t)failTh;    // validador/traspaso va mal

                if (load || netSaturated || failureProne)
                {
                    auto now = std::chrono::steady_clock::now();
                    auto it  = lastScaleTime.find(nodeFD);
                    if (it != lastScaleTime.end() &&
                        std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() < cooldown)
                        return;

                    int32_t width = m.node.chunkXMax - m.node.chunkXMin;
                    if (width < 1)
                    {
                        std::cout << "[Orchestrator] Zona demasiado pequena para dividir (width=" << width << ")" << std::endl;
                        return;
                    }

                    std::cout << "[Orchestrator] Umbral alcanzado. Escalando sistema..."
                              << " (load=" << load << " net=" << netSaturated << " fail=" << failureProne << ")" << std::endl;

                    int32_t midLow  =  (m.node.chunkXMin + m.node.chunkXMax)      / 2;
                    int32_t midHigh = ((m.node.chunkXMin + m.node.chunkXMax) + 1) / 2;

                    if (spawnZoneNode(midHigh, m.node.chunkXMax,
                                      m.node.chunkYMin, m.node.chunkYMax,
                                      m.node.chunkZMin, m.node.chunkZMax))
                    {
                        lastScaleTime[nodeFD] = now;
                        sendResizeCommand(nodeFD, midLow);
                    }
                }

                // §3.9 Escalado DOWN (fusión): solo si el nodo está bajo de carga de forma consistente
                // (ventana + histéresis) y hay vecina menor que drenar. Nunca en el mismo tick de un
                // split (la rama de arriba ya marcó lastScaleTime → el cooldown del merge lo respeta).
                if (load || netSaturated || failureProne) return;   // bajo carga: no fusionar ahora
                if (lowSinceMs.count(nodeFD)) tryMergeDown(nodeFD);
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
            // para no fugar réplicas (§3.9 muerte no planificada, F3/F4).
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
                        std::cout << "[Orchestrator] Eviccion por lease fd=" << fd
                                  << " (ultima metrica hace " << (now - ls->second) / 1000 << "s)" << std::endl;
                        if (zoneState(fd) != ZoneState::DESTROYED) deleteZoneNode(fd);
                        markZoneState(fd, ZoneState::DEAD);
                        drainDeadlineMs.erase(fd);
                        drainRequestId.erase(fd);
                        drainTarget.erase(fd);
                        lastLifecycleMs[fd] = steadyMs();
                        --currentReplicas;
                        it = activeZones.erase(it);
                    }
                    else ++it;
                }
            }

        private:
            std::map<int, MetricsRate> metricRates;
            int currentReplicas;
            int nextNodePort { 30426 };
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
                const std::string name = it->second;
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
                const std::string tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token";
                const std::string caPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
                const std::string ns        = "dgs";

                std::string token = readFile(tokenPath);
                if (token.empty()) { std::cerr << "[Orchestrator] No token de ServiceAccount" << std::endl; return false; }

                httplib::SSLClient k8s("kubernetes.default.svc", 443);
                k8s.set_ca_cert_path(caPath.c_str());
                k8s.set_default_headers({{"Authorization", "Bearer " + token}});

                const std::string image    = fetchZoneImage(k8s, ns);
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

                auto svcRes = k8s.Post("/api/v1/namespaces/" + ns + "/services", svc, "application/json");
                if (!svcRes || svcRes->status != 201)
                {
                    std::cerr << "[Orchestrator] Error creando Service " << name
                              << ": " << (svcRes ? svcRes->status : -1) << std::endl;
                    --nextNodePort;
                    return false;
                }

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
