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
#include <chrono>

namespace DGS
{
    class Orchestrator
    {
        public:
            Orchestrator(DGS::TCPSocket& s) : socket(s), currentReplicas(1) {}
            std::vector<ZoneInfo> activeZones;

            void updateNodeTopology(int fd, const ServerMetrics& m)
            {
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
                if (m.ramUsage > .80f && m.performance < .36f)
                {
                    auto now = std::chrono::steady_clock::now();
                    auto it  = lastScaleTime.find(nodeFD);
                    if (it != lastScaleTime.end() &&
                        std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() < 30)
                        return;

                    int32_t width = m.node.chunkXMax - m.node.chunkXMin;
                    if (width < 1)
                    {
                        std::cout << "[Orchestrator] Zona demasiado pequena para dividir (width=" << width << ")" << std::endl;
                        return;
                    }

                    std::cout << "[Orchestrator] Umbral alcanzado. Escalando sistema..." << std::endl;

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
            }

        private:
            int currentReplicas;
            int nextNodePort { 30426 };
            std::map<int, std::chrono::steady_clock::time_point> lastScaleTime;

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
