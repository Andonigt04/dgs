#ifndef DGS_ORCHESTRATOR_H
#define DGS_ORCHESTRATOR_H

#include "include/dgs/types.h"
#include "include/dgs/network.h"

#include <httplib.h>

#include <vector>
#include <iostream>
#include <fstream>
#include <cstring>

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
                        return;
                    }
                }
                activeZones.push_back({fd,
                    m.node.chunkXMin, m.node.chunkXMax,
                    m.node.chunkYMin, m.node.chunkYMax,
                    m.node.chunkZMin, m.node.chunkZMax});
            }

            int findTargetNode(int32_t chunkX, int32_t chunkY, int32_t chunkZ)
            {
                for (const auto& zone : activeZones)
                {
                    if (chunkX >= zone.chunkXMin && chunkX <= zone.chunkXMax &&
                        chunkY >= zone.chunkYMin && chunkY <= zone.chunkYMax &&
                        chunkZ >= zone.chunkZMin && chunkZ <= zone.chunkZMax)
                    {
                        return zone.fd;
                    }
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

            void evaluateServer(const ServerMetrics& m, int nodeFD)
            {
                if (m.ramUsage > .80f && m.performance < .36f)
                {
                    std::cout << "[Orchestrator] Umbral alcanzado. Escalando sistema..." << std::endl;

                    int32_t midX = (m.node.chunkXMin + m.node.chunkXMax) / 2;

                    scaleZoneDeployment(currentReplicas + 1);
                    sendResizeCommand(nodeFD, midX);
                }
            }

        private:
            int currentReplicas;

            static std::string readFile(const std::string& path)
            {
                std::ifstream f(path);
                return std::string(std::istreambuf_iterator<char>(f),
                                   std::istreambuf_iterator<char>());
            }

            void scaleZoneDeployment(int replicas)
            {
                const std::string tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token";
                const std::string caPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
                const std::string ns        = "dgs";
                const std::string deploy    = "zone-node";

                std::string token = readFile(tokenPath);
                if (token.empty()) { std::cerr << "[Orchestrator] No token de ServiceAccount" << std::endl; return; }

                httplib::SSLClient k8s("kubernetes.default.svc", 443);
                k8s.set_ca_cert_path(caPath.c_str());
                k8s.set_default_headers({{"Authorization", "Bearer " + token}});

                std::string path = "/apis/apps/v1/namespaces/" + ns + "/deployments/" + deploy + "/scale";
                std::string body = R"({"spec":{"replicas":)" + std::to_string(replicas) + "}}";

                auto res = k8s.Patch(path, body, "application/strategic-merge-patch+json");

                if (res && res->status == 200)
                {
                    currentReplicas = replicas;
                    std::cout << "[Orchestrator] ZoneNode escalado a " << replicas << " replicas" << std::endl;
                }
                else
                    std::cerr << "[Orchestrator] Error al escalar: " << (res ? res->status : -1) << std::endl;
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
