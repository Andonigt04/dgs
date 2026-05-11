#ifndef DGS_ORCHESTRATOR_H
#define DGS_ORCHESTRATOR_H

#include "include/dgs/types.h"
#include "include/dgs/network.h"

#include <vector>
#include <iostream>
#include <cstring>

namespace DGS
{
    class Orchestrator
    {
        public:
            Orchestrator(DGS::TCPSocket& s) : socket(s) {}
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

                    spawnNewNode(midX, m.node.chunkXMax, m.node.chunkYMin, m.node.chunkYMax, m.node.chunkZMin, m.node.chunkZMax);
                    sendResizeCommand(nodeFD, midX);
                }
            }
            
            
        private:
            void spawnNewNode(int32_t midX, int32_t xMax, int32_t yMin, int32_t yMax, int32_t zMin, int32_t zMax)
            {
                std::string cmd = "docker run "
                "-e CHUNK_X_MIN=" + std::to_string(midX) + " "
                "-e CHUNK_X_MAX=" + std::to_string(xMax) + " "
                "-e CHUNK_Y_MIN=" + std::to_string(yMin) + " "
                "-e CHUNK_Y_MAX=" + std::to_string(yMax) + " "
                "-e CHUNK_Z_MIN=" + std::to_string(zMin) + " "
                "-e CHUNK_Z_MAX=" + std::to_string(zMax) + " "
                "-d dgs_zone_node";

                std::cout << "[Orchestrator] Lanzando nuevo contenedor ZoneNode..." << std::endl;
                system(cmd.c_str());
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