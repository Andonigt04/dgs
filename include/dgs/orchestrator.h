#ifndef DGS_ORCHESTRATOR_H
#define DGS_ORCHESTRATOR_H

#include "include/dgs/types.h"
#include "include/dgs/network.h"

#include <vector>
#include <iostream>

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
                        zone.xMin = m.node.xMin; zone.xMax = m.node.xMax;
                        zone.yMin = m.node.yMin; zone.yMax = m.node.yMax;
                        return;
                    }
                }
                activeZones.push_back({fd, m.node.xMin, m.node.xMax, m.node.yMin, m.node.yMax});
            }
            
            int findTargetNode(double x, double y)
            {
                for (const auto& zone : activeZones)
                {
                    if (x >= zone.xMin && x <= zone.xMax && y >= zone.yMin && y <= zone.yMax)
                    {
                        return zone.fd;
                    }
                }
                return -1;
            }

            void evaluateServer(const ServerMetrics& m, int nodeFD)
            {
                if (m.ramUsage > .80f && m.performance < .36f)
                {
                    std::cout << "[Orchestrator] Umbral alcanzado. Escalando sistema..." << std::endl;
                    
                    double midX = (m.node.xMin + m.node.xMax) / 2.0;

                    spawnNewNode(midX, m.node.xMax, m.node.yMin, m.node.yMax);
                    sendResizeCommand(nodeFD, midX);
                    
                }
            }
            
            
        private:
            void spawnNewNode(double midX, double xMax, double yMin, double yMax)
            {
                
                std::string cmd = "docker run "
                "-e X_MIN=" + std::to_string(midX) + " "
                "-e X_MAX=" + std::to_string(xMax) + " "
                "-e Y_MIN=" + std::to_string(yMin) + " "
                "-e Y_MAX=" + std::to_string(yMax) + " "
                "-d dgs_zone_node";
                
                std::cout << "[Orchestrator] Lanzando nuevo contenedor ZoneNode..." << std::endl;
                system(cmd.c_str());
            }
            
            void sendResizeCommand(int fd, double newMax)
            {
                DGS::Command cmd;
                cmd.purpose = DGS::CMD_TRANSFER_SERVER;
                cmd.pos[0] = newMax;

                DGS::Packet p;
                p.pack(cmd);

                socket.send(fd, p.getRawData(), p.getSize());

                std::cout << "[Orchestrator] Actualizando contenedor ZoneNode... " << fd << std::endl;
            }

            DGS::TCPSocket& socket;
        };
};

#endif