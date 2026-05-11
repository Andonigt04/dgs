#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <iostream>
#include <fstream>
#include <unistd.h>
#include <chrono>

float getRAM()
{
    long pages;
    long rss;

    std::ifstream stat_file("/proc/self/statm");
    stat_file >> pages >> rss;
    stat_file.close();

    long total_bytes = rss * sysconf(_SC_PAGESIZE);

    float limit = 512.f * 1024.f * 1024.f;
    return (float)total_bytes / limit;
}

void checkAndTransfer(DGS::TCPSocket& tcp_node, std::vector<DGS::EntityTransfer>& entities, double xMin, double xMax, double yMin, double yMax)
{
    DGS::Packet p;
    
    for (auto it = entities.begin(); it != entities.end();)
    {
        bool outOfBounds = (it->pos[0] < xMin || it->pos[0] > xMax || it->pos[1] < yMin || it->pos[1] > yMax);

        if (outOfBounds)
        {
            std::cout << "[ZoneNode] Entidad " << it->uuid << " fuera de limites. Transferenciendo..." << std::endl;

            p.pack(*it);

            tcp_node.send(tcp_node.getSocketFD(), p.getRawData(), p.getSize());

            it = entities.erase(it);
        } else ++it;
    }
}

int main()
{
    DGS::UDPSocket udp_zone_node; DGS::TCPSocket tcp_zone_node;

    std::vector<DGS::EntityTransfer> entities;

    int fd = tcp_zone_node.connect("127.0.0.1", 42424);

    while (true)
    {
        uint8_t recvBuffer[8192];
        int bytes = tcp_zone_node.receive(tcp_zone_node.getSocketFD(), recvBuffer, 8192);
        if (bytes > 0) {
            DGS::Packet pRecv;
            pRecv.setBuffer(recvBuffer, bytes);
            if (pRecv.getType() == DGS::PKT_ENTITY_TRANSFER) {
                auto newEntity = pRecv.unpackEntityTransfer();
                entities.push_back(newEntity);
                std::cout << "[ZoneNode] ¡He recibido una nueva entidad! ID: " << newEntity.uuid << std::endl;
            }
        }

        auto start = std::chrono::high_resolution_clock::now();

        double xMin = std::atof(std::getenv("X_MIN") ? std::getenv("X_MIN") : "0");
        double xMax = std::atof(std::getenv("X_MAX") ? std::getenv("X_MAX") : "100");
        double yMin = std::atof(std::getenv("Y_MIN") ? std::getenv("Y_MIN") : "0");
        double yMax = std::atof(std::getenv("Y_MAX") ? std::getenv("Y_MAX") : "100");
        
        checkAndTransfer(tcp_zone_node, entities, xMin, xMax, yMin, yMax);

        DGS::ServerMetrics metrics;
        metrics.ramUsage    = getRAM();
        metrics.node.xMin = xMin;
        metrics.node.xMax = xMax;
        metrics.node.yMin = yMin;
        metrics.node.yMax = yMax;
        
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<float, std::milli> duration = end - start;
        metrics.performance = duration.count();
        
        DGS::Packet p;
        p.pack(metrics);
        tcp_zone_node.send(tcp_zone_node.getSocketFD(), p.getRawData(), p.getSize());
        
        usleep(100000); //TODO: rebajalo si estas en prod
    }

    return 0;
}