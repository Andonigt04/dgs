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

void checkAndTransfer(DGS::TCPSocket& tcp_node, std::vector<DGS::EntityTransfer>& entities, int32_t chunkXMin, int32_t chunkXMax, int32_t chunkYMin, int32_t chunkYMax, int32_t chunkZMin, int32_t chunkZMax)
{
    DGS::Packet p;

    for (auto it = entities.begin(); it != entities.end();)
    {
        bool outOfBounds = (it->chunkX < chunkXMin || it->chunkX > chunkXMax || it->chunkY < chunkYMin || it->chunkY > chunkYMax || it->chunkZ < chunkZMin || it->chunkZ > chunkZMax);

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

    const char* headHost = std::getenv("HEAD_SERVER_HOST") ? std::getenv("HEAD_SERVER_HOST") : "head-server";
    int         headPort = std::atoi(std::getenv("HEAD_SERVER_PORT") ? std::getenv("HEAD_SERVER_PORT") : "42424");

    int fd = tcp_zone_node.connect(headHost, headPort);

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

        int32_t chunkXMin = std::atoi(std::getenv("CHUNK_X_MIN") ? std::getenv("CHUNK_X_MIN") : "0");
        int32_t chunkXMax = std::atoi(std::getenv("CHUNK_X_MAX") ? std::getenv("CHUNK_X_MAX") : "100");
        int32_t chunkYMin = std::atoi(std::getenv("CHUNK_Y_MIN") ? std::getenv("CHUNK_Y_MIN") : "0");
        int32_t chunkYMax = std::atoi(std::getenv("CHUNK_Y_MAX") ? std::getenv("CHUNK_Y_MAX") : "100");
        int32_t chunkZMin = std::atoi(std::getenv("CHUNK_Z_MIN") ? std::getenv("CHUNK_Z_MIN") : "0");
        int32_t chunkZMax = std::atoi(std::getenv("CHUNK_Z_MAX") ? std::getenv("CHUNK_Z_MAX") : "100");

        checkAndTransfer(tcp_zone_node, entities, chunkXMin, chunkXMax, chunkYMin, chunkYMax, chunkZMin, chunkZMax);

        DGS::ServerMetrics metrics;
        metrics.ramUsage       = getRAM();
        metrics.node.chunkXMin = chunkXMin;
        metrics.node.chunkXMax = chunkXMax;
        metrics.node.chunkYMin = chunkYMin;
        metrics.node.chunkYMax = chunkYMax;
        metrics.node.chunkZMin = chunkZMin;
        metrics.node.chunkZMax = chunkZMax;
        
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