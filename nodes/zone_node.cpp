#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <iostream>
#include <fstream>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <map>
#include <sys/socket.h>

float getRAM()
{
    long pages, rss;
    std::ifstream stat_file("/proc/self/statm");
    stat_file >> pages >> rss;
    stat_file.close();

    long total_bytes = rss * sysconf(_SC_PAGESIZE);
    float limit = 512.f * 1024.f * 1024.f;
    return (float)total_bytes / limit;
}

bool isNearBorder(const DGS::EntityTransfer& e,
                  int32_t xMin, int32_t xMax,
                  int32_t yMin, int32_t yMax,
                  int32_t zMin, int32_t zMax,
                  int32_t threshold)
{
    return e.chunkX <= xMin + threshold || e.chunkX >= xMax - threshold ||
           e.chunkY <= yMin + threshold || e.chunkY >= yMax - threshold ||
           e.chunkZ <= zMin + threshold || e.chunkZ >= zMax - threshold;
}

static void angleToQuat(uint16_t angle, float rot[4])
{
    float yaw     = angle * (3.14159265f / 32767.5f);
    float halfYaw = yaw * 0.5f;
    rot[0] = 0.0f;
    rot[1] = sinf(halfYaw);
    rot[2] = 0.0f;
    rot[3] = cosf(halfYaw);
}

void checkAndTransfer(DGS::TCPSocket& tcp, std::vector<DGS::EntityTransfer>& entities,
                      int32_t xMin, int32_t xMax,
                      int32_t yMin, int32_t yMax,
                      int32_t zMin, int32_t zMax)
{
    for (auto it = entities.begin(); it != entities.end();)
    {
        bool out = it->chunkX < xMin || it->chunkX > xMax ||
                   it->chunkY < yMin || it->chunkY > yMax ||
                   it->chunkZ < zMin || it->chunkZ > zMax;

        if (out)
        {
            std::cout << "[ZoneNode] Entidad " << it->uuid << " fuera de limites. Transfiriendo..." << std::endl;
            DGS::Packet p;
            p.pack(*it);
            tcp.send(tcp.getSocketFD(), p.getRawData(), p.getSize());
            it = entities.erase(it);
        }
        else ++it;
    }
}

void emitGhostDeltas(DGS::TCPSocket& tcp,
                     const std::vector<DGS::EntityTransfer>& entities,
                     std::map<uint32_t, DGS::EntityTransfer>& lastSnapshot,
                     int32_t xMin, int32_t xMax,
                     int32_t yMin, int32_t yMax,
                     int32_t zMin, int32_t zMax,
                     int32_t threshold)
{
    for (const auto& e : entities)
    {
        if (!isNearBorder(e, xMin, xMax, yMin, yMax, zMin, zMax, threshold)) continue;

        DGS::GhostDelta delta{};
        delta.uuid   = e.uuid;
        delta.chunkX = e.chunkX;
        delta.chunkY = e.chunkY;
        delta.chunkZ = e.chunkZ;

        auto it = lastSnapshot.find(e.uuid);
        bool isNew = it == lastSnapshot.end();

        if (isNew ||
            fabsf(e.pos[0] - it->second.pos[0]) > 0.1f ||
            fabsf(e.pos[1] - it->second.pos[1]) > 0.1f ||
            fabsf(e.pos[2] - it->second.pos[2]) > 0.1f ||
            e.angle != it->second.angle)
        {
            delta.dirtyMask |= DGS::DIRTY_TRANSFORM;
            delta.pos[0] = e.pos[0];
            delta.pos[1] = e.pos[1];
            delta.pos[2] = e.pos[2];
            angleToQuat(e.angle, delta.rot);
        }

        if (isNew || std::memcmp(&e.stats, &it->second.stats, sizeof(DGS::Stats)) != 0)
        {
            delta.dirtyMask |= DGS::DIRTY_STATS;
            delta.stats = e.stats;
        }

        if (delta.dirtyMask == 0) continue;

        DGS::Packet p;
        p.pack(delta);
        tcp.send(tcp.getSocketFD(), p.getRawData(), p.getSize());
        lastSnapshot[e.uuid] = e;
    }

    for (auto it = lastSnapshot.begin(); it != lastSnapshot.end();)
    {
        bool exists = false;
        for (const auto& e : entities)
            if (e.uuid == it->first) { exists = true; break; }
        it = exists ? std::next(it) : lastSnapshot.erase(it);
    }
}

// Envía todas las entidades y ghosts a un cliente
void broadcastToClient(DGS::UDPSocket& udp,
                       const std::string& addr, int port,
                       const std::vector<DGS::EntityTransfer>& entities,
                       const std::map<uint32_t, DGS::GhostDelta>& ghosts)
{
    for (const auto& e : entities)
    {
        const uint8_t* raw = reinterpret_cast<const uint8_t*>(&e);
        udp.send(addr, port, raw, sizeof(DGS::EntityTransfer));
    }

    for (const auto& [uuid, ghost] : ghosts)
    {
        DGS::Packet p;
        p.pack(ghost);
        udp.send(addr, port, p.getRawData(), p.getSize());
    }
}

int main()
{
    DGS::UDPSocket udp_zone_node;
    DGS::TCPSocket tcp_zone_node;

    std::vector<DGS::EntityTransfer>          entities;
    std::map<uint32_t, DGS::EntityTransfer>   lastSnapshot;
    std::map<uint32_t, DGS::GhostDelta>       ghostEntities;
    std::map<uint32_t, std::pair<std::string, int>> clientMap; // uuid → {addr, port}

    const char* headHost  = std::getenv("HEAD_SERVER_HOST") ? std::getenv("HEAD_SERVER_HOST") : "head-server";
    int         headPort  = std::atoi(std::getenv("HEAD_SERVER_PORT") ? std::getenv("HEAD_SERVER_PORT") : "42424");
    const char* zoneAddr  = std::getenv("MY_POD_IP")        ? std::getenv("MY_POD_IP")        : "127.0.0.1";
    int         udpPort   = std::atoi(std::getenv("ZONE_UDP_PORT")    ? std::getenv("ZONE_UDP_PORT")    : "42425");
    int32_t     threshold = std::atoi(std::getenv("GHOST_THRESHOLD")  ? std::getenv("GHOST_THRESHOLD")  : "1");

    if (!udp_zone_node.bind(udpPort))
    {
        std::cerr << "[ZoneNode] Error al hacer bind UDP en puerto " << udpPort << std::endl;
        return 1;
    }
    std::cout << "[ZoneNode] UDP escuchando en :" << udpPort << std::endl;

    // SO_RCVTIMEO en UDP para no bloquear el tick
    struct timeval tvUDP { 0, 10000 }; // 10ms
    setsockopt(udp_zone_node.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tvUDP, sizeof(tvUDP));

    auto connectToHead = [&]() -> bool {
        tcp_zone_node = DGS::TCPSocket();
        for (int attempt = 1; ; ++attempt)
        {
            if (tcp_zone_node.connect(headHost, headPort))
            {
                struct timeval tvTCP { 0, 100000 };
                setsockopt(tcp_zone_node.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tvTCP, sizeof(tvTCP));
                std::cout << "[ZoneNode] Conectado a HeadServer (intento " << attempt << ")" << std::endl;
                return true;
            }
            std::cerr << "[ZoneNode] Reintentando conexion a HeadServer..." << std::endl;
            sleep(3);
        }
    };

    connectToHead();

    while (true)
    {
        // Recibir UDP de clientes
        {
            uint8_t udpBuf[sizeof(DGS::EntityTransfer)];
            std::string clientAddr;
            int clientPort = 0;
            int udpBytes = udp_zone_node.receive(udpBuf, sizeof(udpBuf), clientAddr, clientPort);

            if (udpBytes == sizeof(DGS::EntityTransfer))
            {
                DGS::EntityTransfer e;
                std::memcpy(&e, udpBuf, sizeof(e));
                clientMap[e.uuid] = { clientAddr, clientPort };

                // Actualizar o añadir entidad
                bool found = false;
                for (auto& existing : entities)
                {
                    if (existing.uuid == e.uuid) { existing = e; found = true; break; }
                }
                if (!found) entities.push_back(e);
            }
        }

        // Recibir TCP del HeadServer
        {
            uint8_t tcpBuf[8192];
            int bytes = tcp_zone_node.receive(tcp_zone_node.getSocketFD(), tcpBuf, sizeof(tcpBuf));
            if (bytes == 0)
            {
                std::cerr << "[ZoneNode] HeadServer cerro la conexion. Reconectando..." << std::endl;
                connectToHead();
            }
            if (bytes > 0)
            {
                DGS::Packet pRecv;
                pRecv.setBuffer(tcpBuf, bytes);

                switch (pRecv.getType())
                {
                    case DGS::PKT_ENTITY_TRANSFER:
                    {
                        auto e = pRecv.unpackEntityTransfer();
                        entities.push_back(e);
                        std::cout << "[ZoneNode] Nueva entidad recibida uuid=" << e.uuid << std::endl;
                        break;
                    }
                    case DGS::PKT_GHOST_DELTA:
                    {
                        auto ghost = pRecv.unpackGhostDelta();
                        ghostEntities[ghost.uuid] = ghost;
                        break;
                    }
                    default: break;
                }
            }
        }

        int32_t xMin = std::atoi(std::getenv("CHUNK_X_MIN") ? std::getenv("CHUNK_X_MIN") : "0");
        int32_t xMax = std::atoi(std::getenv("CHUNK_X_MAX") ? std::getenv("CHUNK_X_MAX") : "100");
        int32_t yMin = std::atoi(std::getenv("CHUNK_Y_MIN") ? std::getenv("CHUNK_Y_MIN") : "0");
        int32_t yMax = std::atoi(std::getenv("CHUNK_Y_MAX") ? std::getenv("CHUNK_Y_MAX") : "100");
        int32_t zMin = std::atoi(std::getenv("CHUNK_Z_MIN") ? std::getenv("CHUNK_Z_MIN") : "0");
        int32_t zMax = std::atoi(std::getenv("CHUNK_Z_MAX") ? std::getenv("CHUNK_Z_MAX") : "100");

        auto start = std::chrono::high_resolution_clock::now();

        checkAndTransfer(tcp_zone_node, entities, xMin, xMax, yMin, yMax, zMin, zMax);
        emitGhostDeltas(tcp_zone_node, entities, lastSnapshot, xMin, xMax, yMin, yMax, zMin, zMax, threshold);

        // Broadcast a todos los clientes conectados
        for (const auto& [uuid, endpoint] : clientMap)
            broadcastToClient(udp_zone_node, endpoint.first, endpoint.second, entities, ghostEntities);

        DGS::ServerMetrics metrics;
        metrics.ramUsage       = getRAM();
        metrics.node.chunkXMin = xMin; metrics.node.chunkXMax = xMax;
        metrics.node.chunkYMin = yMin; metrics.node.chunkYMax = yMax;
        metrics.node.chunkZMin = zMin; metrics.node.chunkZMax = zMax;
        std::strncpy(metrics.node.addr, zoneAddr, sizeof(metrics.node.addr) - 1);
        metrics.node.port = udpPort;

        auto end = std::chrono::high_resolution_clock::now();
        metrics.performance = std::chrono::duration<float, std::milli>(end - start).count();

        DGS::Packet p;
        p.pack(metrics);
        if (!tcp_zone_node.send(tcp_zone_node.getSocketFD(), p.getRawData(), p.getSize()))
        {
            std::cerr << "[ZoneNode] Conexion con HeadServer perdida. Reconectando..." << std::endl;
            connectToHead();
        }

        usleep(100000);
    }

    return 0;
}
