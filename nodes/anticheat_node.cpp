#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/types.h"

#include <sys/epoll.h>
#include <cstring>
#include <cmath>
#include <map>
#include <set>
#include <iostream>

static constexpr float SCALE       = 1000.0f;
static constexpr float LATENCY_COM = 0.2f;

struct LastKnown
{
    float    gx, gy, gz;
    uint64_t timestamp_ms;
    float    maxSpeed;
};

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static bool validate(const DGS::EntityTransfer& e, const LastKnown& last, float csX, float csY, float csZ)
{
    float dt = (nowMs() - last.timestamp_ms) / 1000.0f;
    float dx = (e.chunkX * csX + e.pos[0]) - last.gx;
    float dy = (e.chunkY * csY + e.pos[1]) - last.gy;
    float dz = (e.chunkZ * csZ + e.pos[2]) - last.gz;
    float d  = std::sqrt(dx*dx + dy*dy + dz*dz);
    float radio = (last.maxSpeed * dt) + SCALE + (LATENCY_COM * last.maxSpeed);
    return d <= radio;
}

int main()
{
    DGS::UDPSocket udpSocket;
    DGS::TCPSocket tcpSocket;
    DGS::TCPSocket headServer;
    DGS::TCPSocket persistence;

    if (!udpSocket.bind(42427))       { std::cerr << "[AntiCheat] Error UDP:42427"           << std::endl; return 1; }
    if (!tcpSocket.listen(42428))     { std::cerr << "[AntiCheat] Error TCP listen:42428"     << std::endl; return 1; }
    if (!headServer.connect("127.0.0.1", 42424)) { std::cerr << "[AntiCheat] Error conectando HeadServer" << std::endl; return 1; }
    if (!persistence.connect("127.0.0.1", 42429)) { std::cerr << "[AntiCheat] Error conectando Persistence:42429" << std::endl; return 1; }

    // Recibir Command inicial del HeadServer con los chunk sizes
    uint8_t cmdBuf[512];
    int cmdBytes = headServer.receive(headServer.getSocketFD(), cmdBuf, sizeof(cmdBuf));
    if (cmdBytes <= 0) { std::cerr << "[AntiCheat] No se recibio Command inicial" << std::endl; return 1; }

    DGS::Packet cmdPacket;
    cmdPacket.setBuffer(cmdBuf, cmdBytes);
    DGS::Command cmd = cmdPacket.unpackCommand();

    float csX = cmd.chunkSizeX;
    float csY = cmd.chunkSizeY;
    float csZ = cmd.chunkSizeZ;

    std::cout << "[AntiCheat] ChunkSize=(" << csX << ", " << csY << ", " << csZ << ") km" << std::endl;
    std::cout << "[AntiCheat] UDP:42427  TCP:42428  Persistence:42429" << std::endl;

    int epollFD = epoll_create1(0);
    epoll_event ev;
    ev.events = EPOLLIN;

    ev.data.fd = udpSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, udpSocket.getSocketFD(), &ev);

    ev.data.fd = tcpSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, tcpSocket.getSocketFD(), &ev);

    epoll_event events[64];
    std::set<int> cacheFDs;
    std::map<uint32_t, LastKnown> lastKnown;

    while (true)
    {
        int n = epoll_wait(epollFD, events, 64, -1);
        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;

            if (fd == udpSocket.getSocketFD())
            {
                uint8_t buffer[sizeof(DGS::EntityTransfer)];
                std::string ip; int port;
                int bytes = udpSocket.receive(buffer, sizeof(buffer), ip, port);
                if (bytes != sizeof(DGS::EntityTransfer)) continue;

                DGS::EntityTransfer e{};
                std::memcpy(&e, buffer, sizeof(e));

                auto it = lastKnown.find(e.uuid);
                if (it != lastKnown.end() && !validate(e, it->second, csX, csY, csZ))
                {
                    std::cout << "[AntiCheat] CHEAT detectado (UDP) uuid=" << e.uuid << std::endl;
                    continue;
                }

                lastKnown[e.uuid] = {
                    e.chunkX * csX + e.pos[0],
                    e.chunkY * csY + e.pos[1],
                    e.chunkZ * csZ + e.pos[2],
                    nowMs(),
                    e.stats.speed[0]
                };
            }
            else if (fd == tcpSocket.getSocketFD())
            {
                int newFD = tcpSocket.accept();
                if (newFD < 0) continue;
                cacheFDs.insert(newFD);
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                std::cout << "[AntiCheat] Cache conectado FD=" << newFD << std::endl;
            }
            else if (cacheFDs.count(fd))
            {
                uint8_t buffer[8192];
                int bytes = tcpSocket.receive(fd, buffer, sizeof(buffer));
                if (bytes <= 0)
                {
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    tcpSocket.closeClient(fd);
                    cacheFDs.erase(fd);
                    continue;
                }

                DGS::Packet p;
                p.setBuffer(buffer, bytes);
                auto e = p.unpackEntityTransfer();

                auto it = lastKnown.find(e.uuid);
                if (it != lastKnown.end() && !validate(e, it->second, csX, csY, csZ))
                {
                    std::cout << "[AntiCheat] CHEAT detectado (TCP) uuid=" << e.uuid << std::endl;
                    continue;
                }

                lastKnown[e.uuid] = {
                    e.chunkX * csX + e.pos[0],
                    e.chunkY * csY + e.pos[1],
                    e.chunkZ * csZ + e.pos[2],
                    nowMs(),
                    e.stats.speed[0]
                };

                persistence.send(persistence.getSocketFD(), p.getRawData(), p.getSize());
            }
        }
    }

    return 0;
}
