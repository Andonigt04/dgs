#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include <csignal>

#include <sys/epoll.h>
#include <algorithm>
#include <queue>
#include <mutex>
#include <iostream>
#include <set>
#include <cstdlib>

int main()
{
    // ⚠️ A NODE MUST NOT DIE BECAUSE A PEER HUNG UP. Writing to a socket whose other end has closed
    // raises SIGPIPE, and its default action is to KILL the process. No node installed this, and the
    // whole suite stayed green anyway: every test calls `signal(SIGPIPE, SIG_IGN)` before `fork()`, and
    // a child INHERITS an ignored disposition — so under CTest the nodes survived, and started from a
    // shell, systemd, Docker or `dgs run` they died the first time a peer disconnected.
    // Measured with the same binary and the same environment: parent ignoring SIGPIPE -> ran the full
    // 6 s; ordinary parent -> exit 141 (128 + SIGPIPE) within seconds of the head closing.
    // A closed peer is an ordinary event: `send` returns EPIPE and the reconnect paths handle it.
    std::signal(SIGPIPE, SIG_IGN);
    DGS::TCPSocket zoneSocket;
    DGS::TCPSocket validadorSocket;

    std::queue<DGS::EntityTransfer> pending;
    std::mutex mutex;

    // Ports from the ENVIRONMENT, like every other node. They were hardcoded, which makes it
    // impossible to bring up two instances on the same machine — a test included, which is how this
    // was found.
    const int zonePort = std::atoi(std::getenv("CACHE_ZONE_PORT")      ? std::getenv("CACHE_ZONE_PORT")      : "42425");
    const int valPort  = std::atoi(std::getenv("CACHE_VALIDATOR_PORT") ? std::getenv("CACHE_VALIDATOR_PORT") : "42426");

    if (!zoneSocket.listen(zonePort))     { std::cerr << "[Cache] Failed to listen for ZoneNodes" << std::endl; return 1; }
    if (!validadorSocket.listen(valPort)) { std::cerr << "[Cache] Failed to listen for the Validator" << std::endl; return 1; }

    std::cout << "[Cache] Listening ZoneNodes:" << zonePort << " Validator:" << valPort << std::endl;

    int epollFD = epoll_create1(0);

    epoll_event ev;
    ev.events = EPOLLIN;
    
    ev.data.fd = validadorSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, validadorSocket.getSocketFD(), &ev);

    ev.data.fd = zoneSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, zoneSocket.getSocketFD(), &ev);

    epoll_event events[64];
    std::set<int> zoneFDs;
    std::set<int> validadorFDs;

    while (true)
    {
        int n = epoll_wait(epollFD, events, 64, 0);
        if (n == 0) n = epoll_wait(epollFD, events, 64, -1);

        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;

            if (fd == validadorSocket.getSocketFD())
            {
                int newFD = validadorSocket.accept();
                if (newFD < 0) continue;
                validadorFDs.insert(newFD);
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                std::cout << "[Cache] Validator connected FD: " << newFD << std::endl;
            }
            else if (fd == zoneSocket.getSocketFD())
            {
                int newFD = zoneSocket.accept();
                if (newFD < 0) continue;
                zoneFDs.insert(newFD);
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                std::cout << "[Cache] ZoneNode connected FD: " << newFD << std::endl;
            }
            else if (validadorFDs.count(fd))
            {
                // ⚠️ THE REQUEST MUST BE READ. It was not: the node only popped from its queue. With
                // LEVEL-TRIGGERED epoll, unconsumed bytes keep the event firing, so the node (a) spun
                // at 100 % CPU on the same data and (b) drained the whole queue with nobody asking.
                // Measured in `cache_e2e`: asking for ONE entity delivered two, and the output order
                // looked like 101 · 103 because 102 had slipped out on its own.
                uint8_t buffer[8192];
                const int bytes = validadorSocket.receive(fd, buffer, sizeof(buffer));
                if (bytes <= 0)
                {
                    // And without this a validator that leaves keeps its descriptor in the set: epoll
                    // goes on reporting it and the loop spins against a dead socket.
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    validadorSocket.closeClient(fd);
                    validadorFDs.erase(fd);
                    continue;
                }

                std::lock_guard<std::mutex> lock(mutex);
                if (!pending.empty())
                {
                    DGS::EntityTransfer e = pending.front();
                    pending.pop();
                    DGS::Packet p;
                    p.pack(e);
                    validadorSocket.send(fd, p.getRawData(), p.getSize());
                }
            }
            else if (zoneFDs.count(fd))
            {
                uint8_t buffer[8192];
                int bytes = zoneSocket.receive(fd, buffer, 8192);
                if (bytes <= 0)
                {
                    // Same reason as above: a zone that disconnects has to LEAVE the epoll set.
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    zoneSocket.closeClient(fd);
                    zoneFDs.erase(fd);
                    continue;
                }
                DGS::Packet p;
                p.setBuffer(buffer, bytes);
                std::lock_guard<std::mutex> lock(mutex);
                pending.push(p.unpackEntityTransfer());
            }
        }
    }

    return 0;
}