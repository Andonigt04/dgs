#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/epoll.h>
#include <algorithm>
#include <queue>
#include <mutex>
#include <iostream>
#include <set>

int main()
{
    DGS::TCPSocket zoneSocket;
    DGS::TCPSocket anticheatSocket;

    std::queue<DGS::EntityTransfer> pending;
    std::mutex mutex;

    if (!zoneSocket.listen(42425)) { std::cerr << "[Cache] Error al escuchar ZoneNodes" << std::endl; return 1; }
    if (!anticheatSocket.listen(42426)) { std::cerr << "[AntiCheat] Error al escuchar AntiCheat" << std::endl; return 1; }

    std::cout << "[Cache] Escuchando ZoneNodes:42425 AntiCheat:42426" << std::endl;

    int epollFD = epoll_create1(0);

    epoll_event ev;
    ev.events = EPOLLIN;
    
    ev.data.fd = anticheatSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, anticheatSocket.getSocketFD(), &ev);

    ev.data.fd = zoneSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, zoneSocket.getSocketFD(), &ev);

    epoll_event events[64];
    std::set<int> zoneFDs;
    std::set<int> anticheatFDs;

    while (true)
    {
        int n = epoll_wait(epollFD, events, 64, 0);
        if (n == 0) n = epoll_wait(epollFD, events, 64, -1);

        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;

            if (fd == anticheatSocket.getSocketFD())
            {
                int newFD = anticheatSocket.accept();
                if (newFD < 0) continue;
                anticheatFDs.insert(newFD);
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                std::cout << "[Cache] AntiCheat conectado FD: " << newFD << std::endl;
            }
            else if (fd == zoneSocket.getSocketFD())
            {
                int newFD = zoneSocket.accept();
                if (newFD < 0) continue;
                zoneFDs.insert(newFD);
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                std::cout << "[Cache] ZoneNode conectado FD: " << newFD << std::endl;
            }
            else if (anticheatFDs.count(fd))
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (!pending.empty())
                {
                    DGS::EntityTransfer e = pending.front();
                    pending.pop();
                    DGS::Packet p;
                    p.pack(e);
                    anticheatSocket.send(fd, p.getRawData(), p.getSize());
                }

            }
            else if (zoneFDs.count(fd))
            {
                uint8_t buffer[8192];
                int bytes = zoneSocket.receive(fd, buffer, 8192);
                if (bytes > 0)
                {
                    DGS::Packet p;
                    p.setBuffer(buffer, bytes);
                    std::lock_guard<std::mutex> lock(mutex);
                    pending.push(p.unpackEntityTransfer());
                }
            }
        }
    }

    return 0;
}