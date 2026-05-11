#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/logger.h"
#include "include/dgs/types.h"

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <bsoncxx/builder/stream/document.hpp>

#include <sys/epoll.h>
#include <set>
#include <iostream>

void insertEntity(mongocxx::collection& col, const DGS::EntityTransfer& e)
{
    bsoncxx::builder::stream::document doc{};
    doc << "uuid"     << (int32_t)e.uuid
        << "type"     << (int32_t)e.type
        << "chunkX"   << e.chunkX
        << "chunkY"   << e.chunkY
        << "chunkZ"   << e.chunkZ
        << "localX"   << (double)e.pos[0]
        << "localY"   << (double)e.pos[1]
        << "localZ"   << (double)e.pos[2]
        << "angle"    << (int32_t)e.angle
        << "dataSize" << (int32_t)e.dataSize
        << "state"    << (int32_t)e.state;

    col.insert_one(doc.view());
}

int main()
{
    mongocxx::instance instance{};
    mongocxx::client   client{mongocxx::uri{"mongodb://localhost:27017"}};

    auto db       = client["dgs_persistance"];
    auto entities = db["entities"];

    DGS::Logger logger("persistence_log.csv");
    DGS::TCPSocket tcpSocket;

    if (!tcpSocket.listen(42429))
    {
        std::cerr << "[Persistence] Error al escuchar en 42429" << std::endl;
        return 1;
    }

    std::cout << "[Persistence] Conectado a MongoDB. Escuchando TCP:42429" << std::endl;

    int epollFD = epoll_create1(0);
    epoll_event ev;
    ev.events  = EPOLLIN;
    ev.data.fd = tcpSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, tcpSocket.getSocketFD(), &ev);

    epoll_event events[64];
    std::set<int> clientFDs;

    while (true)
    {
        int n = epoll_wait(epollFD, events, 64, -1);
        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;

            if (fd == tcpSocket.getSocketFD())
            {
                int newFD = tcpSocket.accept();
                if (newFD < 0) continue;
                clientFDs.insert(newFD);
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                std::cout << "[Persistence] AntiCheat conectado FD=" << newFD << std::endl;
            }
            else if (clientFDs.count(fd))
            {
                uint8_t buffer[8192];
                int bytes = tcpSocket.receive(fd, buffer, sizeof(buffer));
                if (bytes <= 0)
                {
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    tcpSocket.closeClient(fd);
                    clientFDs.erase(fd);
                    continue;
                }

                DGS::Packet p;
                p.setBuffer(buffer, bytes);
                auto e = p.unpackEntityTransfer();

                insertEntity(entities, e);

                DGS::LogEntry entry{};
                entry.time_stamp  = (uint64_t)std::time(nullptr);
                entry.type        = DGS::LOG_TRANSFER;
                entry.entityType  = e.type;
                entry.uuid        = e.uuid;
                entry.fd          = fd;
                entry.bytes       = (uint32_t)bytes;

                logger.log(entry);

                std::cout << "[Persistence] Entidad guardada uuid=" << e.uuid << std::endl;
            }
        }
    }

    return 0;
}
