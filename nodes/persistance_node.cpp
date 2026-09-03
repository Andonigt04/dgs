#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/logger.h"
#include "include/dgs/types.h"
#include <csignal>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <exception>
#include <cstdlib>
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

    // C6 (§3.6): the module's OPAQUE blob. The DGS does not interpret it — it only carries and stores it
    // as binary so the module can rebuild its state (same code on client and server).
    // Without this, reloading a zone loses the module's state even though pos/stats survive.
    if (e.dataSize > 0)
    {
        doc << "moduleBlob" << bsoncxx::types::b_binary{
            bsoncxx::binary_sub_type::k_binary, e.dataSize, e.data};
    }

    col.insert_one(doc.view());
}

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
    mongocxx::instance instance{};
    const char* mongoUri = std::getenv("MONGO_URI") ? std::getenv("MONGO_URI") : "mongodb://mongodb:27017";
    mongocxx::client   client{mongocxx::uri{mongoUri}};

    auto db       = client["dgs_persistance"];
    auto entities = db["entities"];

    DGS::Logger logger("persistence_log.csv");
    DGS::TCPSocket tcpSocket;

    const int persPort = std::atoi(std::getenv("PERSISTENCE_PORT") ? std::getenv("PERSISTENCE_PORT") : "42429");
    if (!tcpSocket.listen(persPort))
    {
        std::cerr << "[Persistence] Failed to listen on 42429" << std::endl;
        return 1;
    }

        // It does NOT say "connected": `mongocxx::client` is LAZY and has not talked to the database
    // yet. The previous message claimed a connection that does not exist, which misleads anyone
    // diagnosing an outage.
    std::cout << "[Persistence] Listening on TCP:" << persPort
              << " (Mongo: " << mongoUri << ", not verified yet)" << std::endl;

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
                std::cout << "[Persistence] Validator connected FD=" << newFD << std::endl;
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

                // ⚠️ WITH `try`. `insert_one` THROWS when the database does not answer, and nothing
                // caught it here: an uncaught exception is `std::terminate`, which means **a MongoDB
                // outage killed the entire persistence node with the first entity that arrived**. A
                // database failure has to degrade the service, not take it down: the node keeps
                // accepting and serving, and the loss is recorded.
                bool stored = true;
                try {
                    insertEntity(entities, e);
                } catch (const std::exception& ex) {
                    stored = false;
                    std::cerr << "[Persistence] FAILED to store uuid=" << e.uuid
                              << ": " << ex.what() << std::endl;
                }

                DGS::LogEntry entry{};
                entry.time_stamp  = (uint64_t)std::time(nullptr);
                entry.type        = DGS::LOG_TRANSFER;
                entry.entityType  = e.type;
                entry.uuid        = e.uuid;
                entry.fd          = fd;
                entry.bytes       = (uint32_t)bytes;

                logger.log(entry);

                std::cout << "[Persistence] Entity " << (stored ? "stored" : "LOST")
                          << " uuid=" << e.uuid << std::endl;
            }
        }
    }

    return 0;
}
