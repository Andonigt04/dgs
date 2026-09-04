#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/auth.h"
#include "include/dgs/orchestrator.h"
#include "include/dgs/logger.h"
#include <csignal>

#include <map>
#include <sys/epoll.h>
#include <algorithm>
#include <ctime>

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
    DGS::TCPSocket serverSocket;
    DGS::Orchestrator orchestrator(serverSocket);
    DGS::PacketDispatcher dispatcher;
    DGS::Logger logger("headserver_log.csv");
    std::vector<int> nodeClients;

    dispatcher.registerHandler(DGS::PKT_ZONE_LIST, [&](int fd, DGS::Packet& p)
    {
        DGS::ZoneListResponse r;

        for (int i = 0; i < orchestrator.activeZones.size(); i++)
        {
            r.zones[i].chunkXMin = orchestrator.activeZones[i].chunkXMin;
            r.zones[i].chunkXMax = orchestrator.activeZones[i].chunkXMax;
            r.zones[i].chunkYMin = orchestrator.activeZones[i].chunkYMin;
            r.zones[i].chunkYMax = orchestrator.activeZones[i].chunkYMax;
            r.zones[i].chunkZMin = orchestrator.activeZones[i].chunkZMin;
            r.zones[i].chunkZMax = orchestrator.activeZones[i].chunkZMax;
            
            std::strncpy(r.zones[i].addr, orchestrator.activeZones[i].addr, sizeof(r.zones[i].addr));
            r.zones[i].port = orchestrator.activeZones[i].port;

        }

        r.count = orchestrator.activeZones.size();
        DGS::Packet resp;
        resp.pack(r);

        std::cout << resp.getRawData() << ":" << resp.getSize() << std::endl;
        serverSocket.send(fd, resp.getRawData(), resp.getSize());
    });
    
    dispatcher.registerHandler(DGS::PKT_ZONE_QUERY, [&](int fd, DGS::Packet& p)
    {
        auto q = p.unpackZoneQuery();
        DGS::ZoneResponse r = orchestrator.findZoneResponse(q.chunkX, q.chunkY, q.chunkZ);
        DGS::Packet resp;
        resp.pack(r);
        serverSocket.send(fd, resp.getRawData(), resp.getSize());
    });

    dispatcher.registerHandler(DGS::PKT_METRICS, [&](int fd, DGS::Packet& p)
    {
        auto m = p.unpackServerMetrics();
        std::cout << "[HeadServer] PKT_METRICS fd=" << fd << " zones=" << orchestrator.activeZones.size() << std::endl;
        orchestrator.updateNodeTopology(fd, m);
        orchestrator.evaluateServer(m, fd);

        DGS::LogEntry entry{};
        entry.time_stamp  = (uint64_t)std::time(nullptr);
        entry.type        = DGS::LOG_METRICS;
        entry.fd          = fd;
        entry.ramUsage    = m.ramUsage;
        entry.performance = m.performance;

        logger.log(entry);
    });

    dispatcher.registerHandler(DGS::PKT_CHAT, [&](int fd, DGS::Packet& p)
    {
        for (int clientFD : nodeClients)
            if (clientFD != fd)
                serverSocket.send(clientFD, p.getRawData(), p.getSize());
    });

    dispatcher.registerHandler(DGS::PKT_VALIDATOR_STATUS, [&](int fd, DGS::Packet& p)
    {
        auto st = p.unpackValidatorStatus();
        std::cout << "[HeadServer] PKT_VALIDATOR_STATUS fd=" << fd
                  << " state=" << (int)st.state
                  << " req=" << st.reqSent
                  << " timeouts=" << st.reqTimeout
                  << " viol=" << st.failedTransfers << std::endl;

        // P6 (F1): state=2 = validator down / breaker open in that zone → the master reassigns its
        // region to a healthy neighbour (handoff on a failed metric) so nothing is served unvalidated.
        if (st.state == 2)
            orchestrator.notifyValidatorDown(fd, st);

        DGS::LogEntry entry{};
        entry.time_stamp  = (uint64_t)std::time(nullptr);
        entry.type        = DGS::LOG_METRICS;
        entry.fd          = fd;
        entry.ramUsage    = st.state == 2 ? 1.0f : 0.0f;   // breaker open → critical load
        entry.performance = st.state == 2 ? 0.0f : 1.0f;
        logger.log(entry);
    });

    dispatcher.registerHandler(DGS::PKT_GHOST_DELTA, [&](int fd, DGS::Packet& p)
    {
        auto neighbors = orchestrator.findNeighbors(fd);
        for (int neighborFD : neighbors)
            serverSocket.send(neighborFD, p.getRawData(), p.getSize());
    });

    dispatcher.registerHandler(DGS::PKT_REASSIGN, [&](int fd, DGS::Packet& p)
    {
        auto ra = p.unpackEntityReassign();

        // §3.6 handoff: reassign an entity's authority to the zone covering its chunk. Authority is
        // resolved by the orchestrator per chunk (never trust the sender's logical ids).
        int targetFD = orchestrator.findTargetNode(ra.chunkX, ra.chunkY, ra.chunkZ);

        std::cout << "[HeadServer] PKT_REASSIGN uuid=" << ra.entityUuid
                  << " chunk=(" << ra.chunkX << "," << ra.chunkY << "," << ra.chunkZ << ")"
                  << " targetFD=" << targetFD << std::endl;

        // ⚠️ ANSWER THE ZONE THAT GAVE IT UP. Without this the handoff was fire-and-forget: the ceding
        // zone erased the entity the moment it wrote the packet, so a chunk this head cannot route
        // (`targetFD == -1`, dropped right here in silence) or a forward that fails made the entity
        // cease to exist anywhere at all. The ack is what lets the zone hold on until it is safe.
        bool routed = false;
        if (targetFD != -1 && targetFD != fd)
            routed = serverSocket.send(targetFD, p.getRawData(), p.getSize());

        DGS::EntityReassign answer = ra;
        answer.ack    = routed ? 1 : 2;
        answer.toZone = routed ? (uint32_t)targetFD : 0u;
        DGS::Packet pa; pa.pack(answer);
        serverSocket.send(fd, pa.getRawData(), pa.getSize());
        if (!routed)
            std::cout << "[HeadServer] PKT_REASSIGN uuid=" << ra.entityUuid
                      << " NOT routed -> telling fd=" << fd << " to keep it" << std::endl;
    });

        dispatcher.registerHandler(DGS::PKT_ZONE_REGION, [&](int fd, DGS::Packet& p)
    {
        // §3.9 Merge/handoff: the ceding zone serialised its region; we route it by anchor to the zone
        // covering that chunk (which folds it in with mergeRegion).
        auto reg = p.unpackZoneRegion();
        int targetFD = orchestrator.findTargetNode(reg.chunkX, reg.chunkY, reg.chunkZ);
        std::cout << "[HeadServer] PKT_ZONE_REGION src=" << reg.srcZone
                  << " bytes=" << reg.size << " targetFD=" << targetFD << std::endl;
        if (targetFD != -1 && targetFD != fd)
            serverSocket.send(targetFD, p.getRawData(), p.getSize());
    });

    dispatcher.registerHandler(DGS::PKT_DRAIN, [&](int fd, DGS::Packet& p)
    {
        // §3.9: the zone_node confirms its drain (ack=1). The orchestrator destroys the zone (pod +
        // topology + replicas) and sends it PKT_DELETE_ZONE. Requests (ack=0) go orchestrator → zone
        // and never arrive here.
        auto lc = p.unpackZoneLifecycle();
        if (lc.ack == 1)
        {
            std::cout << "[HeadServer] PKT_DRAIN ack fd=" << fd << " requestId=" << lc.requestId << std::endl;
            orchestrator.handleZoneLifecycle(fd, lc);
        }
    });

    dispatcher.registerHandler(DGS::PKT_ENTITY_TRANSFER, [&](int fd, DGS::Packet& p)
    {
        DGS::EntityTransfer e{};
        if (!p.tryUnpackEntityTransfer(e)) return;   // malformed: drop it, stay up
        std::cout << "[HeadServer] Entity received uuid=" << e.uuid
                  << " chunk=(" << e.chunkX << "," << e.chunkY << ") from fd=" << fd << std::endl;

        int targetFD = orchestrator.findTargetNode(e.chunkX, e.chunkY, e.chunkZ);
        std::cout << "[HeadServer] active zones: " << orchestrator.activeZones.size()
                  << " targetFD=" << targetFD << std::endl;

        if (targetFD != -1)
        {
            bool ok = serverSocket.send(targetFD, p.getRawData(), p.getSize());
            std::cout << "[HeadServer] Echo sent to fd=" << targetFD << " ok=" << ok << std::endl;

            DGS::LogEntry entry{};
            entry.time_stamp = (uint64_t)std::time(nullptr);
            entry.type       = DGS::LOG_TRANSFER;
            entry.entityType = e.type;
            entry.uuid       = e.uuid;
            entry.fd         = fd;
            entry.bytes      = (uint32_t)p.getSize();
            logger.log(entry);
        }
        else
            std::cout << "[HeadServer] No valid destination found for the entity" << std::endl;
    });

    // ⚠️ THIS PORT SERVES BOTH NODES AND CLIENTS, so it cannot be closed wholesale: a player's client
    // asks it which zone covers a chunk and has no business holding the cluster secret. What is gated
    // is the PRIVILEGED half — registering as a zone, moving authority, draining, region blobs — which
    // is what an unauthenticated stranger could do here: send one `PKT_METRICS` and have the head
    // start routing other people's entities to them.
    DGS::AuthGate gate("HeadServer");

    if (!serverSocket.listen(42424))
    {
        std::cerr << "[HeadServer] Failed to start the server. Port already in use?" << std::endl;
        return 1;
    }
    std::cout << "[HeadServer] Listening..." << std::endl;
    gate.announce();

    int epollFD = epoll_create1(0);

    epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = serverSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, serverSocket.getSocketFD(), &ev);

    epoll_event events[64];

    while (true)
    {
        int n = epoll_wait(epollFD, events, 64, -1);

        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;
            if (fd == serverSocket.getSocketFD())
            {
                int newFD = serverSocket.accept();
                if (newFD < 0) continue;
                ev.events = EPOLLIN;
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                nodeClients.push_back(newFD);
                std::cout << "[HeadServer] New zone connected. FD: " << newFD << std::endl;
            }
            else
            {
                // ⚠️ DRAINED, not read once. `epoll` reports the KERNEL's buffer; with TLS on, OpenSSL
                // can be holding decrypted bytes the kernel no longer has, so a packet would sit
                // unread until the peer happened to send another one. `pending()` is that question.
                // An epoll wake-up is not proof of application data once TLS is on: the handshake's
                // trailing records (a TLS 1.3 `NewSessionTicket`, say) wake it too, and the blocking
                // read below would then wait for a message nobody sent. See `TCPSocket::pending`.
                if (serverSocket.tlsEnabled() && !serverSocket.pending(fd)) continue;

                uint8_t buffer[8192];
                bool peerGone = false;
                do {
                int bytesRead = serverSocket.receive(fd, buffer, 8192);

                if (bytesRead <= 0)
                {
                    peerGone = true;
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    serverSocket.closeClient(fd);
                    gate.forget(fd);
                    nodeClients.erase(std::find(nodeClients.begin(), nodeClients.end(), fd));
                }
                else
                {
                    DGS::Packet p;
                    p.setBuffer(buffer, bytesRead);

                    if (gate.consume(fd, p)) continue;   // it was the credential, not a request

                    // The privileged half. A client's zone query and zone list stay open: clients
                    // authenticate against the login API, not against this.
                    const DGS::PacketType t = p.getType();
                    const bool privileged = t == DGS::PKT_METRICS       || t == DGS::PKT_REASSIGN ||
                                            t == DGS::PKT_ZONE_REGION   || t == DGS::PKT_DRAIN    ||
                                            t == DGS::PKT_DELETE_ZONE   || t == DGS::PKT_VALIDATOR_STATUS ||
                                            t == DGS::PKT_ENTITY_TRANSFER;
                    if (privileged && !gate.allows(fd)) { gate.refuse(fd, (int)t); continue; }

                    dispatcher.dispatch(fd, p); 
                }
                } while (!peerGone && serverSocket.pending(fd));
            }
        }
    }

    return 0;
}