#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/orchestrator.h"
#include "include/dgs/logger.h"

#include <map>
#include <sys/epoll.h>
#include <algorithm>
#include <ctime>

int main()
{
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
        std::cout << "[HeadServer] PKT_METRICS fd=" << fd << " zonas=" << orchestrator.activeZones.size() << std::endl;
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
                  << " estado=" << (int)st.state
                  << " req=" << st.reqSent
                  << " timeouts=" << st.reqTimeout
                  << " viol=" << st.failedTransfers << std::endl;

        DGS::LogEntry entry{};
        entry.time_stamp  = (uint64_t)std::time(nullptr);
        entry.type        = DGS::LOG_METRICS;
        entry.fd          = fd;
        entry.ramUsage    = st.state == 2 ? 1.0f : 0.0f;   // circuito abierto → carga crítica
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

        // §3.6 handoff: reasignar la autoridad de una entidad a la zona que cubre su chunk. La
        // autoridad la resuelve el orquestador por chunk (no confiar en ids lógicos del remitente).
        int targetFD = orchestrator.findTargetNode(ra.chunkX, ra.chunkY, ra.chunkZ);

        std::cout << "[HeadServer] PKT_REASSIGN uuid=" << ra.entityUuid
                  << " chunk=(" << ra.chunkX << "," << ra.chunkY << "," << ra.chunkZ << ")"
                  << " targetFD=" << targetFD << std::endl;

        if (targetFD != -1 && targetFD != fd)
            serverSocket.send(targetFD, p.getRawData(), p.getSize());
    });

        dispatcher.registerHandler(DGS::PKT_ZONE_REGION, [&](int fd, DGS::Packet& p)
    {
        // §3.9 Fusión/traspaso: la zona que cede serializó su región; la enrutamos por ancla a la zona
        // que cubre ese chunk (que la incorporará con mergeRegion).
        auto reg = p.unpackZoneRegion();
        int targetFD = orchestrator.findTargetNode(reg.chunkX, reg.chunkY, reg.chunkZ);
        std::cout << "[HeadServer] PKT_ZONE_REGION src=" << reg.srcZone
                  << " bytes=" << reg.size << " targetFD=" << targetFD << std::endl;
        if (targetFD != -1 && targetFD != fd)
            serverSocket.send(targetFD, p.getRawData(), p.getSize());
    });

    dispatcher.registerHandler(DGS::PKT_DRAIN, [&](int fd, DGS::Packet& p)
    {
        // §3.9: el zone_node confirma su drenado (ack=1). El orquestador destruye la zona (pod +
        // topología + réplicas) y le manda PKT_DELETE_ZONE. Las peticiones (ack=0) salen del
        // orquestador → zona y nunca llegan aquí.
        auto lc = p.unpackZoneLifecycle();
        if (lc.ack == 1)
        {
            std::cout << "[HeadServer] PKT_DRAIN ack fd=" << fd << " requestId=" << lc.requestId << std::endl;
            orchestrator.handleZoneLifecycle(fd, lc);
        }
    });

    dispatcher.registerHandler(DGS::PKT_ENTITY_TRANSFER, [&](int fd, DGS::Packet& p)
    {
        auto e = p.unpackEntityTransfer();
        std::cout << "[HeadServer] Entidad recibida uuid=" << e.uuid
                  << " chunk=(" << e.chunkX << "," << e.chunkY << ") desde fd=" << fd << std::endl;

        int targetFD = orchestrator.findTargetNode(e.chunkX, e.chunkY, e.chunkZ);
        std::cout << "[HeadServer] Zonas activas: " << orchestrator.activeZones.size()
                  << " targetFD=" << targetFD << std::endl;

        if (targetFD != -1)
        {
            bool ok = serverSocket.send(targetFD, p.getRawData(), p.getSize());
            std::cout << "[HeadServer] Echo enviado a fd=" << targetFD << " ok=" << ok << std::endl;

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
            std::cout << "[HeadServer] No se encontro destino valido para la entidad" << std::endl;
    });

    if (!serverSocket.listen(42424))
    {
        std::cerr << "[HeadServer] Error al iniciar el servidor. ¿Puerto en uso?" << std::endl;
        return 1;
    }
    std::cout << "[HeadServer] Escuchando..." << std::endl;

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
                std::cout << "[HeadServer] Nueva zona conectada! FD: " << newFD << std::endl;
            }
            else
            {
                uint8_t buffer[8192];
                int bytesRead = serverSocket.receive(fd, buffer, 8192);

                if (bytesRead <= 0)
                {
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    serverSocket.closeClient(fd);
                    nodeClients.erase(std::find(nodeClients.begin(), nodeClients.end(), fd));
                }
                else
                {
                    DGS::Packet p;
                    p.setBuffer(buffer, bytesRead);
                    dispatcher.dispatch(fd, p); 
                }
            }
        }
    }

    return 0;
}