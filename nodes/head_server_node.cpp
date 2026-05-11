#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/orchestrator.h"

#include <map>
#include <sys/epoll.h>
#include <algorithm>

int main()
{
    DGS::TCPSocket serverSocket;
    DGS::Orchestrator orchestrator(serverSocket);
    DGS::PacketDispatcher dispatcher;
    std::vector<int> nodeClients;

    
    dispatcher.registerHandler(DGS::PKT_METRICS, [&](int fd, DGS::Packet& p) {
        auto m = p.unpackServerMetrics();
        orchestrator.updateNodeTopology(fd, m);
        orchestrator.evaluateServer(m, fd);
    });
    
    dispatcher.registerHandler(DGS::PKT_ENTITY_TRANSFER, [&](int fd, DGS::Packet& p) {
        auto e = p.unpackEntityTransfer();
        std::cout << "[HeadServer] Entidad recibida uuid=" << e.uuid
                  << " pos=(" << e.pos[0] << "," << e.pos[1] << ") desde fd=" << fd << std::endl;

        int targetFD = orchestrator.findTargetNode(e.pos[0], e.pos[1]);
        std::cout << "[HeadServer] Zonas activas: " << orchestrator.activeZones.size()
                  << " targetFD=" << targetFD << std::endl;

        if (targetFD != -1)
        {
            bool ok = serverSocket.send(targetFD, p.getRawData(), p.getSize());
            std::cout << "[HeadServer] Echo enviado a fd=" << targetFD << " ok=" << ok << std::endl;
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
            } else
            {
                uint8_t buffer[8192];
                int bytesRead = serverSocket.receive(fd, buffer, 8192);

                if (bytesRead <= 0)
                {
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    serverSocket.closeClient(fd);
                    nodeClients.erase(std::find(nodeClients.begin(), nodeClients.end(), fd));
                } else
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