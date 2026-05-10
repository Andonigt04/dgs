#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/orchestrator.h"

#include <map>

int main()
{
    DGS::TCPSocket serverSocket;
    DGS::Orchestrator orchestrator;
    DGS::PacketDispatcher dispatcher;
    std::vector<int> nodeClients;

    
    dispatcher.registerHandler(DGS::PKT_METRICS, [&](int fd, DGS::Packet& p) {
        auto m = p.unpackServerMetrics();

        orchestrator.updateNodeInfo(fd, m);
        orchestrator.evaluateServer(m, fd);
    });
    
    dispatcher.registerHandler(DGS::PKT_ENTITY_TRANSFER, [&](int fd, DGS::Packet& p) {
        auto e = p.unpackEntityTransfer();

        int targetFD = orchestrator.findTargetNode(e.pos[0], e.pos[1]);
        
        if (targetFD != -1 && targetFD != fd)
        {
            std::cout << "[HeadServer] Reenvieando entidad " << e.uuid << " recibida del nodo " << fd << std::endl;
            serverSocket.send(targetFD, p.getRawData(), p.getSize());
        } else
        {
            std::cout << "[HeadServer] No se encontro destino valido para la entidad" << std::endl;
        }
    });

    serverSocket.listen(42424);

    while (true)
    {
        fd_set readfds;
        FD_ZERO(&readfds);

        int max_sd = serverSocket.getSocketFD();
        FD_SET(max_sd, &readfds);

        for (int fd : nodeClients)
        {
            FD_SET(fd, &readfds);
            if (fd > max_sd) max_sd = fd;
        }

        select(max_sd + 1, &readfds, NULL, NULL, NULL);

        if(FD_ISSET(serverSocket.getSocketFD(), &readfds))
        {
            int newFD = serverSocket.accept();
            nodeClients.push_back(newFD);
            std::cout << "[HeadServer] Nueva zona conectada! FD: " << newFD << std::endl;
        }

        for (auto it = nodeClients.begin(); it != nodeClients.end(); )
        {
            int fd = *it;
            if (FD_ISSET(fd, &readfds))
            {
                uint8_t buffer[1024];
                int bytesRead = serverSocket.receive(fd, buffer, 1024);

                if (bytesRead <= 0)
                {
                    serverSocket.closeClient(fd);
                    it = nodeClients.erase(it);
                    continue;
                } else
                {
                    DGS::Packet p;
                    p.setBuffer(buffer, bytesRead);
                    
                    dispatcher.dispatch(fd, p); 
                }
            }
            ++it;
        }
    }

    return 0;
}