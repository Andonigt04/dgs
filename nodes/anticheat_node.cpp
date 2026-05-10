#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <iostream>

int main()
{
    DGS::UDPSocket server;
    if (!server.bind(42424)) {
        std::cerr << "FATAL: No se pudo abrir el puerto. Cerrando..." << std::endl;
        return -1;
    }

    std::cout << "Anticheat escuchando en puerto 42424..." << std::endl;

    uint8_t buffer[1024];
    std::string senderIP;
    int senderPort;

    while (true)
    {
        int bytes = server.receive(buffer, sizeof(buffer), senderIP, senderPort);
        if (bytes > 0) std::cout << "Recibidos " << bytes << " bytes desde " << senderIP << std::endl;
    }

    return 0;
}