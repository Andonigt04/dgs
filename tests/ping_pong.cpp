#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include <iostream>
#include <chrono>
#include <vector>

int main() {
    DGS::TCPSocket test_node;
    if (!test_node.connect("127.0.0.1", 42424)) return -1;

    DGS::ServerMetrics metrics{};
    metrics.node.chunkXMin = 100; metrics.node.chunkXMax = 200;
    metrics.node.chunkYMin = 0;   metrics.node.chunkYMax = 100;

    DGS::Packet pReg;
    pReg.pack(metrics);
    test_node.send(test_node.getSocketFD(), pReg.getRawData(), pReg.getSize());
    std::cout << "[Test] Registrado como zona chunks 100-200. Esperando eco..." << std::endl;

    DGS::EntityTransfer e{};
    e.uuid   = 777;
    e.chunkX = 150;
    e.chunkY = 50;

    DGS::Packet pSend;
    pSend.pack(e);

    auto start = std::chrono::high_resolution_clock::now();
    test_node.send(test_node.getSocketFD(), pSend.getRawData(), pSend.getSize());

    uint8_t buffer[8192];
    int bytes = test_node.receive(test_node.getSocketFD(), buffer, 8192);

    if (bytes > 0) {
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<float, std::milli> rtt = end - start;

        DGS::Packet pRecv;
        pRecv.setBuffer(buffer, bytes);
        auto eEcho = pRecv.unpackEntityTransfer();

        std::cout << "[Test] ¡Eco recibido! Entidad ID: " << eEcho.uuid << std::endl;
        std::cout << "[Test] Latencia Round-Trip (RTT): " << rtt.count() << " ms" << std::endl;
    }

    return 0;
}
