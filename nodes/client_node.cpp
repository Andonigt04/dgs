#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/types.h"

#include <httplib.h>
#include <iostream>
#include <cstring>

int main()
{
    // 1. Login via API HTTP
    httplib::Client api("http://127.0.0.1:8080");

    std::string body = R"({"username":"player1","password":"secret"})";
    auto res = api.Post("/login", body, "application/json");

    if (!res || res->status != 200)
    {
        std::cerr << "[Client] Login fallido" << std::endl;
        return 1;
    }

    std::cout << "[Client] Login OK. Token: " << res->body << std::endl;

    // 2. Preguntar al HeadServer qué ZoneNode corresponde a la posición inicial
    DGS::TCPSocket headSocket;
    if (!headSocket.connect("127.0.0.1", 42424))
    {
        std::cerr << "[Client] Error conectando HeadServer" << std::endl;
        return 1;
    }

    DGS::ZoneQuery query{};
    query.uuid   = 1001;
    query.chunkX = 0;
    query.chunkY = 0;
    query.chunkZ = 0;

    DGS::Packet qPacket;
    qPacket.pack(query);
    headSocket.send(headSocket.getSocketFD(), qPacket.getRawData(), qPacket.getSize());

    uint8_t respBuf[256];
    int respBytes = headSocket.receive(headSocket.getSocketFD(), respBuf, sizeof(respBuf));
    if (respBytes <= 0)
    {
        std::cerr << "[Client] No se recibio ZoneResponse" << std::endl;
        return 1;
    }

    DGS::Packet respPacket;
    respPacket.setBuffer(respBuf, respBytes);
    DGS::ZoneResponse zone = respPacket.unpackZoneResponse();

    std::cout << "[Client] ZoneNode asignado: " << zone.addr << ":" << zone.port << std::endl;

    // 3. Conectar al ZoneNode por UDP
    DGS::UDPSocket udpSocket;

    DGS::EntityTransfer entity{};
    entity.uuid   = query.uuid;
    entity.type   = DGS::ENT_PLAYER;
    entity.chunkX = query.chunkX;
    entity.chunkY = query.chunkY;
    entity.chunkZ = query.chunkZ;
    entity.pos[0] = 0.0f;
    entity.pos[1] = 0.0f;
    entity.pos[2] = 0.0f;
    entity.stats.speed[0] = 0.05f;

    const uint8_t* raw = reinterpret_cast<const uint8_t*>(&entity);
    udpSocket.send(zone.addr, zone.port, raw, sizeof(entity));

    std::cout << "[Client] Entidad enviada al ZoneNode por UDP" << std::endl;

    return 0;
}
