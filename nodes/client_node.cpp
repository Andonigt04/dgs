#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/types.h"

#include <httplib.h>
#include <iostream>
#include <cstring>

int main()
{
    const char* apiHost  = std::getenv("API_HOST")         ? std::getenv("API_HOST")         : "api";
    int         apiPort  = std::atoi(std::getenv("API_PORT") ? std::getenv("API_PORT") : "8080");
    const char* headHost = std::getenv("HEAD_SERVER_HOST")  ? std::getenv("HEAD_SERVER_HOST") : "head-server";
    int         headPort = std::atoi(std::getenv("HEAD_SERVER_PORT") ? std::getenv("HEAD_SERVER_PORT") : "42424");

    std::string scheme  = std::getenv("HTTPS_ENABLE") ? "https://" : "http://";
    std::string apiAddr = scheme + apiHost + ":" + std::to_string(apiPort);
    httplib::Client api(apiAddr);

    std::string body = R"({"username":"player1","password":"secret"})";
    auto res = api.Post("/login", body, "application/json");

    if (!res || res->status != 200)
    {
        std::cerr << "[Client] Login fallido" << std::endl;
        return 1;
    }

    std::cout << "[Client] Login OK. Token: " << res->body << std::endl;

    DGS::TCPSocket headSocket;
    if (!headSocket.connect(headHost, headPort))
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
