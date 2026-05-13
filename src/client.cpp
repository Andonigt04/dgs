#include "include/dgs/client.h"

#include <httplib.h>
#include <iostream>
#include <cstring>
#include <sys/socket.h>

namespace DGS
{
    bool Client::connect(const std::string& headHost, int headPort, const std::string& username, const std::string& password, const std::string& apiHost,  int apiPort)
    {
        httplib::Client api(apiHost + ":" + std::to_string(apiPort));
        std::string body = R"({"username":")" + username + R"(","password":")" + password + R"("})";
        auto res = api.Post("/login", body, "application/json");

        if (!res || res->status != 200)
        {
            std::cerr << "[Client] Login fallido" << std::endl;
            return false;
        }
        std::cout << "[Client] Login OK" << std::endl;

        if (!m_tcp.connect(headHost, headPort))
        {
            std::cerr << "[Client] Error conectando HeadServer" << std::endl;
            return false;
        }

        struct timeval tv { 3, 0 };
        setsockopt(m_tcp.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        if (!queryZone(0, 0, 0, 0)) return false;

        m_running = true;
        m_recvThread = std::thread(&Client::recvLoop, this);

        return true;
    }

    void Client::disconnect()
    {
        m_running = false;
        if (m_recvThread.joinable())
            m_recvThread.join();
    }

    bool Client::queryZone(uint32_t uuid, int32_t chunkX, int32_t chunkY, int32_t chunkZ)
    {
        ZoneQuery query{};
        query.uuid   = uuid;
        query.chunkX = chunkX;
        query.chunkY = chunkY;
        query.chunkZ = chunkZ;

        Packet qp;
        qp.pack(query);
        m_tcp.send(m_tcp.getSocketFD(), qp.getRawData(), qp.getSize());

        uint8_t buf[256];
        int bytes = m_tcp.receive(m_tcp.getSocketFD(), buf, sizeof(buf));
        if (bytes <= 0)
        {
            std::cerr << "[Client] No se recibio ZoneResponse" << std::endl;
            return false;
        }

        Packet rp;
        rp.setBuffer(buf, bytes);
        ZoneResponse zone = rp.unpackZoneResponse();

        std::strncpy(&m_zoneAddr[0], zone.addr, 16);
        m_zoneAddr = zone.addr;
        m_zonePort = zone.port;

        std::cout << "[Client] ZoneNode: " << m_zoneAddr << ":" << m_zonePort << std::endl;
        return true;
    }

    void Client::sendEntityUDP(const EntityTransfer& e)
    {
        const uint8_t* raw = reinterpret_cast<const uint8_t*>(&e);
        m_udp.send(m_zoneAddr, m_zonePort, raw, sizeof(EntityTransfer));
    }

    void Client::sendTransform(uint32_t uuid, int32_t chunkX, int32_t chunkY, int32_t chunkZ, const float pos[3], const float rot[4])
    {
        if (chunkX != m_lastChunkX || chunkY != m_lastChunkY || chunkZ != m_lastChunkZ)
        {
            queryZone(uuid, chunkX, chunkY, chunkZ);
            m_lastChunkX = chunkX;
            m_lastChunkY = chunkY;
            m_lastChunkZ = chunkZ;
        }

        EntityTransfer e{};
        e.uuid   = uuid;
        e.type   = ENT_PLAYER;
        e.chunkX = chunkX;
        e.chunkY = chunkY;
        e.chunkZ = chunkZ;
        e.pos[0] = pos[0];
        e.pos[1] = pos[1];
        e.pos[2] = pos[2];
        
        // Quaternion yaw → uint16 angle
        float yaw  = std::atan2f(2.0f * (rot[3] * rot[1]), 1.0f - 2.0f * (rot[1] * rot[1]));
        e.angle    = static_cast<uint16_t>((yaw + 3.14159265f) * (32767.5f / 3.14159265f));

        sendEntityUDP(e);
    }

    void Client::sendStats(uint32_t uuid, const Stats& stats)
    {
        EntityTransfer e{};
        e.uuid   = uuid;
        e.type   = ENT_PLAYER;
        e.chunkX = m_lastChunkX;
        e.chunkY = m_lastChunkY;
        e.chunkZ = m_lastChunkZ;
        e.stats  = stats;
        e.state  = STATE_PHYSICS_OWNED;

        sendEntityUDP(e);
    }

    void Client::sendInventory(uint32_t uuid, const uint8_t* data, uint16_t size)
    {
        EntityTransfer e{};
        e.uuid     = uuid;
        e.type     = ENT_PLAYER;
        e.chunkX   = m_lastChunkX;
        e.chunkY   = m_lastChunkY;
        e.chunkZ   = m_lastChunkZ;
        e.dataSize = size;
        std::memcpy(e.data, data, std::min<uint16_t>(size, sizeof(e.data)));

        sendEntityUDP(e);
    }

    std::vector<EntityTransfer> Client::pollEntities()
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        return std::move(m_incomingEntities);
    }

    std::vector<GhostDelta> Client::pollGhosts()
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        return std::move(m_incomingGhosts);
    }

    void Client::recvLoop()
    {
        uint8_t buf[8192];

        while (m_running)
        {
            int bytes = m_tcp.receive(m_tcp.getSocketFD(), buf, sizeof(buf));
            if (bytes <= 0) continue;

            Packet p;
            p.setBuffer(buf, bytes);

            std::lock_guard<std::mutex> lk(m_mtx);

            switch (p.getType())
            {
                case PKT_ENTITY_TRANSFER:
                    m_incomingEntities.push_back(p.unpackEntityTransfer());
                    break;
                case PKT_GHOST_DELTA:
                    m_incomingGhosts.push_back(p.unpackGhostDelta());
                    break;
                default: break;
            }
        }
    }
}
