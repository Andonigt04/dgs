#ifndef DGS_CLIENT_H
#define DGS_CLIENT_H

#include "include/dgs/types.h"
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace DGS
{
    class Client
    {
    public:
        ~Client() { disconnect(); }

        bool connect(const std::string& headHost, int headPort, const std::string& username, const std::string& password, const std::string& apiHost,  int apiPort);

        void disconnect();

        // Automatic re-query to the HeadServer when the chunk changes
        void sendTransform(uint32_t uuid,
                           int32_t chunkX, int32_t chunkY, int32_t chunkZ,
                           const float pos[3], const float rot[4]);

        void sendStats(uint32_t uuid, const Stats& stats);

        // Opaque payload — the engine serialises its inventory here
        void sendInventory(uint32_t uuid, const uint8_t* data, uint16_t size);

        void sendChat(uint32_t uuid, const std::string& username, const std::string& text);

        std::vector<EntityTransfer> pollEntities();
        std::vector<GhostDelta>     pollGhosts();
        std::vector<ChatMessage>    pollChats();

        bool isConnected() const { return m_running.load(); }

    private:
        TCPSocket m_tcp;  // → HeadServer
        UDPSocket m_udp;  // → ZoneServer

        std::string m_zoneAddr;
        int         m_zonePort = 0;

        int32_t m_lastChunkX = INT32_MIN;
        int32_t m_lastChunkY = INT32_MIN;
        int32_t m_lastChunkZ = INT32_MIN;

        std::thread       m_recvThread;
        std::mutex        m_mtx;
        std::atomic<bool> m_running{ false };

        std::vector<EntityTransfer> m_incomingEntities;
        std::vector<GhostDelta>     m_incomingGhosts;
        std::vector<ChatMessage>    m_incomingChats;

        // ⚠️ ONE READER PER SOCKET. `queryZone` used to read `m_tcp` directly while `recvLoop` was
        // reading the very same descriptor from another thread. Two consequences, and the second is
        // worse than the first:
        //   · the answer was routinely swallowed by `recvLoop` (which discards PKT_ZONE_RESPONSE in its
        //     `default:`), so the query timed out — measured: 6 of 9 runs;
        //   · and because `receive()` reads a 4-byte length prefix and then the payload, the two threads
        //     could split a single message between them and desynchronise the stream for good.
        // Now `recvLoop` owns the descriptor and hands the response over through this pair.
        std::condition_variable m_zoneCv;
        bool                    m_zoneAnswered = false;
        ZoneResponse            m_zoneResp{};

        bool queryZone(uint32_t uuid, int32_t chunkX, int32_t chunkY, int32_t chunkZ);
        void applyZone(const ZoneResponse& z);
        void recvLoop();
        void sendEntityUDP(const EntityTransfer& e);
    };
}

#endif // DGS_CLIENT_H
