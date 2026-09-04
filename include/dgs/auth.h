#ifndef DGS_AUTH_H
#define DGS_AUTH_H

// ─────────────────────────────────────────────────────────────────────────────────────────────────
// Who is allowed to be a NODE.
//
// Nothing between the nodes was authenticated. Anyone who could reach the head's TCP port could send a
// `PKT_METRICS` and be registered as a zone — and the head then ROUTES ENTITIES TO IT: reassignments,
// entity state, region blobs. The same for the validator (send verdicts), for persistence (write to
// the database, and now read it back) and for the social node (issue bans). Not one of those ports
// asked a single question.
//
// WHAT THIS IS. A connector proves it holds the cluster secret by sending, immediately after
// connecting, a `PKT_AUTH` carrying a random nonce, a timestamp and
// `HMAC-SHA256(secret, nonce || timestamp)`. The listener checks the MAC, that the timestamp is inside
// a window, and that the nonce has not been seen before. The secret itself never travels.
//
// WHAT THIS IS NOT, and it matters more than what it is:
//   · it is NOT TLS. The rest of the connection is in clear: anyone on the path still reads every
//     packet and can modify them. This decides WHO MAY CONNECT, not what is confidential.
//   · it authenticates the CONNECTION, not each packet. Someone who can inject into an established
//     TCP stream is not stopped by it.
//   · the replay window is bounded by the timestamp and the seen-nonce cache, not by a challenge, so
//     a captured packet is useless after the window and useless twice inside it — but a full
//     challenge-response would be stronger.
// The real answer is mTLS or a private network for the control plane. This is the floor.
//
// ⚠️ UNSET MEANS DISABLED, and that is a deliberate, uncomfortable choice. Fail-closed is right for
// the observer feed (an optional debug stream: refusing costs nothing) and wrong here, where refusing
// means no cluster starts at all — a default that bricks every existing deployment on upgrade is an
// outage, not a security improvement. So `DGS_CLUSTER_SECRET` unset leaves the ports open and every
// node SAYS SO, loudly, on the line right after it starts listening.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/packet.h"
#include "include/dgs/network.h"

#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/crypto.h>

#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <iostream>
#include <set>
#include <string>

namespace DGS
{
    static constexpr size_t AUTH_NONCE_LEN = 16;
    static constexpr size_t AUTH_MAC_LEN   = 32;   // SHA-256
    static constexpr uint64_t AUTH_WINDOW_MS = 30000;

    inline std::string clusterSecret()
    {
        const char* s = std::getenv("DGS_CLUSTER_SECRET");
        return s ? std::string(s) : std::string();
    }

    inline bool authEnabled() { return !clusterSecret().empty(); }

    inline uint64_t authNowMs()
    {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    }

    inline void authMac(const std::string& secret, const uint8_t* nonce, uint64_t ts, uint8_t out[AUTH_MAC_LEN])
    {
        uint8_t msg[AUTH_NONCE_LEN + sizeof(uint64_t)];
        std::memcpy(msg, nonce, AUTH_NONCE_LEN);
        std::memcpy(msg + AUTH_NONCE_LEN, &ts, sizeof(ts));
        unsigned int len = 0;
        HMAC(EVP_sha256(),
             secret.data(), (int)secret.size(),
             msg, sizeof(msg), out, &len);
    }

    /// Announces this node to a peer it has just connected to. A no-op when no secret is configured.
    /// @return whether the write succeeded (true as well when auth is off: nothing to write).
    inline bool sendAuth(TCPSocket& s)
    {
        const std::string secret = clusterSecret();
        if (secret.empty()) return true;

        uint8_t nonce[AUTH_NONCE_LEN];
        if (RAND_bytes(nonce, (int)sizeof(nonce)) != 1) return false;
        const uint64_t ts = authNowMs();
        uint8_t mac[AUTH_MAC_LEN];
        authMac(secret, nonce, ts, mac);

        Packet p;
        p.pack(PKT_AUTH);
        p.writeRaw(nonce, sizeof(nonce));
        p.write<uint64_t>(ts);
        p.writeRaw(mac, sizeof(mac));
        return s.send(s.getSocketFD(), p.getRawData(), p.getSize());
    }

    /// The listener's side: which descriptors have proved they hold the secret.
    ///
    /// A node that is not enforcing (no secret configured) answers `allows()` with true for everyone,
    /// so a cluster without a secret behaves exactly as it did before — visibly, because `announce()`
    /// says which of the two it is.
    class AuthGate
    {
    public:
        explicit AuthGate(const char* who) : m_who(who), m_secret(clusterSecret()) {}

        bool enabled() const { return !m_secret.empty(); }

        void announce() const
        {
            if (enabled())
                std::cout << "[" << m_who << "] node authentication REQUIRED (DGS_CLUSTER_SECRET)" << std::endl;
            else
                std::cout << "[" << m_who << "] ⚠ node authentication DISABLED: anyone who can reach "
                             "this port can act as a node (set DGS_CLUSTER_SECRET)" << std::endl;
        }

        /// Feeds one packet. @return true if it WAS an auth packet (and must not be processed further).
        bool consume(int fd, const Packet& p)
        {
            if (p.getType() != PKT_AUTH) return false;
            if (!enabled()) return true;          // ignored, but still not a payload packet

            Packet copy = p;                       // `read` moves a cursor; do not disturb the caller's
            uint8_t nonce[AUTH_NONCE_LEN], mac[AUTH_MAC_LEN];
            uint64_t ts = 0;
            try {
                copy.unpackPacketType();
                copy.readRaw(nonce, sizeof(nonce));
                ts = copy.read<uint64_t>();
                copy.readRaw(mac, sizeof(mac));
            } catch (const std::exception&) {
                std::cerr << "[" << m_who << "] malformed auth from fd=" << fd << std::endl;
                return true;
            }

            const uint64_t now = authNowMs();
            const uint64_t age = now > ts ? now - ts : ts - now;
            if (age > AUTH_WINDOW_MS)
            {
                std::cerr << "[" << m_who << "] auth REJECTED (stale by " << age << " ms) fd=" << fd << std::endl;
                return true;
            }

            uint8_t expect[AUTH_MAC_LEN];
            authMac(m_secret, nonce, ts, expect);
            if (CRYPTO_memcmp(expect, mac, AUTH_MAC_LEN) != 0)
            {
                std::cerr << "[" << m_who << "] auth REJECTED (bad MAC) fd=" << fd << std::endl;
                return true;
            }

            // Replay: the same capture must not work twice inside the window.
            const std::string key((const char*)nonce, AUTH_NONCE_LEN);
            if (m_seen.count(key))
            {
                std::cerr << "[" << m_who << "] auth REJECTED (nonce replayed) fd=" << fd << std::endl;
                return true;
            }
            m_seen.insert(key);
            m_order.push_back(key);
            while (m_order.size() > 4096) { m_seen.erase(m_order.front()); m_order.pop_front(); }

            m_authed.insert(fd);
            std::cout << "[" << m_who << "] node authenticated fd=" << fd << std::endl;
            return true;
        }

        bool allows(int fd) const { return !enabled() || m_authed.count(fd) != 0; }
        void forget(int fd) { m_authed.erase(fd); }

        /// Says no, once per rejection, so a misconfigured cluster is diagnosable instead of silent.
        void refuse(int fd, int packetType) const
        {
            std::cerr << "[" << m_who << "] REFUSED packet type " << packetType
                      << " from unauthenticated fd=" << fd << std::endl;
        }

    private:
        const char*             m_who;
        std::string             m_secret;
        std::set<int>           m_authed;
        std::set<std::string>   m_seen;
        std::deque<std::string> m_order;
    };
}

#endif // DGS_AUTH_H
