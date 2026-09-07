#include "include/dgs/network.h"
#include "include/dgs/types.h"

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/rand.h>
#include <array>
#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <cstdlib>
#include <vector>
#include <cstring>

#include <sys/socket.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <unistd.h>
#include <cerrno>
#include <iostream>

namespace DGS
{

#ifdef HARUKA_IPV6
    using SocketAddrType = sockaddr_in6;
    constexpr int AF_FAMILY = AF_INET6;
#else
    using SocketAddrType = sockaddr_in;
    constexpr int AF_FAMILY = AF_INET;
#endif

    static SocketAddrType newAddress(int port)
    {
        SocketAddrType addr{};
#ifdef HARUKA_IPV6
        addr.sin6_family = AF_FAMILY;
        addr.sin6_port   = htons(port);
        addr.sin6_addr   = in6addr_any; // Linux constant for "any IP" over IPv6
#else
        addr.sin_family  = AF_FAMILY;
        addr.sin_port    = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
#endif
        return addr;
    }

    UDPSocket::UDPSocket()
    {
        socketFD = socket(AF_FAMILY, SOCK_DGRAM, 0);
        if (socketFD < 0) std::cerr << "Failed to create the socket" << std::endl;
    }

    UDPSocket::UDPSocket(UDPSocket&& other) noexcept : socketFD(other.socketFD)
    {
        other.socketFD = -1;
    }

    UDPSocket& UDPSocket::operator=(UDPSocket&& other) noexcept
    {
        if (this != &other)
        {
            if (socketFD >= 0) close(socketFD);
            socketFD = other.socketFD;
            other.socketFD = -1;
        }
        return *this;
    }

    UDPSocket::~UDPSocket()
    {
        if (socketFD >= 0) close(socketFD);
    }

    bool UDPSocket::bind(int port)
    {
        int opt = 1;
        if (setsockopt(socketFD, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) std::perror("setsockopt SO_REUSEADDR failed");
        if (setsockopt(socketFD, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) std::perror("setsockopt SO_REUSEPORT failed");
        
        SocketAddrType addr = newAddress(port);

        if (::bind(socketFD, (struct sockaddr*)&addr, sizeof(addr)) < 0)
        {
            perror("bind failed");
            return false;
        }

        return true;
    }

    // ── THE GAME PLANE, ENCRYPTED ───────────────────────────────────────────────────────────────
    // TLS covers the TCP control plane. It leaves the busiest and most personal traffic in the system
    // in clear: **every player's position, twenty times a second, and the zone's broadcast of everyone
    // else's**. Anyone on the path reads where every player in a zone is — the same wallhack feed the
    // observer token was added to protect — and, since nothing was authenticated either, can forge a
    // position for somebody else's uuid.
    //
    // WHY NOT DTLS. A DTLS session is per peer, and a zone's broadcast is one payload to N peers: with
    // DTLS the zone would hold N sessions and encrypt the same snapshot N times, which throws away the
    // serialise-once-per-tick property that interest management and `dataSize` were measured against.
    // What fits this shape is a group cipher: one key, one encryption per frame, N sends.
    //
    // So each datagram is sealed with AES-256-GCM: `nonce(12) || ciphertext || tag(16)`, 28 bytes of
    // overhead. The tag is what makes forgery fail, not just eavesdropping.
    //
    // WHAT THIS IS NOT:
    //   · the GROUP key alone would let any client decrypt another client's uplink. That is what the
    //     per-session keys below are for, and the login now issues them.
    //   · REPLAY is not prevented here. A captured datagram can be re-sent within its lifetime. The
    //     anti-cheat layer above already deals with that for the traffic that matters: the validator's
    //     minimum-dt discard exists precisely to reject duplicated and reordered samples, and S1 throws
    //     out anything implausible. Saying it out loud because a reader would otherwise assume GCM's
    //     nonce gives replay protection, which it does not.
    //   · a client that was issued nothing falls back to `DGS_UDP_KEY`, one value for the whole world.
    // ── PER-SESSION KEYS ────────────────────────────────────────────────────────────────────────
    // The first version used ONE key for the whole world, and that has a real limit: every client
    // holds it, so a client that can capture another client's uplink can read it. For the BROADCAST
    // that changes nothing — everyone receives it anyway, and one key is what lets the zone seal each
    // frame once instead of once per recipient — but what a player says about THEMSELVES should not
    // be readable by the player next to them.
    //
    // So a datagram now carries its session in the clear: `session(4) || nonce(12) || ct || tag(16)`,
    // and the session id is bound in as ADDITIONAL AUTHENTICATED DATA, so it cannot be swapped for
    // another without the tag failing.
    //   session 0  → the GROUP key (`DGS_UDP_KEY`): the zone's broadcast, and any client that has not
    //                been issued a session. Every client holds this one; it has to.
    //   session N  → HMAC-SHA256(`DGS_UDP_MASTER`, N). The SERVERS hold the master and derive; a
    //                client is issued only its own key and cannot derive anybody else's.
    //
    // The LOGIN issues both now, and `Client::applyLoginKeys` adopts them through `setUdpSessionKey`,
    // so a player needs no key in their environment: they are handed the group key to read the world
    // and their own to write to it. `client_world_e2e` runs its whole exchange that way and its
    // counter-proof forgets the keys mid-run to show the client goes deaf without them.
    static const size_t UDP_SESSION = 4, UDP_NONCE = 12, UDP_TAG = 16;
    static const size_t UDP_OVERHEAD = UDP_SESSION + UDP_NONCE + UDP_TAG;

    // ── Keys handed out at runtime ──────────────────────────────────────────────────────────────
    // A node gets its keys from the environment, which is right for something an operator starts. A
    // PLAYER cannot: their session key exists precisely so the player beside them cannot read it, and a
    // value baked into the environment before the process starts is shared with everyone who can read
    // the unit file. `setUdpSessionKey` is where a login response lands.
    //
    // Consulted BEFORE the environment. The atomic is not decoration: a zone never sets one of these,
    // and this is on the path of every datagram it opens, so the common case must not touch the mutex.
    static std::atomic<bool>            g_runtimeKeysSet{false};
    static std::mutex                   g_runtimeKeyMtx;
    static std::map<uint32_t, std::array<uint8_t, 32>> g_runtimeKeys;

    /// 64 hex characters -> 32 bytes. @return false on anything else, including a passphrase: hashing
    /// one end and not the other produces two different keys from the same string, and the only symptom
    /// is that nothing arrives.
    static bool hexKey(const std::string& hex, uint8_t out[32])
    {
        if (hex.size() != 64) return false;
        auto nib = [](char c) -> int {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            return -1;
        };
        for (int i = 0; i < 32; ++i)
        {
            const int hi = nib(hex[i * 2]), lo = nib(hex[i * 2 + 1]);
            if (hi < 0 || lo < 0) return false;
            out[i] = (uint8_t)((hi << 4) | lo);
        }
        return true;
    }

    bool setUdpSessionKey(uint32_t session, const std::string& keyHex)
    {
        if (keyHex.empty())
        {
            std::lock_guard<std::mutex> g(g_runtimeKeyMtx);
            g_runtimeKeys.erase(session);
            g_runtimeKeysSet.store(!g_runtimeKeys.empty(), std::memory_order_release);
            return true;
        }

        std::array<uint8_t, 32> k{};
        if (!hexKey(keyHex, k.data())) return false;

        std::lock_guard<std::mutex> g(g_runtimeKeyMtx);
        g_runtimeKeys[session] = k;
        g_runtimeKeysSet.store(true, std::memory_order_release);
        return true;
    }

    /// The runtime key for `session`, if one was handed to us.
    static bool runtimeKeyFor(uint32_t session, uint8_t out[32])
    {
        if (!g_runtimeKeysSet.load(std::memory_order_acquire)) return false;
        std::lock_guard<std::mutex> g(g_runtimeKeyMtx);
        auto it = g_runtimeKeys.find(session);
        if (it == g_runtimeKeys.end()) return false;
        std::memcpy(out, it->second.data(), 32);
        return true;
    }

    /// The non-zero session this process was issued, if any. 0 means "we only hold the group key".
    static uint32_t runtimeOutSession()
    {
        if (!g_runtimeKeysSet.load(std::memory_order_acquire)) return 0;
        std::lock_guard<std::mutex> g(g_runtimeKeyMtx);
        for (const auto& kv : g_runtimeKeys) if (kv.first != 0) return kv.first;
        return 0;
    }

    static bool keyFromPassphrase(const char* env, uint8_t out[32])
    {
        const char* k = std::getenv(env);
        if (!k || !*k) return false;
        SHA256((const unsigned char*)k, std::strlen(k), out);
        return true;
    }

    /// The key for `session`. 0 is the group key; anything else is derived from the master, or is this
    /// process's own issued session key.
    static bool udpKeyFor(uint32_t session, uint8_t out[32])
    {
        // What a login handed us wins over what the environment was started with.
        if (runtimeKeyFor(session, out)) return true;

        if (session == 0) return keyFromPassphrase("DGS_UDP_KEY", out);

        uint8_t master[32];
        if (keyFromPassphrase("DGS_UDP_MASTER", master))
        {
            unsigned int len = 0;
            HMAC(EVP_sha256(), master, 32,
                 (const unsigned char*)&session, sizeof(session), out, &len);
            return len == 32;
        }
        // A client holds only its own session key, not the master. It arrives as the 64 hex
        // characters an API would hand out — decoded, NOT hashed, or the two sides would compute
        // different keys from the same string and the only symptom would be silence.
        const char* mine = std::getenv("DGS_UDP_SESSION");
        if (mine && (uint32_t)std::strtoul(mine, nullptr, 10) == session)
        {
            const char* k = std::getenv("DGS_UDP_SESSION_KEY");
            if (!k) return false;
            const size_t len = std::strlen(k);
            if (len == 64)
            {
                auto nib = [](char c) -> int {
                    if (c >= '0' && c <= '9') return c - '0';
                    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                    return -1;
                };
                for (int i = 0; i < 32; ++i)
                {
                    const int hi = nib(k[i * 2]), lo = nib(k[i * 2 + 1]);
                    if (hi < 0 || lo < 0) return keyFromPassphrase("DGS_UDP_SESSION_KEY", out);
                    out[i] = (uint8_t)((hi << 4) | lo);
                }
                return true;
            }
            return keyFromPassphrase("DGS_UDP_SESSION_KEY", out);
        }
        return false;
    }

    /// Which session this process seals with. 0 = the group key.
    static uint32_t udpOutSession()
    {
        if (const uint32_t rt = runtimeOutSession()) return rt;

        const char* s = std::getenv("DGS_UDP_SESSION");
        if (!s || !*s) return 0;
        const uint32_t id = (uint32_t)std::strtoul(s, nullptr, 10);
        uint8_t k[32];
        return (id != 0 && udpKeyFor(id, k)) ? id : 0;
    }

    static bool udpEnabled()
    {
        if (g_runtimeKeysSet.load(std::memory_order_acquire)) return true;
        uint8_t k[32];
        return keyFromPassphrase("DGS_UDP_KEY", k) || keyFromPassphrase("DGS_UDP_MASTER", k) ||
               keyFromPassphrase("DGS_UDP_SESSION_KEY", k);
    }

    /// session || nonce || ciphertext || tag. @return the sealed length, or 0 on failure.
    static size_t udpSeal(const uint8_t* in, size_t n, uint8_t* out, size_t outCap)
    {
        const uint32_t session = udpOutSession();
        uint8_t key[32];
        if (!udpKeyFor(session, key)) return 0;
        if (outCap < n + UDP_OVERHEAD) return 0;

        std::memcpy(out, &session, UDP_SESSION);
        uint8_t* nonce = out + UDP_SESSION;
        if (RAND_bytes(nonce, (int)UDP_NONCE) != 1) return 0;

        EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
        if (!ctx) return 0;
        size_t sealed = 0;
        int len = 0;
        bool ok = EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) == 1 &&
                  EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, (int)UDP_NONCE, nullptr) == 1 &&
                  EVP_EncryptInit_ex(ctx, nullptr, nullptr, key, nonce) == 1 &&
                  // The session id travels in the clear, so it is bound in as AAD: swapping it for
                  // somebody else's makes the tag fail instead of silently re-labelling the datagram.
                  EVP_EncryptUpdate(ctx, nullptr, &len, out, (int)UDP_SESSION) == 1 &&
                  EVP_EncryptUpdate(ctx, out + UDP_SESSION + UDP_NONCE, &len, in, (int)n) == 1;
        if (ok)
        {
            sealed = UDP_SESSION + UDP_NONCE + (size_t)len;
            int fin = 0;
            ok = EVP_EncryptFinal_ex(ctx, out + sealed, &fin) == 1;
            sealed += (size_t)fin;
            if (ok) ok = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, (int)UDP_TAG,
                                             out + sealed) == 1;
            sealed += UDP_TAG;
        }
        EVP_CIPHER_CTX_free(ctx);
        return ok ? sealed : 0;
    }

    /// @return the plaintext length, or 0 when the datagram is not ours (wrong key, or tampered with).
    static size_t udpOpen(uint8_t* buf, size_t n)
    {
        if (n < UDP_OVERHEAD) return 0;

        uint32_t session = 0;
        std::memcpy(&session, buf, UDP_SESSION);
        uint8_t key[32];
        if (!udpKeyFor(session, key)) return 0;   // not a session this process can read

        const uint8_t* nonce = buf + UDP_SESSION;
        const size_t ctLen = n - UDP_OVERHEAD;
        std::vector<uint8_t> plain(ctLen ? ctLen : 1);

        EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
        if (!ctx) return 0;
        int len = 0;
        bool ok = EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) == 1 &&
                  EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, (int)UDP_NONCE, nullptr) == 1 &&
                  EVP_DecryptInit_ex(ctx, nullptr, nullptr, key, nonce) == 1 &&
                  EVP_DecryptUpdate(ctx, nullptr, &len, buf, (int)UDP_SESSION) == 1 &&
                  EVP_DecryptUpdate(ctx, plain.data(), &len, buf + UDP_SESSION + UDP_NONCE,
                                    (int)ctLen) == 1 &&
                  EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, (int)UDP_TAG,
                                      (void*)(buf + UDP_SESSION + UDP_NONCE + ctLen)) == 1;
        int fin = 0;
        // ⚠️ THE RETURN OF `DecryptFinal` IS THE AUTHENTICATION. Ignoring it would leave a cipher that
        // hides the contents from a reader and accepts anything from a writer.
        if (ok) ok = EVP_DecryptFinal_ex(ctx, plain.data() + len, &fin) == 1;
        EVP_CIPHER_CTX_free(ctx);
        if (!ok) return 0;
        const size_t total = (size_t)len + (size_t)fin;
        std::memcpy(buf, plain.data(), total);
        return total;
    }

    bool UDPSocket::send(const std::string& address, int port, const uint8_t* data, size_t size)
    {
        SocketAddrType destAddr{};

#ifdef HARUKA_IPV6
        destAddr.sin6_family = AF_FAMILY;
        destAddr.sin6_port = htons(port);
        inet_pton(AF_FAMILY, address.c_str(), &destAddr.sin6_addr);
#else
        destAddr.sin_family = AF_FAMILY;
        destAddr.sin_port = htons(port);
        inet_pton(AF_FAMILY, address.c_str(), &destAddr.sin_addr);
#endif

        // Sealed when a key is configured. The zone encrypts each broadcast frame ONCE and sends the
        // same bytes to everybody, which is what keeps the N-fan-out affordable.
        uint8_t sealed[DGS::MAX_PACKET_SIZE];
        if (udpEnabled())
        {
            const size_t n = udpSeal(data, size, sealed, sizeof(sealed));
            if (n == 0) { std::cerr << "[UDPSocket] could not seal a datagram" << std::endl; return false; }
            data = sealed;
            size = n;
        }

        ssize_t sent = sendto(socketFD, data, size, 0, (struct sockaddr*)&destAddr, sizeof(destAddr));
        
        return sent == (ssize_t)size;
    }

    bool UDPSocket::sendRaw(const std::string& address, int port, const uint8_t* data, size_t size)
    {
        SocketAddrType destAddr{};
#ifdef HARUKA_IPV6
        destAddr.sin6_family = AF_FAMILY;
        destAddr.sin6_port = htons(port);
        inet_pton(AF_FAMILY, address.c_str(), &destAddr.sin6_addr);
#else
        destAddr.sin_family = AF_FAMILY;
        destAddr.sin_port = htons(port);
        inet_pton(AF_FAMILY, address.c_str(), &destAddr.sin_addr);
#endif
        const ssize_t sent = sendto(socketFD, data, size, 0,
                                    (struct sockaddr*)&destAddr, sizeof(destAddr));
        return sent == (ssize_t)size;
    }

    bool udpCryptoEnabled() { return udpEnabled(); }

    size_t udpWireSize(size_t size) { return udpEnabled() ? size + UDP_OVERHEAD : size; }

    bool sealForUdp(const uint8_t* data, size_t size, std::vector<uint8_t>& out)
    {
        if (!udpEnabled()) { out.assign(data, data + size); return false; }
        out.resize(size + UDP_OVERHEAD);
        const size_t n = udpSeal(data, size, out.data(), out.size());
        if (n == 0) { out.assign(data, data + size); return false; }
        out.resize(n);
        return true;
    }

    int UDPSocket::receive(uint8_t* buffer, size_t size, std::string& outAddress, int& outPort)
    {
        SocketAddrType fromAddr{};
        socklen_t fromLen = sizeof(fromAddr);

        int bytesRecived = recvfrom(socketFD, buffer, size, 0, (struct sockaddr*)&fromAddr, &fromLen);

        if (bytesRecived > 0 && udpEnabled())
        {
            // A datagram that does not authenticate is not ours: wrong key, or somebody editing bytes
            // on the path. It is dropped, and the caller sees "nothing arrived" rather than plaintext
            // that has been tampered with.
            const size_t plain = udpOpen(buffer, (size_t)bytesRecived);
            if (plain == 0) return -1;
            bytesRecived = (int)plain;
        }

        if (bytesRecived >= 0)
        {
#ifdef HARUKA_IPV6
            char ipStr[INET6_ADDRSTRLEN] = {0};
            inet_ntop(AF_INET6, &fromAddr.sin6_addr, ipStr, sizeof(ipStr));  // inet_ntoa is IPv4-only
            outAddress = ipStr;
            outPort = ntohs(fromAddr.sin6_port);
#else
            outAddress = inet_ntoa(fromAddr.sin_addr);
            outPort = ntohs(fromAddr.sin_port);
#endif
        }

        return bytesRecived;
    }
    
    // ── TLS ─────────────────────────────────────────────────────────────────────────────────────
    // Node authentication decided who may connect; this decides what anyone on the path can read or
    // change, which until now was everything. Off unless all three of DGS_TLS_CERT / DGS_TLS_KEY /
    // DGS_TLS_CA are set, and then it is MUTUAL: the listener demands a client certificate signed by
    // that CA and the connector verifies the server against the same one.
    static const char* tlsEnv(const char* n) { const char* v = std::getenv(n); return (v && *v) ? v : nullptr; }

    static bool tlsConfigured()
    {
        return tlsEnv("DGS_TLS_CERT") && tlsEnv("DGS_TLS_KEY") && tlsEnv("DGS_TLS_CA");
    }

    static void tlsInitOnce()
    {
        static bool done = false;
        if (done) return;
        done = true;
        SSL_library_init();
        SSL_load_error_strings();
    }

    static void tlsReport(const char* what)
    {
        unsigned long e = ERR_get_error();
        char buf[256] = {0};
        if (e) ERR_error_string_n(e, buf, sizeof(buf));
        std::cerr << "[TCPSocket] TLS " << what << " failed: " << (e ? buf : "no detail") << std::endl;
    }

    // ── One connection, two threads ─────────────────────────────────────────────────────────────
    // ⚠️ THIS WAS A LATENT UNDEFINED BEHAVIOUR AND IT WAS WRITTEN DOWN BEFORE IT WAS FIXED. `Client`
    // sends from the caller's thread (`sendChat`, the query inside `sendTransform`) and receives on
    // `recvLoop`, both over the same descriptor. On plain TCP that is fine — the kernel serialises two
    // `send`s and a concurrent `recv` touches nothing the writer touches. On TLS it is not: `SSL_read`
    // and `SSL_write` share ONE record layer, one sequence number and one error queue, so running them
    // at once corrupts the stream or the SSL object itself. Switching TLS on would not have produced a
    // message ordering bug, it would have produced undefined behaviour.
    //
    // Two locks, because they answer two different questions:
    //   · `io`    — at most one OpenSSL call on this `SSL` at a time. Held only around the call, NEVER
    //               across a wait: a writer parked on a full socket buffer must not stop the reader
    //               from draining the other direction, which is the classic way to deadlock TLS.
    //   · `tx`    — one whole FRAME (4-byte length + payload) at a time. `send` writes two pieces, so
    //               without this two writers interleave and the peer reads one message's length
    //               followed by another message's bytes. It also keeps `SSL_write`'s retry contract:
    //               after WANT_WRITE, OpenSSL demands the retry use the same buffer, which is only
    //               true while no other thread can write in between.
    // The map is guarded separately and only for the lookup, so a slow connection cannot block the
    // accept path. A plain link gets a `Conn` too (with `ssl == nullptr`) so that `tx` applies there
    // as well: the interleaving problem is not TLS-specific, it was simply never reachable before.
    struct Conn
    {
        SSL*       ssl = nullptr;   // nullptr on a plain link
        std::mutex io;
        std::mutex tx;
        // ⚠️ A GROWING BUFFER, NOT A FIXED ONE, and the difference is 65 MB. The first version of the
        // single-write fix gave every connection a `MAX_PACKET_SIZE + 4` array — 65 540 bytes each,
        // which on a head holding a thousand players is 65 MB of send buffers for frames that are
        // typically seventy bytes. It is only needed on the TLS path (`SSL_write` takes one buffer);
        // the plain path uses `writev` and copies nothing at all.
        std::vector<uint8_t> txBuf;
    };

    static void freeConn(void* p)
    {
        Conn* c = (Conn*)p;
        if (!c) return;
        if (c->ssl) SSL_free(c->ssl);
        delete c;
    }

    void* TCPSocket::linkFor(int fd, bool create) const
    {
        std::lock_guard<std::mutex> g(m_connMtx);
        auto it = m_conn.find(fd);
        if (it != m_conn.end()) return it->second;
        if (!create) return nullptr;
        Conn* c = new Conn();
        m_conn[fd] = c;
        return c;
    }

    /// After the handshake the descriptor stops being blocking, ON PURPOSE.
    ///
    /// A blocking `SSL_read` would hold `io` for as long as the peer stays quiet — which is for ever on
    /// an idle link — and the writer sharing that connection would never get in. Non-blocking plus an
    /// explicit `poll` outside the lock gives the caller the same blocking API with none of that.
    static void setNonBlocking(int fd)
    {
        const int f = ::fcntl(fd, F_GETFL, 0);
        if (f >= 0) ::fcntl(fd, F_SETFL, f | O_NONBLOCK);
    }

    /// ⚠️ THIS IS INSURANCE, NOT THE FIX, and the measurement says so plainly. The 80 ms a zone query
    /// used to cost was Nagle, but the cause was `send` writing the length prefix and the payload
    /// SEPARATELY — and coalescing them into one write is what removed it: on loopback, 82 ms → 41 ms
    /// with one end fixed → **0.07 ms** with both. Disabling this option and measuring again gave the
    /// same 0.07 ms, so it earns nothing on that path and is not credited with it.
    ///
    /// It stays for the path that still writes twice: a frame larger than the assembly buffer falls
    /// back to prefix-then-payload, and that is the pattern Nagle punishes. One setsockopt per
    /// connection against a 40 ms stall nobody would find twice.
    ///
    /// Right for THIS plane and wrong for a bulk one: these are small messages whose latency is the
    /// product. The UDP game plane never comes through here.
    static void setNoDelay(int fd)
    {
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }

    /// The deadline the CALLER configured, as a poll timeout.
    ///
    /// TLS descriptors are non-blocking now, so `SO_RCVTIMEO`/`SO_SNDTIMEO` no longer reach the kernel
    /// — and half this repository sets them and expects `receive` to give up after that long (the
    /// client's receive loop checks `m_running` between timeouts; without this it would never check it
    /// again). So the value is read back and honoured here instead. 0 means "no timeout" -> block.
    static int soTimeoutMs(int fd, int which)
    {
        timeval tv{};
        socklen_t len = sizeof(tv);
        if (::getsockopt(fd, SOL_SOCKET, which, &tv, &len) != 0) return -1;
        const long ms = (long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
        return ms > 0 ? (int)ms : -1;
    }

    static bool waitFd(int fd, short events, int timeoutMs)
    {
        pollfd p{ fd, events, 0 };
        return ::poll(&p, 1, timeoutMs) > 0;
    }

    /// A HANDSHAKE WITH A DEADLINE, for the same reason `connectWithDeadline` exists.
    ///
    /// ⚠️ TLS PUT BACK THE FREEZE THAT FUNCTION WAS WRITTEN TO REMOVE. `connect(host, port, timeoutMs)`
    /// promises to come back within `timeoutMs`; it bounded the TCP handshake and then called
    /// `SSL_connect` on a BLOCKING descriptor, which waits for a peer that may never answer. A service
    /// that accepts and then says nothing — an overloaded node, a hung arbiter, a port held by
    /// something that is not us — froze the caller for ever, and the caller is a node's tick.
    ///
    /// The listener had the worse half of it: `accept()` runs `SSL_accept` on the node's own loop, so
    /// ANY peer could connect, send nothing, and stop that node accepting anybody else. No credentials
    /// needed, one socket, no traffic. That is a denial of service reachable from the network, and the
    /// bound below is what makes it a 5-second nuisance instead.
    static int tlsHandshakeMs()
    {
        const char* v = std::getenv("DGS_TLS_HANDSHAKE_MS");
        const int ms = v ? std::atoi(v) : 0;
        return ms > 0 ? ms : 5000;
    }

    static bool tlsHandshake(SSL* ssl, int fd, bool server, int timeoutMs)
    {
        // Non-blocking BEFORE the handshake, and it stays that way afterwards — which is what the I/O
        // path wants anyway.
        setNonBlocking(fd);
        const auto deadline = std::chrono::steady_clock::now()
                            + std::chrono::milliseconds(timeoutMs > 0 ? timeoutMs : tlsHandshakeMs());
        for (;;)
        {
            const int r = server ? SSL_accept(ssl) : SSL_connect(ssl);
            if (r == 1) return true;

            const int err = SSL_get_error(ssl, r);
            short ev = 0;
            if      (err == SSL_ERROR_WANT_READ)  ev = POLLIN;
            else if (err == SSL_ERROR_WANT_WRITE) ev = POLLOUT;
            else return false;

            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) return false;
            const int left = (int)std::chrono::duration_cast<std::chrono::milliseconds>(
                                 deadline - now).count();
            if (!waitFd(fd, ev, left ? left : 1)) return false;
        }
    }

    /// Does this failed SSL call mean the CONNECTION IS OVER, as opposed to "nothing yet"?
    ///
    /// Three spellings of the same event. `close_notify` is what a polite peer sends and is the only
    /// one that looks like an orderly close; a node that is KILLED sends nothing and the socket simply
    /// ends, which OpenSSL 3 reports as `unexpected eof while reading` and OpenSSL 1 as a syscall
    /// error with an empty queue. Treating only the first as a close is what made a dead peer
    /// indistinguishable from a quiet one — the exact bug `receive` already documents for plain TCP.
    static bool tlsIsClosed(int err, unsigned long reason)
    {
        if (err == SSL_ERROR_ZERO_RETURN) return true;
#ifdef SSL_R_UNEXPECTED_EOF_WHILE_READING
        if (err == SSL_ERROR_SSL && ERR_GET_REASON(reason) == SSL_R_UNEXPECTED_EOF_WHILE_READING)
            return true;
#endif
        return err == SSL_ERROR_SYSCALL && reason == 0;
    }

    bool TCPSocket::tlsEnabled() const { return m_ctx != nullptr; }

    bool TCPSocket::ensureContext(bool server)
    {
        if (m_ctx) return true;
        if (!tlsConfigured()) return false;
        tlsInitOnce();

        SSL_CTX* ctx = SSL_CTX_new(server ? TLS_server_method() : TLS_client_method());
        if (!ctx) { tlsReport("context"); return false; }
        SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

        if (SSL_CTX_use_certificate_chain_file(ctx, tlsEnv("DGS_TLS_CERT")) != 1 ||
            SSL_CTX_use_PrivateKey_file(ctx, tlsEnv("DGS_TLS_KEY"), SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_load_verify_locations(ctx, tlsEnv("DGS_TLS_CA"), nullptr) != 1)
        {
            tlsReport("certificates");
            SSL_CTX_free(ctx);
            return false;
        }
        // MUTUAL. A server that encrypts but accepts any client has swapped one open door for a
        // private one; both ends present a certificate and both are checked against the same CA.
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
        m_ctx = ctx;
        return true;
    }

    void TCPSocket::dropTls(int fd)
    {
        void* c = nullptr;
        {
            std::lock_guard<std::mutex> g(m_connMtx);
            auto it = m_conn.find(fd);
            if (it == m_conn.end()) return;
            c = it->second;
            m_conn.erase(it);
        }
        // ⚠️ The caller must not be doing I/O on this descriptor from another thread. That is not a new
        // rule introduced by the locks: `closeClient` also calls `close(fd)`, and reading from a
        // descriptor another thread has just closed is a use-after-close with or without TLS.
        freeConn(c);
    }

    /// Is there APPLICATION data to read on this descriptor right now?
    ///
    /// ⚠️ THIS IS NOT THE SAME QUESTION AS `poll`, and the difference froze a whole cluster the first
    /// time TLS was switched on. Two ways for them to disagree, in opposite directions:
    ///   · OpenSSL can hold DECRYPTED bytes the kernel no longer has, so `poll` says "nothing" while a
    ///     message is already in hand;
    ///   · and the kernel can have bytes that are NOT application data — a TLS 1.3 server sends
    ///     `NewSessionTicket` right after the handshake — so `poll` says "readable" and the blocking
    ///     read that follows waits for four bytes that never come. Measured: the validator connected
    ///     to the head, polled, saw "readable", and hung there for ever with an empty log while the
    ///     zone waited on it. Nothing in either log said why.
    /// So this asks OpenSSL, which is the only layer that knows: buffered plaintext, or a non-blocking
    /// peek that processes tickets and reports only real data.
    bool TCPSocket::pending(int fd) const
    {
        Conn* c = (Conn*)linkFor(fd, /*create*/ false);
        if (!c || !c->ssl) return false;

        std::lock_guard<std::mutex> g(c->io);
        if (SSL_pending(c->ssl) > 0) return true;
        // The descriptor is already non-blocking (set right after the handshake), so the peek returns
        // immediately. It used to flip O_NONBLOCK on and off around this call — which was itself a race
        // as soon as a second thread touched the same descriptor: the other thread's read could land in
        // the window and come back EAGAIN, or the flag could be restored to a value it never had.
        char probe = 0;
        const int k = SSL_peek(c->ssl, &probe, 1);
        if (k > 0) return true;

        // ⚠️ A CLOSED CONNECTION IS ALSO SOMETHING TO READ, and saying otherwise hid a hang-up.
        // The readiness gates ask `poll(fd) && pending(fd)`; when the peer died, `poll` reported the
        // descriptor readable (that is what EOF looks like) and this said "no application data", so
        // the node NEVER CALLED `receive` and never got its 0. Measured with TLS on: `zone_node`
        // logged "Connection with HeadServer lost" — the failed-`send` path — instead of "HeadServer
        // closed the connection", so the orderly-close branch was unreachable again, this time for a
        // different reason than the one `receive` records.
        // The question this answers is "will a read make progress", and the end of the connection is
        // progress: the caller reads, gets 0, and tears the link down.
        const int err = SSL_get_error(c->ssl, k);
        return tlsIsClosed(err, (err == SSL_ERROR_SSL || err == SSL_ERROR_SYSCALL) ? ERR_peek_error() : 0);
    }

    TCPSocket::TCPSocket()
    {
        socketFD = socket(AF_FAMILY, SOCK_STREAM, 0);
        if (socketFD < 0) std::cerr << "Failed to create the socket" << std::endl;
    }

    TCPSocket::TCPSocket(TCPSocket&& other) noexcept : socketFD(other.socketFD)
    {
        other.socketFD = -1;
        m_ctx = other.m_ctx; other.m_ctx = nullptr;
        // The mutexes are NOT moved (they cannot be): the moved-to object gets fresh ones. A move is
        // only ever done on a socket nobody else is using — `zone_node` reassigns its head link while
        // reconnecting — so there is nothing to hand over.
        m_conn = std::move(other.m_conn); other.m_conn.clear();
    }

    TCPSocket& TCPSocket::operator=(TCPSocket&& other) noexcept
    {
        if (this != &other)
        {
            for (auto& kv : m_conn) freeConn(kv.second);
            m_conn.clear();
            if (m_ctx) SSL_CTX_free((SSL_CTX*)m_ctx);
            if (socketFD >= 0) close(socketFD);
            socketFD = other.socketFD;
            other.socketFD = -1;
            m_ctx = other.m_ctx; other.m_ctx = nullptr;
            m_conn = std::move(other.m_conn); other.m_conn.clear();
        }
        return *this;
    }

    TCPSocket::~TCPSocket()
    {
        for (auto& kv : m_conn) freeConn(kv.second);
        m_conn.clear();
        if (m_ctx) SSL_CTX_free((SSL_CTX*)m_ctx);
        if (socketFD >= 0) close(socketFD);
    }

    bool TCPSocket::listen(int port)
    {
        int opt = 1;
        if (setsockopt(socketFD, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) std::perror("setsockopt SO_REUSEADDR failed");

        SocketAddrType addr = newAddress(port);

        if (::bind(socketFD, (struct sockaddr*)&addr, sizeof(addr)) < 0)
        {
            perror("bind failed");
            return false;
        }

        // ⚠️ THE BACKLOG WAS 10, and a connection storm is not an attack — it is a restart. Measured:
        // 96 clients connecting at once and the kernel refused most of them, because ten is the number
        // of connections allowed to sit between `connect` and this process's next `accept`. A node
        // that accepts in a loop empties it quickly; ten only ever bought a refusal.
        if (::listen(socketFD, 512) < 0)
        {
            perror("listen failed");
            return false;
        }

        if (tlsConfigured() && !ensureContext(/*server*/ true))
        {
            std::cerr << "[TCPSocket] TLS is configured but the certificates could not be loaded"
                      << std::endl;
            return false;
        }

        return true;
    }

    int TCPSocket::accept()
    {
        SocketAddrType clientAddr;
        socklen_t addrLen = sizeof(clientAddr);

        int clientFD = ::accept(socketFD, (struct sockaddr*)&clientAddr, &addrLen);
        if (clientFD < 0) { std::perror("[TCPSocket] accept failed"); return clientFD; }
        setNoDelay(clientFD);

        if (m_ctx)
        {
            SSL* ssl = SSL_new((SSL_CTX*)m_ctx);
            if (!ssl || SSL_set_fd(ssl, clientFD) != 1 ||
                !tlsHandshake(ssl, clientFD, /*server*/ true, 0))
            {
                // A peer without a certificate this CA signed is not a peer. Closed here rather than
                // handed to the node, which would otherwise read an empty stream and call it a hangup.
                tlsReport("handshake (accept)");
                if (ssl) SSL_free(ssl);
                close(clientFD);
                return -1;
            }
            Conn* c = (Conn*)linkFor(clientFD, /*create*/ true);
            // A descriptor NUMBER can come back: if this one was closed without `closeClient` the
            // system reuses it, and the entry left behind would hold a dead `SSL`.
            if (c->ssl) SSL_free(c->ssl);
            c->ssl = ssl;
        }
        return clientFD;
    }

    /// A bounded `::connect`. Returns 0 on success, -1 on failure or deadline expiry.
    ///
    /// The socket is made NON-BLOCKING only for the duration of the handshake and restored on the way
    /// out: every other call site expects a blocking descriptor driven by SO_RCVTIMEO, and handing one
    /// back in a different mode would turn every `receive` into a silent EAGAIN.
    static int connectWithDeadline(int fd, const sockaddr* sa, socklen_t slen, int timeoutMs)
    {
        if (timeoutMs <= 0) return ::connect(fd, sa, slen) == 0 ? 0 : -1;

        const int flags = ::fcntl(fd, F_GETFL, 0);
        if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
            return ::connect(fd, sa, slen) == 0 ? 0 : -1;   // without fcntl no deadline is possible

        int result = -1;
        if (::connect(fd, sa, slen) == 0)
            result = 0;                                      // loopback usually connects immediately
        else if (errno == EINPROGRESS)
        {
            pollfd pfd{ fd, POLLOUT, 0 };
            if (::poll(&pfd, 1, timeoutMs) > 0)
            {
                // POLLOUT only says the attempt FINISHED, not that it succeeded: the real error is in
                // SO_ERROR.
                int err = 0; socklen_t elen = sizeof(err);
                if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen) == 0 && err == 0)
                    result = 0;
            }
        }

        ::fcntl(fd, F_SETFL, flags);
        return result;
    }

    bool TCPSocket::connect(const std::string& address, int port, int timeoutMs)
    {
        // ⚠️ AF_UNSPEC, NOT `AF_FAMILY`. With `HARUKA_IPV6` — which is ON by default — this asked for
        // AF_INET6, and `getaddrinfo("127.0.0.1", ..., AF_INET6)` **fails**: an IPv4 literal has no
        // IPv6 representation. Which means the default build COULD NOT CONNECT TO ANY IPv4 ADDRESS —
        // not the loopback, not a node's IP. The server side never suffered it: an AF_INET6 socket
        // bound to `in6addr_any` accepts IPv4 as v4-mapped, so the failure was client-only and
        // invisible from outside.
        //
        // Resolved without pinning a family, trying each candidate by creating the socket with THAT
        // candidate's family. The constructor's socket has a fixed family, so it cannot serve both.
        addrinfo hints{}, *res = nullptr;
        hints.ai_family   = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        const int rc = getaddrinfo(address.c_str(), std::to_string(port).c_str(), &hints, &res);
        if (rc != 0 || !res)
        {
            std::cerr << "[TCPSocket] Could not resolve: " << address
                      << " (" << gai_strerror(rc) << ")" << std::endl;
            return false;
        }

        bool ok = false;
        for (addrinfo* a = res; a && !ok; a = a->ai_next)
        {
            const int fd = ::socket(a->ai_family, a->ai_socktype, a->ai_protocol);
            if (fd < 0) continue;
            if (connectWithDeadline(fd, a->ai_addr, a->ai_addrlen, timeoutMs) == 0)
            {
                // `socketFD != fd` is not paranoia: if the object was carrying an already-closed
                // descriptor, the system may have reassigned THAT VERY NUMBER to the freshly created
                // socket, and closing it here would leave a "successful" connection on a dead fd.
                // `dropTls` goes with the `close`: the per-connection state is keyed by descriptor, and
                // leaving the old one behind both leaks it and hands a REUSED number a stale entry.
                if (socketFD >= 0 && socketFD != fd) { dropTls(socketFD); close(socketFD); }
                socketFD = fd;
                setNoDelay(fd);
                ok = true;

                if (tlsConfigured())
                {
                    if (!ensureContext(/*server*/ false)) { ok = false; }
                    else
                    {
                        SSL* ssl = SSL_new((SSL_CTX*)m_ctx);
                        // The caller's deadline covers the WHOLE connect, TLS included: it asked for a
                        // bounded call, not for a bounded first half of one.
                        if (!ssl || SSL_set_fd(ssl, fd) != 1 ||
                            !tlsHandshake(ssl, fd, /*server*/ false, timeoutMs))
                        {
                            tlsReport("handshake (connect)");
                            if (ssl) SSL_free(ssl);
                            ok = false;
                        }
                        else
                        {
                            Conn* c = (Conn*)linkFor(fd, /*create*/ true);
                            if (c->ssl) SSL_free(c->ssl);
                            c->ssl = ssl;
                        }
                    }
                    if (!ok) { close(fd); socketFD = -1; }
                }
            }
            else close(fd);
        }
        if (!ok) std::cerr << "[TCPSocket] connect failed to " << address << ":" << port << std::endl;
        freeaddrinfo(res);
        return ok;
    }

    // Writes exactly `n` bytes, through TLS when this descriptor has it. `SSL_write` can return a
    // short count for the same reasons `send` can, so it loops.
    //
    // ⚠️ THE LOCK IS TAKEN AROUND THE CALL AND NOT AROUND THE WAIT. Holding `io` while parked in
    // `poll` would mean a writer blocked on a full send buffer stops the reader on the other thread —
    // and the peer, which is waiting for us to read before it reads, would never drain it. That is a
    // deadlock built out of two correct-looking locks, so the wait happens with the lock released.
    static bool writeAll(Conn* c, int fd, const void* buf, size_t n)
    {
        SSL* const ssl = c ? c->ssl : nullptr;
        const uint8_t* p = (const uint8_t*)buf;
        size_t done = 0;
        const int tmo = ssl ? soTimeoutMs(fd, SO_SNDTIMEO) : -1;

        while (done < n)
        {
            if (!ssl)
            {
                const ssize_t w = ::send(fd, p + done, n - done, 0);
                if (w <= 0) return false;
                done += (size_t)w;
                continue;
            }

            int w = 0, err = 0;
            {
                std::lock_guard<std::mutex> g(c->io);
                w = SSL_write(ssl, p + done, (int)(n - done));
                if (w <= 0) err = SSL_get_error(ssl, w);   // must be read before another thread's call
            }
            if (w > 0) { done += (size_t)w; continue; }

            // WANT_READ on a write is not a contradiction: TLS 1.3 can need to process a post-handshake
            // message before it can send. Both retries re-issue `SSL_write` with the SAME pointer and
            // length, which is what OpenSSL requires — and what `tx` in `send()` guarantees stays true.
            if (err == SSL_ERROR_WANT_WRITE) { if (!waitFd(fd, POLLOUT, tmo)) return false; continue; }
            if (err == SSL_ERROR_WANT_READ)  { if (!waitFd(fd, POLLIN,  tmo)) return false; continue; }
            return false;
        }
        return true;
    }

    /// Writes both pieces of a frame as ONE segment, and keeps writing until they are gone.
    ///
    /// A partial `writev` is rare on a blocking socket but not impossible (a signal, a full buffer),
    /// and getting the resume wrong would put the length prefix on the wire twice — a desynchronised
    /// stream, which is the failure `receive` carries its longest comment about. So the leftover is
    /// advanced across the vectors rather than assumed away.
    static bool writevAll(int fd, iovec* iov, int n)
    {
        while (n > 0)
        {
            const ssize_t w = ::writev(fd, iov, n);
            if (w <= 0) return false;

            size_t left = (size_t)w;
            while (n > 0 && left >= iov[0].iov_len)
            {
                left -= iov[0].iov_len;
                ++iov; --n;
            }
            if (n > 0 && left > 0)
            {
                iov[0].iov_base = (uint8_t*)iov[0].iov_base + left;
                iov[0].iov_len -= left;
            }
        }
        return true;
    }

    bool TCPSocket::send(int fd, const uint8_t* data, size_t size)
    {
        if (fd < 0) return false;
        Conn* c = (Conn*)linkFor(fd, /*create*/ true);

        // One whole frame at a time. Without this two threads sending on the same descriptor produce a
        // length prefix followed by somebody else's payload — a stream the peer cannot resynchronise,
        // which is exactly the failure `receive()` further down was fixed for.
        std::lock_guard<std::mutex> frame(c->tx);

        const uint32_t len = htonl((uint32_t)size);

        // ⚠️ ONE WRITE, NOT TWO, AND IT IS WORTH 80 ms A ROUND TRIP. This used to write the 4-byte
        // prefix and then the payload as two separate calls — the textbook write-write-read that
        // Nagle's algorithm exists to punish. The small first segment goes out, the second waits for
        // its ACK, and the peer's delayed-ACK timer holds that back for 40 ms. Both directions do it,
        // so a request and its answer cost ~80 ms of nothing at all.
        //
        // Measured on LOOPBACK, a zone query to the head — the thing a player does at every chunk
        // border: **82 ms → 41 ms with one end fixed → 0.07 ms with both**. 1170x, and `TCP_NODELAY`
        // was measured to add nothing on top of it: the framing was the whole of it.
        //
        // There is no two-write path left. A frame of ANY size leaves as one: `writev` on the plain
        // path, which copies nothing, and one assembled buffer under TLS because `SSL_write` takes one
        // pointer. Leaving a fallback that wrote twice would have left the 40 ms stall lying in wait
        // for the first message big enough to reach it.
        if (!c->ssl)
        {
            iovec iov[2];
            iov[0].iov_base = (void*)&len;  iov[0].iov_len = 4;
            iov[1].iov_base = (void*)data;  iov[1].iov_len = size;
            return writevAll(fd, iov, size ? 2 : 1);
        }

        c->txBuf.resize(4 + size);
        std::memcpy(c->txBuf.data(), &len, 4);
        if (size) std::memcpy(c->txBuf.data() + 4, data, size);
        return writeAll(c, fd, c->txBuf.data(), c->txBuf.size());
    }

    static ssize_t recvAll(Conn* c, int fd, void* buf, size_t n)
    {
        SSL* const ssl = c ? c->ssl : nullptr;
        size_t got = 0;
        const int tmo = ssl ? soTimeoutMs(fd, SO_RCVTIMEO) : -1;

        while (got < n)
        {
            if (!ssl)
            {
                const ssize_t r = ::recv(fd, static_cast<char*>(buf) + got, n - got, 0);
                if (r <= 0) return r;
                got += (size_t)r;
                continue;
            }

            int k = 0, err = 0;
            unsigned long reason = 0;
            {
                std::lock_guard<std::mutex> g(c->io);
                k = SSL_read(ssl, static_cast<char*>(buf) + got, (int)(n - got));
                if (k <= 0)
                {
                    err = SSL_get_error(ssl, k);
                    if (err == SSL_ERROR_SSL || err == SSL_ERROR_SYSCALL) reason = ERR_peek_error();
                }
            }
            if (k > 0) { got += (size_t)k; continue; }

            // Nothing decrypted yet: wait on the descriptor, NOT holding the lock, so the writer on the
            // other thread can keep working while this side is idle. `tmo` reproduces the SO_RCVTIMEO
            // the caller set, which a non-blocking descriptor no longer honours by itself.
            if (err == SSL_ERROR_WANT_READ)  { if (!waitFd(fd, POLLIN,  tmo)) return -1; continue; }
            if (err == SSL_ERROR_WANT_WRITE) { if (!waitFd(fd, POLLOUT, tmo)) return -1; continue; }

            // A clean TLS shutdown is an orderly close, like recv() returning 0; anything else is an
            // error. Collapsing the two would make a peer that hung up look like a quiet one, which is
            // the bug this function's own comment further down records.
            //
            // ⚠️ AND A PEER THAT SIMPLY DIES COUNTS AS ONE, which TLS does not say the same way — see
            // `tlsIsClosed`. That is exactly the distinction `receive` was fixed to preserve:
            // `zone_node` keys its head-reconnect on `bytes == 0`. Measured with TLS on,
            // `reconnect_e2e` went red on precisely that check, so the property the plain path
            // guarantees was being lost the moment certificates were configured.
            return tlsIsClosed(err, reason) ? 0 : -1;
        }
        return (ssize_t)got;
    }

    int TCPSocket::receive(int fd, uint8_t* buffer, size_t size)
    {
        Conn* c = (Conn*)linkFor(fd, /*create*/ false);

        uint32_t netLen;
        // 0 means ORDERLY CLOSE, -1 means "nothing yet, or an error". Collapsing both into -1 (which is
        // what this did) makes a dead peer indistinguishable from a quiet one — and `zone_node` keys its
        // head-reconnect on `bytes == 0`, a value this function could never return. That branch was
        // unreachable: a head that hung up was only ever noticed later, via a failed `send`.
        const ssize_t head = recvAll(c, fd, &netLen, 4);
        if (head == 0) return 0;
        if (head != 4) return -1;
        uint32_t len = ntohl(netLen);

        // ⚠️ A LENGTH THAT DOES NOT FIT USED TO DESYNCHRONISE THE CONNECTION FOR EVER. This returned
        // -1 WITHOUT consuming the payload, so the next call read four bytes from the middle of that
        // payload as a length. Measured with a 277-byte message into a 256-byte reader followed by
        // three ordinary ones: the three were lost AND one read came back with **255 bytes of garbage
        // presented as a valid packet** — a node would then decode whatever that happened to be.
        // Every message after the first is affected, and the connection never recovers on its own.
        //
        // A length beyond MAX_PACKET_SIZE cannot be trusted enough to skip: the stream is corrupt (or
        // the peer is hostile), and the only safe reading is that this connection is finished. It is
        // reported as a close so the caller's existing path tears it down and reconnects.
        if (len == 0 || len > DGS::MAX_PACKET_SIZE)
        {
            std::cerr << "[TCPSocket] corrupt frame length " << len
                      << " -> dropping the connection" << std::endl;
            return 0;
        }

        // Too big for THIS caller's buffer, but a legal message: consume it so the stream stays in
        // step, and report the loss of that one message rather than of everything after it.
        if (len > size)
        {
            uint8_t sink[4096];
            uint32_t left = len;
            while (left > 0)
            {
                const size_t chunk = left < sizeof(sink) ? (size_t)left : sizeof(sink);
                const ssize_t r = recvAll(c, fd, sink, chunk);
                if (r <= 0) return (int)r;
                left -= (uint32_t)chunk;
            }
            std::cerr << "[TCPSocket] message of " << len << " B does not fit a " << size
                      << " B buffer -> discarded, stream kept in sync" << std::endl;
            return -1;
        }

        ssize_t r = recvAll(c, fd, buffer, len);

        // ⚠️ A DETECTOR, not a fix. Each message is written as two `SSL_write` calls (prefix, payload)
        // and read back at exactly those sizes, so every TLS record is consumed whole and OpenSSL
        // should never be left holding decrypted bytes. If that assumption is ever wrong, `epoll` on
        // the raw descriptor would stop reporting a peer that HAS sent something and the port would go
        // quiet under load — the hardest kind of TLS bug to see. So it says so, once, instead of
        // silently becoming true. (`pending()` is the same question asked by the readiness gates.)
        if (c && c->ssl)
        {
            std::lock_guard<std::mutex> g(c->io);
            const int left = SSL_pending(c->ssl);
            static std::atomic<bool> said{false};
            if (left > 0 && !said.exchange(true))
                std::cerr << "[TCPSocket] TLS left " << left
                          << " bytes buffered after a complete message: readiness gates that only "
                             "poll the descriptor can now miss data" << std::endl;
        }
        return (int)r;
    }

    void TCPSocket::closeClient(int fd)
    {
        dropTls(fd);
        close(fd);
    }
};