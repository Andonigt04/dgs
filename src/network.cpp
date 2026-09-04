#include "include/dgs/network.h"
#include "include/dgs/types.h"

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/rand.h>
#include <vector>
#include <cstring>

#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
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
    //   · it is a GROUP key. Every client that can talk to the zone holds it, so a client can decrypt
    //     another client's uplink if it can capture it. For the broadcast that changes nothing (they
    //     all receive it anyway); for the uplink it is a real limit, and per-session keys handed out by
    //     the login API are the answer. Not done.
    //   · REPLAY is not prevented here. A captured datagram can be re-sent within its lifetime. The
    //     anti-cheat layer above already deals with that for the traffic that matters: the validator's
    //     minimum-dt discard exists precisely to reject duplicated and reordered samples, and S1 throws
    //     out anything implausible. Saying it out loud because a reader would otherwise assume GCM's
    //     nonce gives replay protection, which it does not.
    //   · the key comes from `DGS_UDP_KEY`, one value for the whole world. A deployment would issue it
    //     per session from the login API.
    static const size_t UDP_NONCE = 12, UDP_TAG = 16;

    static bool udpKey(uint8_t out[32])
    {
        const char* k = std::getenv("DGS_UDP_KEY");
        if (!k || !*k) return false;
        SHA256((const unsigned char*)k, std::strlen(k), out);
        return true;
    }

    static bool udpEnabled() { uint8_t k[32]; return udpKey(k); }

    /// nonce || ciphertext || tag. @return the sealed length, or 0 on failure.
    static size_t udpSeal(const uint8_t* in, size_t n, uint8_t* out, size_t outCap)
    {
        uint8_t key[32];
        if (!udpKey(key)) return 0;
        if (outCap < n + UDP_NONCE + UDP_TAG) return 0;
        if (RAND_bytes(out, (int)UDP_NONCE) != 1) return 0;

        EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
        if (!ctx) return 0;
        size_t sealed = 0;
        int len = 0;
        bool ok = EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) == 1 &&
                  EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, (int)UDP_NONCE, nullptr) == 1 &&
                  EVP_EncryptInit_ex(ctx, nullptr, nullptr, key, out) == 1 &&
                  EVP_EncryptUpdate(ctx, out + UDP_NONCE, &len, in, (int)n) == 1;
        if (ok)
        {
            sealed = UDP_NONCE + (size_t)len;
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
        uint8_t key[32];
        if (!udpKey(key)) return 0;
        if (n < UDP_NONCE + UDP_TAG) return 0;

        const size_t ctLen = n - UDP_NONCE - UDP_TAG;
        std::vector<uint8_t> plain(ctLen ? ctLen : 1);

        EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
        if (!ctx) return 0;
        int len = 0;
        bool ok = EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) == 1 &&
                  EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, (int)UDP_NONCE, nullptr) == 1 &&
                  EVP_DecryptInit_ex(ctx, nullptr, nullptr, key, buf) == 1 &&
                  EVP_DecryptUpdate(ctx, plain.data(), &len, buf + UDP_NONCE, (int)ctLen) == 1 &&
                  EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, (int)UDP_TAG,
                                      (void*)(buf + UDP_NONCE + ctLen)) == 1;
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

    size_t udpWireSize(size_t size) { return udpEnabled() ? size + UDP_NONCE + UDP_TAG : size; }

    bool sealForUdp(const uint8_t* data, size_t size, std::vector<uint8_t>& out)
    {
        if (!udpEnabled()) { out.assign(data, data + size); return false; }
        out.resize(size + UDP_NONCE + UDP_TAG);
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
        auto it = m_ssl.find(fd);
        if (it == m_ssl.end()) return;
        SSL_free((SSL*)it->second);
        m_ssl.erase(it);
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
        auto it = m_ssl.find(fd);
        if (it == m_ssl.end()) return false;
        SSL* ssl = (SSL*)it->second;
        if (SSL_pending(ssl) > 0) return true;

        const int flags = fcntl(fd, F_GETFL, 0);
        if (flags < 0) return false;
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return false;
        char probe = 0;
        const int k = SSL_peek(ssl, &probe, 1);
        fcntl(fd, F_SETFL, flags);
        return k > 0;
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
        m_ssl = std::move(other.m_ssl); other.m_ssl.clear();
    }

    TCPSocket& TCPSocket::operator=(TCPSocket&& other) noexcept
    {
        if (this != &other)
        {
            for (auto& kv : m_ssl) SSL_free((SSL*)kv.second);
            m_ssl.clear();
            if (m_ctx) SSL_CTX_free((SSL_CTX*)m_ctx);
            if (socketFD >= 0) close(socketFD);
            socketFD = other.socketFD;
            other.socketFD = -1;
            m_ctx = other.m_ctx; other.m_ctx = nullptr;
            m_ssl = std::move(other.m_ssl); other.m_ssl.clear();
        }
        return *this;
    }

    TCPSocket::~TCPSocket()
    {
        for (auto& kv : m_ssl) SSL_free((SSL*)kv.second);
        m_ssl.clear();
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

        if (::listen(socketFD, 10) < 0)
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

        if (m_ctx)
        {
            SSL* ssl = SSL_new((SSL_CTX*)m_ctx);
            if (!ssl || SSL_set_fd(ssl, clientFD) != 1 || SSL_accept(ssl) != 1)
            {
                // A peer without a certificate this CA signed is not a peer. Closed here rather than
                // handed to the node, which would otherwise read an empty stream and call it a hangup.
                tlsReport("handshake (accept)");
                if (ssl) SSL_free(ssl);
                close(clientFD);
                return -1;
            }
            m_ssl[clientFD] = ssl;
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
                if (socketFD >= 0 && socketFD != fd) close(socketFD);
                socketFD = fd;
                ok = true;

                if (tlsConfigured())
                {
                    if (!ensureContext(/*server*/ false)) { ok = false; }
                    else
                    {
                        SSL* ssl = SSL_new((SSL_CTX*)m_ctx);
                        if (!ssl || SSL_set_fd(ssl, fd) != 1 || SSL_connect(ssl) != 1)
                        {
                            tlsReport("handshake (connect)");
                            if (ssl) SSL_free(ssl);
                            ok = false;
                        }
                        else m_ssl[fd] = ssl;
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
    static bool writeAll(SSL* ssl, int fd, const void* buf, size_t n)
    {
        const uint8_t* p = (const uint8_t*)buf;
        size_t done = 0;
        while (done < n)
        {
            const int w = ssl ? SSL_write(ssl, p + done, (int)(n - done))
                              : (int)::send(fd, p + done, n - done, 0);
            if (w <= 0) return false;
            done += (size_t)w;
        }
        return true;
    }

    bool TCPSocket::send(int fd, const uint8_t* data, size_t size)
    {
        auto it = m_ssl.find(fd);
        SSL* ssl = it == m_ssl.end() ? nullptr : (SSL*)it->second;

        uint32_t len = htonl((uint32_t)size);
        if (!writeAll(ssl, fd, &len, 4)) return false;
        return writeAll(ssl, fd, data, size);
    }

    static ssize_t recvAll(SSL* ssl, int fd, void* buf, size_t n)
    {
        size_t got = 0;
        while (got < n)
        {
            ssize_t r;
            if (ssl)
            {
                const int k = SSL_read(ssl, static_cast<char*>(buf) + got, (int)(n - got));
                if (k > 0) r = k;
                else
                {
                    // A clean TLS shutdown is an orderly close, like recv() returning 0; anything else
                    // is an error. Collapsing the two would make a peer that hung up look like a quiet
                    // one, which is the bug this function's own comment further down records.
                    const int err = SSL_get_error(ssl, k);
                    r = (err == SSL_ERROR_ZERO_RETURN) ? 0 : -1;
                }
            }
            else r = ::recv(fd, static_cast<char*>(buf) + got, n - got, 0);
            if (r <= 0) return r;
            got += r;
        }
        return (ssize_t)got;
    }

    int TCPSocket::receive(int fd, uint8_t* buffer, size_t size)
    {
        auto it = m_ssl.find(fd);
        SSL* ssl = it == m_ssl.end() ? nullptr : (SSL*)it->second;

        uint32_t netLen;
        // 0 means ORDERLY CLOSE, -1 means "nothing yet, or an error". Collapsing both into -1 (which is
        // what this did) makes a dead peer indistinguishable from a quiet one — and `zone_node` keys its
        // head-reconnect on `bytes == 0`, a value this function could never return. That branch was
        // unreachable: a head that hung up was only ever noticed later, via a failed `send`.
        const ssize_t head = recvAll(ssl, fd, &netLen, 4);
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
                const ssize_t r = recvAll(ssl, fd, sink, chunk);
                if (r <= 0) return (int)r;
                left -= (uint32_t)chunk;
            }
            std::cerr << "[TCPSocket] message of " << len << " B does not fit a " << size
                      << " B buffer -> discarded, stream kept in sync" << std::endl;
            return -1;
        }

        ssize_t r = recvAll(ssl, fd, buffer, len);

        // ⚠️ A DETECTOR, not a fix. Each message is written as two `SSL_write` calls (prefix, payload)
        // and read back at exactly those sizes, so every TLS record is consumed whole and OpenSSL
        // should never be left holding decrypted bytes. If that assumption is ever wrong, `epoll` on
        // the raw descriptor would stop reporting a peer that HAS sent something and the port would go
        // quiet under load — the hardest kind of TLS bug to see. So it says so, once, instead of
        // silently becoming true. (`pending()` is the same question asked by the readiness gates.)
        if (ssl && SSL_pending(ssl) > 0)
        {
            static bool said = false;
            if (!said) {
                said = true;
                std::cerr << "[TCPSocket] TLS left " << SSL_pending(ssl)
                          << " bytes buffered after a complete message: readiness gates that only "
                             "poll the descriptor can now miss data" << std::endl;
            }
        }
        return (int)r;
    }

    void TCPSocket::closeClient(int fd)
    {
        dropTls(fd);
        close(fd);
    }
};