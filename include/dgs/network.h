#ifndef DGS_NETWORK_H
#define DGS_NETWORK_H

#include <map>
#include <mutex>
#include <string>
#include <vector>
#include <netinet/in.h>

namespace DGS
{
    class UDPSocket
    {
        public:
            UDPSocket();
            ~UDPSocket();

            // ⚠️ RULE OF THREE: this class OWNS a descriptor. Without these four lines the compiler's
            // generated assignment operator copies `socketFD` verbatim — and `nodes/zone_node.cpp`
            // literally does `tcp_zone_node = DGS::TCPSocket();` when reconnecting: the temporary is
            // destroyed, CLOSES the descriptor, and the object is left holding a dead number (on top of
            // leaking the previous one). The system reuses that number almost immediately, so from then
            // on every call operates on the WRONG socket. It looked like "Connection with HeadServer
            // lost. Reconnecting..." in a loop, with nothing explaining why.
            UDPSocket(const UDPSocket&)            = delete;
            UDPSocket& operator=(const UDPSocket&) = delete;
            UDPSocket(UDPSocket&& other) noexcept;
            UDPSocket& operator=(UDPSocket&& other) noexcept;

            bool bind(int port);

            bool send(const std::string& address, int port, const uint8_t* data, size_t size);
            /// Sends bytes that are ALREADY sealed (or that must not be sealed at all).
            ///
            /// A zone's broadcast is one payload to N recipients. Sealing inside `send` would encrypt
            /// the same frame once per recipient — measured at +37 % of loop time at 256 players —
            /// and would throw away the serialise-once-per-tick property the capacity figures rest on.
            /// So the zone seals each frame once with `sealForUdp` and sends the result to everybody.
            bool sendRaw(const std::string& address, int port, const uint8_t* data, size_t size);
            int receive(uint8_t* buffer, size_t size, std::string& outAddress, int& outPort);

            int getSocketFD() { return socketFD; }
        private:
            int socketFD;
    };

    class TCPSocket
    {
        public:
            TCPSocket();
            ~TCPSocket();

            // ⚠️ RULE OF THREE: this class OWNS a descriptor. Without these four lines the compiler's
            // generated assignment operator copies `socketFD` verbatim — and `nodes/zone_node.cpp`
            // literally does `tcp_zone_node = DGS::TCPSocket();` when reconnecting: the temporary is
            // destroyed, CLOSES the descriptor, and the object is left holding a dead number (on top of
            // leaking the previous one). The system reuses that number almost immediately, so from then
            // on every call operates on the WRONG socket. It looked like "Connection with HeadServer
            // lost. Reconnecting..." in a loop, with nothing explaining why.
            TCPSocket(const TCPSocket&)            = delete;
            TCPSocket& operator=(const TCPSocket&) = delete;
            TCPSocket(TCPSocket&& other) noexcept;
            TCPSocket& operator=(TCPSocket&& other) noexcept;

            bool listen(int port);
            int accept();

            /// Connects with a DEADLINE. `timeoutMs <= 0` means blocking (raw kernel behaviour).
            ///
            /// ⚠️ THE DEADLINE IS NOT DECORATION. A blocking `::connect` against a service that LISTENS
            /// but does not accept — a full accept queue, which is exactly what an overloaded node has —
            /// does not fail: the kernel drops the SYN and retries with backoff for ~127 s
            /// (`tcp_syn_retries` = 6). Measured with a probe: with backlog 10 and nobody accepting, the
            /// first 11 `connect` calls take 0.1 ms each and the TWELFTH was still blocked after 50 s.
            /// Inside a node's loop that freezes the whole tick: no metrics, no heartbeats, no broadcast
            /// to clients. The head goes blind EXACTLY when something is wrong. A refused connect (RST)
            /// does return fast, which is why the "validator switched off" case tested fine and this one
            /// stayed invisible.
            bool connect(const std::string& address, int port, int timeoutMs = 3000);

            bool send(int fd, const uint8_t* data, size_t size);
            int receive(int fd, uint8_t* buffer, size_t size);

            void closeClient(int fd);

            int getSocketFD() { return socketFD; }

            // ── TLS ──────────────────────────────────────────────────────────────────────────────
            // Node authentication decided WHO may connect. This decides what anyone on the path can
            // read or change, which was: everything. Every packet on every link travelled in clear —
            // player positions, verdicts, bans, the entity state a zone hands to its neighbour.
            //
            // Enabled per process by the environment, and OFF by default for the same reason node auth
            // is: a transport change that bricks every existing deployment on upgrade is an outage.
            //   DGS_TLS_CERT / DGS_TLS_KEY   this node's certificate and key
            //   DGS_TLS_CA                   the CA both ends are verified against (mutual TLS)
            // With all three set, a listener REQUIRES a client certificate signed by that CA and a
            // connector verifies the server against it, so the encryption comes with mutual identity
            // rather than with a stranger.
            //
            // ⚠️ `poll`/`epoll` ON THE RAW DESCRIPTOR IS NOT ENOUGH ONCE TLS IS ON. A TLS record can
            // carry several messages, so OpenSSL may hold decrypted bytes that the kernel no longer
            // reports as readable — the classic way a TLS port goes quiet under load. `pending(fd)`
            // exposes that, and every readiness gate in this repository asks it too.
            //
            // ⚠️ ONE CONNECTION MAY BE USED FROM TWO THREADS, and TLS does not tolerate what plain TCP
            // does. `Client` sends from the caller's thread and receives on its own, over this very
            // object: with no TLS the kernel serialises that and it is correct, but `SSL_read` and
            // `SSL_write` on the SAME `SSL` share one record layer and one error state — running them
            // at once is undefined behaviour, not a lost message. So each connection carries its own
            // locks (see `Conn` in network.cpp) and this class is safe for ONE READER PLUS ONE WRITER,
            // and for several writers. It is still not safe for several readers: two threads calling
            // `receive` on the same descriptor split each other's frames, which no lock here can fix.
            bool tlsEnabled() const;
            bool pending(int fd) const;

        private:
            int socketFD;
            void*                        m_ctx = nullptr;   // SSL_CTX*
            mutable std::map<int, void*> m_conn;            // fd -> Conn* (defined in network.cpp)
            mutable std::mutex           m_connMtx;         // the MAP only; never held across I/O
            bool  ensureContext(bool server);
            void  dropTls(int fd);
            void* linkFor(int fd, bool create) const;
    };

    /// Adopts a UDP key handed out at RUNTIME, which is how a client is supposed to get one.
    ///
    /// ⚠️ EVERY KEY USED TO COME FROM THE ENVIRONMENT, and that is fine for a node started by an
    /// operator and wrong for a player. A session key exists so that what one player says about
    /// themselves is not readable by the player next to them; handing it out through `DGS_UDP_SESSION`
    /// means it is decided before the process starts and shared by everyone who can read the unit file.
    /// The login response is where it belongs, and this is what lets the client take it from there.
    ///
    /// `session` 0 is the GROUP key — the zone's broadcast, which every client must be able to open, so
    /// a client typically adopts two: the group key to read the world and its own to write to it.
    /// `keyHex` is 64 hex characters (the raw 32-byte key, NOT a passphrase: hashing one end and not
    /// the other is a mismatch whose only symptom is silence). An empty `keyHex` forgets that session.
    /// @return false if the key is malformed, in which case nothing is changed.
    ///
    /// Runtime keys are consulted BEFORE the environment, so a login can override a deployment default,
    /// and they are process-wide because the sealing is.
    bool setUdpSessionKey(uint32_t session, const std::string& keyHex);

    /// Is the UDP game plane encrypted in this process? (`DGS_UDP_KEY`)
    bool udpCryptoEnabled();

    /// Seals one datagram for the UDP game plane. @return false when no key is configured (and `out`
    /// is then a copy of the input, so a caller can always send `out` and be correct either way).
    bool sealForUdp(const uint8_t* data, size_t size, std::vector<uint8_t>& out);

    /// What `size` bytes of payload actually cost on the wire. The node's own `bytesTx` used to count
    /// the PLAINTEXT, so once encryption was switched on its bandwidth metric under-reported by 28
    /// bytes per datagram — a capacity number quietly measuring something other than the wire.
    size_t udpWireSize(size_t size);

    /// Was this `receive` truncated? (§4.6 bug 6)
    ///
    /// A datagram larger than the buffer is silently cut and the parser would read garbage.
    /// Convention: ALWAYS call `receive` with a buffer of at least MAX_PACKET_SIZE and treat
    /// `received >= size` as truncation (discard + count it in the node's failedTransfers). This helper
    /// centralises the check so the nodes cannot forget it at each recv site.
    ///
    /// ⚠️ PORTED BACK FROM THE VENDORED COPY. It lived only in `haruka-cpp/external/dgs/`, which is
    /// supposed to be an EXPORT of this repository — so the copy had drifted in both directions at
    /// once: behind on everything else, and ahead on this. Refreshing the export broke the engine's
    /// build, which is how it surfaced.
    inline bool receivedWasTruncated(int received, size_t bufferSize)
    {
        // received < 0 = error/timeout (no data); received == bufferSize = the datagram may have fitted
        // exactly, but a UDP payload that fills the buffer to the brim is as rare as it is dangerous.
        return received <= 0 || (size_t)received >= bufferSize;
    }
};

#endif