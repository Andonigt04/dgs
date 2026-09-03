#ifndef DGS_NETWORK_H
#define DGS_NETWORK_H

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
        private:
            int socketFD;
    };

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