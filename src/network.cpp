#include "include/dgs/network.h"

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

        ssize_t sent = sendto(socketFD, data, size, 0, (struct sockaddr*)&destAddr, sizeof(destAddr));
        
        return sent == (ssize_t)size;
    }

    int UDPSocket::receive(uint8_t* buffer, size_t size, std::string& outAddress, int& outPort)
    {
        SocketAddrType fromAddr{};
        socklen_t fromLen = sizeof(fromAddr);

        int bytesRecived = recvfrom(socketFD, buffer, size, 0, (struct sockaddr*)&fromAddr, &fromLen);
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
    
    TCPSocket::TCPSocket()
    {
        socketFD = socket(AF_FAMILY, SOCK_STREAM, 0);
        if (socketFD < 0) std::cerr << "Failed to create the socket" << std::endl;
    }

    TCPSocket::TCPSocket(TCPSocket&& other) noexcept : socketFD(other.socketFD)
    {
        other.socketFD = -1;
    }

    TCPSocket& TCPSocket::operator=(TCPSocket&& other) noexcept
    {
        if (this != &other)
        {
            if (socketFD >= 0) close(socketFD);
            socketFD = other.socketFD;
            other.socketFD = -1;
        }
        return *this;
    }

    TCPSocket::~TCPSocket()
    {
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
        
        return true;
    }

    int TCPSocket::accept()
    {
        SocketAddrType clientAddr;
        socklen_t addrLen = sizeof(clientAddr);

        int clientFD = ::accept(socketFD, (struct sockaddr*)&clientAddr, &addrLen);
        if (clientFD < 0) std::perror("[TCPSocket] accept failed");
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
            }
            else close(fd);
        }
        if (!ok) std::cerr << "[TCPSocket] connect failed to " << address << ":" << port << std::endl;
        freeaddrinfo(res);
        return ok;
    }

    bool TCPSocket::send(int fd, const uint8_t* data, size_t size)
    {
        uint32_t len = htonl((uint32_t)size);
        if (::send(fd, &len, 4, 0) != 4) return false;
        ssize_t sent = ::send(fd, data, size, 0);
        return sent == (ssize_t)size;
    }

    static ssize_t recvAll(int fd, void* buf, size_t n)
    {
        size_t got = 0;
        while (got < n)
        {
            ssize_t r = ::recv(fd, static_cast<char*>(buf) + got, n - got, 0);
            if (r <= 0) return r;
            got += r;
        }
        return (ssize_t)got;
    }

    int TCPSocket::receive(int fd, uint8_t* buffer, size_t size)
    {
        uint32_t netLen;
        // 0 means ORDERLY CLOSE, -1 means "nothing yet, or an error". Collapsing both into -1 (which is
        // what this did) makes a dead peer indistinguishable from a quiet one — and `zone_node` keys its
        // head-reconnect on `bytes == 0`, a value this function could never return. That branch was
        // unreachable: a head that hung up was only ever noticed later, via a failed `send`.
        const ssize_t head = recvAll(fd, &netLen, 4);
        if (head == 0) return 0;
        if (head != 4) return -1;
        uint32_t len = ntohl(netLen);
        if (len > size) return -1;
        ssize_t r = recvAll(fd, buffer, len);
        return (int)r;
    }

    void TCPSocket::closeClient(int fd)
    {
        close(fd);
    }
};