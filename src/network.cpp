#include "include/dgs/network.h"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
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
        addr.sin6_addr   = in6addr_any; // Constante de Linux para "cualquier IP" en IPv6
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
        if (socketFD < 0) std::cerr << "Error al crear el socket" << std::endl;
    }

    UDPSocket::~UDPSocket()
    {
        if (socketFD >= 0) close(socketFD);
    }

    bool UDPSocket::bind(int port)
    {
        int opt = 1;
        if (setsockopt(socketFD, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) std::perror("Error en setsockopt SO_REUSEADDR");
        if (setsockopt(socketFD, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) std::perror("Error en setsockopt SO_REUSEPORT");
        
        SocketAddrType addr = newAddress(port);

        if (::bind(socketFD, (struct sockaddr*)&addr, sizeof(addr)) < 0)
        {
            perror("ERROR EN BIND");
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
            inet_ntop(AF_INET6, &fromAddr.sin6_addr, ipStr, sizeof(ipStr));  // inet_ntoa es IPv4-only
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
        if (socketFD < 0) std::cerr << "Error al crear el socket" << std::endl;
    }

    TCPSocket::~TCPSocket()
    {
        if (socketFD >= 0) close(socketFD);
    }

    bool TCPSocket::listen(int port)
    {
        int opt = 1;
        if (setsockopt(socketFD, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) std::perror("Error en setsockopt SO_REUSEADDR");

        SocketAddrType addr = newAddress(port);

        if (::bind(socketFD, (struct sockaddr*)&addr, sizeof(addr)) < 0)
        {
            perror("ERROR EN BIND");
            return false;
        }

        if (::listen(socketFD, 10) < 0)
        {
            perror("ERROR EN ESCUCHAR");
            return false;
        }
        
        return true;
    }

    int TCPSocket::accept()
    {
        SocketAddrType clientAddr;
        socklen_t addrLen = sizeof(clientAddr);

        int clientFD = ::accept(socketFD, (struct sockaddr*)&clientAddr, &addrLen);
        if (clientFD < 0) std::perror("[TCPSocket] accept falló");
        return clientFD;
    }

    bool TCPSocket::connect(const std::string& address, int port)
    {
        addrinfo hints{}, *res = nullptr;
        hints.ai_family   = AF_FAMILY;
        hints.ai_socktype = SOCK_STREAM;

        if (getaddrinfo(address.c_str(), std::to_string(port).c_str(), &hints, &res) != 0 || !res)
        {
            std::cerr << "[TCPSocket] No se pudo resolver: " << address << std::endl;
            return false;
        }

        bool ok = ::connect(socketFD, res->ai_addr, res->ai_addrlen) == 0;
        if (!ok) perror("[TCPSocket] connect");
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
        if (recvAll(fd, &netLen, 4) != 4) return -1;
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