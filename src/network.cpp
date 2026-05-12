#include "include/dgs/network.h"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <iostream>

namespace DGS
{
    UDPSocket::UDPSocket()
    {
        socketFD = socket(AF_INET, SOCK_DGRAM, 0);
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
        
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        if (::bind(socketFD, (struct sockaddr*)&addr, sizeof(addr)) < 0)
        {
            perror("ERROR EN BIND");
            return false;
        }

        return true;
    }

    bool UDPSocket::send(const std::string& address, int port, const uint8_t* data, size_t size)
    {
        sockaddr_in destAddr{};
        destAddr.sin_family = AF_INET;
        destAddr.sin_port = htons(port);
        inet_pton(AF_INET, address.c_str(), &destAddr.sin_addr);

        ssize_t sent = sendto(socketFD, data, size, 0, (struct sockaddr*)&destAddr, sizeof(destAddr));
        
        return sent == (ssize_t)size;
    }

    int UDPSocket::receive(uint8_t* buffer, size_t size, std::string& outAddress, int& outPort)
    {
        sockaddr_in fromAddr{};
        socklen_t fromLen = sizeof(fromAddr);

        int bytesRecived = recvfrom(socketFD, buffer, size, 0, (struct sockaddr*)&fromAddr, &fromLen);
        if (bytesRecived >= 0)
        {
            outAddress = inet_ntoa(fromAddr.sin_addr);
            outPort = ntohs(fromAddr.sin_port);
        }

        return bytesRecived;
    }
    
    TCPSocket::TCPSocket()
    {
        socketFD = socket(AF_INET, SOCK_STREAM, 0);
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

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

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
        sockaddr_in clientAddr;
        socklen_t addrLen = sizeof(clientAddr);

        int clientFD = ::accept(socketFD, (struct sockaddr*)&clientAddr, &addrLen);
        if (clientFD < 0) std::perror("[TCPSocket] accept falló");
        return clientFD;
    }

    bool TCPSocket::connect(const std::string& address, int port)
    {
        addrinfo hints{}, *res = nullptr;
        hints.ai_family   = AF_INET;
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