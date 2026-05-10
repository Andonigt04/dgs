#include "include/dgs/network.h"

#include <sys/socket.h>
#include <arpa/inet.h>
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
        if (setsockopt(socketFD, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) std::perror("Error en setsockopt SO_REUSEADDR");

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

        return clientFD;
    }

    bool TCPSocket::connect(const std::string& address, int port)
    {
        sockaddr_in servAddr;
        servAddr.sin_family = AF_INET;
        servAddr.sin_port = htons(port);
        inet_pton(AF_INET, address.c_str(), &servAddr.sin_addr);

        return ::connect(socketFD, (struct sockaddr*)&servAddr, sizeof(servAddr)) == 0;
    }

    bool TCPSocket::send(int fd, const uint8_t* data, size_t size)
    {
        ssize_t sent = ::send(fd, data, size, 0);
        return sent == (ssize_t)size;
    }

    int TCPSocket::receive(int fd, uint8_t* buffer, size_t size)
    {
        return ::recv(fd, buffer, size, 0);
    }

    void TCPSocket::closeClient(int fd)
    {
        close(fd);
    }
};