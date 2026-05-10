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

            bool bind(int port);

            bool send(const std::string& address, int port, const uint8_t* data, size_t size);
            int receive(uint8_t* buffer, size_t size, std::string& outAddress, int& outPort);
        private:
            int socketFD;
    };

    class TCPSocket
    {
        public:
            TCPSocket();
            ~TCPSocket();

            bool listen(int port);
            int accept();

            bool connect(const std::string& address, int port);

            bool send(int fd, const uint8_t* data, size_t size);
            int receive(int fd, uint8_t* buffer, size_t size);

            void closeClient(int fd);

            int getSocketFD() { return socketFD; }
        private:
            int socketFD;
    };
};

#endif