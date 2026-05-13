#ifndef DGS_PACKET_H
#define DGS_PACKET_H

#include <vector>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <stdexcept>

#include "include/dgs/types.h"

namespace DGS
{

    class Packet
    {

    public:

        Packet() : readPos(0) {}

        template<typename T>
        void write(T data)
        {
            const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&data);
            buffer.insert(buffer.end(), ptr, ptr + sizeof(T));
        }

        template<typename T>
        T read()
        {
            if (readPos + sizeof(T) > buffer.size()) throw std::runtime_error("Packet read overflow");

            T data;
            std::memcpy(&data, &buffer[readPos], sizeof(T));
            readPos += sizeof(T);

            return data;
        }

        void writeRaw(const uint8_t* data, size_t size)
        {
            buffer.insert(buffer.end(), data, data + size);
        }

        void readRaw(uint8_t* dest, size_t size)
        {
            if (readPos + size > buffer.size()) throw std::runtime_error("Packet read overflow (Raw)");
            std::memcpy(dest, &buffer[readPos], size);
            readPos += size;
        }

        void writeString(const std::string& str)
        {
            write<uint16_t>(static_cast<uint16_t>(str.size()));
            buffer.insert(buffer.end(), str.begin(), str.end());
        }

        std::string readString()
        {
            uint16_t size = read<uint16_t>();
            if (readPos + size > buffer.size()) throw std::runtime_error("String read overflow");

            std::string str(buffer.begin() + readPos, buffer.begin() + readPos + size);
            readPos += size;

            return str;
        }

        void clear() { buffer.clear(); readPos = 0; }

        void pack(const EntityTransfer& data);
        void pack(const Command& data);
        void pack(const ServerMetrics& data);
        void pack(const ZoneQuery& data);
        void pack(const ZoneResponse& data);
        void pack(const ZoneListResponse& data);
        void pack(const GhostDelta& data);
        void pack(const PacketType& t) { clear(); write<PacketType>(t); }

        EntityTransfer unpackEntityTransfer();
        Command unpackCommand();
        ServerMetrics unpackServerMetrics();
        ZoneQuery unpackZoneQuery();
        ZoneResponse unpackZoneResponse();
        ZoneListResponse unpackZoneListResponse();
        GhostDelta unpackGhostDelta();
        PacketType unpackPacketType() { PacketType data; data = read<PacketType>(); return data; };
        
        PacketType getType() const { return buffer.empty() ? static_cast<PacketType>(0) : static_cast<PacketType>(buffer[0]); }
        
        const uint8_t* getRawData() const { return buffer.data(); }
        size_t getSize() const { return buffer.size(); }
        void setBuffer(const uint8_t* data, size_t size) { buffer.assign(data, data + size); }

    private:
        std::vector<uint8_t> buffer;
        size_t readPos;
    };

    using PacketHandler = std::function<void(int, Packet&)>;

    class PacketDispatcher
    {
        public:
            void registerHandler(PacketType type, PacketHandler handler) {
                handlers[type] = handler;
            }

            void dispatch(int fd, Packet& p) {
                PacketType type = p.getType();
                if (handlers.count(type)) {
                    handlers[type](fd, p);
                }
            }

        private:
            std::map<PacketType, PacketHandler> handlers;
    };
};

#endif // DGS_PACKET_H