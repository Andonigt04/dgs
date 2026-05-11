#include "include/dgs/packet.h"

#include <cstring>
#include <stdexcept>

namespace DGS
{
    void Packet::pack(const EntityTransfer& data)
    {
        clear();

        write<uint8_t>(PKT_ENTITY_TRANSFER);
        write<uint8_t>(data.type);
        write<uint32_t>(data.uuid);
        write<int32_t>(data.chunkX);
        write<int32_t>(data.chunkY);
        write<int32_t>(data.chunkZ);
        write<float>(data.pos[0]);
        write<float>(data.pos[1]);
        write<float>(data.pos[2]);
        write<uint16_t>(data.angle);
        write<uint16_t>(data.dataSize);
        writeRaw(data.data, data.dataSize);
        write<uint32_t>(data.state);
    }

    EntityTransfer Packet::unpackEntityTransfer()
    {
        readPos = 1;
        EntityTransfer data;
        data.type = read<EntityType>();
        data.uuid = read<uint32_t>();
        data.chunkX = read<int32_t>();
        data.chunkY = read<int32_t>();
        data.chunkZ = read<int32_t>();
        data.pos[0] = read<float>();
        data.pos[1] = read<float>();
        data.pos[2] = read<float>();
        data.angle = read<uint16_t>();
        data.dataSize = read<uint16_t>();
        readRaw(data.data, data.dataSize);
        data.state = read<EntityState>();
        
        return data;
    }

    void Packet::pack(const Command& data)
    {
        clear();

        write<uint8_t>(PKT_COMMAND);
        write<HeadPurpose>(data.purpose);
        write<int32_t>(data.chunkX);
        write<int32_t>(data.chunkY);
        writeString(std::string(data.addr));
        write<int>(data.port);
    }

    Command Packet::unpackCommand()
    {
        readPos = 1;
        Command data;
        data.purpose = read<HeadPurpose>();
        data.chunkX = read<int32_t>();
        data.chunkY = read<int32_t>();

        std::string tempAddr = readString();
        std::strncpy(data.addr, tempAddr.c_str(), sizeof(data.addr) - 1);
        data.addr[sizeof(data.addr) - 1] = '\0'; 

        data.port = read<int>();
        return data;
    }

    void Packet::pack(const ServerMetrics& data)
    {
        clear();

        write<uint8_t>(PKT_METRICS);
        write<float>(data.ramUsage);
        write<float>(data.performance);
        write<int32_t>(data.node.chunkXMin);
        write<int32_t>(data.node.chunkXMax);
        write<int32_t>(data.node.chunkYMin);
        write<int32_t>(data.node.chunkYMax);
    }

    ServerMetrics Packet::unpackServerMetrics()
    {
        readPos = 1;
        ServerMetrics data;
        data.ramUsage = read<float>();
        data.performance = read<float>();
        data.node.chunkXMin = read<int32_t>();
        data.node.chunkXMax = read<int32_t>();
        data.node.chunkYMin = read<int32_t>();
        data.node.chunkYMax = read<int32_t>();
        
        return data;
    }

    
};