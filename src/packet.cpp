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
        write<float>(data.pos[0]);
        write<float>(data.pos[1]);
        write<float>(data.pos[2]);
        write<uint16_t>(data.angle);
        write<uint8_t>(data.data);
        write<uint32_t>(data.state);
    }

    EntityTransfer Packet::unpackEntityTransfer()
    {
        readPos = 1;
        EntityTransfer data;
        data.type = read<EntityType>();
        data.uuid = read<uint32_t>();
        data.pos[0] = read<float>();
        data.pos[1] = read<float>();
        data.pos[2] = read<float>();
        data.angle = read<uint16_t>();
        data.data = read<uint8_t>();
        data.state = read<EntityState>();
        
        return data;
    }

    void Packet::pack(const Command& data)
    {
        clear();

        write<uint8_t>(PKT_COMMAND);
        write<HeadPurpose>(data.purpose);
        write<double>(data.pos[0]);
        write<double>(data.pos[1]);
        write<double>(data.pos[2]);
        writeString(std::string(data.addr));
        write<int>(data.port);
    }

    Command Packet::unpackCommand()
    {
        readPos = 1;
        Command data;
        data.purpose = read<HeadPurpose>();
        data.pos[0] = read<double>();
        data.pos[1] = read<double>();
        data.pos[2] = read<double>();

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
        write<double>(data.node.xMin);
        write<double>(data.node.xMax);
        write<double>(data.node.yMin);
        write<double>(data.node.yMax);
    }

    ServerMetrics Packet::unpackServerMetrics()
    {
        readPos = 1;
        ServerMetrics data;
        data.ramUsage = read<float>();
        data.performance = read<float>();
        data.node.xMin = read<double>();
        data.node.xMax = read<double>();
        data.node.yMin = read<double>();
        data.node.yMax = read<double>();
        
        return data;
    }

    
};