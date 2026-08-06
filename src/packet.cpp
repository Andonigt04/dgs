#include "include/dgs/packet.h"
#include "include/dgs/types.h"

#include <cstring>
#include <stdexcept>

namespace DGS
{
    void Packet::pack(const EntityTransfer& data)
    {
        clear();

        write<PacketType>(PKT_ENTITY_TRANSFER);
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
        write<EntityState>(data.state);
        write<Stats>(data.stats);
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
        data.stats = read<Stats>();
        
        return data;
    }

    void Packet::pack(const Command& data)
    {
        clear();

        write<PacketType>(PKT_COMMAND);
        write<HeadPurpose>(data.purpose);
        write<int32_t>(data.chunkX);
        write<int32_t>(data.chunkY);
        writeString(std::string(data.addr));
        write<int>(data.port);
        write<float>(data.chunkSizeX);
        write<float>(data.chunkSizeY);
        write<float>(data.chunkSizeZ);
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

        data.port       = read<int>();
        data.chunkSizeX = read<float>();
        data.chunkSizeY = read<float>();
        data.chunkSizeZ = read<float>();

        return data;
    }

    void Packet::pack(const ServerMetrics& data)
    {
        clear();

        write<PacketType>(PKT_METRICS);
        write<float>(data.ramUsage);
        write<float>(data.performance);
        write<int32_t>(data.node.chunkXMin);
        write<int32_t>(data.node.chunkXMax);
        write<int32_t>(data.node.chunkYMin);
        write<int32_t>(data.node.chunkYMax);
        write<int32_t>(data.node.chunkZMin);
        write<int32_t>(data.node.chunkZMax);
        writeString(std::string(data.node.addr));
        write<int>(data.node.port);
        // Campos nuevos del plan (§4): monotónicos desde arranque → distintos de 0 para un nodo sano.
        write<uint64_t>(data.startTimeS);
        write<uint64_t>(data.bytesRx);
        write<uint64_t>(data.bytesTx);
        write<uint32_t>(data.failedTransfers);
        write<uint32_t>(data.activeEntities);
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
        data.node.chunkZMin = read<int32_t>();
        data.node.chunkZMax = read<int32_t>();
        std::string addr = readString();
        std::strncpy(data.node.addr, addr.c_str(), sizeof(data.node.addr) - 1);
        data.node.port = read<int>();
        data.startTimeS = read<uint64_t>();
        data.bytesRx = read<uint64_t>();
        data.bytesTx = read<uint64_t>();
        data.failedTransfers = read<uint32_t>();
        data.activeEntities = read<uint32_t>();

        return data;
    }

    void Packet::pack(const ZoneQuery& data)
    {
        clear();

        write<PacketType>(PKT_ZONE_QUERY);
        write<uint32_t>(data.uuid);
        write<int32_t>(data.chunkX);
        write<int32_t>(data.chunkY);
        write<int32_t>(data.chunkZ);
    }

    ZoneQuery Packet::unpackZoneQuery()
    {
        readPos = 1;
        ZoneQuery data;
        data.uuid   = read<uint32_t>();
        data.chunkX = read<int32_t>();
        data.chunkY = read<int32_t>();
        data.chunkZ = read<int32_t>();

        return data;
    }

    void Packet::pack(const ZoneResponse& data)
    {
        clear();
        write<PacketType>(PKT_ZONE_RESPONSE);
        writeString(std::string(data.addr));
        write<int>(data.port);
    }

    ZoneResponse Packet::unpackZoneResponse()
    {
        readPos = 1;
        ZoneResponse data{};
        std::string addr = readString();
        std::strncpy(data.addr, addr.c_str(), sizeof(data.addr) - 1);
        data.port = read<int>();

        return data;
    }

    void Packet::pack(const ZoneListResponse& data)
    {
        clear();

        write<PacketType>(PKT_ZONE_LIST);
        write(data.count);
        for (int i = 0; i < data.count; i++)
            write(data.zones[i]);
    } 

    ZoneListResponse Packet::unpackZoneListResponse()
    {
        readPos = 1;
        ZoneListResponse data{};
        data.count = read<uint8_t>();
        for (int i = 0; i < data.count; i++)
            data.zones[i] = read<ZoneInfoPublic>();

        return data;
    }

    void Packet::pack(const GhostDelta& data)
    {
        clear();
        write<PacketType>(PKT_GHOST_DELTA);
        write<uint64_t>(data.uuid);
        write<int32_t>(data.chunkX);
        write<int32_t>(data.chunkY);
        write<int32_t>(data.chunkZ);
        write<uint32_t>(data.dirtyMask);

        if (data.dirtyMask & DIRTY_TRANSFORM)
        {
            write<float>(data.pos[0]);
            write<float>(data.pos[1]);
            write<float>(data.pos[2]);
            write<float>(data.rot[0]);
            write<float>(data.rot[1]);
            write<float>(data.rot[2]);
            write<float>(data.rot[3]);
        }

        if (data.dirtyMask & DIRTY_STATS)
            write<Stats>(data.stats);

        if (data.dirtyMask & DIRTY_INVENTORY)
        {
            write<uint16_t>(data.dataSize);
            writeRaw(data.data, data.dataSize);
        }
    }

    GhostDelta Packet::unpackGhostDelta()
    {
        readPos = 1;
        GhostDelta data{};
        data.uuid      = read<uint64_t>();
        data.chunkX    = read<int32_t>();
        data.chunkY    = read<int32_t>();
        data.chunkZ    = read<int32_t>();
        data.dirtyMask = read<uint32_t>();

        if (data.dirtyMask & DIRTY_TRANSFORM)
        {
            data.pos[0] = read<float>();
            data.pos[1] = read<float>();
            data.pos[2] = read<float>();
            data.rot[0] = read<float>();
            data.rot[1] = read<float>();
            data.rot[2] = read<float>();
            data.rot[3] = read<float>();
        }

        if (data.dirtyMask & DIRTY_STATS)
            data.stats = read<Stats>();

        if (data.dirtyMask & DIRTY_INVENTORY)
        {
            data.dataSize = read<uint16_t>();
            readRaw(data.data, data.dataSize);
        }

        return data;
    }

    void Packet::pack(const ChatMessage& data)
    {
        clear();
        write<PacketType>(PKT_CHAT);
        write<uint32_t>(data.uuid);
        writeString(std::string(data.username));
        writeString(std::string(data.text));
        // §3.7: canal + seq + época (rate-limit y orden por canal en el servicio de chat).
        write<uint8_t>(data.channel);
        write<uint64_t>(data.seq);
        write<uint32_t>(data.timestampMs);
    }

    ChatMessage Packet::unpackChatMessage()
    {
        readPos = 1;
        ChatMessage data{};
        data.uuid = read<uint32_t>();
        std::string uname = readString();
        std::strncpy(data.username, uname.c_str(), sizeof(data.username) - 1);
        std::string text = readString();
        std::strncpy(data.text, text.c_str(), sizeof(data.text) - 1);
        data.channel     = read<uint8_t>();
        data.seq         = read<uint64_t>();
        data.timestampMs = read<uint32_t>();
        return data;
    }

    void Packet::pack(const ValidateRequest& data)
    {
        clear();
        write<PacketType>(PKT_VALIDATE_REQ);
        write<uint32_t>(data.requestId);
        write<uint64_t>(data.entityUuid);
        write<uint32_t>(data.ownerZone);
        write<uint8_t>(data.moduleId);
        write<uint8_t>(data.kind);
        // payload opaco (kind=0 MOVIMIENTO): estado predicho por la zona + afirmación del cliente
        writeRaw(reinterpret_cast<const uint8_t*>(&data.entity), sizeof(DGS::EntityTransfer));
        write<float>(data.lastGX);
        write<float>(data.lastGY);
        write<float>(data.lastGZ);
        write<float>(data.maxSpeed);
        write<float>(data.dtSeconds);
    }

    ValidateRequest Packet::unpackValidateRequest()
    {
        readPos = 1;
        ValidateRequest data{};
        data.requestId = read<uint32_t>();
        data.entityUuid = read<uint64_t>();
        data.ownerZone = read<uint32_t>();
        data.moduleId = read<uint8_t>();
        data.kind = read<uint8_t>();
        readRaw(reinterpret_cast<uint8_t*>(&data.entity), sizeof(DGS::EntityTransfer));
        data.lastGX = read<float>();
        data.lastGY = read<float>();
        data.lastGZ = read<float>();
        data.maxSpeed = read<float>();
        data.dtSeconds = read<float>();
        return data;
    }

    void Packet::pack(const ValidateAck& data)
    {
        clear();
        write<PacketType>(PKT_VALIDATE_ACK);
        write<uint32_t>(data.requestId);
        write<int8_t>(data.verdict);
        write<uint16_t>(data.weight);
    }

    ValidateAck Packet::unpackValidateAck()
    {
        readPos = 1;
        ValidateAck data{};
        data.requestId = read<uint32_t>();
        data.verdict = read<int8_t>();
        data.weight = read<uint16_t>();
        return data;
    }

    void Packet::pack(const ValidatorStatus& data)
    {
        clear();
        write<PacketType>(PKT_VALIDATOR_STATUS);
        write<int8_t>(data.state);
        write<uint32_t>(data.reqSent);
        write<uint32_t>(data.reqTimeout);
        write<uint64_t>(data.bytesRecv);
        write<uint32_t>(data.failedTransfers);
        write<uint32_t>(data.activeEntities);
        write<uint64_t>(data.timestampMs);
    }

    ValidatorStatus Packet::unpackValidatorStatus()
    {
        readPos = 1;
        ValidatorStatus data{};
        data.state = read<int8_t>();
        data.reqSent = read<uint32_t>();
        data.reqTimeout = read<uint32_t>();
        data.bytesRecv = read<uint64_t>();
        data.failedTransfers = read<uint32_t>();
        data.activeEntities = read<uint32_t>();
        data.timestampMs = read<uint64_t>();
        return data;
    }

    void Packet::pack(const EntityReassign& data)
    {
        clear();
        write<PacketType>(PKT_REASSIGN);
        write<uint64_t>(data.entityUuid);
        write<int32_t>(data.chunkX);
        write<int32_t>(data.chunkY);
        write<int32_t>(data.chunkZ);
        write<uint32_t>(data.fromZone);
        write<uint32_t>(data.toZone);
    }

    EntityReassign Packet::unpackEntityReassign()
    {
        readPos = 1;
        EntityReassign data{};
        data.entityUuid = read<uint64_t>();
        data.chunkX = read<int32_t>();
        data.chunkY = read<int32_t>();
        data.chunkZ = read<int32_t>();
        data.fromZone = read<uint32_t>();
        data.toZone = read<uint32_t>();
        return data;
    }

    void Packet::pack(const ZoneLifecycle& data)
    {
        clear();
        write<PacketType>(PKT_DRAIN);          // mismo struct para PKT_DRAIN y PKT_DELETE_ZONE
        write<uint32_t>(data.requestId);
        write<uint8_t>(data.ack);
    }

    ZoneLifecycle Packet::unpackZoneLifecycle()
    {
        readPos = 1;
        ZoneLifecycle data{};
        data.requestId = read<uint32_t>();
        data.ack = read<uint8_t>();
        return data;
    }

    void Packet::pack(const ZoneRegion& data)
    {
        clear();
        write<PacketType>(PKT_ZONE_REGION);
        write<int32_t>(data.chunkX);
        write<int32_t>(data.chunkY);
        write<int32_t>(data.chunkZ);
        write<uint32_t>(data.srcZone);
        write<uint32_t>(data.size);
        writeRaw(data.data, data.size);
    }

    ZoneRegion Packet::unpackZoneRegion()
    {
        readPos = 1;
        ZoneRegion data{};
        data.chunkX = read<int32_t>();
        data.chunkY = read<int32_t>();
        data.chunkZ = read<int32_t>();
        data.srcZone = read<uint32_t>();
        data.size = read<uint32_t>();
        if (data.size > sizeof(data.data)) throw std::runtime_error("ZoneRegion overflow");
        readRaw(data.data, data.size);
        return data;
    }

    void Packet::packDelete(const ZoneLifecycle& data)
    {
        clear();
        write<PacketType>(PKT_DELETE_ZONE);
        write<uint32_t>(data.requestId);
        write<uint8_t>(data.ack);
    }

    // §3.7: delta social de guild/party (PKT_SOCIAL_DELTA).
    void Packet::pack(const SocialDelta& data)
    {
        clear();
        write<PacketType>(PKT_SOCIAL_DELTA);
        write<uint32_t>(data.targetUuid);
        write<uint32_t>(data.scopeUuid);
        write<uint8_t>(data.kind);
        write<uint8_t>(data.rank);
        write<int32_t>(data.zoneId);
        write<uint64_t>(data.seq);
    }

    SocialDelta Packet::unpackSocialDelta()
    {
        readPos = 1;
        SocialDelta data{};
        data.targetUuid = read<uint32_t>();
        data.scopeUuid  = read<uint32_t>();
        data.kind       = read<uint8_t>();
        data.rank       = read<uint8_t>();
        data.zoneId     = read<int32_t>();
        data.seq        = read<uint64_t>();
        return data;
    }

    // §3.7: acción de cuenta (PKT_ACCOUNT): ban/permisos.
    void Packet::pack(const AccountAction& data)
    {
        clear();
        write<PacketType>(PKT_ACCOUNT);
        write<uint32_t>(data.actorUuid);
        write<uint32_t>(data.targetUuid);
        write<uint8_t>(data.action);
        write<uint32_t>(data.permFlags);
        write<uint32_t>(data.durationS);
        writeRaw(reinterpret_cast<const uint8_t*>(data.reason), sizeof(data.reason));
    }

    AccountAction Packet::unpackAccountAction()
    {
        readPos = 1;
        AccountAction data{};
        data.actorUuid = read<uint32_t>();
        data.targetUuid = read<uint32_t>();
        data.action     = read<uint8_t>();
        data.permFlags  = read<uint32_t>();
        data.durationS  = read<uint32_t>();
        readRaw(reinterpret_cast<uint8_t*>(data.reason), sizeof(data.reason));
        return data;
    }
};