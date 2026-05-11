#ifndef DGS_TYPES_H
#define DGS_TYPES_H

#include <cstdint>

namespace DGS
{
    enum PacketType : uint8_t
    {
        PKT_NONE            = 0,
        PKT_METRICS         = 1,
        PKT_COMMAND         = 2,
        PKT_ENTITY_TRANSFER = 3,
        PKT_CHAT            = 4,
        PKT_DISCONNECT      = 255
    };
    
    enum EntityType : uint8_t
    {
        ENT_PLAYER = 0,
        ENT_ITEM   = 1,
        ENT_NPC    = 2
    };

    // Entity state save like rwx = 7 etc
    enum EntityState : uint32_t
    {
        STATE_MOVING        = 1 << 0,
        STATE_PHYSICS_OWNED = 1 << 1,
        STATE_INTERACTABLE  = 1 << 2,
        STATE_DESTRUCTIBLE  = 1 << 3,
        STATE_EMITTING      = 1 << 4,
        STATE_CONSUMABLE    = 1 << 5
    };

    enum HeadPurpose : uint8_t
    {
        CMD_CREAT_SERVER    = 1 << 0,
        CMD_REMOVE_SERVER   = 1 << 1,
        CMD_TRANSFER_SERVER = 1 << 2
    };

    enum LogType : uint8_t
    {
        LOG_TRANSFER = 0,
        LOG_ERROR    = 1,
        LOG_METRICS  = 2
    };

    struct Stats
    {
        float health;
        float speed[3];
        float baseDMG;
        float healing;
    };

    // EntityTransfer struct to hold object relevant information for anti-cheat purposes and transfering data through servers
    struct EntityTransfer
    {
        EntityType type;
        uint32_t uuid;
        float pos[3];
        uint16_t angle;
        uint16_t dataSize;
        uint8_t data[4096];
        EntityState state;
    };

    struct Command
    {
        HeadPurpose purpose;
        double pos[3];
        char addr[16];
        int port;
    };

    struct ZoneInfo {
        int fd;
        double xMin, xMax, yMin, yMax;
    };

    struct ServerMetrics
    {
        ZoneInfo node;
        float ramUsage;
        float performance;
        double areaCenter[3];
    };

    struct LogEntry
    {
        uint64_t time_stamp;
        LogType type;
        EntityType entityType;
        uint32_t uuid;
        int32_t fd;
        uint32_t bytes;
        float ramUsage;
        float performance;
    };
};

#endif // DGS_TYPES_H