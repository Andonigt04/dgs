#ifndef DGS_TYPES_H
#define DGS_TYPES_H

#include <cstdint>

namespace DGS
{
    static constexpr uint8_t MAX_ZONES = 16;

    enum PacketType : uint8_t
    {
        PKT_NONE            = 0,
        PKT_METRICS         = 1,
        PKT_COMMAND         = 2,
        PKT_ENTITY_TRANSFER = 3,
        PKT_CHAT            = 4,
        PKT_ZONE_QUERY      = 5,
        PKT_ZONE_RESPONSE   = 6,
        PKT_ZONE_LIST       = 7,
        PKT_GHOST_DELTA     = 8,
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

    enum class NeighborMode
    {
        FACE,
        FACE_EDGE,
        FACE_EDGE_CORNER
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
        EntityType  type;
        uint32_t    uuid;
        int32_t     chunkX, chunkY, chunkZ;
        float       pos[3];
        uint16_t    angle;
        uint16_t    dataSize;
        uint8_t     data[4096];
        EntityState state;
        Stats       stats;
    };

    struct Command
    {
        HeadPurpose purpose;
        int32_t     chunkX, chunkY;
        char        addr[16];
        int         port;
        float       chunkSizeX, chunkSizeY, chunkSizeZ;
    };

    struct ZoneInfo {
        int     fd;
        int32_t chunkXMin, chunkXMax;
        int32_t chunkYMin, chunkYMax;
        int32_t chunkZMin, chunkZMax;
        char    addr[16];
        int     port;
    };

    struct ZoneInfoPublic {
        int32_t chunkXMin, chunkXMax;
        int32_t chunkYMin, chunkYMax;
        int32_t chunkZMin, chunkZMax;
        char    addr[16];
        int     port;
    };

    struct ServerMetrics
    {
        ZoneInfo node;
        float    ramUsage;
        float    performance;
    };

    struct ZoneQuery
    {
        uint32_t uuid;
        int32_t  chunkX, chunkY, chunkZ;
    };


    struct ZoneListResponse {
        uint8_t        count;
        ZoneInfoPublic zones[MAX_ZONES];
    };

    struct ZoneResponse
    {
        char addr[16];
        int  port;
    };

    enum DirtyFlag : uint32_t
    {
        DIRTY_TRANSFORM = 1 << 0,  // pos + rot
        DIRTY_STATS     = 1 << 1,
        DIRTY_INVENTORY = 1 << 2,
    };

    static constexpr uint16_t MAX_GHOST_DATA = 4096;

    struct GhostDelta
    {
        uint64_t uuid;
        int32_t  chunkX, chunkY, chunkZ;
        uint32_t dirtyMask;
        float    pos[3];              // local dentro del chunk, metros
        float    rot[4];              // quaternion xyzw
        Stats    stats;
        uint16_t dataSize;            // bytes válidos en data[]
        uint8_t  data[MAX_GHOST_DATA]; // payload opaco definido por el engine
    };

    struct LogEntry
    {
        uint64_t   time_stamp;
        LogType    type;
        EntityType entityType;
        uint32_t   uuid;
        int32_t    fd;
        uint32_t   bytes;
        float      ramUsage;
        float      performance;
    };
};

#endif // DGS_TYPES_H