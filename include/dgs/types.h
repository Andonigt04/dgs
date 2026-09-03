#ifndef DGS_TYPES_H
#define DGS_TYPES_H

#include <cstdint>

namespace DGS
{
    static constexpr uint8_t  MAX_ZONES       = 16;
    // Max lengths (see §4.6 of the plan): replaces the magic 16/4096.
    static constexpr uint32_t MAX_ADDR_LEN    = 16;    // "255.255.255.255"+null. IPv6 → 46 (see the HARUKA_USE_IPV6 build).
    static constexpr uint32_t MAX_ENTITY_DATA = 4096;  // max opaque payload of one entity.
    static constexpr uint32_t MAX_PACKET_SIZE = 65536; // datagram/packet cap (see §4.6 bug 6 and network.h).
    static constexpr uint64_t DEFAULT_LEASE_MS = 30000; // default lease of a zone (see §4.6 bug 1).
    static constexpr uint32_t MAX_REGION_BYTES = 4096;   // region blob inside a PKT_ZONE_REGION (§3.9, transport cap)

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
        // Packets added by the plan (PLAN_DGS_VALIDADOR): validation request/ack + telemetry to the master.
        PKT_VALIDATE_REQ    = 9,   // zone_node → validator  (claim vs predicted state, §2.2)
        PKT_VALIDATE_ACK    = 10,  // validator → zone_node  (correlated verdict, §2.2)
        PKT_VALIDATOR_STATUS= 11,  // zone_node → head_server (telemetry/lease, §2.2 and §4)
        PKT_SOCIAL_DELTA    = 12,  // guild/party deltas (§3.7)
        PKT_ACCOUNT         = 13,  // account ban/permissions (§3.7)
        PKT_DRAIN           = 14,  // lifecycle: request draining before destroying a zone (§3.9)
        PKT_DELETE_ZONE     = 15,  // lifecycle: confirm zone destruction (§3.9)
        PKT_REASSIGN        = 16,  // authority handoff: zone → head → new owning zone (§3.6)
        PKT_ZONE_REGION     = 17,  // region state blob (serializeRegion/mergeRegion, §3.9)
        // READ-ONLY OBSERVER (viewer/ops). Sent by UDP to a zone: "add me to your broadcast". It is a
        // 1-byte packet and carries nothing else on purpose — an observer must be unable to introduce
        // anything into the world. The zone answers with the SAME stream it already sends its clients
        // (entities + ghosts), keeps observers in their own registry, and expires them on a lease so a
        // viewer that goes away stops being fed.
        PKT_OBSERVE         = 18,
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
        float speed[3];
        float health;
        float baseDMG;
        float healing;
    };

    // EntityTransfer struct to hold object relevant information for anti-cheat purposes and transfering data through servers
    struct EntityTransfer
    {
        uint32_t    uuid;
        float       pos[3];
        int32_t     chunkX, chunkY, chunkZ;
        uint16_t    angle;
        EntityType  type;
        EntityState state;
        Stats       stats;
        uint16_t    dataSize;
        uint8_t     data[MAX_ENTITY_DATA];
    };

    struct Command
    {
        HeadPurpose purpose;
        int32_t     chunkX, chunkY;
        char        addr[MAX_ADDR_LEN];
        int         port;
        float       chunkSizeX, chunkSizeY, chunkSizeZ;
    };

    struct ZoneInfoPublic {
        int32_t chunkXMin, chunkXMax;
        int32_t chunkYMin, chunkYMax;
        int32_t chunkZMin, chunkZMax;
        char    addr[MAX_ADDR_LEN];
        int     port;
    };

    // A zone as the ORCHESTRATOR sees it (local): a ZoneInfoPublic (what travels / is queried) plus the
    // `fd` of the control socket to the node plus its liveness state (see §3.9). ⚠️ The base is declared
    // BEFORE: you cannot derive from an incomplete type. `fd` does NOT go on the wire
    // (`pack(ServerMetrics)` only serialises bounds/addr/port, see packet.cpp) → this change does NOT
    // affect the network format.
    struct ZoneInfo : public ZoneInfoPublic
    {
        int fd;
    };

    // Metrics the node reports to the orchestrator (§4 of the plan). New fields go AT THE END so the
    // existing ones do not shift. ⚠️ Adding fields changes the wire layout → recompile pack/unpack
    // together (P0, §4.5). Counters are MONOTONIC since start-up (§4.1).
    struct ServerMetrics
    {
        ZoneInfo node;          // zones (by bounds + addr:port; `fd` does not travel)
        float    ramUsage;      // 0..1
        float    performance;   // 0..1
        uint64_t startTimeS;    // node start epoch (tells a fresh node from a healthy one, §4.6 bug 5)
        uint64_t bytesRx;       // bytes received since start-up
        uint64_t bytesTx;       // bytes sent since start-up
        uint32_t failedTransfers; // failed transfers/validations (timeout or error)
        uint32_t activeEntities;  // entities served (scaling heuristic)
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
        char addr[MAX_ADDR_LEN];
        int  port;
    };

    // Authority handoff (§3.6): an entity (or region) changes owning zone. The zone_node that was
    // simulating it sends it to the head; the head routes it to the new zone covering its chunk. The new
    // owner promotes it (ghost→real) and STARTS simulating it; the old one stops (lease).
    struct EntityReassign
    {
        uint64_t entityUuid;
        int32_t  chunkX, chunkY, chunkZ;
        uint32_t fromZone;      // zone that gave it up (debug/audit)
        uint32_t toZone;        // destination zone (0 = let the head resolve it by chunk)
    };

    // Zone lifecycle (§3.9). The same struct carries BOTH signals:
    //   · PKT_DRAIN:        orchestrator → zone  (ack=0: "drain, you are being retired"; zone → orchestrator ack=1)
    //   · PKT_DELETE_ZONE:  orchestrator → zone  (draining finished: destroy your zone and exit)
    // `requestId` correlates the request with its ack (timeout/fail-safe in the orchestrator).
    struct ZoneLifecycle
    {
        uint32_t requestId;     // seq of the lifecycle operation
        uint8_t  ack;           // 0 = request, 1 = the node's confirmation
    };

    // Serialised region state (merge/handoff §3.9): the ceding zone extracts its region's authoritative
    // state with `serializeRegion` and sends it (→ head → new zone), which folds it in with
    // `mergeRegion`. `chunkX/Y/Z` is the anchor the head uses to route to the node covering it.
    struct ZoneRegion
    {
        int32_t  chunkX, chunkY, chunkZ;   // region anchor (metres → which node covers it)
        uint32_t srcZone;                  // ceding node (debug/audit)
        uint32_t size;                     // valid bytes in data[]
        uint8_t  data[MAX_REGION_BYTES];   // opaque blob, the module's format
    };

    // A zone's liveness state (see §3.9 of the plan): the orchestrator keeps it alongside the ZoneInfo.
    // It does NOT go on the wire: it is local bookkeeping.
    enum class ZoneState : uint8_t
    {
        PROVISIONING = 0,   // spawn in flight, no first ServerMetrics yet
        READY,              // serving and registered in activeZones
        DRAINING,           // being handed over (scale-down / death), §3.9
        DEAD,               // lease expired, to be reassigned/cleaned up
        DESTROYED           // pod removed, entry to be purged
    };

    // --- Validation request/ack (PLAN_DGS_VALIDADOR §2.2) ------------------------------------------------
    // The validator does NOT re-simulate: it compares the client's claim against the state PREDICTED by
    // the owning zone (same rule as §3.6). `requestId` is a per-sender seq → idempotency + anti-replay (§2.3).
    struct ValidateRequest
    {
        uint32_t requestId;     // per-sender seq (anti-replay)
        uint64_t entityUuid;    // entity/op to validate
        uint32_t ownerZone;     // zone that owns the simulation (the one predicting)
        uint8_t  moduleId;      // expected rules module (GAME_MODULE_SO)
        uint8_t  kind;          // 0=move, 1=action (critical verbs → fail-closed)
        // Opaque payload (kind=0 MOVEMENT): the state PREDICTED by the zone plus the client's claim.
        // The owning zone does NOT re-simulate; the validator compares this claim against its own
        // predictor plus an RTT tolerance.
        EntityTransfer entity;   // current (predicted) entity state
        float lastGX, lastGY, lastGZ;  // previous GLOBAL position known to the zone
        float maxSpeed;          // the player's limit (Stats.speed[0])
        float dtSeconds;         // time between the last position and the claim
    };

    struct ValidateAck
    {
        uint32_t requestId;
        int8_t   verdict;       // 1 = plausible, 0 = violation
        uint16_t weight;        // weight of the violation (0 when verdict = 1)
    };

    struct ValidatorStatus
    {
        int8_t   state;         // 0=UP 1=DEGRADED 2=DOWN/OPEN (circuit breaker §2.3)
        uint32_t reqSent;       // validations requested
        uint32_t reqTimeout;    // timeouts (→ the node's failedTransfers)
        uint64_t bytesRecv;     // validation bytes received
        uint32_t failedTransfers;
        uint32_t activeEntities; // entities served by the reporting node
        uint64_t timestampMs;    // epoch of the report
    };

    enum DirtyFlag : uint32_t
    {
        DIRTY_TRANSFORM = 1 << 0,  // pos + rot
        DIRTY_STATS     = 1 << 1,
        DIRTY_INVENTORY = 1 << 2,
    };

    static constexpr uint16_t MAX_GHOST_DATA = 4096;

    // ⚠️ `alignas` goes AFTER `struct`: written in front of it the compiler IGNORES it (-Wattributes)
    // and the struct kept alignment 8. sizeof is already a multiple of 16 (4176) → aligning it does NOT
    // change the layout or the wire format, it just actually aligns.
    struct alignas(16) GhostDelta
    {
        uint64_t uuid;
        int32_t  chunkX, chunkY, chunkZ;
        uint32_t dirtyMask;
        float    pos[3];              // local within the chunk, metres
        float    rot[4];              // quaternion xyzw
        Stats    stats;
        uint16_t dataSize;            // valid bytes in data[]
        uint8_t  data[MAX_GHOST_DATA]; // opaque payload defined by the engine
    };

    struct ChatMessage
    {
        uint32_t uuid;
        char     username[32];
        char     text[256];
        uint8_t  channel;        // ChatChannel (§3.7): local/guild/trade/global
        uint64_t seq;            // sequence number in the channel (GhostDelta-style fan-out, anti-replay)
        uint32_t timestampMs;    // message epoch (rate limit + moderation in the chat service)
    };

    // §3.7: chat channel. `local` does go through `zone_node` (spatial interest); the rest is routed by
    // the chat service (head/social node) with subscription fan-out, not proximity.
    enum ChatChannel : uint8_t
    {
        CHAT_LOCAL  = 0,   // spatial interest (owning zone)
        CHAT_GUILD  = 1,   // guild membership (social node)
        CHAT_TRADE  = 2,   // economy (rate limit + moderation)
        CHAT_GLOBAL = 3
    };

    // §3.7: kind of social delta (guild/party). The state is small and travels as EVENTS to the ONLINE
    // members (the UI subscribes, it does not poll). Source of truth: persistance_node (MongoDB).
    enum SocialKind : uint8_t
    {
        SOCIAL_GUILD_JOIN    = 0,   // targetUuid joins scopeUuid (guildId)
        SOCIAL_GUILD_LEAVE   = 1,   // targetUuid leaves
        SOCIAL_GUILD_RANK    = 2,   // targetUuid's rank changes (rank)
        SOCIAL_GUILD_DISBAND = 3,   // scopeUuid is disbanded
        SOCIAL_PARTY_JOIN    = 4,   // scopeUuid's party
        SOCIAL_PARTY_LEAVE   = 5,
        SOCIAL_FRIEND_ADD    = 6,   // relationship between targetUuid and scopeUuid
        SOCIAL_FRIEND_REMOVE = 7,
        SOCIAL_ZONE_UPDATE   = 8    // the member's zoneId (for routing/state)
    };

    // §3.7: guild/party delta (PKT_SOCIAL_DELTA). A single social node writes (rule 1 of §3.7: one
    // owner per data type); zones and the head only read ids.
    struct SocialDelta
    {
        uint32_t targetUuid;   // affected member
        uint32_t scopeUuid;    // guildId / partyId (0 = personal/friends)
        uint8_t  kind;         // SocialKind
        uint8_t  rank;         // new rank (SOCIAL_GUILD_RANK)
        int32_t  zoneId;       // the member's zone id (SOCIAL_ZONE_UPDATE)
        uint64_t seq;          // ordering per channel/event (anti-replay)
    };

    // §3.7: ACCOUNT actions (PKT_ACCOUNT): ban/permissions. A ban escalated from suspicion materialises
    // here (validator → head → social node → every zone sees it: "uuid banned" → entry blocked).
    enum AccountActionKind : uint8_t
    {
        ACC_BAN      = 0,   // ban an account (optionally with a duration)
        ACC_UNBAN    = 1,   // lift a ban
        ACC_SET_PERM = 2    // change permissions
    };

    struct AccountAction
    {
        uint32_t actorUuid;    // who performs it (admin/moderator, permission-checked)
        uint32_t targetUuid;   // affected account
        uint8_t  action;       // AccountActionKind
        uint32_t permFlags;    // permisos (ACC_SET_PERM)
        uint32_t durationS;    // 0 = permanent (ACC_BAN)
        char     reason[64];   // reason (moderation/log)
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