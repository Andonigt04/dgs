#ifndef DGS_TYPES_H
#define DGS_TYPES_H

#include <cstdint>

namespace DGS
{
    static constexpr uint8_t  MAX_ZONES       = 16;
    // Longitudes máx (véase §4.6 del plan): reemplaza los mágicos 16/4096.
    static constexpr uint32_t MAX_ADDR_LEN    = 16;    // "255.255.255.255"+null. IPv6 → 46 (ver build HARUKA_USE_IPV6).
    static constexpr uint32_t MAX_ENTITY_DATA = 4096;  // payload opaco máximo de una entidad.
    static constexpr uint32_t MAX_PACKET_SIZE = 65536; // cap de datagrama/paquete (véase §4.6 bug 6 y network.h).
    static constexpr uint64_t DEFAULT_LEASE_MS = 30000; // lease por defecto de una zona (véase §4.6 bug 1).
    static constexpr uint32_t MAX_REGION_BYTES = 4096;   // blob de región en un PKT_ZONE_REGION (§3.9, cap transport)

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
        // Nuevos packets del plan (PLAN_DGS_VALIDADOR): request/ack de validación + telemetría al master.
        PKT_VALIDATE_REQ    = 9,   // zone_node → validador  (afirmación vs estado predicho, §2.2)
        PKT_VALIDATE_ACK    = 10,  // validador → zone_node  (veredicto correlacionado, §2.2)
        PKT_VALIDATOR_STATUS= 11,  // zone_node → head_server (telemetría/lease, §2.2 y §4)
        PKT_SOCIAL_DELTA    = 12,  // guild/party deltas (§3.7)
        PKT_ACCOUNT         = 13,  // ban/permisos de cuenta (§3.7)
        PKT_DRAIN           = 14,  // ciclo de vida: pedir drenado antes de destruir zona (§3.9)
        PKT_DELETE_ZONE     = 15,  // ciclo de vida: confirmar destrucción de zona (§3.9)
        PKT_REASSIGN        = 16,  // handoff de autoridad: zona → head → nueva zona dueña (§3.6)
        PKT_ZONE_REGION     = 17,  // blob de estado de región (serializeRegion/mergeRegion, §3.9)
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

    // Zona como la ve el ORQUESTADOR (local): es un ZoneInfoPublic (lo que viaja / se consulta) + `fd` del
    // socket de control con el nodo + estado de vida (véase §3.9). ⚠️ La base se declara ANTES: no se puede
    // derivar de un tipo incompleto. `fd` NO va al wire (`pack(ServerMetrics)` solo serializa bounds/
    // addr/port, véase packet.cpp) → este cambio NO afecta al formato de red.
    struct ZoneInfo : public ZoneInfoPublic
    {
        int fd;
    };

    // Métricas que el nodo reporta al orquestador (§4 del plan). Los campos nuevos van AL FINAL para no
    // desplazar los existentes. ⚠️ Añadir campos cambia el layout de red → recompilar pack/unpack a la vez
    // (P0, §4.5). Contadores MONOTÓNICOS desde arranque (§4.1).
    struct ServerMetrics
    {
        ZoneInfo node;          // zonas (por bounds + addr:port; `fd` no viaja)
        float    ramUsage;      // 0..1
        float    performance;   // 0..1
        uint64_t startTimeS;    // época de arranque del nodo (distinguir nodo nuevo de sano, §4.6 bug 5)
        uint64_t bytesRx;       // bytes recibidos desde arranque
        uint64_t bytesTx;       // bytes enviados desde arranque
        uint32_t failedTransfers; // transferencias/validaciones fallidas (timeout o error)
        uint32_t activeEntities;  // entidades servidas (heurística de escalado)
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

    // Handoff de autoridad (§3.6): una entidad (o región) cambia de zona dueña. El zone_node que la
    // simulaba la manda al head; el head la enruta a la nueva zona que cubre su chunk. La nueva dueña
    // la promueve (ghost→real) y EMPIEZA a simularla; la vieja la deja de simular (lease).
    struct EntityReassign
    {
        uint64_t entityUuid;
        int32_t  chunkX, chunkY, chunkZ;
        uint32_t fromZone;      // zona que la cedía (debug/auditoría)
        uint32_t toZone;        // zona destino (0 = que el head la resuelva por chunk)
    };

    // Ciclo de vida de una zona (§3.9). Mismo struct para las DOS señales:
    //   · PKT_DRAIN:        orchestrator → zona  (ack=0: "drena, vas a cesar"; zona → orchestrator ack=1)
    //   · PKT_DELETE_ZONE:  orchestrator → zona  (el drenaje terminó: destruye tu zona y sal)
    // `requestId` correlaciona la petición con su ack (timeout/fail-safe en el orquestador).
    struct ZoneLifecycle
    {
        uint32_t requestId;     // seq de la operación de ciclo de vida
        uint8_t  ack;           // 0 = petición, 1 = confirmación del nodo
    };

    // Estado de región serializado (fusión/traspaso §3.9): la zona que cede extrae el estado autoritativo
    // de su región con `serializeRegion` y lo envía (→ head → nueva zona), que lo incorpora con
    // `mergeRegion`. `chunkX/Y/Z` es el ancla que usa el head para enrutar al nodo que lo cubre.
    struct ZoneRegion
    {
        int32_t  chunkX, chunkY, chunkZ;   // ancla de la región (metros→qué nodo la cubre)
        uint32_t srcZone;                  // nodo que cede (debug/auditoría)
        uint32_t size;                     // bytes válidos en data[]
        uint8_t  data[MAX_REGION_BYTES];   // blob opaco, formato del módulo
    };

    // Estado de vida de una zona (véase §3.9 del plan): el orquestador lo gestiona en paralelo al
    // ZoneInfo. NO va al wire: es bookkeeping local.
    enum class ZoneState : uint8_t
    {
        PROVISIONING = 0,   // spawn en curso, sin primer ServerMetrics
        READY,              // sirviendo y registrada en activeZones
        DRAINING,           // en proceso de cesión (scale-down / muerte), §3.9
        DEAD,               // lease vencido, a reasignar/limpiar
        DESTROYED           // pod/pod eliminado, entrada a purgar
    };

    // --- Request/ack de validación (PLAN_DGS_VALIDADOR §2.2) ---------------------------------------------
    // El validador NO re-simula: compara la afirmación del cliente contra el estado PREDICHO por la zona
    // dueña (misma regla que §3.6). `requestId` es un seq por remitente → idempotencia + anti-replay (§2.3).
    struct ValidateRequest
    {
        uint32_t requestId;     // seq por remitente (anti-replay)
        uint64_t entityUuid;    // entidad/op a validar
        uint32_t ownerZone;     // zona dueña de la simulación (la que predice)
        uint8_t  moduleId;      // módulo de reglas esperado (GAME_MODULE_SO)
        uint8_t  kind;          // 0=move, 1=action (verbos críticos → fail-closed)
        // payload opaco (kind=0 MOVIMIENTO): el estado PREDICHO por la zona + la afirmación del cliente.
        // La zona dueña NO re-simula; el validador compara esta afirmación con su predictora + tolerancia RTT.
        EntityTransfer entity;   // estado actual (predicho) de la entidad
        float lastGX, lastGY, lastGZ;  // posición GLOBAL previa conocida por la zona
        float maxSpeed;          // límite del jugador (Stats.speed[0])
        float dtSeconds;         // tiempo entre la última posición y la afirmación
    };

    struct ValidateAck
    {
        uint32_t requestId;
        int8_t   verdict;       // 1 = plausible, 0 = violación
        uint16_t weight;        // peso de la violación (0 si veredicto=1)
    };

    struct ValidatorStatus
    {
        int8_t   state;         // 0=UP 1=DEGRADED 2=DOWN/OPEN (circuit breaker §2.3)
        uint32_t reqSent;       // validaciones pedidas
        uint32_t reqTimeout;    // timeouts (→ failedTransfers del nodo)
        uint64_t bytesRecv;     // bytes de validación recibidos
        uint32_t failedTransfers;
        uint32_t activeEntities; // entidades servidas por el nodo emisor
        uint64_t timestampMs;    // época del reporte
    };

    enum DirtyFlag : uint32_t
    {
        DIRTY_TRANSFORM = 1 << 0,  // pos + rot
        DIRTY_STATS     = 1 << 1,
        DIRTY_INVENTORY = 1 << 2,
    };

    static constexpr uint16_t MAX_GHOST_DATA = 4096;

    // ⚠️ `alignas` va DESPUÉS de `struct`: escrito delante, el compilador lo IGNORA (-Wattributes) y la
    // estructura se quedaba con alineación 8. sizeof ya es múltiplo de 16 (4176) → alinearlo NO cambia el
    // layout ni el formato de red, solo alinea de verdad.
    struct alignas(16) GhostDelta
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

    struct ChatMessage
    {
        uint32_t uuid;
        char     username[32];
        char     text[256];
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