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
    // Buckets per axis in the population histogram a zone reports with its metrics (see ServerMetrics).
    // 32 keeps the whole thing to 192 bytes per sample and is plenty: the cut only needs to land in the
    // right neighbourhood, and the next split refines it.
    static constexpr uint32_t MAX_SPLIT_BUCKETS = 32;

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
        // READ-ONLY OBSERVER (viewer/ops). Sent by UDP to a zone: "add me to your broadcast". It
        // carries the zone's shared secret and NOTHING else on purpose — an observer must be unable to
        // introduce anything into the world. The zone answers with the SAME stream it already sends its
        // clients (entities + ghosts), keeps observers in their own registry, and expires them on a
        // lease so a viewer that goes away stops being fed. With no token configured the zone refuses
        // every observer: this feed is every entity's position ten times a second.
        PKT_OBSERVE         = 18,
        // Ask the persistence node for an entity's LAST STORED STATE (payload: uuid). It answers with a
        // PKT_ENTITY_TRANSFER carrying the state, or a bare PKT_NONE when it has never seen that uuid.
        // Persistence was write-only until this existed: entities went into Mongo and nothing in the
        // system could ever read one back, so "persistence" could not restore anything.
        PKT_PERSIST_QUERY   = 19,
        // Ask persistence for EVERY entity stored inside a chunk range (payload: the six bounds plus a
        // cap). This is what a zone needs at start-up: it knows the region it serves and not a single
        // uuid in it. The answer is a stream of PKT_ENTITY_TRANSFER terminated by a bare PKT_NONE, so
        // "no entities" and "the answer has ended" are the same well-formed thing.
        PKT_PERSIST_RANGE   = 20,
        // Ask persistence for the whole DURABLE social plane: guild memberships, friendships and
        // account state (bans, permissions). The answer is a stream of PKT_SOCIAL_DELTA and
        // PKT_ACCOUNT, terminated by a bare PKT_NONE.
        //
        // The social node holds all of it in memory and had no way to get any of it back, so a restart
        // UNBANNED EVERY ACCOUNT, dissolved every guild and forgot every friendship — and its own
        // write-through was being dropped on the floor by a persistence node that only understood
        // entities. Parties are deliberately NOT in here: they are session-scoped, and pretending they
        // survive a restart would be worse than losing them.
        PKT_SOCIAL_QUERY    = 21,
        // A node proving it holds the cluster secret, sent as the first thing after connecting:
        // nonce + timestamp + HMAC-SHA256(secret, nonce||timestamp). Nothing between the nodes was
        // authenticated at all — anyone who could reach the head could register as a zone and have
        // entities routed to them. See `auth.h` for what this does and, more importantly, what it
        // does not (it is not TLS, and the rest of the connection is still in clear).
        PKT_AUTH            = 22,
        // ⚠️ LO QUE UN JUGADOR PIDE, no lo que afirma. Un `PKT_ENTITY_TRANSFER` es un hecho
        // consumado: "estoy aqui, tengo esto". Una accion es una PETICION — "quiero poner una mesa
        // aqui" — y esa diferencia es la que permite que el servidor decida en vez de creer.
        //
        // Va al validador con `kind = 1`, que es la ruta que FALLA CERRADO: sin regla que la acepte,
        // se rechaza. El movimiento puede permitirse fallar abierto porque una posicion mal aceptada
        // se corrige en el siguiente paquete; una accion crea, destruye o transfiere objetos, y una
        // colada no se deshace. Esa asimetria ya existia en el validador y no la alcanzaba nadie: la
        // zona ponia `kind = 0` en todas sus peticiones. Esto es lo que la conecta.
        PKT_ACTION          = 23,
        // ⚠️ LA RESPUESTA, que no existia. El cliente pedia algo y no se enteraba NUNCA de si habia
        // ocurrido: si el servidor decia que no, el objeto se quedaba puesto en su pantalla y en la de
        // nadie mas. Dos mundos distintos y ni un mensaje. Una peticion sin respuesta no es una
        // peticion, es una esperanza.
        PKT_ACTION_ACK      = 24,

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
        STATE_CONSUMABLE    = 1 << 5,

        /// ⚠️ THE WORLD OWNS THIS, NOT A CLIENT — and without it a world could not contain objects.
        ///
        /// Every entity is leased to whoever reports it, and `zone_node`'s GC purges anything it has
        /// not heard from within `ENTITY_LEASE_MS` (3 s by default). For a player who disconnects that
        /// is exactly right. For a crate on the ground it is fatal: nothing reports a crate, so it
        /// evaporates three seconds after it appears. Measured — three `ENT_ITEM`s placed once and
        /// never touched again were served for 3 s and then gone, and a zone that restored them from
        /// the database lost them again just as fast.
        ///
        /// An entity carrying this bit is exempt from the lease GC. It is still broadcast, still
        /// persisted, still handed over when it crosses a border — it simply does not need anybody to
        /// keep vouching for it. The bit travels with the entity (it is part of the stored document),
        /// so it survives a restart the same way its position does.
        STATE_WORLD_OWNED   = 1 << 6
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

    // Which axis a CMD_TRANSFER_SERVER resize applies to. A split used to be able to cut on X and
    // nothing else, so a zone could only ever become a thinner and thinner SLAB: fifty players in one
    // place on the same X band could not be separated no matter how many times it scaled up. The cut
    // now goes along whichever axis is longest, which is what turns repeated splits into a grid.
    enum ResizeAxis : uint8_t
    {
        AXIS_X = 0,
        AXIS_Y = 1,
        AXIS_Z = 2
    };

    struct Command
    {
        HeadPurpose purpose;
        int32_t     chunkX, chunkY;
        char        addr[MAX_ADDR_LEN];
        int         port;
        float       chunkSizeX, chunkSizeY, chunkSizeZ;
        // CMD_TRANSFER_SERVER only: which bound `chunkX` is the new maximum OF. Appended at the end so
        // the existing fields do not shift, and read back only if the datagram actually carries it —
        // a Command from something built before this field still parses, as AXIS_X.
        ResizeAxis  resizeAxis;
    };

    // Payload of PKT_PERSIST_RANGE: "every entity you have stored inside these chunks". `limit` caps
    // the answer so one query can never turn into an unbounded stream — a zone covering a large region
    // of a populated world must not be able to ask for more than it can take.
    struct PersistRange
    {
        int32_t  chunkXMin, chunkXMax;
        int32_t  chunkYMin, chunkYMax;
        int32_t  chunkZMin, chunkZMax;
        uint32_t limit;
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
        // ⚠️ NOT 0..1, whatever this said for a long time: `zone_node` puts THE TICK TIME IN
        // MILLISECONDS here, measured against its own `ZONE_TICK_US` budget (100 ms). The comment
        // being wrong is not cosmetic — the orchestrator's load signal was written to match it and
        // ended up demanding that a zone be FAST before it would call it overloaded.
        float    performance;   // tick time in ms
        uint64_t startTimeS;    // node start epoch (tells a fresh node from a healthy one, §4.6 bug 5)
        uint64_t bytesRx;       // bytes received since start-up
        uint64_t bytesTx;       // bytes sent since start-up
        uint32_t failedTransfers; // failed transfers/validations (timeout or error)
        uint32_t activeEntities;  // entities served (scaling heuristic)

        // ⚠️ WHERE THE LOAD IS, not just how much of it there is. Until this existed the orchestrator
        // knew a zone's box and one number — `activeEntities` — and nothing else, so a SPLIT had no
        // choice but to cut the box down the geometric middle. With everybody standing in chunk 3 of a
        // 0..100 zone that cut lands at 50: the child gets 51..100, empty, and the parent keeps the
        // whole crowd. It costs a process and relieves nothing, and it takes about five generations
        // (~2.5 min at one split per 30 s) to walk the cut down to where the people actually are.
        //
        // These are three coarse histograms — one per axis — of the entities the zone is simulating,
        // bucketed across its own box. The orchestrator cuts where the population divides in half
        // instead of where the box does. Coarse on purpose: 32 buckets is 192 bytes per metrics sample
        // at 10 Hz, and the cut only has to land in the right neighbourhood — the next split refines it.
        //
        // Players, NPCs and items all count the same, because the tick cost they cause is the same:
        // every simulated entity is broadcast to every interested client. What this does NOT capture is
        // that the number of CLIENTS also multiplies that cost, so a bucket holding 50 items next to
        // one player is cheaper than a bucket holding 50 players. Weighting by client count is a
        // refinement this does not attempt.
        uint16_t popX[MAX_SPLIT_BUCKETS];
        uint16_t popY[MAX_SPLIT_BUCKETS];
        uint16_t popZ[MAX_SPLIT_BUCKETS];
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
        // ⚠️ THE HANDOFF USED TO BE FIRE-AND-FORGET, AND IT LOST ENTITIES. The ceding zone sent the
        // reassign plus the state and erased the entity IMMEDIATELY — ignoring both send results — so
        // a head that was down, reconnecting, or simply unable to route the chunk (`targetFD = -1`,
        // which it drops in silence) meant the entity ceased to exist anywhere. Nobody owned it, no
        // counter moved, and nothing in the logs said so.
        //   0 = REQUEST   zone -> head    "take this entity off my hands"
        //   1 = ACCEPTED  head -> zone    "it is routed to its new owner; you may let go"
        //   2 = NO OWNER  head -> zone    "no zone covers that chunk; KEEP it"
        // The ceding zone holds the entity until it is told 1, and re-sends until then.
        uint8_t  ack;
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
    /// Que pide el jugador. Deliberadamente corto: cada verbo que se anade aqui es superficie de
    /// ataque, y el que no este no se puede pedir.
    /// LA REGLA, antes que la lista: una accion es DISCRETA, IRREVERSIBLE y sobre algo que NO ES
    /// TUYO. El movimiento no cumple ninguna de las tres — es continuo, se autocorrige en el paquete
    /// siguiente y es sobre uno mismo — y por eso no esta aqui: pasarlo por la ruta que falla cerrado
    /// seria un viaje de ida y vuelta por paso, y un rechazo se sentiria como un tiron.
    ///
    /// Cada verbo que se anade es superficie de ataque, asi que la lista se gana el sitio de uno en
    /// uno. Estos tres son operaciones GENERICAS sobre una entidad del mundo, que es lo que la zona
    /// sabe hacer sin conocer el juego. Coger algo al inventario o usar una puerta NO estan: su efecto
    /// es del juego, y el ABI del modulo solo devuelve un veredicto — no puede aplicar nada todavia.
    /// ⚠️ LOS MISMOS NUMEROS QUE `ActionVerb` de `game_module.h`, y no por casualidad: ese vocabulario
    /// ya existia (ACT_DESTROY, ACT_PLACE, ACT_TRANSFER…) y lo lee el modulo de reglas del proyecto.
    /// Inventar aqui una numeracion paralela habria obligado a traducir en medio, que es donde se
    /// cuelan los desajustes silenciosos. `types.h` no incluye `game_module.h` a proposito —un cliente
    /// no necesita el ABI del modulo—, asi que la unica atadura son estos valores.
    enum ActionKind : uint16_t
    {
        ACTION_REMOVE = 2,  ///< ACT_DESTROY: destruir el objeto `target`
        ACTION_PLACE  = 5,  ///< ACT_PLACE:   crear un objeto del mundo en `pos` (que, en `data`)
        ACTION_MOVE   = 6,  ///< ACT_MOVE:    mover el objeto `target` a `pos` (arrastrar, no andar)
        /// ACT_DROP: un objeto SALE de un inventario y se convierte en objeto del mundo. Es lo que
        /// pasa al tirar algo, y NO es ACTION_PLACE: colocar una pieza de construccion se valida con
        /// geometria (que no solape, que apoye), y tirar una piedra no tiene ni catalogo ni encaje.
        /// Meterlos por el mismo verbo obligaria al modulo a adivinar cual es cual por el payload.
        ACTION_DROP   = 7
    };

    /// Una peticion de accion, del cliente a SU zona. No lleva uuid del objeto a proposito: quien
    /// pide no elige la identidad de lo que se cree — la asigna el servidor, o un cliente podria
    /// sobrescribir la mesa de otro con solo acertar su numero.
    struct ActionRequest
    {
        /// Para que el que pide pueda reconocer la respuesta. Lo elige el CLIENTE y solo le sirve a el:
        /// el servidor lo devuelve tal cual y no decide nada con el, asi que inventarselo solo confunde
        /// a quien se lo invento.
        uint32_t requestId;
        uint32_t actor;                    ///< quien lo pide (su uuid de sesion)
        uint16_t action;                   ///< ActionKind
        /// Sobre QUE, cuando la accion es sobre algo que ya existe (REMOVE, MOVE). Cero en PLACE, que
        /// crea. Que el objetivo lo elija quien pide es inevitable —hay que decir cual—, y por eso el
        /// servidor comprueba que exista y que sea del mundo antes de preguntarle a nadie: un uuid
        /// inventado no puede convertirse en una peticion valida sobre la mesa de otro.
        uint32_t target;
        int32_t  chunkX, chunkY, chunkZ;
        float    pos[3];
        uint16_t angle;
        uint16_t dataSize;
        uint8_t  data[MAX_ENTITY_DATA];    ///< opaco: que objeto, definido por el juego
    };

    /// Lo que el servidor contesta a una accion. Llega por el mismo enlace UDP y sellado igual.
    struct ActionAck
    {
        uint32_t requestId;   ///< el que mando el cliente
        uint16_t action;      ///< ActionKind, para no tener que recordar que pediste
        uint8_t  accepted;    ///< 1 = ocurrio; 0 = el servidor dijo que no
        uint32_t uuid;        ///< identidad que le dio el servidor (0 si no se acepto)
    };

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
        uint32_t permFlags;    // permissions (ACC_SET_PERM)
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