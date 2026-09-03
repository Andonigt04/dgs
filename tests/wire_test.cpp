// ================================================================================================
// wire_test — pack/unpack round trip for EVERY DGS packet (P5, §4.3 of the plan).
// For each type: fill the fields, pack, unpack and compare. It also verifies that the first byte (the
// PacketType) is the expected one. Structs carrying an opaque payload (data[]) are filled with a
// recognisable pattern so a loss of bytes shows up.
//
// Linked against src/packet.cpp (COMMON_SRCS) → it validates the REAL wire FORMAT, not just the layout.
// This test was added along with §3.9 (PKT_DRAIN/PKT_DELETE_ZONE/PKT_ZONE_REGION) and additionally
// covers everything from P2/P3/P4 (Validate*, ValidatorStatus, EntityReassign).
// ================================================================================================
#include "include/dgs/packet.h"
#include "include/dgs/types.h"

#include <cstdio>
#include <cstring>

static int g_failures = 0;
static int g_checks   = 0;

#define CHECK(cond, msg) \
    do { g_checks++; \
         if (!(cond)) { g_failures++; \
             std::printf("    FAIL %s (line %d)\n", msg, __LINE__); } \
    } while (0)

static uint8_t patternByte(uint32_t i) { return (uint8_t)(0x5A + (i & 0x3F)); }

// fills an EntityTransfer with a deterministic pattern
static void fillEntity(DGS::EntityTransfer& e, uint32_t uuid)
{
    std::memset(&e, 0, sizeof(e));
    e.uuid = uuid;
    e.pos[0] = 1.5f; e.pos[1] = -2.25f; e.pos[2] = 3.125f;
    e.chunkX = 7; e.chunkY = -3; e.chunkZ = 12;
    e.angle = 20000;
    e.type = DGS::ENT_NPC;
    e.state = (DGS::EntityState)(DGS::STATE_MOVING | DGS::STATE_PHYSICS_OWNED);
    e.stats.speed[0] = 5.0f; e.stats.health = 100.f;
    e.dataSize = 64;
    for (uint32_t i = 0; i < e.dataSize; i++) e.data[i] = patternByte(i);
}

static void roundTripEntity()
{
    DGS::EntityTransfer a{}, b{};
    fillEntity(a, 0xDEADBEEF);

    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_ENTITY_TRANSFER, "EntityTransfer: primer byte = PKT_ENTITY_TRANSFER");
    b = p.unpackEntityTransfer();
    CHECK(b.uuid == a.uuid && b.chunkX == a.chunkX && b.chunkY == a.chunkY && b.chunkZ == a.chunkZ,
          "EntityTransfer: uuid/bounds");
    CHECK(b.pos[0] == a.pos[0] && b.pos[1] == a.pos[1] && b.pos[2] == a.pos[2], "EntityTransfer: pos");
    CHECK(b.angle == a.angle && b.type == a.type && b.state == a.state, "EntityTransfer: angle/type/state");
    CHECK(b.stats.speed[0] == a.stats.speed[0] && b.stats.health == a.stats.health, "EntityTransfer: stats");
    CHECK(b.dataSize == a.dataSize, "EntityTransfer: dataSize");
    bool dataOk = b.dataSize == a.dataSize;
    for (uint32_t i = 0; dataOk && i < a.dataSize; i++) dataOk = (b.data[i] == patternByte(i));
    CHECK(dataOk, "EntityTransfer: opaque payload intact");
}

static void roundTripCommand()
{
    DGS::Command a{}, b{};
    a.purpose = DGS::CMD_TRANSFER_SERVER;
    a.chunkX = 42; a.chunkY = -7;
    std::strncpy(a.addr, "10.0.0.9", sizeof(a.addr) - 1);
    a.port = 30426;
    a.chunkSizeX = 1.5f; a.chunkSizeY = 2.5f; a.chunkSizeZ = 3.5f;

    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_COMMAND, "Command: primer byte = PKT_COMMAND");
    b = p.unpackCommand();
    CHECK(b.purpose == a.purpose && b.chunkX == a.chunkX && b.chunkY == a.chunkY, "Command: purpose/bounds");
    CHECK(std::strcmp(b.addr, a.addr) == 0 && b.port == a.port, "Command: addr/port");
    CHECK(b.chunkSizeX == a.chunkSizeX && b.chunkSizeY == a.chunkSizeY && b.chunkSizeZ == a.chunkSizeZ,
          "Command: chunkSize");
}

static void roundTripMetrics()
{
    DGS::ServerMetrics a{}, b{};
    a.ramUsage = 0.73f; a.performance = 0.42f;
    a.node.chunkXMin = 0; a.node.chunkXMax = 4;
    a.node.chunkYMin = -2; a.node.chunkYMax = 1;
    a.node.chunkZMin = 0; a.node.chunkZMax = 9;
    std::strncpy(a.node.addr, "10.0.0.3", sizeof(a.node.addr) - 1);
    a.node.port = 30425;
    a.startTimeS = 1234567890u;
    a.bytesRx = 987654321u; a.bytesTx = 1122334455u;
    a.failedTransfers = 7; a.activeEntities = 250;

    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_METRICS, "ServerMetrics: primer byte = PKT_METRICS");
    b = p.unpackServerMetrics();
    CHECK(b.ramUsage == a.ramUsage && b.performance == a.performance, "ServerMetrics: ram/perf");
    CHECK(b.node.chunkXMin == a.node.chunkXMin && b.node.chunkXMax == a.node.chunkXMax, "ServerMetrics: X");
    CHECK(b.node.chunkYMin == a.node.chunkYMin && b.node.chunkYMax == a.node.chunkYMax, "ServerMetrics: Y");
    CHECK(b.node.chunkZMin == a.node.chunkZMin && b.node.chunkZMax == a.node.chunkZMax, "ServerMetrics: Z");
    CHECK(std::strcmp(b.node.addr, a.node.addr) == 0 && b.node.port == a.node.port, "ServerMetrics: addr/port");
    CHECK(b.startTimeS == a.startTimeS, "ServerMetrics: startTimeS");
    CHECK(b.bytesRx == a.bytesRx && b.bytesTx == a.bytesTx, "ServerMetrics: bytes");
    CHECK(b.failedTransfers == a.failedTransfers && b.activeEntities == a.activeEntities,
          "ServerMetrics: failedTransfers/activeEntities");
}

static void roundTripZoneQuery()
{
    DGS::ZoneQuery a{}, b{};
    a.uuid = 999; a.chunkX = 1; a.chunkY = 2; a.chunkZ = 3;
    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_ZONE_QUERY, "ZoneQuery: primer byte");
    b = p.unpackZoneQuery();
    CHECK(b.uuid == a.uuid && b.chunkX == a.chunkX && b.chunkY == a.chunkY && b.chunkZ == a.chunkZ,
          "ZoneQuery: campos");
}

static void roundTripZoneResponse()
{
    DGS::ZoneResponse a{}, b{};
    std::strncpy(a.addr, "10.1.2.3", sizeof(a.addr) - 1);
    a.port = 31337;
    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_ZONE_RESPONSE, "ZoneResponse: primer byte");
    b = p.unpackZoneResponse();
    CHECK(std::strcmp(b.addr, a.addr) == 0 && b.port == a.port, "ZoneResponse: addr/port");
}

static void roundTripZoneList()
{
    DGS::ZoneListResponse a{}, b{};
    a.count = 2;
    for (int i = 0; i < a.count; i++)
    {
        a.zones[i].chunkXMin = i; a.zones[i].chunkXMax = i + 4;
        a.zones[i].chunkYMin = i * 2; a.zones[i].chunkYMax = i * 2 + 1;
        a.zones[i].chunkZMin = 0; a.zones[i].chunkZMax = 3;
        std::strncpy(a.zones[i].addr, i ? "10.0.0.2" : "10.0.0.1", sizeof(a.zones[i].addr) - 1);
        a.zones[i].port = 42425 + i;
    }
    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_ZONE_LIST, "ZoneListResponse: primer byte");
    b = p.unpackZoneListResponse();
    CHECK(b.count == a.count, "ZoneListResponse: count");
    bool ok = true;
    for (int i = 0; i < a.count; i++)
        ok = ok && b.zones[i].chunkXMin == a.zones[i].chunkXMin &&
                  std::strcmp(b.zones[i].addr, a.zones[i].addr) == 0 && b.zones[i].port == a.zones[i].port;
    CHECK(ok, "ZoneListResponse: zones intact");
}

static void roundTripGhost()
{
    DGS::GhostDelta a{}, b{};
    a.uuid = 0xABCDEF;
    a.chunkX = 5; a.chunkY = -5; a.chunkZ = 1;
    a.dirtyMask = DGS::DIRTY_TRANSFORM | DGS::DIRTY_STATS | DGS::DIRTY_INVENTORY;
    a.pos[0] = 0.1f; a.pos[1] = 0.2f; a.pos[2] = 0.3f;
    a.rot[0] = 0.f; a.rot[1] = 0.707f; a.rot[2] = 0.f; a.rot[3] = 0.707f;
    a.stats.speed[0] = 3.5f;
    a.dataSize = 32;
    for (uint16_t i = 0; i < a.dataSize; i++) a.data[i] = patternByte(i);

    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_GHOST_DELTA, "GhostDelta: primer byte");
    b = p.unpackGhostDelta();
    CHECK(b.uuid == a.uuid && b.chunkX == a.chunkX && b.chunkY == a.chunkY && b.chunkZ == a.chunkZ,
          "GhostDelta: uuid/bounds");
    CHECK(b.pos[0] == a.pos[0] && b.rot[3] == a.rot[3], "GhostDelta: transform");
    CHECK(b.stats.speed[0] == a.stats.speed[0], "GhostDelta: stats");
    bool dataOk = b.dataSize == a.dataSize;
    for (uint16_t i = 0; dataOk && i < a.dataSize; i++) dataOk = (b.data[i] == patternByte(i));
    CHECK(dataOk, "GhostDelta: opaque payload intact");
}

static void roundTripChat()
{
    DGS::ChatMessage a{}, b{};
    a.uuid = 77;
    std::strncpy(a.username, "andoni", sizeof(a.username) - 1);
    std::strncpy(a.text, "hola mundo", sizeof(a.text) - 1);
    a.channel = DGS::CHAT_GUILD;
    a.seq = 42;
    a.timestampMs = 1234567890;
    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_CHAT, "ChatMessage: primer byte");
    b = p.unpackChatMessage();
    CHECK(b.uuid == a.uuid && std::strcmp(b.username, a.username) == 0 && std::strcmp(b.text, a.text) == 0,
          "ChatMessage: campos");
    CHECK(b.channel == a.channel && b.seq == a.seq && b.timestampMs == a.timestampMs,
          "ChatMessage: channel/seq/epoch (§3.7)");
}

static void roundTripSocial()
{
    DGS::SocialDelta a{}, b{};
    a.targetUuid = 1001; a.scopeUuid = 7; a.kind = DGS::SOCIAL_GUILD_RANK;
    a.rank = 3; a.zoneId = 5; a.seq = 9;
    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_SOCIAL_DELTA, "SocialDelta: primer byte");
    b = p.unpackSocialDelta();
    CHECK(b.targetUuid == a.targetUuid && b.scopeUuid == a.scopeUuid && b.kind == a.kind &&
          b.rank == a.rank && b.zoneId == a.zoneId && b.seq == a.seq,
          "SocialDelta: campos");
}

static void roundTripAccount()
{
    DGS::AccountAction a{}, b{};
    a.actorUuid = 1; a.targetUuid = 555; a.action = DGS::ACC_BAN;
    a.permFlags = 0; a.durationS = 86400;
    std::strncpy(a.reason, "trampa de velocidad", sizeof(a.reason) - 1);
    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_ACCOUNT, "AccountAction: primer byte");
    b = p.unpackAccountAction();
    CHECK(b.actorUuid == a.actorUuid && b.targetUuid == a.targetUuid && b.action == a.action &&
          b.permFlags == a.permFlags && b.durationS == a.durationS &&
          std::memcmp(b.reason, a.reason, sizeof(a.reason)) == 0,
          "AccountAction: campos");
}

static void roundTripValidate()
{
    DGS::ValidateRequest a{}, b{};
    a.requestId = 1; a.entityUuid = 0x1000; a.ownerZone = 3; a.moduleId = 1; a.kind = 0;
    fillEntity(a.entity, 0x2000);
    a.lastGX = 1.1f; a.lastGY = 2.2f; a.lastGZ = 3.3f;
    a.maxSpeed = 8.0f; a.dtSeconds = 0.1f;

    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_VALIDATE_REQ, "ValidateRequest: primer byte");
    b = p.unpackValidateRequest();
    CHECK(b.requestId == a.requestId && b.entityUuid == a.entityUuid, "ValidateRequest: ids");
    CHECK(b.ownerZone == a.ownerZone && b.moduleId == a.moduleId && b.kind == a.kind, "ValidateRequest: meta");
    CHECK(b.entity.uuid == a.entity.uuid && b.entity.chunkX == a.entity.chunkX, "ValidateRequest: entity");
    CHECK(b.lastGX == a.lastGX && b.lastGY == a.lastGY && b.lastGZ == a.lastGZ, "ValidateRequest: lastG");
    CHECK(b.maxSpeed == a.maxSpeed && b.dtSeconds == a.dtSeconds, "ValidateRequest: speed/dt");

    DGS::ValidateAck ac{}, acB{};
    ac.requestId = 9; ac.verdict = 0; ac.weight = 12;
    DGS::Packet pa; pa.pack(ac);
    CHECK(pa.getType() == DGS::PKT_VALIDATE_ACK, "ValidateAck: primer byte");
    acB = pa.unpackValidateAck();
    CHECK(acB.requestId == ac.requestId && acB.verdict == ac.verdict && acB.weight == ac.weight,
          "ValidateAck: campos");
}

static void roundTripValidatorStatus()
{
    DGS::ValidatorStatus a{}, b{};
    a.state = 1; a.reqSent = 500; a.reqTimeout = 3;
    a.bytesRecv = 12345; a.failedTransfers = 2; a.activeEntities = 88; a.timestampMs = 1700000000000ull;

    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_VALIDATOR_STATUS, "ValidatorStatus: primer byte");
    b = p.unpackValidatorStatus();
    CHECK(b.state == a.state && b.reqSent == a.reqSent && b.reqTimeout == a.reqTimeout,
          "ValidatorStatus: state/req");
    CHECK(b.bytesRecv == a.bytesRecv && b.failedTransfers == a.failedTransfers &&
          b.activeEntities == a.activeEntities && b.timestampMs == a.timestampMs,
          "ValidatorStatus: bytes/entidades/timestamp");
}

static void roundTripReassign()
{
    DGS::EntityReassign a{}, b{};
    a.entityUuid = 0xCAFE; a.chunkX = 10; a.chunkY = 20; a.chunkZ = 30;
    a.fromZone = 1; a.toZone = 4;

    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_REASSIGN, "EntityReassign: primer byte");
    b = p.unpackEntityReassign();
    CHECK(b.entityUuid == a.entityUuid && b.chunkX == a.chunkX && b.chunkY == a.chunkY &&
          b.chunkZ == a.chunkZ && b.fromZone == a.fromZone && b.toZone == a.toZone,
          "EntityReassign: campos");
}

static void roundTripLifecycle()
{
    DGS::ZoneLifecycle a{}, b{};
    a.requestId = 4242; a.ack = 1;   // the node's ack (DRAIN)
    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_DRAIN, "ZoneLifecycle(pack): primer byte = PKT_DRAIN");
    b = p.unpackZoneLifecycle();
    CHECK(b.requestId == a.requestId && b.ack == a.ack, "ZoneLifecycle: requestId/ack");

    // packDelete → PKT_DELETE_ZONE
    DGS::ZoneLifecycle del{}, delB{};
    del.requestId = 7; del.ack = 0;
    DGS::Packet pd; pd.packDelete(del);
    CHECK(pd.getType() == DGS::PKT_DELETE_ZONE, "ZoneLifecycle(packDelete): primer byte = PKT_DELETE_ZONE");
    delB = pd.unpackZoneLifecycle();
    CHECK(delB.requestId == del.requestId && delB.ack == del.ack, "packDelete: requestId/ack");
}

static void roundTripZoneRegion()
{
    DGS::ZoneRegion a{}, b{};
    a.chunkX = 5; a.chunkY = -2; a.chunkZ = 0;
    a.srcZone = 9;
    a.size = 128;
    for (uint32_t i = 0; i < a.size; i++) a.data[i] = patternByte(i);

    DGS::Packet p; p.pack(a);
    CHECK(p.getType() == DGS::PKT_ZONE_REGION, "ZoneRegion: primer byte");
    b = p.unpackZoneRegion();
    CHECK(b.chunkX == a.chunkX && b.chunkY == a.chunkY && b.chunkZ == a.chunkZ, "ZoneRegion: ancla");
    CHECK(b.srcZone == a.srcZone && b.size == a.size, "ZoneRegion: srcZone/size");
    bool ok = b.size == a.size;
    for (uint32_t i = 0; ok && i < a.size; i++) ok = (b.data[i] == patternByte(i));
    CHECK(ok, "ZoneRegion: region blob intact");
}

int main()
{
    std::printf("[wire_test] round-trip de pack/unpack de los packets del DGS\n");
    roundTripEntity();
    roundTripCommand();
    roundTripMetrics();
    roundTripZoneQuery();
    roundTripZoneResponse();
    roundTripZoneList();
    roundTripGhost();
    roundTripChat();
    roundTripSocial();
    roundTripAccount();
    roundTripValidate();
    roundTripValidatorStatus();
    roundTripReassign();
    roundTripLifecycle();
    roundTripZoneRegion();

    std::printf("[wire_test] %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
