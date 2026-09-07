// ─────────────────────────────────────────────────────────────────────────────────────────────────
// world_object_e2e — what a zone KEEPS about an entity between updates.
//
// Two things it was not keeping, and they have the same shape: the zone treats each incoming update as
// the whole truth about an entity, so anything the update does not mention is gone.
//
// ── PART ONE: can the world contain an OBJECT?
//
// It could not, and nothing said so. Every entity in a zone is leased to whoever reports it, and the
// GC purges anything it has not heard from within `ENTITY_LEASE_MS` — 3 seconds by default. For a
// player who disconnects that is the point. For a crate on the ground it is fatal: nothing reports a
// crate, so three seconds after it appears it is gone, and a zone that restores it from the database
// loses it again just as fast.
//
// The symptom looked like a persistence problem, which is what made it hard to see: "there are no
// objects in my database". There were. They were written in under a second, read back by a fresh zone,
// broadcast to observers — and then purged, because the model had no way to say that an entity belongs
// to the WORLD rather than to a client. `STATE_WORLD_OWNED` is that word, and this measures it.
//
//   A. an item carrying the bit is still being served well past the lease.
//   B. THE COUNTER-PROOF, and it is the same item: identical in every respect except the bit, placed
//      the same way, is purged on schedule. Without B, (A) would also pass on a zone whose GC had
//      simply stopped working — which is a far more likely bug than the one being fixed.
//
// ── PART TWO: does an entity's PAYLOAD survive the next movement update?
//
// It did not. `existing = e` replaced the whole struct, and an ordinary movement update carries
// `dataSize = 0` — which is the entire point of `dataSize`, the 67x bandwidth saving this system's
// capacity rests on: a player who is only moving does not re-send four kilobytes of inventory. So an
// inventory survived exactly one tick. Measured against a live zone: a 64-byte payload was broadcast
// as `dataSize = 64`, and one second of ordinary transforms later as **0**.
//
//   C. the payload is still there after a second of movement.
//   D. THE COUNTER-PROOF: the movement in those updates was applied all the same. Without it, "the
//      payload survived" would also pass on a zone that had started ignoring updates altogether.
//
// Persistence is deliberately NOT part of this test (there is no persistence node here at all): the
// subject is the lease, and `restore_e2e` owns the database half. The bit is an ordinary field of the
// stored document, so it travels with the entity like its position does.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "views/viewer_state.h"

#include <sys/socket.h>
#include <sys/wait.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);
}

// Below the ephemeral range on purpose — see the note in `validator_e2e.cpp`.
static const int   kHeadPort = 21941;
static const int   kZoneUdp  = 21943;
static const float kChunkM   = 1000.0f;
static const char* kToken    = "world-object-token";
static const int32_t kChunk  = 50;
static const int   kItems    = 3;
static const int   kLeaseMs  = 1500;    // short, so the whole test costs a few seconds

static std::atomic<bool> g_done{false};
static std::atomic<int>  g_atNode{-1};

static uint64_t nowMs()
{
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// ⚠️ EL ENLACE DE CONFIANZA. Un objeto del MUNDO no lo puede crear un cliente: desde que la zona deja
// de creerse lo que le declara un jugador (ver `client_claims_e2e`), `STATE_WORLD_OWNED` enviado por
// el enlace UDP de alguien se le quita — si no, cualquiera sembraria objetos que no caducan nunca.
// Un objeto del mundo entra por donde entran de verdad: otra zona a traves del head, la restauracion
// desde persistencia o, cuando exista, una ACCION validada. Aqui se usa el primero, que es el que este
// test ya tiene montado.
static DGS::TCPSocket* g_headSock = nullptr;
static std::atomic<int> g_zoneFd{-1};

static void fakeHead(std::atomic<bool>& ready)
{
    static DGS::TCPSocket s;
    g_headSock = &s;
    if (!s.listen(kHeadPort)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;
    while (!g_done)
    {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        g_zoneFd = fd;
        DGS::Command cmd{};
        cmd.chunkSizeX = kChunkM; cmd.chunkSizeY = kChunkM; cmd.chunkSizeZ = kChunkM;
        cmd.port = kZoneUdp;
        std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");
        DGS::Packet p; p.pack(cmd);
        s.send(fd, p.getRawData(), p.getSize());
        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 300000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done)
        {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;
            if (n < 0)  continue;
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() == DGS::PKT_METRICS) g_atNode = (int)r.unpackServerMetrics().activeEntities;
        }
        s.closeClient(fd);
    }
}

static pid_t spawnZone(const char* bin)
{
    std::fflush(stdout);   // see the note in persistence_e2e: fork duplicates an unflushed buffer
    const pid_t p = fork();
    if (p != 0) return p;
    if (!std::getenv("WORLD_OBJECT_VERBOSE")) { std::freopen("/dev/null", "w", stdout); }
    setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
    setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
    setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
    setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
    setenv("VALIDATOR_TCP_PORT", "21949", 1);
    setenv("SOCIAL_HOST",        "127.0.0.1", 1);
    setenv("SOCIAL_TCP_PORT",    "21948", 1);
    setenv("ZONE_RESTORE",       "0",  1);    // no database in this test: the subject is the lease
    setenv("ZONE_PERSIST_MS",    "0",  1);
    setenv("CHUNK_X_MIN", "0", 1); setenv("CHUNK_X_MAX", "100", 1);
    setenv("CHUNK_Y_MIN", "0", 1); setenv("CHUNK_Y_MAX", "100", 1);
    setenv("CHUNK_Z_MIN", "0", 1); setenv("CHUNK_Z_MAX", "100", 1);
    setenv("CHUNK_SIZE_X", "1000.0", 1);
    setenv("CHUNK_SIZE_Y", "1000.0", 1);
    setenv("CHUNK_SIZE_Z", "1000.0", 1);
    setenv("ENTITY_LEASE_MS", std::to_string(kLeaseMs).c_str(), 1);
    // ⚠️ EL TECHO DE VELOCIDAD ES DEL SERVIDOR, y estas fases mueven entidades a sitios muy lejanos a
    // proposito (otro chunk, otra region) porque el sujeto es el traspaso o la lease, no S1. Antes
    // colaba porque S1 tenia una puerta de atras —`dt > 2 s -> pasa`— que se ha cerrado: esperar ya
    // no compra un teletransporte. Asi que el permiso se pide donde corresponde, al despliegue.
    setenv("CLIENT_MAX_SPEED_MPS", "1000000", 1);
    setenv("DGS_OBSERVE_TOKEN", kToken, 1);
    setenv("GAME_MODULE_SO", "", 1);          // no rules module: nothing moves underneath us
    char tmpl[] = "/tmp/dgs_worldobj_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(bin, bin, (char*)nullptr);
    _exit(127);
}

/// Sends one entity, optionally carrying a payload. `dataSize` 0 is what an ordinary movement update
/// looks like.
static void sendEntity(DGS::UDPSocket& udp, uint32_t uuid, float x, uint16_t dataSize)
{
    DGS::EntityTransfer e{};
    e.uuid   = uuid;
    e.type   = DGS::ENT_PLAYER;
    e.chunkX = kChunk; e.chunkY = kChunk; e.chunkZ = kChunk;
    e.pos[0] = x;
    e.stats.speed[0] = 500.0f;   // S1 is not the subject: never let it reject a step
    e.stats.health   = 70.0f;
    e.dataSize = dataSize;
    for (uint16_t i = 0; i < dataSize; ++i) e.data[i] = (uint8_t)(i & 0xFF);
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

/// What the zone is broadcasting for `uuid` right now: its payload size and its x. -1 / NaN if it did
/// not appear at all, which must not be confused with "it appeared carrying nothing".
static void observe(DGS::UDPSocket& udp, uint32_t uuid, int ms, int& outSize, float& outX)
{
    uint8_t buf[8192]; std::string from; int port = 0;
    while (udp.receive(buf, sizeof(buf), from, port) > 0) {}   // drain the past

    outSize = -1; outX = 0.0f;
    const uint64_t until = nowMs() + (uint64_t)ms;
    while (nowMs() < until)
    {
        DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(kToken);
        udp.send("127.0.0.1", kZoneUdp, hello.getRawData(), hello.getSize());
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        for (;;)
        {
            const int n = udp.receive(buf, sizeof(buf), from, port);
            if (n <= 0) break;
            DGS::Packet p; p.setBuffer(buf, (size_t)n);
            if (p.getType() != DGS::PKT_ENTITY_TRANSFER) continue;
            DGS::EntityTransfer e{};
            if (p.tryUnpackEntityTransfer(e) && e.uuid == uuid) { outSize = (int)e.dataSize; outX = e.pos[0]; }
        }
    }
}

/// Places one item. `worldOwned` is the ONLY difference between the two phases — y tambien decide POR
/// DONDE entra, que no es un detalle del test sino la regla: lo del mundo por el enlace de confianza,
/// lo de un cliente por el suyo.
static void placeItem(DGS::UDPSocket& udp, uint32_t uuid, float x, bool worldOwned)
{
    DGS::EntityTransfer e{};
    e.uuid   = uuid;
    e.type   = DGS::ENT_ITEM;
    e.chunkX = kChunk; e.chunkY = kChunk; e.chunkZ = kChunk;
    e.pos[0] = x;
    e.stats.speed[0] = 200.0f;   // S1 is not the subject: never let it reject the placement
    e.stats.health   = 55.0f;
    if (worldOwned) e.state = DGS::STATE_WORLD_OWNED;
    DGS::Packet p; p.pack(e);
    if (worldOwned)
    {
        // Por el enlace del head: es de donde llega un objeto del mundo de verdad (otra zona que lo
        // cede, o la restauracion). Por el UDP de un cliente la zona le quitaria el bit, y con razon.
        const int fd = g_zoneFd.load();
        if (fd >= 0 && g_headSock) g_headSock->send(fd, p.getRawData(), p.getSize());
        return;
    }
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

/// @return how many distinct entities the zone is broadcasting RIGHT NOW.
///
/// The TTL is short and the socket is drained first for the reason `handoff_e2e` records: with the
/// viewer's 5 s default, one datagram at the start of the window keeps the count up for the whole
/// window, and the test would be measuring "was it ever there" instead of "is it there".
static size_t watch(DGS::UDPSocket& udp, int ms)
{
    uint8_t buf[8192];
    std::string from; int port = 0;
    while (udp.receive(buf, sizeof(buf), from, port) > 0) {}   // drain the past

    DGS::ViewerState st(kChunkM, /*ttlMs*/ 600);
    const uint64_t until = nowMs() + (uint64_t)ms;
    while (nowMs() < until)
    {
        DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(kToken);
        udp.send("127.0.0.1", kZoneUdp, hello.getRawData(), hello.getSize());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        for (;;)
        {
            const int n = udp.receive(buf, sizeof(buf), from, port);
            if (n <= 0) break;
            st.onDatagram(buf, n, nowMs());
        }
    }
    st.expire(nowMs());
    return st.entityCount();
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);
    char abs[PATH_MAX];
    const char* argPath = (argc > 1) ? argv[1] : "./build/zone_node";
    const char* zoneBin = realpath(argPath, abs) ? abs : argPath;

    std::atomic<bool> hReady{false};
    std::thread th(fakeHead, std::ref(hReady));
    while (!hReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t zone = spawnZone(zoneBin);
    for (int i = 0; i < 200 && g_atNode.load() < 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    check(g_atNode.load() >= 0, "the zone is up and reporting to the head");

    DGS::UDPSocket udp;
    udp.bind(0);
    { timeval tv{}; tv.tv_usec = 60000;
      setsockopt(udp.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

    // ══ A. Items that the WORLD owns ═════════════════════════════════════════════════════════════
    for (int i = 0; i < kItems; ++i) placeItem(udp, 9100 + (uint32_t)i, 100.0f + 10.0f * i, true);
    const size_t ownedEarly = watch(udp, 700);
    std::printf("    world-owned: %zu of %d served right after placing them\n", ownedEarly, kItems);
    check(ownedEarly == (size_t)kItems, "A · the items are placed and served");

    // Well past the lease, and NOTHING has reported them since.
    std::this_thread::sleep_for(std::chrono::milliseconds(kLeaseMs * 2));
    const size_t ownedLate = watch(udp, 700);
    std::printf("    world-owned: %zu of %d still served after %d ms (lease is %d ms)\n",
                ownedLate, kItems, kLeaseMs * 2, kLeaseMs);
    check(ownedLate == (size_t)kItems,
          "A · an item the WORLD owns outlives the lease with nobody vouching for it");

    // ══ B. The counter-proof: the same items without the bit ═════════════════════════════════════
    for (int i = 0; i < kItems; ++i) placeItem(udp, 9200 + (uint32_t)i, 400.0f + 10.0f * i, false);
    const size_t plainEarly = watch(udp, 700);
    std::printf("    ordinary: %zu new items served right after placing them\n",
                plainEarly - ownedLate);
    check(plainEarly == (size_t)(kItems * 2), "B · the ordinary items are placed and served too");

    std::this_thread::sleep_for(std::chrono::milliseconds(kLeaseMs * 2));
    const size_t plainLate = watch(udp, 700);
    std::printf("    after another %d ms: %zu entities served in total "
                "(%d world-owned + %zu ordinary)\n",
                kLeaseMs * 2, plainLate, kItems, plainLate - (size_t)kItems);
    check(plainLate == (size_t)kItems,
          "B · and an item WITHOUT the bit is purged on schedule (the GC still works)");

    // ══ C + D. An entity's payload across ordinary movement ══════════════════════════════════════
    {
        const uint32_t uuid = 9300;
        for (int i = 0; i < 6; ++i)
        { sendEntity(udp, uuid, 100.0f + 0.5f * i, 64); std::this_thread::sleep_for(std::chrono::milliseconds(50)); }

        int size = -1; float x = 0.0f;
        observe(udp, uuid, 600, size, x);
        std::printf("    payload just sent: dataSize %d at x=%.1f\n", size, x);
        check(size == 64, "C · an entity's payload reaches the zone");

        const float xBefore = x;
        for (int i = 0; i < 20; ++i)
        { sendEntity(udp, uuid, 200.0f + 1.0f * i, 0); std::this_thread::sleep_for(std::chrono::milliseconds(50)); }

        observe(udp, uuid, 600, size, x);
        std::printf("    after a second of movement with dataSize 0: dataSize %d at x=%.1f (was %.1f)\n",
                    size, x, xBefore);
        check(size == 64,
              "C · and it SURVIVES a second of ordinary movement updates (dataSize 0 = unchanged)");
        check(x > xBefore + 50.0f,
              "D · while the movement in those same updates WAS applied (it is a merge, not a veto)");
    }

    g_done = true;
    kill(zone, SIGTERM);
    waitpid(zone, nullptr, 0);
    th.join();

    std::printf("\n== world_object_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
