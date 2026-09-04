// ─────────────────────────────────────────────────────────────────────────────────────────────────
// restore_e2e — a zone that comes back with its world still in it.
//
// A zone used to start EMPTY, always, and there was nothing anyone could do about it: BOTH ends of
// persistence were orphaned.
//
//   · Nothing could READ. `persistance_node` only ever called `insert_one`; there was no `find`
//     anywhere in the repository. Whatever was in Mongo was unreachable.
//   · Nothing ever WROTE, either — in a live cluster. The write path was reachable only by sending the
//     persistence node a raw PKT_ENTITY_TRANSFER over TCP, and nobody does: the zone sends the
//     validator a PKT_VALIDATE_REQ, the validator answers an ACK and forwards nothing, and the
//     `cache_node` that would have relayed one had no client at all (it has since been deleted).
//     Measured before writing any of this, with head + validator + persistence + zone + four players
//     running for five seconds:
//     `Entity stored` 0, documents in Mongo 0.
//
// So this test could not have been written at all, and neither half could be added without the other:
// restoring from a database nothing writes to is theatre, and writing to one nothing reads from is a
// slow delete. What is checked here is the whole loop, end to end:
//
//     players -> zone A -> persistence -> Mongo -> [zone A killed] -> zone B -> broadcast
//
// The zone that reads is a DIFFERENT PROCESS from the one that wrote, and NO CLIENT connects to it. It
// serves what it serves because the database said so, not because anyone told it during this run.
//
// THE COUNTER-PROOF is a third zone covering a region that does not contain those entities. Same
// database, same node, same query path: it must restore NOTHING. Without it, "zone B has entities"
// would also pass on a node that handed back every document it owned regardless of what was asked.
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
#include <set>
#include <string>
#include <thread>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);
}

// Below the ephemeral range on purpose — see the note in `validator_e2e.cpp`.
static const int kHeadPort = 21631;
static const int kPersPort = 21632;
static const int kZoneUdp  = 21633;

static const float       kChunkM       = 1000.0f;
static const char*       kTestDb       = "dgs_restore_test";
static const char*       kObserveToken = "restore-e2e-token";
static const int32_t     kChunk        = 50;     // where the players live
static const int         kPlayers      = 4;

static std::atomic<bool> g_done{false};
static std::atomic<int>  g_activeAtNode{-1};   // what the NODE says it is serving
static std::atomic<int>  g_metrics{0};

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void fakeHead(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kHeadPort)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;
    while (!g_done) {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        DGS::Command cmd{};
        cmd.chunkSizeX = kChunkM; cmd.chunkSizeY = kChunkM; cmd.chunkSizeZ = kChunkM;
        cmd.port = kZoneUdp;
        std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");
        DGS::Packet p; p.pack(cmd);
        s.send(fd, p.getRawData(), p.getSize());
        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 300000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;
            if (n < 0)  continue;
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() == DGS::PKT_METRICS) {
                g_activeAtNode = (int)r.unpackServerMetrics().activeEntities;
                ++g_metrics;
            }
        }
        s.closeClient(fd);
    }
}

static pid_t spawn(const char* path, void (*envFn)(void*), void* arg, const char* dirTag)
{
    std::fflush(stdout);   // see the note in persistence_e2e: fork duplicates an unflushed buffer
    const pid_t p = fork();
    if (p != 0) return p;
    if (!std::getenv("RESTORE_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
    envFn(arg);
    std::string tmpl = std::string("/tmp/dgs_") + dirTag + "_XXXXXX";
    std::vector<char> t(tmpl.begin(), tmpl.end()); t.push_back('\0');
    if (const char* d = mkdtemp(t.data())) { if (chdir(d) != 0) {} }
    execl(path, path, (char*)nullptr);
    _exit(127);
}

struct ZoneCfg { int32_t min, max; int persistMs; const char* uri; };

static void zoneEnv(void* arg)
{
    const ZoneCfg* c = (const ZoneCfg*)arg;
    setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
    setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
    setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
    // No validator and no social node in this test: the subject is persistence, and the zone is
    // required to work without them (fail-open S1).
    setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
    setenv("VALIDATOR_TCP_PORT", "21639", 1);
    setenv("SOCIAL_HOST",        "127.0.0.1", 1);
    setenv("SOCIAL_TCP_PORT",    "21640", 1);
    setenv("PERSISTENCE_HOST",   "127.0.0.1", 1);
    setenv("PERSISTENCE_PORT",   std::to_string(kPersPort).c_str(), 1);
    const std::string mn = std::to_string(c->min), mx = std::to_string(c->max);
    setenv("CHUNK_X_MIN", mn.c_str(), 1); setenv("CHUNK_X_MAX", mx.c_str(), 1);
    setenv("CHUNK_Y_MIN", mn.c_str(), 1); setenv("CHUNK_Y_MAX", mx.c_str(), 1);
    setenv("CHUNK_Z_MIN", mn.c_str(), 1); setenv("CHUNK_Z_MAX", mx.c_str(), 1);
    setenv("CHUNK_SIZE_X", "1000.0", 1);
    setenv("CHUNK_SIZE_Y", "1000.0", 1);
    setenv("CHUNK_SIZE_Z", "1000.0", 1);
    // Long, so nothing is purged mid-measurement: the subject is the restore, not the GC.
    setenv("ENTITY_LEASE_MS", "60000", 1);
    setenv("ZONE_PERSIST_MS", std::to_string(c->persistMs).c_str(), 1);
    setenv("DGS_OBSERVE_TOKEN", kObserveToken, 1);
    setenv("GAME_MODULE_SO", "", 1);   // no rules module: no simulation moving things underneath us
}

static void persEnv(void* arg)
{
    setenv("PERSISTENCE_PORT", std::to_string(kPersPort).c_str(), 1);
    setenv("MONGO_URI", (const char*)arg, 1);
    setenv("MONGO_DB",  kTestDb, 1);
}

static void stop(pid_t pid)
{
    if (pid <= 0) return;
    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
}

static void sendPlayer(DGS::UDPSocket& udp, uint32_t uuid, float x)
{
    DGS::EntityTransfer e{};
    e.uuid   = uuid;
    e.type   = DGS::ENT_PLAYER;
    e.chunkX = kChunk; e.chunkY = kChunk; e.chunkZ = kChunk;
    e.pos[0] = x;
    e.stats.speed[0] = 200.0f;   // a vehicle: the steps here stay well under it, so S1 lets them by
    e.stats.health   = 77.0f;    // distinctive and non-zero: a dropped field must not look like zero
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

static void subscribe(DGS::UDPSocket& udp, const char* token = kObserveToken)
{
    DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(token);
    udp.send("127.0.0.1", kZoneUdp, hello.getRawData(), hello.getSize());
}

/// Watches a zone from the outside for `ms`, as the viewer does. @return what it is serving.
static DGS::ViewerState watch(DGS::UDPSocket& udp, int ms)
{
    DGS::ViewerState st(kChunkM, /*ttlMs*/ 5000);
    uint8_t buf[8192];
    std::string from; int port = 0;
    const uint64_t until = nowMs() + (uint64_t)ms;
    while (nowMs() < until)
    {
        subscribe(udp);   // the subscription is a lease: renew it, as the viewer does
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        int n;
        while ((n = udp.receive(buf, sizeof(buf), from, port)) > 0)
            st.onDatagram(buf, n, nowMs());
    }
    return st;
}

static bool waitMetric(int want, int msLimit)
{
    const uint64_t until = nowMs() + (uint64_t)msLimit;
    while (nowMs() < until) {
        if (g_activeAtNode.load() == want) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char absZone[PATH_MAX], absPers[PATH_MAX];
    const char* zonePath = realpath((argc > 1) ? argv[1] : "./build/zone_node", absZone)
                           ? absZone : "./build/zone_node";
    const char* persPath = realpath((argc > 2) ? argv[2] : "./build/persistance_node", absPers)
                           ? absPers : "./build/persistance_node";

    const char* mHost = std::getenv("MONGO_HOST") ? std::getenv("MONGO_HOST") : "127.0.0.1";
    const int   mPort = std::atoi(std::getenv("MONGO_PORT") ? std::getenv("MONGO_PORT") : "27017");
    const std::string uri = "mongodb://" + std::string(mHost) + ":" + std::to_string(mPort);

    {
        DGS::TCPSocket probe;
        if (!probe.connect(mHost, mPort, 1000))
        {
            // ⚠️ A skip nobody sees is a test that does not exist. Loud, and `DGS_REQUIRE_MONGO`
            // (which CI sets, because CI provides the database) makes it a failure instead.
            std::printf("  ──────────────────────────────────────────────────────────────────────\n");
            std::printf("  SKIPPED: restoring a zone needs a live Mongo at %s:%d.\n", mHost, mPort);
            std::printf("           podman run -d --rm -p 27017:27017 docker.io/library/mongo:7\n");
            std::printf("  ──────────────────────────────────────────────────────────────────────\n");
            if (std::getenv("DGS_REQUIRE_MONGO"))
            {
                check(false, "DGS_REQUIRE_MONGO is set but no database answered: a failure, not a skip");
                std::printf("\n== restore_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
                return 1;
            }
            std::printf("\n== restore_e2e: skipped ==\n");
            return 0;
        }
    }

    std::atomic<bool> headReady{false};
    std::thread head(fakeHead, std::ref(headReady));
    while (!headReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pers = spawn(persPath, persEnv, (void*)uri.c_str(), "restore_pers");
    { DGS::TCPSocket up; bool ok = false;
      for (int i = 0; i < 300 && !ok; ++i) {
          DGS::TCPSocket t; if (t.connect("127.0.0.1", kPersPort)) { ok = true; break; }
          std::this_thread::sleep_for(std::chrono::milliseconds(25));
      }
      check(ok, "the persistence node is up against a live database");
      if (!ok) { stop(pers); g_done = true; head.join(); return 1; }
    }

    // ══ (1) A zone with players in it, writing through ═══════════════════════════════════════════
    ZoneCfg cfgA{ 0, 100, /*persistMs*/ 1000, uri.c_str() };
    pid_t zoneA = spawn(zonePath, zoneEnv, &cfgA, "restore_zoneA");

    DGS::UDPSocket player;
    player.bind(0);
    { timeval tv{}; tv.tv_usec = 50000;
      setsockopt(player.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

    for (int round = 0; round < 40 && g_activeAtNode.load() != kPlayers; ++round) {
        for (int i = 0; i < kPlayers; ++i) sendPlayer(player, 7000 + i, 100.0f + 10.0f * round);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    check(waitMetric(kPlayers, 3000), "zone A serves the four players that connected to it");

    // Give the write-through (1 s here) time to run at least twice, so the test is not racing the
    // very first snapshot.
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    stop(zoneA);
    zoneA = 0;
    g_activeAtNode = -1;

    // ══ (2) A NEW process, same region, and NO client ════════════════════════════════════════════
    ZoneCfg cfgB{ 0, 100, /*persistMs*/ 0, uri.c_str() };   // 0: B must not write, only read
    pid_t zoneB = spawn(zonePath, zoneEnv, &cfgB, "restore_zoneB");

    const bool restored = waitMetric(kPlayers, 8000);
    std::printf("    zone B (fresh process, nobody connected) is serving: %d\n", g_activeAtNode.load());
    check(restored,
          "a NEW zone comes up already serving the four entities, with no client in sight");

    // And it is not merely counting them: it broadcasts them, so a viewer sees the world repopulated.
    DGS::UDPSocket viewer;
    viewer.bind(0);
    { timeval tv{}; tv.tv_usec = 50000;
      setsockopt(viewer.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    DGS::ViewerState seen = watch(viewer, 1500);
    std::printf("    an outside observer of zone B sees %zu entities\n", seen.entityCount());
    check(seen.entityCount() == (size_t)kPlayers,
          "and it BROADCASTS them: the restored world is visible from outside, not just counted");

    std::set<uint32_t> uuids;
    for (const auto& e : seen.entities()) uuids.insert(e.uuid);
    bool sameIds = uuids.size() == (size_t)kPlayers;
    for (int i = 0; i < kPlayers && sameIds; ++i) sameIds = uuids.count(7000 + i) != 0;
    check(sameIds, "with the same uuids that were written (they are those entities, not new ones)");

    stop(zoneB);
    g_activeAtNode = -1;

    // ══ (3) COUNTER-PROOF: a zone somewhere else restores nothing ════════════════════════════════
    // Same database, same persistence node, same query path — only the region differs. If this also
    // came up with four entities, every check above would be measuring "the node hands back whatever
    // it has" rather than "the zone got back ITS region".
    ZoneCfg cfgC{ 200, 300, /*persistMs*/ 0, uri.c_str() };
    pid_t zoneC = spawn(zonePath, zoneEnv, &cfgC, "restore_zoneC");

    // Wait for it to have reported at least once, then read what it says.
    const int before = g_metrics.load();
    for (int i = 0; i < 200 && g_metrics.load() <= before + 2; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::printf("    zone C covers chunks 200-300; the entities are at %d: serving %d\n",
                kChunk, g_activeAtNode.load());
    check(g_activeAtNode.load() == 0,
          "a zone covering a DIFFERENT region restores nothing (it asked for its own, and got it)");

    stop(zoneC);
    stop(pers);
    g_done = true;
    head.join();

    std::printf("\n== restore_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
