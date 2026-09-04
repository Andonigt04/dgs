// ─────────────────────────────────────────────────────────────────────────────────────────────────
// `persistance_node` — that a database outage does NOT take the node down.
//
// The node receives entities and inserts them into MongoDB. `insert_one` **throws** when the database
// does not answer, and nothing caught that exception: an uncaught exception is `std::terminate`, so a
// Mongo outage killed the entire persistence node with the first entity that arrived. A database
// failure has to DEGRADE the service, not take it down.
//
// This test does not need a live database — quite the opposite: it needs a DEAD one. It is given a URI
// that resolves nowhere, entities are sent to it, and we check the node IS STILL STANDING and still
// accepting connections. It is the only way to test the resilience without standing up a Mongo.
//
// EXECUTED, and the reading of the exception held up. It had gone unrun for a long time because
// `mongocxx` was absent, so `persistance_node` was not even built (`DGS_HAVE_MONGO` = FALSE) and the
// test was not registered. Built against mongo-cxx-driver 3.11.0 (with mongo-c-driver 1.30.2) it runs,
// and the node's own log shows the mechanism rather than merely the outcome:
//
//     [Persistence] FAILED to store uuid=201: No suitable servers found
//                   (`serverSelectionTryOnce` set): [connection refused calling hello on
//                   '127.0.0.1:1']. Topology type: Single: generic server error
//     [Persistence] Entity LOST uuid=201
//
// `insert_one` does throw, the catch does catch it, the loss is recorded and the node stays up.
//
// COUNTER-PROOF, because "the node is still standing" would otherwise be satisfied by a node that
// never had a problem: with the `try`/`catch` removed the run goes to **1 OK · 2 FAILED** — the node
// dies on the first entity (an uncaught exception is `std::terminate`) and stops accepting.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
#include <sys/wait.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
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

static const int kPersPort = 21495;
static const int kLivePort = 21497;   // the live phase, kept off the dead phase's port

/// Is the process still alive? `waitpid` with WNOHANG does not block: if it died, it returns its pid.
static bool stillAlive(pid_t pid)
{
    int st = 0;
    return waitpid(pid, &st, WNOHANG) == 0;
}

static void sendEntity(DGS::TCPSocket& s, uint32_t uuid)
{
    DGS::EntityTransfer e{};
    e.uuid = uuid; e.chunkX = 3; e.chunkY = 4;
    e.pos[0] = 1.0f;
    DGS::Packet p; p.pack(e);
    s.send(s.getSocketFD(), p.getRawData(), p.getSize());
}

// ─────────────────────────────────────────────────────────────────────────────────────────────────
// PART TWO: a LIVE database, and the half of "persistence" that did not exist.
//
// Everything above tests that an outage does not kill the node. It says nothing about whether the data
// ever reaches Mongo, or whether it can be got back — and it could not: the node ONLY EVER WROTE.
// `insert_one` and nothing else; no `find` anywhere in the repository. A persistence node that cannot
// restore anything is not persisting, it is discarding slowly.
//
// Measured against a live Mongo before any of this was written, and each one is now fixed above:
//   · six updates of ONE entity left SIX documents (insert per update, no upsert);
//   · the collection had only the `_id_` index, so a lookup by uuid was a full scan;
//   · no document carried a timestamp, so "the latest" was not even expressible;
//   · `stats` — health, speed, damage — were never stored at all. An entity restored from the
//     database came back at zero health. Only reading one back could show that.
//
// THE DECISIVE POINT IS THE RESTART. The node is stopped and a NEW PROCESS is started before the read,
// so what comes back cannot be anything this run held in memory. Without that, an in-process cache
// would pass every check below.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
static const char* kTestDb = "dgs_e2e_test";

static pid_t startNode(const char* nodePath, const char* uri, int port, bool verbose)
{
    // ⚠️ FLUSH BEFORE FORKING. stdout is fully buffered when it is not a terminal, `fork` duplicates
    // the unflushed buffer into the child, and the child's `freopen` closes that stream — which flushes
    // it. Every line printed and not yet flushed came out TWICE, once from each process. Harmless here,
    // but it is exactly how a test ends up appearing to have run a phase it never ran.
    std::fflush(stdout);
    const pid_t p = fork();
    if (p != 0) return p;
    if (!verbose) std::freopen("/dev/null", "w", stdout);
    setenv("PERSISTENCE_PORT", std::to_string(port).c_str(), 1);
    setenv("MONGO_URI", uri, 1);
    setenv("MONGO_DB",  kTestDb, 1);
    char tmpl[] = "/tmp/dgs_pers_live_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(nodePath, nodePath, (char*)nullptr);
    _exit(127);
}

static bool waitPort(DGS::TCPSocket& s, int port, int tries)
{
    for (int i = 0; i < tries; ++i) {
        if (s.connect("127.0.0.1", port)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return false;
}

static void stopNode(pid_t pid)
{
    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
}

/// Fills an entity with values that are all distinct and none of them zero — a field that is dropped
/// on the way and comes back as 0 has to be visible as a difference, not indistinguishable from
/// "correctly stored a zero".
static DGS::EntityTransfer makeEntity(uint32_t uuid, float x, float health, float speed)
{
    DGS::EntityTransfer e{};
    e.uuid   = uuid;
    e.type   = DGS::ENT_NPC;
    e.chunkX = 3; e.chunkY = -4; e.chunkZ = 5;
    e.pos[0] = x; e.pos[1] = 2.5f; e.pos[2] = -3.75f;
    e.angle  = 1234;
    e.stats.health   = health;
    e.stats.speed[0] = speed;
    e.stats.baseDMG  = health * 0.5f;
    e.stats.healing  = 0.25f;
    e.dataSize = 8;
    for (int i = 0; i < 8; ++i) e.data[i] = (uint8_t)(0xA0 + i);
    return e;
}

static void livePhase(const char* nodePath, bool verbose)
{
    const char* host = std::getenv("MONGO_HOST") ? std::getenv("MONGO_HOST") : "127.0.0.1";
    const int   port = std::atoi(std::getenv("MONGO_PORT") ? std::getenv("MONGO_PORT") : "27017");
    const std::string uri = "mongodb://" + std::string(host) + ":" + std::to_string(port);

    // Is there a database at all? Asked with a plain connect, before spending anything else.
    DGS::TCPSocket probe;
    const bool haveMongo = probe.connect(host, port, 1000);

    if (!haveMongo)
    {
        // ⚠️ A SKIP THAT NOBODY SEES IS A TEST THAT DOES NOT EXIST. This whole suite was once a set of
        // `|| true` demos, so the skip is loud, and `DGS_REQUIRE_MONGO=1` (which CI sets, because CI
        // provides the database) turns it into a FAILURE — where the database is guaranteed, a skip is
        // a regression, not an excuse.
        std::printf("\n  ──────────────────────────────────────────────────────────────────────\n");
        std::printf("  SKIPPED: the live save-and-retrieve needs a Mongo at %s:%d.\n", host, port);
        std::printf("           podman run -d --rm -p 27017:27017 docker.io/library/mongo:7\n");
        std::printf("  ──────────────────────────────────────────────────────────────────────\n");
        if (std::getenv("DGS_REQUIRE_MONGO"))
            check(false, "DGS_REQUIRE_MONGO is set but no database answered: this is a failure, not a skip");
        return;
    }

    // ⚠️ THE VALUES CHANGE ON EVERY RUN, and that is not decoration. The first version used the same
    // numbers each time, and its counter-proof came out GREEN: with the writer patched to stop storing
    // `stats` the test still passed, because the document left by the PREVIOUS run still had the right
    // health in it — `$set` updates the fields it is given and never removes the ones it is not, so a
    // field the writer stops writing does not disappear, it goes stale. The assertion was comparing
    // against last run's answer. Values derived from the pid make a stale document impossible to
    // mistake for a fresh one; the uuid stays fixed so runs overwrite instead of piling up.
    const int      salt   = (int)(getpid() % 500);
    const uint32_t kUuid  = 90001;
    const float    health = 30.0f + (float)salt;
    const float    speed  = 3.5f  + (float)salt;
    const float    x2     = 40.5f + (float)salt;
    std::printf("\n  live database at %s:%d, db=%s (run values salted with %d)\n",
                host, port, kTestDb, salt);

    // ── Write, with a process that then goes away ────────────────────────────────────────────────
    const pid_t writer = startNode(nodePath, uri.c_str(), kLivePort, verbose);
    DGS::TCPSocket w;
    const bool writerUp = waitPort(w, kLivePort, 300);
    check(writerUp, "the persistence node starts against a LIVE database");
    if (!writerUp) { stopNode(writer); return; }

    const DGS::EntityTransfer first  = makeEntity(kUuid, 1.25f, health + 1000.0f, speed + 1000.0f);
    const DGS::EntityTransfer second = makeEntity(kUuid, x2,    health,               speed);
    { DGS::Packet p; p.pack(first);  w.send(w.getSocketFD(), p.getRawData(), p.getSize()); }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    // The SAME uuid again. With the old `insert_one` this second write would collide with the unique
    // index and be recorded as LOST; with the upsert it replaces the state.
    { DGS::Packet p; p.pack(second); w.send(w.getSocketFD(), p.getRawData(), p.getSize()); }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    stopNode(writer);

    // ── Read, from a DIFFERENT process ──────────────────────────────────────────────────────────
    const pid_t reader = startNode(nodePath, uri.c_str(), kLivePort, verbose);
    DGS::TCPSocket r;
    const bool readerUp = waitPort(r, kLivePort, 300);
    check(readerUp, "a SECOND, fresh process starts and can be asked (its memory holds nothing)");
    if (!readerUp) { stopNode(reader); return; }

    uint8_t buf[8192];
    int lastLen = 0;
    // One send, one receive: `TCPSocket` length-prefixes every message, so a read returns exactly one
    // packet. Nothing here has to reassemble a stream.
    auto askAndRead = [&](DGS::Packet& q) -> bool {
        r.send(r.getSocketFD(), q.getRawData(), q.getSize());
        for (int i = 0; i < 40; ++i) {
            const int got = r.receive(r.getSocketFD(), buf, sizeof(buf));
            if (got > 0) { lastLen = got; return true; }
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        return false;
    };

    DGS::Packet q; q.packPersistQuery(kUuid);
    const bool got1 = askAndRead(q);
    const int n = got1 ? lastLen : 0;
    DGS::Packet resp;
    if (n > 0) resp.setBuffer(buf, (size_t)n);
    DGS::EntityTransfer back{};
    const bool decoded = n > 0 && resp.tryUnpackEntityTransfer(back);
    check(decoded, "the entity comes BACK out of the database (persistence that can restore)");

    if (decoded)
    {
        check(back.uuid == kUuid, "with its uuid");
        check(back.chunkX == second.chunkX && back.chunkY == second.chunkY &&
              back.chunkZ == second.chunkZ, "its chunk, negative component included");
        check(back.pos[1] == second.pos[1] && back.pos[2] == second.pos[2],
              "its position inside the chunk");
        check(back.angle == second.angle, "its angle");
        // The one that was silently dropped for the whole life of this node.
        check(back.stats.health  == second.stats.health &&
              back.stats.speed[0] == second.stats.speed[0] &&
              back.stats.baseDMG == second.stats.baseDMG,
              "and its STATS: health/speed/damage used never to be stored at all");

        bool blobOk = back.dataSize == 8;
        for (int i = 0; i < 8 && blobOk; ++i) blobOk = back.data[i] == (uint8_t)(0xA0 + i);
        check(blobOk, "the module's opaque blob survives the round trip byte for byte");

        // ── Upsert, not append ───────────────────────────────────────────────────────────────────
        // Two writes of one uuid must leave ONE state, and it must be the SECOND. This is what stops
        // the collection growing by one document per position update at 10-20 Hz per player.
        std::printf("    stored x: first %.2f, then %.2f · retrieved %.2f\n",
                    first.pos[0], second.pos[0], back.pos[0]);
        check(back.pos[0] == second.pos[0],
              "the second write REPLACED the first: it stores state, not one document per update");
    }

    // ── Counter-proof ────────────────────────────────────────────────────────────────────────────
    // Without this, every check above would also pass on a node that simply echoed back whatever it
    // was asked for. An entity that was never stored has to come back as nothing.
    DGS::Packet q2; q2.packPersistQuery(4242424u);
    const bool got2 = askAndRead(q2);
    const int n2 = got2 ? lastLen : 0;
    DGS::Packet resp2;
    if (n2 > 0) resp2.setBuffer(buf, (size_t)n2);
    DGS::EntityTransfer ghost{};
    const bool inventedOne = n2 > 0 && resp2.tryUnpackEntityTransfer(ghost);
    std::printf("    asking for a uuid that was never stored: %d bytes, type %d\n",
                n2, n2 > 0 ? (int)resp2.getType() : -1);
    check(n2 > 0 && !inventedOne && resp2.getType() == DGS::PKT_NONE,
          "an entity that was NEVER stored comes back as nothing (it is not echoing the question)");

    stopNode(reader);
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX];
    const char* argPath  = (argc > 1) ? argv[1] : "./build/persistance_node";
    const char* nodePath = realpath(argPath, abs) ? abs : argPath;

    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); return 1; }
    if (pid == 0) {
        if (!std::getenv("PERS_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
        setenv("PERSISTENCE_PORT", std::to_string(kPersPort).c_str(), 1);
        // A closed port on the loopback: the database does NOT answer. That is the scenario under test.
        setenv("MONGO_URI", "mongodb://127.0.0.1:1/", 1);
        char tmpl[] = "/tmp/dgs_pers_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    DGS::TCPSocket cli;
    bool up = false;
    for (int i = 0; i < 200 && !up; ++i) {
        if (cli.connect("127.0.0.1", kPersPort)) { up = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    check(up, "the persistence node starts even though the database does NOT answer");

    if (up) {
        // Several entities: with the old version the FIRST one already killed it.
        for (uint32_t id : {201u, 202u, 203u}) {
            sendEntity(cli, id);
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
        check(stillAlive(pid),
              "after several entities with the database down, the node IS STILL STANDING (no abort)");

        // And it keeps serving: a second connection has to get in. Being "alive" is not enough — a node
        // stuck in an exception loop would not accept anyone either.
        DGS::TCPSocket cli2;
        bool another = false;
        for (int i = 0; i < 80 && !another; ++i) {
            if (cli2.connect("127.0.0.1", kPersPort)) { another = true; break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        check(another, "and it KEEPS ACCEPTING new connections (it degrades, it does not jam)");
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);

    livePhase(nodePath, std::getenv("PERS_E2E_VERBOSE") != nullptr);

    std::printf("\n== persistence_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
