// ─────────────────────────────────────────────────────────────────────────────────────────────────
// THE VALIDATOR END TO END — the anti-cheat path, which had not a single test.
//
// What existed: the rules module's tests live in the engine (`haruka-cpp/tests/test_dgs.cpp`) and
// dlopen the `.so` to check the FORMULA. Nobody checked the NODE that uses it: that it comes up, loads
// the module, asks it about EVERY request and answers with a correlated verdict. A `validador_node`
// that replied "legal" to everything went unnoticed.
//
// This test stands up the minimum topology the node demands to start — it refuses to proceed without a
// head server and persistence, then blocks waiting for an initial `Command` — and interrogates it:
//
//     [fake head] --Command--> [validador_node] <--PKT_VALIDATE_REQ-- [this test, acting as a zone]
//     [fake persistence] <-------'                        `--ValidateAck-->
//
// The two assertions hold each other up: a node that ALWAYS said yes fails the teleport case, and one
// that ALWAYS said no fails the legal-movement case. Neither on its own proves the module is being
// consulted at all.
//
// It uses `stub_rules.so` (from this repo) and NOT the game's module: the network project has to be
// testable on its own.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
}

// ⚠️ PORTS BELOW 32768, AND THAT IS NOT ARBITRARY. Every e2e test in this suite used to listen on
// 47xxx, which sits inside Linux's default ephemeral range (`ip_local_port_range` = 32768..60999). A
// client socket in ANY test can be handed one of those as its SOURCE port, and then another test's
// `listen()` fails with EADDRINUSE — `SO_REUSEADDR` does not help against that. Observed exactly
// once in a full run: `net_degraded` could not bind 47502 because `reconnect_e2e` was holding it as
// the source port of a connection to 47453. Ports in 21xxx are outside that range, so the kernel
// never hands them out on its own. (`ping_pong` and `head_routing_e2e` still use 42424 because the
// head server hardcodes it — that one remains exposed.)
static const int kHeadPort  = 21421;
static const int kPersPort  = 21422;
static const int kValTcp    = 21423;
static const int kValUdp    = 21424;

static const float kChunkM = 1.0f;    // the test's arithmetic stays in round metres

/** @brief Fake head server: accepts ONE connection and sends the initial `Command` the validator waits
 *  for to learn the chunk size. Without it the node exits with "No initial Command received". */
static void fakeHead(std::atomic<bool>& ready, std::atomic<bool>& done)
{
    DGS::TCPSocket s;
    if (!s.listen(kHeadPort)) { ready = true; return; }
    ready = true;
    const int fd = s.accept();
    if (fd < 0) return;

    DGS::Command cmd{};
    cmd.purpose    = DGS::HeadPurpose{};
    cmd.chunkX     = 0; cmd.chunkY = 0;
    cmd.port       = kValTcp;
    cmd.chunkSizeX = kChunkM; cmd.chunkSizeY = kChunkM; cmd.chunkSizeZ = kChunkM;
    std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");

    DGS::Packet p;
    p.pack(cmd);
    s.send(fd, p.getRawData(), p.getSize());

    while (!done) std::this_thread::sleep_for(std::chrono::milliseconds(20));
    s.closeClient(fd);
}

/** @brief Fake persistence: it only has to accept and swallow. The validator forwards what it
 *  validates there, and will not even start if it cannot connect. */
static void fakePersistence(std::atomic<bool>& ready, std::atomic<bool>& done)
{
    DGS::TCPSocket s;
    if (!s.listen(kPersPort)) { ready = true; return; }
    ready = true;
    const int fd = s.accept();
    if (fd < 0) return;
    uint8_t buf[4096];
    while (!done) {
        timeval tv{}; tv.tv_usec = 100000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        if (s.receive(fd, buf, sizeof(buf)) < 0 && done) break;
    }
    s.closeClient(fd);
}

/** @brief One MOVEMENT validation request, and the verdict the node returns.
 *  @return 1 legal · 0 violation · -1 no answer. */
static int askVerdict(DGS::TCPSocket& zone, uint32_t reqId,
                          double lastGX, double lastGY, double lastGZ,
                          double gx, double gy, double gz,
                          float maxSpeed, float dt, uint32_t& ackIdOut)
{
    DGS::ValidateRequest req{};
    req.requestId  = reqId;
    req.entityUuid = 4242;
    req.ownerZone  = 1;
    req.moduleId   = 0;
    req.kind       = 0;                 // 0 = movement
    req.entity.uuid   = 4242;
    req.entity.chunkX = 0; req.entity.chunkY = 0; req.entity.chunkZ = 0;
    req.entity.pos[0] = (float)gx; req.entity.pos[1] = (float)gy; req.entity.pos[2] = (float)gz;
    req.lastGX = (float)lastGX; req.lastGY = (float)lastGY; req.lastGZ = (float)lastGZ;
    req.maxSpeed  = maxSpeed;
    req.dtSeconds = dt;

    DGS::Packet p;
    p.pack(req);
    if (!zone.send(zone.getSocketFD(), p.getRawData(), p.getSize())) return -1;

    uint8_t buf[4096];
    const int n = zone.receive(zone.getSocketFD(), buf, sizeof(buf));
    if (n <= 0) return -1;

    DGS::Packet r;
    r.setBuffer(buf, n);
    const DGS::ValidateAck ack = r.unpackValidateAck();
    ackIdOut = ack.requestId;
    return (int)ack.verdict;
}

int main(int argc, char** argv)
{
    const char* nodePath = (argc > 1) ? argv[1] : "./build/validador_node";
    const char* stubPath = (argc > 2) ? argv[2] : "./build/stub_rules.so";

    std::atomic<bool> headReady{false}, persReady{false}, done{false};
    std::thread th(fakeHead, std::ref(headReady), std::ref(done));
    std::thread tp(fakePersistence, std::ref(persReady), std::ref(done));
    while (!headReady || !persReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); done = true; th.join(); tp.join(); return 1; }
    if (pid == 0) {
        std::freopen("/dev/null", "w", stdout);
        setenv("VALIDADOR_TCP_PORT", std::to_string(kValTcp).c_str(), 1);
        setenv("VALIDADOR_UDP_PORT", std::to_string(kValUdp).c_str(), 1);
        setenv("HEAD_SERVER_HOST", "127.0.0.1", 1);
        setenv("HEAD_SERVER_PORT", std::to_string(kHeadPort).c_str(), 1);
        setenv("PERSISTENCE_HOST", "127.0.0.1", 1);
        setenv("PERSISTENCE_PORT", std::to_string(kPersPort).c_str(), 1);
        setenv("GAME_MODULE_SO", stubPath, 1);   // the toy one, NOT the game's
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    // Connect to the validator ACTING AS A ZONE. Retry, not `sleep`: the node takes as long as it
    // takes to talk to the fake head and load the module.
    DGS::TCPSocket zone;
    bool up = false;
    for (int i = 0; i < 200 && !up; ++i) {
        if (zone.connect("127.0.0.1", kValTcp)) { up = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    check(up, "the validator starts with head+persistence and accepts the zone");

    if (up) {
        timeval tv{}; tv.tv_sec = 3;
        setsockopt(zone.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        // (1) LEGAL MOVEMENT: 5 m/s for 1 s -> 5 m. Well under the cap.
        uint32_t ackId = 0;
        const int v1 = askVerdict(zone, 101, 0, 0, 0, 5.0, 0.0, 0.0, 5.0f, 1.0f, ackId);
        check(v1 == 1, "a 5 m step at 5 m/s over 1 s is declared LEGAL");
        check(ackId == 101, "the ack is correlated with the request's requestId");

        // (2) TELEPORT: 5 km in 1 s with the same 5 m/s limit. This is (1)'s counter-proof: without it,
        //     a node that answered "legal" to everything would pass the previous case without consulting
        //     the module even once.
        uint32_t ackId2 = 0;
        const int v2 = askVerdict(zone, 102, 0, 0, 0, 5000.0, 0.0, 0.0, 5.0f, 1.0f, ackId2);
        check(v2 == 0, "a 5 km jump in 1 s with a 5 m/s cap is declared a VIOLATION");
        check(ackId2 == 102, "the second case's ack correlates too");

        // (3) And the edge: just under the module's cap (maxSpeed * dt * 1.5). It checks that the
        //     verdict comes from the RULE and not from some arbitrary threshold in the node.
        uint32_t ackId3 = 0;
        const int v3 = askVerdict(zone, 103, 0, 0, 0, 7.0, 0.0, 0.0, 5.0f, 1.0f, ackId3);
        check(v3 == 1, "7 m against an effective cap of 7.5 m is still legal (the module's margin rules)");
        uint32_t ackId4 = 0;
        const int v4 = askVerdict(zone, 104, 0, 0, 0, 8.0, 0.0, 0.0, 5.0f, 1.0f, ackId4);
        check(v4 == 0, "8 m against an effective cap of 7.5 m is already a violation");
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    done = true;
    th.join();
    tp.join();

    std::printf("\n== validator_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
