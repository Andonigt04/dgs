// ─────────────────────────────────────────────────────────────────────────────────────────────────
// `zone_node` END TO END — 928 lines that had not a single test.
//
// `validator_e2e` covers the ARBITER: that the validator consults the module and answers. This covers
// the other half, the one that decides whether the anti-cheat is worth anything: **that the zone asks,
// and that it OBEYS the answer**. A zone that requested a verdict and filed it in a drawer would sail
// through the validator's test.
//
// Minimum topology (the zone connects to all three or never fully starts):
//
//     [fake head]  <--ServerMetrics--  [zone_node]  <--EntityTransfer over UDP--  [this test]
//     [fake validator] <--PKT_VALIDATE_REQ--'  `--PKT_VALIDATE_ACK (a verdict I choose)-->
//     [fake social]  <--'
//
// The observable is `ServerMetrics::activeEntities`, which the zone publishes to the head 10 times a
// second: its registry of live entities seen from outside, without touching its innards.
//
// The three phases hold each other up:
//   A) plausible movement + verdict 1  -> the entity STAYS
//   B) teleport                        -> the LOCAL S1 filter cuts it and it never gets to ask
//   C) plausible movement + verdict 0  -> the entity IS EVICTED
// Without (C), a zone that ignored the verdict would pass (A). Without (A), one that always evicted
// would pass (C). And (B) is what separates "has its own defence" from "delegates everything".
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

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

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);   // if the test dies on a timeout, what already passed must still be visible
}

static const int kHeadPort   = 21431;
static const int kValPort    = 21432;
static const int kSocialPort = 21433;
static const int kZoneUdp    = 21434;

static const float kChunkM = 1.0f;

// What the fake head sees of the zone.
static std::atomic<int>  g_activeEntities{-1};   // -1 = no metric has arrived yet
static std::atomic<int>  g_metricsCount{0};
static std::atomic<int>  g_chunkXMax{-1};

// What the fake validator sees, and what it answers.
static std::atomic<int>  g_reqCount{0};
static std::atomic<int>  g_verdict{1};           // the test changes it between phases
static std::atomic<unsigned> g_lastUuid{0};

static std::atomic<bool> g_done{false};

static void fakeHead(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kHeadPort)) { ready = true; return; }
    ready = true;
    // ⚠️ Timeout on the LISTENING socket: without it `accept()` blocks forever once there are no more
    // reconnections, the thread never looks at `g_done` again, and the final `join()` hangs the test.
    { timeval ta{}; ta.tv_sec = 0; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }

    // ⚠️ ACCEPT IN A LOOP, not once. The zone reconnects to the head as soon as a send fails
    // (`connectToHead()`), so a fake head that accepted a single connection leaves the zone spinning
    // in "Connection with HeadServer lost. Reconnecting..." forever. That is exactly what happened
    // while writing this.
    while (!g_done)
    {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        if (std::getenv("ZONE_E2E_VERBOSE")) { std::printf("[head] accepted fd=%d\n", fd); std::fflush(stdout); }

        DGS::Command cmd{};
        cmd.chunkSizeX = kChunkM; cmd.chunkSizeY = kChunkM; cmd.chunkSizeZ = kChunkM;
        cmd.port = kZoneUdp;
        std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");
        DGS::Packet p; p.pack(cmd);
        s.send(fd, p.getRawData(), p.getSize());

        uint8_t buf[8192];
        timeval tv{}; tv.tv_sec = 1; tv.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done)
        {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (std::getenv("ZONE_E2E_VERBOSE")) { std::printf("[head] receive -> %d\n", n); std::fflush(stdout); }
            if (n <= 0) break;                       // timeout or close -> go back to accept
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() == DGS::PKT_METRICS) {
                const DGS::ServerMetrics m = r.unpackServerMetrics();
                g_activeEntities = (int)m.activeEntities;
                g_chunkXMax      = (int)m.node.chunkXMax;
                ++g_metricsCount;
            }
        }
        s.closeClient(fd);
    }
}

static void fakeValidator(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kValPort)) { ready = true; return; }
    ready = true;
    const int fd = s.accept();
    if (fd < 0) return;

    uint8_t buf[8192];
    timeval tv{}; tv.tv_usec = 200000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    while (!g_done) {
        const int n = s.receive(fd, buf, sizeof(buf));
        if (n <= 0) continue;
        DGS::Packet r; r.setBuffer(buf, n);
        if (r.getType() != DGS::PKT_VALIDATE_REQ) continue;

        const DGS::ValidateRequest req = r.unpackValidateRequest();
        g_lastUuid = req.entityUuid;
        ++g_reqCount;

        DGS::ValidateAck ack{};
        ack.requestId = req.requestId;
        ack.verdict   = (uint8_t)g_verdict.load();
        ack.weight    = ack.verdict ? 0 : 1;
        DGS::Packet a; a.pack(ack);
        s.send(fd, a.getRawData(), a.getSize());
    }
    s.closeClient(fd);
}

static void fakeSocial(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kSocialPort)) { ready = true; return; }
    ready = true;
    const int fd = s.accept();
    if (fd < 0) return;
    uint8_t buf[4096];
    timeval tv{}; tv.tv_usec = 200000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    while (!g_done) s.receive(fd, buf, sizeof(buf));
    s.closeClient(fd);
}

/** @brief Sends the entity over UDP the way a client does: the RAW struct, not wrapped in a Packet
 *  (the zone compares `udpBytes == sizeof(EntityTransfer)`). */
static void sendEntity(DGS::UDPSocket& udp, uint32_t uuid, float x, float maxSpeed)
{
    DGS::EntityTransfer e{};
    e.uuid   = uuid;
    e.chunkX = 0; e.chunkY = 0; e.chunkZ = 0;
    e.pos[0] = x; e.pos[1] = 0.0f; e.pos[2] = 0.0f;
    e.stats.speed[0] = maxSpeed;
    udp.send("127.0.0.1", kZoneUdp, (const uint8_t*)&e, sizeof(e));
}

/** @brief Waits up to `msLimit` for `activeEntities` to equal `target`. Polling with a deadline is the
 *  only honest option here: the zone runs at 10 Hz and a fixed `sleep` would be either a race or waste. */
static bool waitForEntities(int target, int msLimit)
{
    const auto t0 = std::chrono::steady_clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - t0).count() < msLimit) {
        if (g_activeEntities.load() == target) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

int main(int argc, char** argv)
{
    // ⚠️ Without this the test DIES with SIGPIPE (exit 141) the moment it writes to a socket whose peer
    // has closed — and the zone opens and closes connections while retrying. It is the default signal
    // for any process speaking over sockets; `send` returns EPIPE and is handled as a normal error.
    signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX];
    const char* argPath = (argc > 1) ? argv[1] : "./build/zone_node";
    const char* nodePath = realpath(argPath, abs) ? abs : argPath;
    char absStub[PATH_MAX];
    const char* argStub = (argc > 2) ? argv[2] : "./build/stub_rules.so";
    const char* stubPath = realpath(argStub, absStub) ? absStub : argStub;

    std::atomic<bool> h{false}, v{false}, so{false};
    std::thread th(fakeHead, std::ref(h));
    std::thread tv(fakeValidator, std::ref(v));
    std::thread ts(fakeSocial, std::ref(so));
    while (!h || !v || !so) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); g_done = true; th.join(); tv.join(); ts.join(); return 1; }
    if (pid == 0) {
        if (!std::getenv("ZONE_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
        setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
        setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
        setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
        setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
        setenv("VALIDATOR_TCP_PORT", std::to_string(kValPort).c_str(), 1);
        setenv("SOCIAL_HOST",        "127.0.0.1", 1);
        setenv("SOCIAL_TCP_PORT",    std::to_string(kSocialPort).c_str(), 1);
        setenv("CHUNK_X_MIN", "0", 1); setenv("CHUNK_X_MAX", "100", 1);
        setenv("CHUNK_Y_MIN", "0", 1); setenv("CHUNK_Y_MAX", "100", 1);
        setenv("CHUNK_Z_MIN", "0", 1); setenv("CHUNK_Z_MAX", "100", 1);
        setenv("GAME_MODULE_SO", stubPath, 1);
        char tmpl[] = "/tmp/dgs_zone_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    // The zone registers as soon as it connects to the head. Polling with a deadline, not `sleep`.
    bool started = false;
    for (int i = 0; i < 200 && !started; ++i) {
        if (g_metricsCount.load() > 0) started = true;
        else std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    check(started, "the zone starts up and publishes its metrics to the head server");
    check(g_chunkXMax.load() == 100, "it registers the chunk bounds the environment gave it (X max=100)");

    if (started) {
        DGS::UDPSocket udp;
        udp.bind(0);                     // ephemeral port: this side only sends
        const uint32_t uuid = 9001;
        const float    vmax = 5.0f;

        // ── PHASE A: plausible movement + verdict 1 -> it stays ───────────────────────────────
        g_verdict = 1;
        const int reqBeforeA = g_reqCount.load();
        sendEntity(udp, uuid, 0.0f, vmax);               // 1st packet: only sets the baseline
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        sendEntity(udp, uuid, 0.5f, vmax);               // 0.5 m in ~0.2 s: within 5 m/s + 1 m

        bool asked = false;
        for (int i = 0; i < 100 && !asked; ++i) {
            if (g_reqCount.load() > reqBeforeA) asked = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        check(asked, "the zone ASKS the validator for a verdict on a plausible movement");
        check(g_lastUuid.load() == uuid, "the request carries the uuid of the entity that moved");
        check(waitForEntities(1, 3000), "with verdict 1 the entity STAYS (activeEntities=1)");

        // ── PHASE B: teleport -> the LOCAL filter cuts it, without asking ─────────────────────
        // This is phase A's counter-proof: if the zone had no defence of its own, this jump would also
        // end up as a request to the validator.
        const int reqBeforeB = g_reqCount.load();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        sendEntity(udp, uuid, 1000.0f, vmax);            // 1 km in ~0.2 s
        std::this_thread::sleep_for(std::chrono::milliseconds(600));
        check(g_reqCount.load() == reqBeforeB,
              "a teleport is cut by the zone's S1 filter and NEVER EVEN reaches the validator");

        // ── PHASE C: plausible movement + verdict 0 -> it is evicted ──────────────────────────
        g_verdict = 0;
        const int reqBeforeC = g_reqCount.load();
        // After the discarded teleport the baseline is still 0.5 m: a short step from there.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        sendEntity(udp, uuid, 1.0f, vmax);

        bool asked2 = false;
        for (int i = 0; i < 100 && !asked2; ++i) {
            if (g_reqCount.load() > reqBeforeC) asked2 = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        check(asked2, "it asks for a verdict again on the next movement");
        check(waitForEntities(0, 3000),
              "with verdict 0 the zone EVICTS the entity (activeEntities returns to 0)");
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    g_done = true;
    th.join(); tv.join(); ts.join();

    std::printf("\n== zone_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
