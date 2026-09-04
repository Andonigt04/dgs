// ─────────────────────────────────────────────────────────────────────────────────────────────────
// GHOSTS AND ZONE HANDOFF — what separates this from an ordinary server, and it had no test.
//
// A world split into zones needs two things a single server does not:
//   · **ghosts**: an entity near the border has to be VISIBLE to the neighbouring zone before it
//     crosses, or players pop out of nowhere at the frontier;
//   · **authority handoff**: on leaving the bounds the zone CEDES the entity (`PKT_REASSIGN`) and stops
//     simulating it, or two zones simulate it at once and fight over it.
//
// Both come out over the connection to the head, so a fake head sees them without touching anybody's
// internals. Three entities, three positions, each checking something different:
//
//     (50,50,50)  in the centre -> NO ghost is emitted     <- the counter-proof
//     (99,50,50)  on the border -> a ghost IS emitted
//     (150,50,50) outside       -> a REASSIGN is emitted and the zone LETS GO
//
// ⚠️ The centre case is what gives the border case its value: without it, a zone that emitted a ghost
// for EVERY entity — that is, one that never looked at the border — would pass just the same.
//
// ⚠️ And mind the centre: `isNearBorder` ORs SIX bounds together, so an entity at `chunkZ = 0` is "on
// the border" even with X and Y in the middle of the zone. That is why the central case is (50,50,50)
// and not (50,50,0) — the first attempt was the latter and would have produced a false failure.
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
    std::fflush(stdout);
}

static const int kHeadPort   = 21471;
static const int kValPort    = 21472;
static const int kSocialPort = 21473;
static const int kZoneUdp    = 21474;

static const uint32_t kCentre = 9101;
static const uint32_t kBorder = 9102;
static const uint32_t kOutside = 9103;

static std::atomic<bool> g_done{false};
static std::atomic<int>  g_metrics{0};
static std::atomic<int>  g_active{-1};
static std::atomic<int>  g_ghostCentre{0}, g_ghostBorder{0}, g_ghostOutside{0};
static std::atomic<int>  g_reassignOutside{0};

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
        cmd.chunkSizeX = 1000.0f; cmd.chunkSizeY = 1000.0f; cmd.chunkSizeZ = 1000.0f;
        cmd.port = kZoneUdp;
        std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");
        DGS::Packet p; p.pack(cmd);
        s.send(fd, p.getRawData(), p.getSize());

        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 300000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n <= 0) continue;
            DGS::Packet r; r.setBuffer(buf, n);
            switch (r.getType()) {
                case DGS::PKT_METRICS: {
                    const DGS::ServerMetrics m = r.unpackServerMetrics();
                    g_active = (int)m.activeEntities;
                    ++g_metrics;
                    break;
                }
                case DGS::PKT_GHOST_DELTA: {
                    const DGS::GhostDelta d = r.unpackGhostDelta();
                    if (d.uuid == kCentre)  ++g_ghostCentre;
                    if (d.uuid == kBorder)  ++g_ghostBorder;
                    if (d.uuid == kOutside) ++g_ghostOutside;
                    break;
                }
                case DGS::PKT_REASSIGN: {
                    const DGS::EntityReassign ra = r.unpackEntityReassign();
                    if (ra.ack != 0) break;          // our own answer bouncing back
                    if (ra.entityUuid == kOutside) ++g_reassignOutside;
                    // ⚠️ THE ACK IS REQUIRED, and this fake used to be silent. The handoff is
                    // at-least-once now: a zone HOLDS the entity until the head confirms it was
                    // routed, precisely so that a head that cannot route it (or is not there) can no
                    // longer make the entity vanish. A stub that never answers is a head that never
                    // routes, so the zone kept it and this test read 3 active instead of 2 — the test
                    // was right to notice, and the fix is to answer like the real head does.
                    DGS::EntityReassign answer = ra;
                    answer.ack = 1;                  // routed: the zone may let go
                    DGS::Packet pa; pa.pack(answer);
                    s.send(fd, pa.getRawData(), pa.getSize());
                    break;
                }
                default: break;
            }
        }
        s.closeClient(fd);
    }
}

static void fakeSimple(int port, std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(port)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;
    while (!g_done) {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        uint8_t buf[4096];
        timeval tv{}; tv.tv_usec = 200000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) s.receive(fd, buf, sizeof(buf));
        s.closeClient(fd);
    }
}

/// Sends the entity over UDP like a client: the RAW struct. Each uuid is sent ONCE, at a different
/// position: that way there is no prior baseline and the S1 filter never comes into play — what is
/// tested here is the per-zone distribution, not movement validation.
static void sendEntity(DGS::UDPSocket& udp, uint32_t uuid, int cx, int cy, int cz)
{
    DGS::EntityTransfer e{};
    e.uuid   = uuid;
    e.chunkX = cx; e.chunkY = cy; e.chunkZ = cz;
    e.pos[0] = 10.0f; e.pos[1] = 0.0f; e.pos[2] = 0.0f;
    e.stats.speed[0] = 5.0f;
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

static bool waitFor(std::atomic<int>& c, int atLeast, int msLimit)
{
    const auto t0 = std::chrono::steady_clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - t0).count() < msLimit) {
        if (c.load() >= atLeast) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX], absStub[PATH_MAX];
    const char* argPath  = (argc > 1) ? argv[1] : "./build/zone_node";
    const char* nodePath = realpath(argPath, abs) ? abs : argPath;
    const char* argStub  = (argc > 2) ? argv[2] : "./build/stub_rules.so";
    const char* stubPath = realpath(argStub, absStub) ? absStub : argStub;

    std::atomic<bool> h{false}, v{false}, so{false};
    std::thread th(fakeHead, std::ref(h));
    std::thread tv(fakeSimple, kValPort, std::ref(v));
    std::thread ts(fakeSimple, kSocialPort, std::ref(so));
    while (!h || !v || !so) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); g_done = true; th.join(); tv.join(); ts.join(); return 1; }
    if (pid == 0) {
        if (!std::getenv("GHOST_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
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
        setenv("GHOST_THRESHOLD", "1", 1);
        setenv("GAME_MODULE_SO", stubPath, 1);
        char tmpl[] = "/tmp/dgs_ghost_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    check(waitFor(g_metrics, 1, 8000), "the zone starts up and talks to the head");

    if (g_metrics.load() > 0) {
        DGS::UDPSocket udp;
        udp.bind(0);

        // (1) In the CENTRE of the zone: no ghosts. The counter-proof.
        sendEntity(udp, kCentre, 50, 50, 50);
        // (2) On the BORDER (x >= xMax-1): a ghost.
        sendEntity(udp, kBorder, 99, 50, 50);
        // (3) OUTSIDE: authority handoff.
        sendEntity(udp, kOutside, 150, 50, 50);

        check(waitFor(g_ghostBorder, 1, 5000),
              "an entity on the border (99 of 0..100) produces a GHOST for the neighbouring zone");
        check(waitFor(g_reassignOutside, 1, 5000),
              "an entity OUTSIDE the bounds is handed over with PKT_REASSIGN (authority ceded)");

        // Slack so the zone has had several loop turns before asserting the negative.
        std::this_thread::sleep_for(std::chrono::milliseconds(800));
        check(g_ghostCentre.load() == 0,
              "an entity in the CENTRE produces no ghost (it is not emitted for every entity)");

        // And on ceding authority it stops simulating it: otherwise TWO zones simulate it at once.
        // ⚠️ EXACT EQUALITY, not "<= 2". THREE entities were sent and ONE was ceded, so TWO remain. A
        // "<=" would also go green if the zone had dropped the other two, which is a different and
        // worse failure. (My first version had that "<=" and discriminated nothing.)
        check(g_active.load() == 2,
              "after the handoff EXACTLY the 2 still inside remain (it cedes one, no more)");

        std::printf("    ghosts  centre %d · border %d · outside %d   ·  reassign %d  ·  active %d\n",
                    g_ghostCentre.load(), g_ghostBorder.load(), g_ghostOutside.load(),
                    g_reassignOutside.load(), g_active.load());
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    g_done = true;
    th.join(); tv.join(); ts.join();

    std::printf("\n== ghost_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
