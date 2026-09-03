// ─────────────────────────────────────────────────────────────────────────────────────────────────
// RECONNECTION — the path that used to break, with no safety net.
//
// `zone_node.cpp` reconnects to the head when a send fails: `tcp_zone_node = DGS::TCPSocket();` and try
// again. That whole path had NOT ONE test, and on top of that it touched two real `TCPSocket` defects
// (a pinned family in `connect`, and the rule of three left unimplemented).
//
// This test does two different things on purpose, because they MEASURE DIFFERENT THINGS:
//
//   1. `testDescriptorLeak()` — what actually discriminates the RULE OF THREE.
//   2. The cut/reconnect cycle — what covers the RECOVERY PATH end to end.
//
// ⚠️ And it has to be said which proves what, because I got it wrong at first: removing the move
// semantics leaves the reconnection cycle GREEN. I checked. The reason is that `connect()` creates a
// fresh descriptor on every attempt, so the object heals itself — meaning what restores reconnection is
// that rewrite, not the rule of three. The leak (and the unusable descriptor it leaves behind) only
// shows up by counting descriptors and reusing the socket, which is (1).
//
// The cycle: the node is left talking to the head, the connection is CUT from underneath it, and we
// check that (a) it reconnects and (b) metrics START FLOWING AGAIN — because reconnecting without being
// able to send was exactly the symptom. And it is repeated, so a one-shot recovery does not count.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
#include <sys/wait.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#include <dirent.h>

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

static const int kHeadPort   = 21451;
static const int kValPort    = 21452;
static const int kSocialPort = 21453;
static const int kZoneUdp    = 21454;

static std::atomic<bool> g_done{false};
static std::atomic<int>  g_metrics{0};      // how many the head has received in total
static std::atomic<int>  g_connections{0};  // how many times the zone has connected
static std::atomic<bool> g_cut{false};      // the test asks for the live connection to be cut

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
        ++g_connections;

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
            if (g_cut.exchange(false)) break;            // the test cuts from underneath
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n <= 0) continue;                        // timeout: keep waiting
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() == DGS::PKT_METRICS) ++g_metrics;
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

/// Waits up to `msLimit` for the counter to go above `from`. Poll with a deadline, never guess a sleep.
static bool waitToGrow(std::atomic<int>& counter, int from, int msLimit)
{
    const auto t0 = std::chrono::steady_clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - t0).count() < msLimit) {
        if (counter.load() > from) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

static const char* kZoneLog = "/tmp/dgs_reconn_zone.log";

/// Has the zone taken the ORDERLY-CLOSE path (`receive` returned 0), or only noticed later through a
/// failed `send`? The two paths log different lines, and that is the whole point of this check.
///
/// ⚠️ THIS IS WHAT DISCRIMINATES `TCPSocket::receive` RETURNING 0. That function used to collapse "the
/// peer hung up" and "nothing arrived yet" into a single -1, so `zone_node`'s `if (bytes == 0)` branch
/// was UNREACHABLE by construction: a head that closed the connection was only ever noticed later, when
/// a `send` failed. Everything still worked — which is exactly why it went unseen — but the node was
/// blind to a clean disconnect and reacted a beat late, through an error path meant for something else.
/// Counting connections does not discriminate it: the test below stays green either way. Only the log
/// line does.
static bool orderlyClosePath()
{
    std::FILE* f = std::fopen(kZoneLog, "r");
    if (!f) return false;
    bool found = false;
    char line[512];
    while (std::fgets(line, sizeof(line), f))
        if (std::strstr(line, "HeadServer closed the connection")) { found = true; break; }
    std::fclose(f);
    return found;
}

/// How many descriptors the process has open right now.
static int openFds()
{
    int n = 0;
    if (DIR* d = opendir("/proc/self/fd")) {
        while (readdir(d)) ++n;
        closedir(d);
    }
    return n;
}

/// ⚠️ THIS IS WHAT DISCRIMINATES THE RULE OF THREE — the reconnection test does NOT.
///
/// `zone_node` reconnects with `tcp_zone_node = DGS::TCPSocket();`. If the class defines no move
/// semantics, that line uses the compiler-generated COPY assignment: it copies `socketFD` verbatim and
/// LEAKS the descriptor that was there. Verified by measurement: removing the move semantics leaves the
/// reconnection test GREEN — because `connect()` creates a fresh descriptor on every attempt and the
/// object heals itself — so the leak is only visible by counting it. Without this, the fix would have
/// no coverage at all.
static void testDescriptorLeak()
{
    const int before = openFds();
    {
        DGS::TCPSocket s;
        for (int i = 0; i < 200; ++i) s = DGS::TCPSocket();   // the exact pattern of `connectToHead`
    }
    const int after = openFds();
    std::printf("    descriptors: %d before · %d after 200 reassignments\n", before, after);
    // ⚠️ STRICT EQUALITY, not a margin. With the bug the leak is exactly ONE descriptor — after the
    // first round the system reuses the same number over and over — so a "<= before + 2" lets it
    // through. I verified it by measuring: 6 -> 7, and my original threshold came out green.
    check(after == before, "reassigning a TCPSocket leaks NOT ONE descriptor (rule of three)");

    // And the consequence that really hurts: after the reassignment the object is left holding a number
    // the temporary already CLOSED. It is not just a leak — the socket is unusable, which is what broke
    // `connectToHead()`.
    {
        DGS::TCPSocket s2;
        s2 = DGS::TCPSocket();
        check(s2.listen(21459), "after reassignment the socket is STILL USABLE (listen works)");
    }
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);
    testDescriptorLeak();

    char abs[PATH_MAX];
    const char* argPath = (argc > 1) ? argv[1] : "./build/zone_node";
    const char* nodePath = realpath(argPath, abs) ? abs : argPath;
    char absStub[PATH_MAX];
    const char* argStub = (argc > 2) ? argv[2] : "./build/stub_rules.so";
    const char* stubPath = realpath(argStub, absStub) ? absStub : argStub;

    std::atomic<bool> h{false}, v{false}, so{false};
    std::thread th(fakeHead, std::ref(h));
    std::thread tv(fakeSimple, kValPort, std::ref(v));
    std::thread ts(fakeSimple, kSocialPort, std::ref(so));
    while (!h || !v || !so) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); g_done = true; th.join(); tv.join(); ts.join(); return 1; }
    if (pid == 0) {
        std::freopen("/dev/null", "w", stdout);
        // stderr goes to a FILE, not to /dev/null: the zone announces WHICH of its two reconnect paths
        // it took, and that is the only observable that separates them. See `orderlyClosePath()`.
        std::freopen(kZoneLog, "w", stderr);
        setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
        setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
        setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
        setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
        setenv("VALIDATOR_TCP_PORT", std::to_string(kValPort).c_str(), 1);
        setenv("SOCIAL_HOST",        "127.0.0.1", 1);
        setenv("SOCIAL_TCP_PORT",    std::to_string(kSocialPort).c_str(), 1);
        setenv("GAME_MODULE_SO", stubPath, 1);
        char tmpl[] = "/tmp/dgs_reconn_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    // ── (1) Healthy state ───────────────────────────────────────────────────────────────────────
    const bool started = waitToGrow(g_metrics, 0, 8000);
    check(started, "the zone connects to the head and publishes metrics");
    const int connectionsBefore = g_connections.load();
    check(connectionsBefore >= 1, "the head has accepted at least one connection");

    if (started) {
        // ── (2) The connection is cut from underneath it ────────────────────────────────────────
        const int metricsBefore = g_metrics.load();
        g_cut = true;

        // (2a) It has to reconnect. This is the counter that stayed STUCK with the rule-of-three bug:
        //      the node believed it was reconnecting ("attempt 1" over and over) but operated on a dead
        //      descriptor, so the head never saw a usable new connection.
        const bool reconnects = waitToGrow(g_connections, connectionsBefore, 15000);
        check(reconnects, "after the connection is cut, the zone RECONNECTS to the head");

        // (2b) And above all: that the new connection WORKS. Reconnecting and being unable to send is
        //      precisely the failure that existed — which is why counting connections is not enough.
        const bool metricsReturn = waitToGrow(g_metrics, metricsBefore, 15000);
        check(metricsReturn, "and metrics START FLOWING AGAIN over the new connection");

        // (2c) And that it noticed for the RIGHT REASON. The two assertions above are satisfied by
        //      either path, so on their own they say nothing about how the disconnect was detected.
        check(orderlyClosePath(),
              "it detects the hang-up as a CLEAN CLOSE (receive == 0), not through a later failed send");

        std::printf("    connections to the head: %d  ·  metrics received: %d\n",
                    g_connections.load(), g_metrics.load());

        // ── (3) And it survives a second cut: recovery is not a one-shot ────────────────────────
        const int m2 = g_metrics.load(), c2 = g_connections.load();
        g_cut = true;
        check(waitToGrow(g_connections, c2, 15000) && waitToGrow(g_metrics, m2, 15000),
              "it survives a SECOND cut (recovery does not run out)");
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    g_done = true;
    th.join(); tv.join(); ts.join();

    std::printf("\n== reconnect_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
