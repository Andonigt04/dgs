// ─────────────────────────────────────────────────────────────────────────────────────────────────
// `cache_node` — the queue between the zones and the validator. 94 lines without a single test.
//
// What it does: zones connect on one port and ENQUEUE entities; the validator connects on another and,
// when it asks, is handed ONE. It is a buffer, and only three things matter about a buffer: that it
// does not lose, does not invent, and respects ordering.
//
//     (1) what goes in COMES OUT      — it does not lose
//     (2) it comes out IN ORDER (FIFO)— it does not shuffle
//     (3) ONE request -> ONE entity   — it does not drain the queue at once
//     (4) empty queue -> nothing      — it does not invent
//
// (3) and (4) are the ones that really bite. (3) because the node uses LEVEL-TRIGGERED epoll and
// **never reads what the validator sends it**: if the bytes stay unconsumed on the socket the event
// keeps re-firing on its own and the queue drains with nobody asking. (4) because a node that always
// answered something would pass (1) and (2) without being a queue at all.
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

static const int kZonePort = 21481;
static const int kValPort  = 21482;

/// Asks the cache for one entity and returns its uuid, or 0 if nothing arrives before the timeout.
static uint32_t askForOne(DGS::TCPSocket& val)
{
    DGS::EntityTransfer poll{};
    poll.uuid = 0;
    DGS::Packet p; p.pack(poll);
    if (!val.send(val.getSocketFD(), p.getRawData(), p.getSize())) return 0;

    uint8_t buf[8192];
    const int n = val.receive(val.getSocketFD(), buf, sizeof(buf));
    if (n <= 0) return 0;
    DGS::Packet r; r.setBuffer(buf, n);
    return r.unpackEntityTransfer().uuid;
}

/// Reads whatever is there WITHOUT asking. This is what (3) needs: if the node still drains the queue
/// on its own, entities nobody requested show up here.
static uint32_t readWithoutAsking(DGS::TCPSocket& val, int msLimit)
{
    timeval tv{}; tv.tv_sec = msLimit / 1000; tv.tv_usec = (msLimit % 1000) * 1000;
    setsockopt(val.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    uint8_t buf[8192];
    const int n = val.receive(val.getSocketFD(), buf, sizeof(buf));
    if (n <= 0) return 0;
    DGS::Packet r; r.setBuffer(buf, n);
    return r.unpackEntityTransfer().uuid;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX];
    const char* argPath  = (argc > 1) ? argv[1] : "./build/cache_node";
    const char* nodePath = realpath(argPath, abs) ? abs : argPath;

    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); return 1; }
    if (pid == 0) {
        if (!std::getenv("CACHE_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
        setenv("CACHE_ZONE_PORT",      std::to_string(kZonePort).c_str(), 1);
        setenv("CACHE_VALIDATOR_PORT", std::to_string(kValPort).c_str(), 1);
        char tmpl[] = "/tmp/dgs_cache_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    DGS::TCPSocket zone, val;
    bool up = false;
    for (int i = 0; i < 200 && !up; ++i) {
        if (zone.connect("127.0.0.1", kZonePort) && val.connect("127.0.0.1", kValPort)) up = true;
        else std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    check(up, "the cache accepts both the zone and the validator");

    if (up) {
        timeval tv{}; tv.tv_sec = 2;
        setsockopt(val.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        // (4) EMPTY QUEUE first: asking without having enqueued cannot return anything.
        check(askForOne(val) == 0, "with an EMPTY queue, asking returns nothing (it invents nothing)");

        // (1)(2) Enqueue three and pull them out in order.
        for (uint32_t id : {101u, 102u, 103u}) {
            DGS::EntityTransfer e{}; e.uuid = id; e.chunkX = 7;
            DGS::Packet p; p.pack(e);
            zone.send(zone.getSocketFD(), p.getRawData(), p.getSize());
            std::this_thread::sleep_for(std::chrono::milliseconds(60));
        }

        const uint32_t a = askForOne(val);
        check(a == 101, "the first thing enqueued is the first thing out (FIFO)");

        // (3) And now the one aimed at level-triggered epoll: WITHOUT asking again, does more come out?
        const uint32_t unsolicited = readWithoutAsking(val, 700);
        check(unsolicited == 0,
              "it delivers nothing unless asked (one request = one entity)");
        if (unsolicited != 0)
            std::printf("         entity %u arrived UNREQUESTED: the queue drains on its own\n", unsolicited);

        timeval tv2{}; tv2.tv_sec = 2;
        setsockopt(val.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv2, sizeof(tv2));
        const uint32_t b = askForOne(val);
        const uint32_t c = askForOne(val);
        std::printf("    output order: %u · %u · %u  (enqueued 101, 102, 103)\n", a, b, c);
        check(b == 102 && c == 103, "the rest come out in the same order they went in");

        // And back to an empty queue: closes the loop.
        check(askForOne(val) == 0, "once drained, it returns nothing again");
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);

    std::printf("\n== cache_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
