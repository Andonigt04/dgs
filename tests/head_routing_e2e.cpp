// ─────────────────────────────────────────────────────────────────────────────────────────────────
// `head_server_node` WITH TWO ZONES — its real job, and it was only ever tested with one.
//
// `ping_pong` registers ONE zone and checks that its own entity comes back. That does not separate
// "routes by chunk" from "echoes to whoever speaks": with a single possible destination the two look
// identical.
//
// Here there are two zones with DISJOINT ranges, and every assertion needs the other zone to mean
// anything:
//
//     A (chunks 0..99) sends an entity in chunk 150  ->  B receives it, and A does NOT
//     B (chunks 100..199) sends one in chunk 50      ->  A receives it, and B does NOT
//     one in chunk 9000                              ->  NOBODY receives it
//
// The "and NOT the other one" is half the test: a head that broadcast to everyone would pass the first
// two lines without routing anything.
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

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);
}

static const int kPort = 42424;   // hardcoded in the head

static void registerZone(DGS::TCPSocket& s, int32_t xMin, int32_t xMax)
{
    DGS::ServerMetrics m{};
    m.node.chunkXMin = xMin; m.node.chunkXMax = xMax;
    m.node.chunkYMin = 0;    m.node.chunkYMax = 1000;
    DGS::Packet p; p.pack(m);
    s.send(s.getSocketFD(), p.getRawData(), p.getSize());
}

static void sendEntity(DGS::TCPSocket& s, uint32_t uuid, int32_t cx)
{
    DGS::EntityTransfer e{};
    e.uuid = uuid; e.chunkX = cx; e.chunkY = 10;
    DGS::Packet p; p.pack(e);
    s.send(s.getSocketFD(), p.getRawData(), p.getSize());
}

/// @return the uuid received, or 0 if nothing arrives before the deadline.
static uint32_t receiveUuid(DGS::TCPSocket& s, int msLimit)
{
    timeval tv{}; tv.tv_sec = msLimit / 1000; tv.tv_usec = (msLimit % 1000) * 1000;
    setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    uint8_t buf[8192];
    const int n = s.receive(s.getSocketFD(), buf, sizeof(buf));
    if (n <= 0) return 0;
    DGS::Packet r; r.setBuffer(buf, n);
    return r.unpackEntityTransfer().uuid;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX];
    const char* nodePath = realpath((argc > 1) ? argv[1] : "./build/head_server_node", abs)
                           ? abs : "./build/head_server_node";

    { DGS::TCPSocket probe;
      if (probe.connect("127.0.0.1", kPort)) {
          std::printf("[FAIL] port %d is already taken by another head\n", kPort);
          return 1;
      } }

    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); return 1; }
    if (pid == 0) {
        if (!std::getenv("HEADROUT_VERBOSE")) std::freopen("/dev/null", "w", stdout);
        char tmpl[] = "/tmp/dgs_headrout_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    DGS::TCPSocket A, B;
    bool up = false;
    for (int i = 0; i < 200 && !up; ++i) {
        if (A.connect("127.0.0.1", kPort)) { up = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    if (up) up = B.connect("127.0.0.1", kPort);
    check(up, "the head accepts TWO zones");

    if (up) {
        registerZone(A,   0,  99);
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        registerZone(B, 100, 199);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        // (1) A sends something that falls in B's range.
        sendEntity(A, 7001, 150);
        check(receiveUuid(B, 2000) == 7001, "an entity in chunk 150 is received by zone 100..199");
        check(receiveUuid(A, 500) == 0,     "and does NOT come back to its sender (it is not an echo)");

        // (2) And the other way round, so routing in a single direction is not enough.
        sendEntity(B, 7002, 50);
        check(receiveUuid(A, 2000) == 7002, "an entity in chunk 50 is received by zone 0..99");
        check(receiveUuid(B, 500) == 0,     "and does not come back to its sender either");

        // (3) With no zone covering it: nobody.
        sendEntity(A, 7003, 9000);
        const uint32_t inB = receiveUuid(B, 800);
        const uint32_t inA = receiveUuid(A, 500);
        check(inA == 0 && inB == 0,
              "an entity with no zone covering it is delivered to NOBODY (there is no broadcast)");
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);

    std::printf("\n== head_routing_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
