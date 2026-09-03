// ─────────────────────────────────────────────────────────────────────────────────────────────────
// A REAL round trip against the head server: it starts the node, registers as a zone, sends it an
// entity and checks that the SAME one comes back.
//
// ⚠️ This used to not be a test. It was a script: it needed a server already brought up by hand, and if
// nothing arrived it did `return 0` anyway — the only failure path was being unable to connect. Which
// is why in CI it lived as `timeout 5 ./ping_pong_test || true`, meaning its result was thrown away. A
// test whose verdict is discarded covers nothing; it only creates the feeling of coverage.
//
// What changes:
//   · it starts `head_server_node` itself and kills it on the way out -> no dependence on the
//     environment; it waits for the port by RETRYING, not with a guessed `sleep`;
//   · `SO_RCVTIMEO` on the socket -> it cannot hang, so no external `timeout 5` is needed;
//   · it checks the echo IS the entity sent (uuid and chunk), not merely that bytes arrived;
//   · COUNTER-PROOF: an entity OUTSIDE the registered zone must NOT come back. Without this, a server
//     that echoed everything would pass just the same and we would not be measuring chunk routing;
//   · it returns != 0 when something fails.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <limits.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <thread>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
}

static const int kPort = 42424;

/** @brief Is something already listening on the port? Checked BEFORE starting: an orphaned node from
 *  another run would give us a green that is not ours. */
static bool portBusy()
{
    DGS::TCPSocket probe;
    return probe.connect("127.0.0.1", kPort);
}

int main(int argc, char** argv)
{
    // ⚠️ ABSOLUTE, and this is not cosmetic: the child `chdir`s to a temporary before the `execl`, so a
    // relative path stops resolving. It went unnoticed under CTest — which passes
    // `$<TARGET_FILE:...>`, already absolute — and only failed when launched by hand with
    // `./build/head_server_node`.
    char abs[4096];
    const char* argPath = (argc > 1) ? argv[1] : "./build/head_server_node";
    const char* nodePath = realpath(argPath, abs) ? abs : argPath;

    if (portBusy()) {
        std::printf("[FAIL] port %d is already taken: another node is alive. We do not measure on it.\n",
                    kPort);
        return 1;
    }

    // ── Levantar el head server ────────────────────────────────────────────────────────────────
    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); return 1; }
    if (pid == 0) {
        // The node is noisy on stdout; silence it so the test's own log is readable.
        std::freopen("/dev/null", "w", stdout);
        // And it writes its CSV to the CURRENT DIRECTORY: without this, every run of the test leaves a
        // `headserver_log.csv` at the repository root. A test that dirties the working tree ends up
        // either in .gitignore or in an accidental commit; better that it does not dirty it.
        char tmpl[] = "/tmp/dgs_ping_pong_XXXXXX";
        if (const char* dir = mkdtemp(tmpl)) { if (chdir(dir) != 0) { /* worst case: the usual CWD */ } }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);                              // execl only returns on failure
    }

    // Wait for the port by RETRYING. A fixed `sleep` is a race: too short and the test fails on a slow
    // machine, too long and you pay for it on every run.
    DGS::TCPSocket zone;
    bool up = false;
    for (int i = 0; i < 100 && !up; ++i) {
        if (zone.connect("127.0.0.1", kPort)) { up = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    check(up, "the head server accepts connections (started by the test itself)");
    if (!up) { kill(pid, SIGTERM); waitpid(pid, nullptr, 0); return 1; }

    // Without this, a server that accepts and never answers hangs the test forever: that is why CI
    // needed an external `timeout`. The limit lives HERE, which is where it is known how long is
    // reasonable to wait.
    timeval tv{}; tv.tv_sec = 2; tv.tv_usec = 0;
    setsockopt(zone.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // ── Register as a zone: chunks X 100..200, Y 0..100 ───────────────────────────────────────
    DGS::ServerMetrics metrics{};
    metrics.node.chunkXMin = 100; metrics.node.chunkXMax = 200;
    metrics.node.chunkYMin = 0;   metrics.node.chunkYMax = 100;

    DGS::Packet pReg;
    pReg.pack(metrics);
    check(zone.send(zone.getSocketFD(), pReg.getRawData(), pReg.getSize()),
          "zone registration sent");

    // ── (1) Entity INSIDE the zone: it has to come back ───────────────────────────────────────
    DGS::EntityTransfer e{};
    e.uuid = 777; e.chunkX = 150; e.chunkY = 50;

    DGS::Packet pSend;
    pSend.pack(e);

    const auto t0 = std::chrono::steady_clock::now();
    check(zone.send(zone.getSocketFD(), pSend.getRawData(), pSend.getSize()),
          "entity 777 sent to chunk (150,50), inside the zone");

    uint8_t buffer[8192];
    const int bytes = zone.receive(zone.getSocketFD(), buffer, sizeof(buffer));
    const auto t1 = std::chrono::steady_clock::now();

    check(bytes > 0, "a reply arrives before the 2 s timeout");
    if (bytes > 0) {
        DGS::Packet pRecv;
        pRecv.setBuffer(buffer, bytes);
        const DGS::EntityTransfer echo = pRecv.unpackEntityTransfer();
        // What makes this a test rather than a "bytes arrived": identity and destination.
        check(echo.uuid == 777u, "the echo IS entity 777 (not something else passing through)");
        check(echo.chunkX == 150 && echo.chunkY == 50, "the echo preserves the chunk (150,50)");
        const double rttMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
        std::printf("  loopback RTT: %.3f ms\n", rttMs);
        // This is not a performance test: it is a safety net against "it answers, but glacially".
        check(rttMs < 1000.0, "the loopback RTT is below 1 s");
    }

    // ── (2) COUNTER-PROOF: entity OUTSIDE the zone -> it must NOT come back ───────────────────
    // Without this, a server that echoed everything it received would pass just the same, and we would
    // not be checking chunk routing, which is the only interesting thing about this path.
    DGS::EntityTransfer outside{};
    outside.uuid = 778; outside.chunkX = 9000; outside.chunkY = 9000;

    DGS::Packet pOutside;
    pOutside.pack(outside);
    zone.send(zone.getSocketFD(), pOutside.getRawData(), pOutside.getSize());

    timeval tv2{}; tv2.tv_sec = 1; tv2.tv_usec = 0;   // less is enough: silence is what is expected
    setsockopt(zone.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv2, sizeof(tv2));
    const int bytes2 = zone.receive(zone.getSocketFD(), buffer, sizeof(buffer));
    check(bytes2 <= 0,
          "an entity in chunk (9000,9000) does NOT come back: no zone covers it");

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);

    std::printf("\n== ping_pong: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
