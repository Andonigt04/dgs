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

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);
}

static const int kPersPort = 21495;

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

    std::printf("\n== persistence_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
