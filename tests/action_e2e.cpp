// ─────────────────────────────────────────────────────────────────────────────────────────────────
// ACTIONS (kind=1) — the FAIL-CLOSED path, which is direct attack surface and had no test.
//
// The validator treats movement and actions differently on purpose:
//   · movement (kind=0) -> with no module it falls back to a generic rule (a softened fail-OPEN);
//   · action   (kind=1) -> with no module or no rule, it **REJECTS**.
//
// That asymmetry is deliberate: an action destroys, places or transfers objects — guild bank, loot,
// trade — and letting one through unvalidated is worse than rejecting a legitimate one. But "rejects
// everything" and "rejects because there is no rule" look EXACTLY THE SAME from outside, so a broken
// validator that rejected everything would look safe. Hence three cases rather than one:
//
//     A) module WITHOUT `validateAction` (null pointer) -> REJECTS  (the host's fail-closed)
//     B) module WITH a rule that accepts                -> ACCEPTS  (so the host really does delegate)
//     C) module WITH a rule that rejects                -> REJECTS  (so the host obeys the NO)
//
// (B) is what stops (A) and (C) being read as "rejects everything". The three together say the host
// genuinely delegates and that fail-closed is a decision, not a malfunction.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/game_module.h"   // ActionHeader/ActionVerb: the blob is opaque to the DGS, but
                                       // the test has to fill it in the way the game would

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

static const int kHeadPort = 21461;
static const int kPersPort = 21462;
static const int kValTcp   = 21463;
static const int kValUdp   = 21464;

static std::atomic<bool> g_done{false};

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
        cmd.port = kValTcp;
        std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");
        DGS::Packet p; p.pack(cmd);
        s.send(fd, p.getRawData(), p.getSize());
        uint8_t buf[8192];
        timeval tv{}; tv.tv_sec = 1;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) { if (s.receive(fd, buf, sizeof(buf)) <= 0) break; }
        s.closeClient(fd);
    }
}

static void fakePersistence(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kPersPort)) { ready = true; return; }
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

/// Brings up the validator with the given module, sends it ONE action and returns the verdict.
/// @return 1 accepted · 0 rejected · -1 no answer.
static int askAction(const char* nodePath, const char* soPath, const char* verdictEnv)
{
    const pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        if (!std::getenv("ACTION_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
        setenv("VALIDADOR_TCP_PORT", std::to_string(kValTcp).c_str(), 1);
        setenv("VALIDADOR_UDP_PORT", std::to_string(kValUdp).c_str(), 1);
        setenv("HEAD_SERVER_HOST", "127.0.0.1", 1);
        setenv("HEAD_SERVER_PORT", std::to_string(kHeadPort).c_str(), 1);
        setenv("PERSISTENCE_HOST", "127.0.0.1", 1);
        setenv("PERSISTENCE_PORT", std::to_string(kPersPort).c_str(), 1);
        setenv("GAME_MODULE_SO", soPath, 1);
        if (verdictEnv) setenv("STUB_ACTION_VERDICT", verdictEnv, 1);
        else            unsetenv("STUB_ACTION_VERDICT");
        char tmpl[] = "/tmp/dgs_action_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    int verdict = -1;
    DGS::TCPSocket zone;
    bool up = false;
    for (int i = 0; i < 200 && !up; ++i) {
        if (zone.connect("127.0.0.1", kValTcp)) { up = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    if (up) {
        timeval tv{}; tv.tv_sec = 3;
        setsockopt(zone.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        DGS::ValidateRequest req{};
        req.requestId  = 55;
        req.entityUuid = 8001;
        req.ownerZone  = 1;
        req.kind       = 1;                       // ACTION
        req.entity.uuid = 8001;
        // OPAQUE blob: the DGS never looks inside, it only carries it. A plausible header is sent so
        // the module receives something with a size rather than an empty buffer.
        DGS::ActionHeader hdr{};
        hdr.verb = DGS::ACT_DESTROY;
        std::memcpy(req.entity.data, &hdr, sizeof(hdr));
        req.entity.dataSize = (uint16_t)sizeof(hdr);

        DGS::Packet p; p.pack(req);
        if (zone.send(zone.getSocketFD(), p.getRawData(), p.getSize())) {
            uint8_t buf[4096];
            const int n = zone.receive(zone.getSocketFD(), buf, sizeof(buf));
            if (n > 0) {
                DGS::Packet r; r.setBuffer(buf, n);
                const DGS::ValidateAck ack = r.unpackValidateAck();
                if (ack.requestId == 55) verdict = (int)ack.verdict;
            }
        }
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    return verdict;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    auto path = [](const char* rel, char* buf) -> const char* {
        return realpath(rel, buf) ? buf : rel;
    };
    char b1[PATH_MAX], b2[PATH_MAX], b3[PATH_MAX];
    const char* nodePath  = path((argc > 1) ? argv[1] : "./build/validador_node", b1);
    const char* soNoRule  = path((argc > 2) ? argv[2] : "./build/stub_rules.so", b2);
    const char* soWithRule = path((argc > 3) ? argv[3] : "./build/stub_rules_actions.so", b3);

    std::atomic<bool> h{false}, p{false};
    std::thread th(fakeHead, std::ref(h));
    std::thread tp(fakePersistence, std::ref(p));
    while (!h || !p) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // (A) Module WITHOUT an action rule -> the host's fail-closed.
    const int vNoRule = askAction(nodePath, soNoRule, nullptr);
    check(vNoRule == 0,
          "with NO action rule in the module, the validator REJECTS (fail-closed)");

    // (B) Module WITH a rule that accepts. This is what stops (A) being read as "rejects everything".
    const int vAccepts = askAction(nodePath, soWithRule, "1");
    check(vAccepts == 1,
          "WITH an accepting rule the action goes through (so the host really does DELEGATE)");

    // (C) Module WITH a rule that rejects.
    const int vRejects = askAction(nodePath, soWithRule, "0");
    check(vRejects == 0,
          "WITH a rejecting rule the action is denied (the host obeys the NO)");

    std::printf("    action verdicts  ·  no rule %d  ·  rule=yes %d  ·  rule=no %d\n",
                vNoRule, vAccepts, vRejects);
    check(vAccepts != vNoRule,
          "the no-rule rejection is NOT 'rejects everything': with a rule the same blob is accepted");

    g_done = true;
    th.join(); tp.join();

    std::printf("\n== action_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
