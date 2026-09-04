// ─────────────────────────────────────────────────────────────────────────────────────────────────
// THE VALIDATOR'S UDP PATH, AND WHAT A BAD NETWORK DOES TO IT.
//
// Two things in one binary, deliberately kept apart:
//
//   PART 1 — CORRECTNESS of the UDP path (assertions). Only its TCP side was ever tested, and UDP is
//   the high-frequency one: it is where the real movement travels.
//
//   PART 2 — MEASUREMENT with a degraded network (numbers, not verdicts). The whole system has always
//   been tested over loopback, the most benevolent network in existence: 0 % loss, 0.1 ms of latency
//   and perfect ordering. The question nobody had answered: **does a bad connection turn a legitimate
//   player into a cheater?**
//
// Why it matters: the validator computes `dt` from ITS OWN clock (`nowMs() - last.timestamp_ms`), so
// latency and loss widen the distance budget and should NOT accuse anyone. REORDERING is another
// matter: if an old packet arrives after a newer one, the sample has a small `dt` and a large distance
// — which is exactly the signature of a teleport. The hypothesis is that loss is harmless and
// reordering is not. It gets measured, not assumed.
//
// ⚠️ THE OBSERVABLE, AND A FAILED ATTEMPT WORTH WRITING DOWN. The first thing I tried was counting
// what reaches persistence, as in every other test. **It does not work: the UDP path does NOT forward
// to persistence** — it only updates its internal state; TCP is what persists. It reported 0 arrivals
// even on a clean network, and that looked like a finding when it was an observable that did not exist.
//
// The only thing the UDP path says to the outside world is its LOG: `[Validator] VIOLATION detected
// (UDP)`. Its output is redirected to a file and those lines are counted. This works because the node
// writes them with `std::endl`, which flushes — if that is ever changed to "\n", this measurement goes
// blind.
//
// And since the proxy knows what it dropped itself, the arithmetic closes:
//
//     false violations = the ones the validator rejects from a player who is NOT cheating
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
#include <sys/wait.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <deque>
#include <mutex>
#include <random>
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

static const int kHeadPort  = 21501;
static const int kPersPort  = 21502;
static const int kValTcp    = 21503;
static const int kValUdp    = 21504;
static const int kProxyPort = 21505;   // the "bad router" sitting in front of the validator

static const float kChunkM = 1000.0f;

static std::atomic<bool> g_done{false};        // end of EVERYTHING (head, persistence)
// ⚠️ A SEPARATE FLAG FOR THE PROXY. With a single one, stopping the proxy between cases also killed
// the fake head and persistence — and then "nothing arrives" looked like a finding when it was the
// instrument switching itself off. It reported 0 arrivals even on the CLEAN network, which gave it away.
static std::atomic<bool> g_proxyDone{false};
static int               g_backwardJumpDetected = 0;
static int               g_casesWithNoTraffic = 0;
static std::string       g_logPath;          // the validator writes here; the only UDP observable

/// Counts the UDP violation lines the node has written so far.
static int udpViolations()
{
    std::FILE* f = std::fopen(g_logPath.c_str(), "r");
    if (!f) return 0;
    int n = 0; char line[512];
    while (std::fgets(line, sizeof(line), f))
        if (std::strstr(line, "VIOLATION detected (UDP)")) ++n;
    std::fclose(f);
    return n;
}

// ── Fake head and persistence ───────────────────────────────────────────────────────────────────
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
        cmd.chunkSizeX = kChunkM; cmd.chunkSizeY = kChunkM; cmd.chunkSizeZ = kChunkM;
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
        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 200000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) {
            s.receive(fd, buf, sizeof(buf));   // accepting and swallowing is all that is needed: the
                                               // node will not start unless it can reach persistence
        }
        s.closeClient(fd);
    }
}

// ── THE BAD ROUTER ──────────────────────────────────────────────────────────────────────────────
// A user-space UDP relay. `tc netem` would do this better, but it needs root and CI should not: this
// way the degradation is reproducible and ships inside the test itself.
struct Degradation {
    double loss      = 0.0;   // fraction [0,1]
    int    delayMs   = 0;     // fixed delay before forwarding
    double reorder   = 0.0;   // fraction held back and released AFTER the following packet
    int    depth     = 1;     // how many packets go by before the held one is released
};

static std::atomic<int> g_proxySent{0}, g_proxyDropped{0}, g_proxyReordered{0};
static std::atomic<int> g_proxySendFailures{0};

static void udpProxy(const Degradation d, std::atomic<bool>& ready)
{
    DGS::UDPSocket in;
    if (!in.bind(kProxyPort)) { ready = true; return; }
    { timeval tv{}; tv.tv_usec = 100000;
      setsockopt(in.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    DGS::UDPSocket out;
    // ⚠️ BIND THE OUTGOING SOCKET. Without `bind`, datagrams went out (sendto reported success) but
    // never reached the validator: the positive control failed in ALL SIX cases. Every figure published
    // before that control existed was measuring the void.
    out.bind(0);
    ready = true;

    std::mt19937 rng(1234);                       // FIXED seed: the degradation is reproducible
    std::uniform_real_distribution<double> uni(0.0, 1.0);
    std::vector<uint8_t> held;                    // the one released out of order
    int releaseIn = 0;

    // ⚠️ THE BUFFER SIZE WAS THE BUG. At 2048 bytes `recvfrom` TRUNCATED every datagram (the raw
    // `EntityTransfer` the client then sent is larger) and the proxy forwarded exactly 2048 bytes. The
    // validator discarded anything that was not EXACTLY `sizeof(EntityTransfer)`, so it dropped
    // everything in silence: the proxy counted happy sends, `sendto` reported success, and nothing
    // arrived at the other end. The positive control uncovered it; without it this would have been
    // published as "the validator withstands a bad network".
    //
    // Both halves of that have since been fixed at the source. The datagram is now a Packet honouring
    // `dataSize` (62 B for a moving player, not 4160), and the validator recognises it by its TYPE
    // BYTE and reports a decode failure instead of vanishing — a truncated datagram is now visible as
    // a truncated datagram. The buffer stays generous anyway: a receive buffer that is too small is a
    // bug that hides itself, and this file is the record of it.
    uint8_t buf[sizeof(DGS::EntityTransfer) * 2];
    std::string ip; int port = 0;
    while (!g_proxyDone) {
        const int n = in.receive(buf, sizeof(buf), ip, port);
        if (n <= 0) continue;

        if (uni(rng) < d.loss) { ++g_proxyDropped; continue; }
        if (d.delayMs > 0) std::this_thread::sleep_for(std::chrono::milliseconds(d.delayMs));

        if (!held.empty()) {
            if (!out.send("127.0.0.1", kValUdp, buf, (size_t)n)) ++g_proxySendFailures;
            ++g_proxySent;
            if (--releaseIn <= 0) {
                // Now it goes: the held packet leaves AFTER `depth` newer ones. The deeper it is, the
                // further back its position lands and the more it resembles a backwards teleport —
                // which is the signature the validator might mistake for cheating.
                ++g_proxyReordered;
                if (!out.send("127.0.0.1", kValUdp, held.data(), held.size()))
                    ++g_proxySendFailures;
                ++g_proxySent;
                held.clear();
            }
            continue;
        }
        if (uni(rng) < d.reorder) {
            held.assign(buf, buf + n);
            releaseIn = d.depth;
            continue;
        }

        if (!out.send("127.0.0.1", kValUdp, buf, (size_t)n)) ++g_proxySendFailures;
        ++g_proxySent;
    }
    if (!held.empty()) { ++g_proxyDropped; }   // never released: counts as lost
}

// ── The player ──────────────────────────────────────────────────────────────────────────────────
static void sendSample(DGS::UDPSocket& udp, int port, uint32_t uuid, double x, float vmax)
{
    DGS::EntityTransfer e{};
    e.uuid = uuid;
    e.chunkX = 0; e.chunkY = 0; e.chunkZ = 0;
    e.pos[0] = (float)x; e.pos[1] = 0.0f; e.pos[2] = 0.0f;
    e.stats.speed[0] = vmax;
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", port, p.getRawData(), p.getSize());
}

static pid_t launchValidator(const char* nodePath, const char* soPath)
{
    const pid_t pid = fork();
    if (pid == 0) {
        std::freopen(g_logPath.c_str(), "w", stdout);   // its log IS the instrument
        setenv("VALIDADOR_TCP_PORT", std::to_string(kValTcp).c_str(), 1);
        setenv("VALIDADOR_UDP_PORT", std::to_string(kValUdp).c_str(), 1);
        setenv("HEAD_SERVER_HOST", "127.0.0.1", 1);
        setenv("HEAD_SERVER_PORT", std::to_string(kHeadPort).c_str(), 1);
        setenv("PERSISTENCE_HOST", "127.0.0.1", 1);
        setenv("PERSISTENCE_PORT", std::to_string(kPersPort).c_str(), 1);
        setenv("GAME_MODULE_SO", soPath, 1);
        char tmpl[] = "/tmp/dgs_netdeg_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }
    return pid;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX], absStub[PATH_MAX];
    const char* nodePath = realpath((argc > 1) ? argv[1] : "./build/validador_node", abs)
                           ? abs : "./build/validador_node";
    const char* soPath   = realpath((argc > 2) ? argv[2] : "./build/stub_rules.so", absStub)
                           ? absStub : "./build/stub_rules.so";

    g_logPath = "/tmp/dgs_netdeg_validator.log";
    std::remove(g_logPath.c_str());

    std::atomic<bool> h{false}, p{false};
    std::thread th(fakeHead, std::ref(h));
    std::thread tp(fakePersistence, std::ref(p));
    while (!h || !p) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pid = launchValidator(nodePath, soPath);
    DGS::TCPSocket probe;
    bool up = false;
    for (int i = 0; i < 200 && !up; ++i) {
        if (probe.connect("127.0.0.1", kValTcp)) { up = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    check(up, "the validator is up (checked over its TCP side before touching UDP)");
    if (!up) { kill(pid, SIGTERM); waitpid(pid, nullptr, 0); g_done = true; th.join(); tp.join(); return 1; }

    DGS::UDPSocket udp;
    udp.bind(0);
    const float vmax = 5.0f;

    // ══ PART 1: CORRECTNESS OF THE UDP PATH ═══════════════════════════════════════════════════
    {
        const int v0 = udpViolations();
        sendSample(udp, kValUdp, 3001, 0.0, vmax);             // first one: sets the baseline
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        sendSample(udp, kValUdp, 3001, 1.0, vmax);             // 1 m in ~0.3 s: legal
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        check(udpViolations() == v0, "over UDP, a plausible step does NOT raise a violation");

        sendSample(udp, kValUdp, 3001, 5000.0, vmax);          // 5 km: a teleport
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        check(udpViolations() == v0 + 1, "a teleport over UDP DOES raise a violation");

        // Counter-proof for the observable: if the counter moved for just anything, the pair above
        // would mean nothing. Another plausible step must not move it.
        sendSample(udp, kValUdp, 3002, 0.0, vmax);
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        sendSample(udp, kValUdp, 3002, 0.5, vmax);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        check(udpViolations() == v0 + 1, "and another legitimate player does not increment the counter");

        // ── WHY REORDERING ACCUSES NOBODY ─────────────────────────────────────────────────────
        // Part 2 measures 0 false positives even while reordering, and that did NOT square with the
        // arithmetic: an old sample after a newer one has a large distance and a near-zero `dt`, the
        // signature of a teleport. So the EXACT pattern is reproduced by hand — two back-to-back
        // datagrams, the second one 1.2 m BEHIND — and the result written down. A number measured
        // without knowing why it comes out is half a number.
        const int vb = udpViolations();
        sendSample(udp, kValUdp, 3003, 0.0, vmax);
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        sendSample(udp, kValUdp, 3003, 1.2, vmax);   // advances 1.2 m (legal at 5 m/s over 0.25 s)
        sendSample(udp, kValUdp, 3003, 0.0, vmax);   // and IMMEDIATELY the old one, 1.2 m back
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        g_backwardJumpDetected = udpViolations() - vb;
        std::printf("    BACKWARD jump of 1.2 m with dt~0 (the reordering pattern): %d violation(s)\n",
                    g_backwardJumpDetected);
        check(g_backwardJumpDetected == 0,
              "the reordering pattern (backward jump with dt~0) is DISCARDED, not judged");

        // ── AND THAT THE DISCARD OPENS NO HOLE ────────────────────────────────────────────────
        // If ignoring closely spaced samples let the baseline ADVANCE, a cheater would flood samples
        // at 1 ms and move for free. It does not: a discarded sample updates nothing. Checked the
        // brutal way — 20 jumps of 100 m at 1 ms apart — and then ONE sample with a real dt, which has
        // to be measured against the GOOD baseline and come out flagged.
        const int vc = udpViolations();
        sendSample(udp, kValUdp, 3004, 0.0, vmax);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        for (int i = 1; i <= 20; ++i) {
            sendSample(udp, kValUdp, 3004, i * 100.0, vmax);   // 100 m per jump, almost no dt
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        sendSample(udp, kValUdp, 3004, 2100.0, vmax);          // now with a real dt
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        const int afterFlood = udpViolations() - vc;
        std::printf("    flood of 20 samples 1 ms apart advancing 100 m each: %d violation(s)\n",
                    afterFlood);
        check(afterFlood >= 1,
              "flooding with closely spaced samples buys NO distance: the cheat is still caught");
    }

    // ══ PART 2: MEASUREMENT WITH A DEGRADED NETWORK ═══════════════════════════════════════════
    // A LEGITIMATE player walks at 4 m/s sending at 20 Hz. The ground truth is that they are not
    // cheating: any rejection is a FALSE POSITIVE, and in production that is an honest player kicked.
    std::printf("\n    ── legitimate player at 4 m/s, 20 Hz, 60 samples · the proxy degrades the network ──\n");
    std::printf("    %-22s %8s %8s %8s %8s %10s   %s\n",
                "condition", "sent", "dropped", "reord.", "arrive", "FALSE+", "control");

    struct Case { const char* name; Degradation d; };
    const Case cases[] = {
        { "clean",               { 0.00, 0,  0.00, 1 } },
        { "5 % loss",            { 0.05, 0,  0.00, 1 } },
        { "20 % loss",           { 0.20, 0,  0.00, 1 } },
        { "80 ms of delay",      { 0.00, 80, 0.00, 1 } },
        { "20 % REORDERING",     { 0.00, 0,  0.20, 1 } },
        { "DEEP REORDERING",     { 0.00, 0,  0.20, 6 } },   // the held packet leaves 6 packets late
    };

    int falseReorder = -1, falseLoss20 = -1, falseDeep = -1;
    uint32_t uuid = 4000;

    for (const Case& c : cases) {
        g_proxySent = 0; g_proxyDropped = 0; g_proxyReordered = 0; g_proxySendFailures = 0;
        std::atomic<bool> lp{false};
        g_proxyDone = false;
        std::thread tproxy(udpProxy, c.d, std::ref(lp));
        while (!lp) std::this_thread::sleep_for(std::chrono::milliseconds(5));

        ++uuid;
        const int before = udpViolations();
        const int N = 60;
        double x = 0.0;
        for (int i = 0; i < N; ++i) {
            sendSample(udp, kProxyPort, uuid, x, vmax);
            x += 0.20;                                    // 4 m/s at 20 Hz = 0.20 m per sample
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        // ⚠️ The DELAY case needs more slack than it looks: the proxy sleeps SYNCHRONOUSLY, so at 80 ms
        // per packet with a 20 Hz client a queue builds up — when the sending finishes there are still
        // seconds of backlog to drain. With a short margin the control failed, and that failure was the
        // instrument's, not the node's.
        std::this_thread::sleep_for(std::chrono::milliseconds(800 + c.d.delayMs * 40));

        // ⚠️ THE TALLY CLOSES BEFORE THE CONTROL. The control teleport raises — on purpose — a
        // violation; measured afterwards, that violation would be counted as a false positive and every
        // case would come out with a phantom +1. It happened: the CLEAN network reported 1.
        const int falseMeasured = udpViolations() - before;

        // ⚠️ POSITIVE CONTROL, and this is what was missing. "0 false violations" has TWO opposite
        // readings: "the validator withstands a bad network" or "nothing is reaching it through the
        // proxy". The `arrive` column does not separate them because it is a SUBTRACTION, not a
        // measurement. So a deliberate teleport is slipped through the SAME path: if the validator is
        // receiving, it MUST flag it. If it does not, this case measured nothing and its figures are
        // discarded.
        const int vBeforeCtrl = udpViolations();
        sendSample(udp, kProxyPort, uuid, x + 5000.0, vmax);
        std::this_thread::sleep_for(std::chrono::milliseconds(500 + c.d.delayMs * 2));
        const bool controlOk = (udpViolations() > vBeforeCtrl);

        const int sent    = g_proxySent.load();
        const int dropped = g_proxyDropped.load();
        const int falsePos = falseMeasured;              // rejections of a player who is NOT cheating
        const int arrive  = sent - falsePos;

        // ⚠️ The `reord.` column is NOT decoration: without it, "0 false positives under reordering"
        // could mean "reordering is harmless" or "I never got round to reordering anything". Those are
        // different things and from outside they look the same.
        std::printf("    %-22s %8d %8d %8d %8d %10d   %s\n",
                    c.name, sent, dropped, g_proxyReordered.load(), arrive, falsePos,
                    controlOk ? "control OK" : "*** CONTROL FAILS ***");
        if (g_proxySendFailures.load() > 0)
            std::printf("         (the proxy failed to forward %d of %d datagrams)\n",
                        g_proxySendFailures.load(), sent);
        if (!controlOk) ++g_casesWithNoTraffic;

        if (std::strcmp(c.name, "20 % REORDERING") == 0) {
            falseReorder = falsePos;
            check(g_proxyReordered.load() > 5,
                  "the proxy REALLY did reorder packets (otherwise the 0 above would say nothing)");
        }
        if (std::strcmp(c.name, "20 % loss")       == 0) falseLoss20 = falsePos;
        if (std::strcmp(c.name, "DEEP REORDERING") == 0) falseDeep   = falsePos;

        // Stop ONLY this case's proxy; the head and persistence stay alive.
        g_proxyDone = true;
        tproxy.join();
    }

    std::printf("\n");
    // LOSS must accuse nobody: the validator measures `dt` from its own clock, so losing samples only
    // widens the next one's distance budget.
    check(falseLoss20 >= 0 && falseLoss20 <= 2,
          "with 20 % LOSS a legitimate player accumulates no false violations");
    // REORDERING is the suspect: an old sample after a newer one has a small dt and a large distance,
    // the signature of a teleport. This is NOT a quality threshold: it pins the number down so any
    // regression shows.
    std::printf("    false violations · shallow reordering %d · DEEP reordering %d (out of 60)\n",
                falseReorder, falseDeep);

    // ⚠️ WHETHER THESE ZEROS MEAN ANYTHING depends on each case's POSITIVE CONTROL, not on this block:
    // in every one a deliberate teleport is slipped through the SAME path and has to come out flagged.
    // Without it a zero would be indistinguishable from "nothing comes through here" — which is exactly
    // the mistake made the first time this was measured (the proxy truncated the datagrams so
    // absolutely nothing arrived, and the figures were published as if the system held up).
    check(falseReorder == 0 && falseDeep == 0,
          "with the minimum-dt discard, reordering no longer produces false violations");
    check(g_casesWithNoTraffic == 0,
          "in EVERY case traffic reaches the validator through the proxy (positive control)");
    check(falseLoss20 >= 0, "the loss and delay figures are MEASURED");

    g_done = true;
    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    th.join(); tp.join();

    std::printf("\n== net_degraded: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
