// ─────────────────────────────────────────────────────────────────────────────────────────────────
// THE THREE `zone_node` POLICIES NOBODY TESTED: bans, lease expiry and the circuit breaker.
//
// These are not features, they are DECISIONS — and all three are observable from outside if you know
// where to look:
//
//   A) BAN (§3.7). The social node decides; the zone only APPLIES. A banned uuid must not even get in.
//      Counter-proof: another uuid at the same instant DOES get in, or "nobody gets in" would pass too.
//
//   B) LEASE (§3.6). An entity that stops reporting past `ENTITY_LEASE_MS` is purged, otherwise the
//      node would serve it forever and `entities` would grow without bound in long sessions.
//      Counter-proof: another entity that KEEPS reporting is not purged in the same window.
//
//   C) CIRCUIT BREAKER (§2.3). What does the zone do when the arbiter stops answering? This had THREE
//      layers of defect stacked on top of each other, all three now fixed and asserted here:
//
//        Layer 1 — `cbState = 1` appeared NOWHERE in `zone_node.cpp`. The variable was declared, read
//        in `circuitBreakerOk()` and cleared in two places, but never set. The OPEN state was dead
//        code by construction, so the head could never see `state = 2`.
//
//        Layer 2 — `cbOpenCount` was never reset. Three timeouts across the whole life of the process
//        exhausted it forever; after that the branch that trips the breaker never ran again. It now
//        counts CONSECUTIVE failures.
//
//        Layer 3 — the one that mattered most, and it was not in the breaker at all: the zone STOPPED
//        TALKING TO THE HEAD entirely. Root cause, isolated with a probe: `connectToValidator()` ran a
//        BLOCKING `connect` with no deadline, inside the main loop, on EVERY timeout. Against an
//        arbiter that listens but does not accept, the first 11 connects succeed instantly (backlog
//        10) and the twelfth blocks for ~127 s while the kernel retries the SYN. Measured before the
//        fix: the head received nothing at all — not one metric, not one status report — from t≈8.2 s
//        to the end of the run, while the zone's own log recorded eleven unanswered validations. The
//        head went blind exactly when something was wrong.
//
//      The fix is threefold: a bounded `connect` (`TCPSocket::connect(..., timeoutMs)`), spaced retries
//      with exponential backoff instead of one per timeout, and a HALF-OPEN probe so the breaker can
//      close again — without it `validated` stayed false forever and a recovered validator was never
//      used again.
//
// The observable for (A) and (B) is `ServerMetrics::activeEntities`, published to the head 10 times a
// second. For (C) it is `ValidatorStatus::state` (0 = no validator · 1 = ok · 2 = breaker open), sent
// every 5 s — plus, for layer 3, the LONGEST SILENCE between reads at the head, which is what a frozen
// tick actually looks like from the other end of the socket.
//
// ⚠️ ORDER MATTERS: case (C) drives the validator into fail-open, so it goes LAST. Placed earlier it
// would poison the other two.
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

static const int kHeadPort   = 21511;
static const int kValPort    = 21512;
static const int kSocialPort = 21513;
static const int kZoneUdp    = 21514;

static const int kLeaseMs = 1200;   // deliberately short: the test cannot wait out the 3 s default

static std::atomic<bool> g_done{false};
static std::atomic<int>  g_activeEntities{-1};
static std::atomic<int>  g_metricsSeen{0};
static std::atomic<int>  g_validatorState{-1};   // last ValidatorStatus::state observed
static std::atomic<int>  g_reqSent{0}, g_reqTimeout{0};
static std::atomic<int>  g_statusCount{0};
static std::atomic<bool> g_validatorMute{false}; // the validator accepts but never answers
static std::atomic<int>  g_socialFD{-1};         // so the main thread can push the ban
static std::atomic<int>  g_headAccepts{0};       // how many times the head accepted the zone
static std::atomic<unsigned long long> g_lastReadMs{0};
static std::atomic<unsigned long long> g_worstSilenceMs{0};   // longest gap between reads at the head

static const std::chrono::steady_clock::time_point g_t0 = std::chrono::steady_clock::now();
static unsigned long long msSinceStart()
{
    return (unsigned long long)std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - g_t0).count();
}

/// Worst silence so far, INCLUDING the one still running.
///
/// ⚠️ The gap between two reads is only recorded when the second one arrives — so a silence that lasts
/// until the end of the run is never recorded at all. That is the failure mode this whole file is about:
/// a zone frozen inside a blocking `connect` never reads again, so the worst gap stayed at a healthy
/// 217 ms while the head heard NOTHING for the rest of the test. Caught by running the counter-proof:
/// with the fix disabled the assertion still passed. A silence has to be measured against the clock, not
/// against the next thing to arrive.
static unsigned long long worstSilenceMs()
{
    const unsigned long long last = g_lastReadMs.load();
    const unsigned long long open = (last == 0) ? 0 : msSinceStart() - last;
    return std::max(g_worstSilenceMs.load(), open);
}

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
        ++g_headAccepts;
        DGS::Command cmd{};
        cmd.chunkSizeX = 1000.0f; cmd.chunkSizeY = 1000.0f; cmd.chunkSizeZ = 1000.0f;
        cmd.port = kZoneUdp;
        std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");
        DGS::Packet p; p.pack(cmd);
        s.send(fd, p.getRawData(), p.getSize());
        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 300000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        // ⚠️ A READ TIMEOUT IS NOT A HANG-UP. Bailing out on the first 300 ms of silence forces the zone
        // to reconnect, and every reconnection loses status reports — which is exactly what is being
        // measured here. With a blanket `break` only 2 reports arrived in 20 s and it looked like the
        // zone had stopped reporting: that was the instrument hanging up the phone.
        while (!g_done) {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;          // peer really did hang up: go back to accept()
            if (n < 0)  continue;       // just the 300 ms read timeout
            // The gap between reads is how long the zone went WITHOUT SAYING ANYTHING. It is the direct
            // observable for layer 3: a frozen tick shows up here as a long silence.
            {
                const unsigned long long t = msSinceStart();
                const unsigned long long prev = g_lastReadMs.exchange(t);
                if (prev != 0) {
                    const unsigned long long gap = t - prev;
                    unsigned long long worst = g_worstSilenceMs.load();
                    while (gap > worst && !g_worstSilenceMs.compare_exchange_weak(worst, gap)) {}
                }
            }
            if (std::getenv("ZPOL_TRACE_READS"))
                std::printf("      [head read %4d B  type0=%d  t=%llu ms]\n",
                            n, (int)buf[0], msSinceStart());
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() == DGS::PKT_METRICS) {
                g_activeEntities = (int)r.unpackServerMetrics().activeEntities;
                ++g_metricsSeen;
            } else if (r.getType() == DGS::PKT_VALIDATOR_STATUS) {
                const DGS::ValidatorStatus st = r.unpackValidatorStatus();
                g_validatorState = (int)st.state;
                g_reqSent    = (int)st.reqSent;
                g_reqTimeout = (int)st.reqTimeout;
                ++g_statusCount;
                std::printf("      [status #%d  t=%llu ms] state=%d reqSent=%u reqTimeout=%u\n",
                            g_statusCount.load(), msSinceStart(),
                            (int)st.state, st.reqSent, st.reqTimeout);
                std::fflush(stdout);
            }
        }
        s.closeClient(fd);
    }
}

/// Fake validator: answers verdict 1 to everything... unless asked to go MUTE, which is how the
/// circuit breaker is provoked without killing the process. A hung node is alive but stuck, and being
/// stuck means two things at once, BOTH of which matter here:
///
///   · it stops answering on connections it already holds, and
///   · it stops calling accept(), so its accept queue fills up.
///
/// The second half is the one that used to freeze the zone. A full accept queue does not refuse
/// connections — the kernel silently drops the SYN and an unbounded `connect` retries for ~127 s. If
/// this fake kept accepting while mute, the outage would be far too polite and reverting the bounded
/// `connect` would no longer be caught by this test.
static void fakeValidator(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kValPort)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;
    while (!g_done) {
        if (g_validatorMute) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 200000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;                             // peer hung up: go back to accept()
            if (n < 0)  continue;                          // just the read timeout
            if (g_validatorMute) continue;                 // holds the connection and stays silent
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() != DGS::PKT_VALIDATE_REQ) continue;
            DGS::ValidateAck ack{};
            ack.requestId = r.unpackValidateRequest().requestId;
            ack.verdict = 1;
            DGS::Packet a; a.pack(ack);
            s.send(fd, a.getRawData(), a.getSize());
        }
        s.closeClient(fd);
    }
}

static void fakeSocial(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kSocialPort)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;
    while (!g_done) {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        g_socialFD = fd;                                   // the test pushes the ban through here
        uint8_t buf[4096];
        timeval tv{}; tv.tv_usec = 200000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) s.receive(fd, buf, sizeof(buf));
        g_socialFD = -1;
        s.closeClient(fd);
    }
}

static void sendEntity(DGS::UDPSocket& udp, uint32_t uuid, float x)
{
    DGS::EntityTransfer e{};
    e.uuid = uuid;
    e.chunkX = 50; e.chunkY = 50; e.chunkZ = 50;   // dead centre: no ghosts or handoffs in the way
    e.pos[0] = x;
    e.stats.speed[0] = 5.0f;
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

static bool waitForActive(int target, int msLimit)
{
    const auto t0 = std::chrono::steady_clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - t0).count() < msLimit) {
        if (g_activeEntities.load() == target) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

/// Waits until the head observes a validator state other than `notThis`. @return the state observed.
/// Waits until the head observes exactly `want`, and reports what it saw last if it never does.
///
/// ⚠️ IT USED TO ACCEPT "ANY STATE OTHER THAN THE BROKEN ONE", and that stopped being good enough the
/// moment the zone's tick was fixed to its nominal 10 Hz: at twice the rate it asks for twice as many
/// verdicts, the breaker trips and re-trips more often inside the same wall clock, and the first state
/// that is not 0 on the way back can perfectly well be 2 (breaker OPEN) rather than 1 (recovered).
/// "It changed" was never the property under test — "it closed again" is.
static int waitForState(int want, int msLimit)
{
    const auto t0 = std::chrono::steady_clock::now();
    int lastSeen = g_validatorState.load();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - t0).count() < msLimit) {
        lastSeen = g_validatorState.load();
        if (lastSeen == want) return lastSeen;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return lastSeen;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX], absStub[PATH_MAX];
    const char* nodePath = realpath((argc > 1) ? argv[1] : "./build/zone_node", abs)
                           ? abs : "./build/zone_node";
    const char* stubPath = realpath((argc > 2) ? argv[2] : "./build/stub_rules.so", absStub)
                           ? absStub : "./build/stub_rules.so";

    std::atomic<bool> h{false}, v{false}, so{false};
    std::thread th(fakeHead, std::ref(h));
    std::thread tv(fakeValidator, std::ref(v));
    std::thread ts(fakeSocial, std::ref(so));
    while (!h || !v || !so) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); g_done = true; th.join(); tv.join(); ts.join(); return 1; }
    if (pid == 0) {
        if (!std::getenv("ZPOL_VERBOSE")) std::freopen("/tmp/dgs_zpol_zone.log", "w", stdout);
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
        setenv("ENTITY_LEASE_MS", std::to_string(kLeaseMs).c_str(), 1);
        setenv("GAME_MODULE_SO", stubPath, 1);
        // Tight retry spacing so the half-open recovery is observable inside a short test. The
        // production defaults (2000 ms, backing off to 30 s) would need a run an order of magnitude
        // longer to show the same transition.
        //
        // ⚠️ overwrite = 0 ON PURPOSE, and it is the difference between a test and a tautology. These
        // three knobs ARE the layer-3 fix, so turning them off has to reproduce the original freeze:
        //
        //     VALIDATOR_CONNECT_MS=0 VALIDATOR_RETRY_MS=0 ./build/zone_policy_e2e_test
        //
        // (0 = blocking connect, no spacing = reconnect on every timeout). With overwrite = 1 the child
        // silently overrode the environment and that counter-proof passed green while changing nothing
        // — the run looked identical to the fixed one, which is exactly how a green suite lies.
        setenv("VALIDATOR_RETRY_MS",     "700",  0);
        setenv("VALIDATOR_RETRY_MAX_MS", "1500", 0);
        setenv("VALIDATOR_CONNECT_MS",   "500",  0);
        char tmpl[] = "/tmp/dgs_zpol_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    bool started = false;
    for (int i = 0; i < 300 && !started; ++i) {
        if (g_metricsSeen.load() > 0 && g_socialFD.load() >= 0) started = true;
        else std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    check(started, "the zone starts up and subscribes to the social node");

    DGS::UDPSocket udp;
    udp.bind(0);

    if (started) {
        // ══ (A) BAN ═══════════════════════════════════════════════════════════════════════════
        DGS::AccountAction ban{};
        ban.actorUuid = 1; ban.targetUuid = 6001; ban.action = DGS::ACC_BAN; ban.durationS = 0;
        std::snprintf(ban.reason, sizeof(ban.reason), "test");
        DGS::Packet pb; pb.pack(ban);
        DGS::TCPSocket dummy;   // only to reuse `send(fd, ...)` over the social node's fd
        dummy.send(g_socialFD.load(), pb.getRawData(), pb.getSize());
        std::this_thread::sleep_for(std::chrono::milliseconds(400));

        sendEntity(udp, 6001, 0.0f);                   // the BANNED one
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        check(g_activeEntities.load() == 0, "a BANNED uuid does not even enter the zone");

        sendEntity(udp, 6002, 0.0f);                   // COUNTER-PROOF: another uuid, same instant
        check(waitForActive(1, 3000), "another uuid DOES get in (the ban is selective, not a blackout)");

        // ══ (B) LEASE ═════════════════════════════════════════════════════════════════════════
        // 6002 stops reporting; 6003 keeps going. Past the lease, only 6003 should remain.
        sendEntity(udp, 6003, 0.0f);
        check(waitForActive(2, 3000), "both entities get in");

        const auto t0 = std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - t0).count() < kLeaseMs + 900) {
            sendEntity(udp, 6003, 0.1f);               // only 6003 stays alive
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
        check(waitForActive(1, 3000),
              "the one that stopped reporting is PURGED when its lease expires, and the live one is not");

        // ══ (C) CIRCUIT BREAKER ═══════════════════════════════════════════════════════════════
        // Baseline first. Without it, "state == 2 while the arbiter is mute" would also pass on a zone
        // that reported 2 permanently — a broken breaker stuck open looks identical to a working one.
        const int stateBefore   = g_validatorState.load();
        const int statusBefore  = g_statusCount.load();
        const int metricsBefore = g_metricsSeen.load();
        check(stateBefore == 1,
              "BEFORE breaking anything the head sees a HEALTHY arbiter (state=1) — the baseline");

        g_worstSilenceMs = 0;                          // measured only across the outage
        g_validatorMute = true;                        // the arbiter accepts but stops answering

        // Steady traffic so the zone keeps asking for verdicts and piles up timeouts.
        const auto t1 = std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - t1).count() < 9000) {
            sendEntity(udp, 6003, 0.2f);
            std::this_thread::sleep_for(std::chrono::milliseconds(120));
        }

        const int stateAfter = g_validatorState.load();
        const unsigned long long worstSilence = worstSilenceMs();
        const int statusDuring  = g_statusCount.load()  - statusBefore;
        const int metricsDuring = g_metricsSeen.load() - metricsBefore;
        std::printf("    validator state as seen by the head: %d -> %d  (0=none · 1=ok · 2=open)\n"
                    "    the zone's own counters: requests=%d  timeouts=%d\n"
                    "    the zone kept talking: status reports +%d · metrics +%d · worst silence %llu ms\n",
                    stateBefore, stateAfter, g_reqSent.load(), g_reqTimeout.load(),
                    statusDuring, metricsDuring, worstSilence);

        // LAYER 3 — the sharpest assertion in this file, and the one that was failing hardest.
        // A frozen tick is invisible in `state`; it is only visible as SILENCE. Before the fix the head
        // received nothing for ~127 s while `connect` retried a dropped SYN. The bound is 1000 ms:
        // metrics go out every ~100 ms and the bounded connect costs at most 500 ms, so anything near a
        // second means the tick stalled on something.
        check(worstSilence < 1000,
              "LAYER 3: the tick NEVER freezes — with the arbiter hung the head keeps hearing the zone");
        check(metricsDuring > 20,
              "and the metric stream keeps flowing throughout the outage (not just a couple of reads)");
        check(statusDuring >= 1,
              "at least one status report reaches the head DURING the outage (the head is not blind)");

        // LAYERS 1 AND 2 — the breaker actually trips, and the head is told.
        check(stateAfter != 1,
              "LAYERS 1+2: the breaker reports the fault (state leaves 'ok' when the arbiter goes mute)");
        check(g_reqTimeout.load() > 0,
              "and the timeouts are COUNTED and published (reqTimeout no longer stuck at 0)");

        // FAIL-OPEN, the decision that was never written down: with the arbiter down the zone KEEPS
        // SERVING (local S1 only). Defensible — availability over rigour — but it has to be on record:
        // if it is ever switched to fail-closed, this test will say so.
        check(g_activeEntities.load() >= 1,
              "FAIL-OPEN: with the arbiter down the zone keeps serving (a decision, not an accident)");

        // ══ (C bis) RECOVERY ══════════════════════════════════════════════════════════════════
        // The half-open probe. Without it `validated` stayed false forever and a recovered validator was
        // never used again — a breaker that cannot close is a fuse. This is also the counter-proof for
        // the assertions above: a state that never comes back to 1 would mean the zone had simply given
        // up, and "state != 1" would be passing for the wrong reason.
        g_validatorMute = false;
        // The status heartbeat is every 5 s, so the window has to hold several of them.
        const int stateRecovered = waitForState(1, 20000);
        std::printf("    after the arbiter recovers, the head sees state=%d\n", stateRecovered);
        check(stateRecovered == 1,
              "RECOVERY: once the arbiter answers again the breaker CLOSES and the head sees 'ok' again");

        check(g_headAccepts.load() == 1,
              "and all of this happened over ONE head connection (no reconnection storm)");
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    g_done = true;
    th.join(); tv.join(); ts.join();

    std::printf("\n== zone_policy_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
