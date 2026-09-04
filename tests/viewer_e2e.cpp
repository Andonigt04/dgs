// ─────────────────────────────────────────────────────────────────────────────────────────────────
// THE VIEWER'S FEED — driven headless against a real `zone_node`.
//
// A viewer is only worth anything if what it shows is what the cluster is doing, and that has nothing
// to do with the drawing. So the decoding, the ownership colouring and the expiry live in
// `views/viewer_state.h` with no raylib, and this test drives them against a node that is actually
// running: a real `zone_node`, a real player moving over UDP, and the observer subscription that the
// viewer uses.
//
//     [fake head] --Command--> [zone_node] <--EntityTransfer (UDP)-- [fake player]
//                                         <--PKT_OBSERVE (UDP)------ [this test, as the viewer]
//                                         --entities + ghosts------->
//
// What has to hold, and what each one would hide if it were missing:
//
//   1. subscribing shows the world      — otherwise the viewer is a black screen;
//   2. and does NOT put anything in it  — an observer that registered as a player would be inventing
//                                         the very thing it claims to be watching. This is the one
//                                         that matters most, and it is checked against the node's own
//                                         entity count, not against the viewer's opinion of itself;
//   3. movement is SEEN moving          — a still picture of a moving world is the classic viewer bug:
//                                         first frame decoded, nothing after;
//   4. what leaves DISAPPEARS           — UDP has no goodbye. Without the TTL the viewer would keep
//                                         drawing a player who is gone, which is worse than blank;
//   5. the lease stops the feed         — a viewer that closes must stop costing the zone bandwidth.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "views/viewer_state.h"

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

// Below the ephemeral range on purpose — see the note in `validator_e2e.cpp`.
static const int kHeadPort   = 21621;
static const int kValPort    = 21622;
static const int kSocialPort = 21623;
static const int kZoneUdp    = 21624;

static const float kChunkM = 1000.0f;

static std::atomic<bool> g_done{false};
static std::atomic<int>  g_activeAtNode{-1};   // what the NODE says it is serving
static std::atomic<int>  g_metrics{0};

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
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
        DGS::Command cmd{};
        cmd.chunkSizeX = kChunkM; cmd.chunkSizeY = kChunkM; cmd.chunkSizeZ = kChunkM;
        cmd.port = kZoneUdp;
        std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");
        DGS::Packet p; p.pack(cmd);
        s.send(fd, p.getRawData(), p.getSize());
        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 300000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;
            if (n < 0)  continue;
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() == DGS::PKT_METRICS) {
                g_activeAtNode = (int)r.unpackServerMetrics().activeEntities;
                ++g_metrics;
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
        while (!g_done) { if (s.receive(fd, buf, sizeof(buf)) == 0) break; }
        s.closeClient(fd);
    }
}

/// A player sending its position, exactly as `Client::sendTransform` does: the RAW struct.
///
/// ⚠️ `maxSpeed` HAS TO COVER THE STEPS THIS TEST TAKES. The first version moved 120 m every 150 ms
/// while declaring 5 m/s, and the zone's S1 filter threw every single update out as a teleport — quite
/// correctly. The viewer then showed a frozen position and it looked like a decoding bug, when the
/// picture was right and the test was the one cheating. A viewer test has to move like a player.
static void sendPlayer(DGS::UDPSocket& udp, uint32_t uuid, int32_t cx, float x)
{
    DGS::EntityTransfer e{};
    e.uuid   = uuid;
    e.chunkX = cx; e.chunkY = 50; e.chunkZ = 50;
    e.pos[0] = x;
    e.stats.speed[0] = 200.0f;   // a vehicle: the steps below stay under it
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

/// Throws away whatever is already queued on the socket. @return how many were dropped.
static int drain(DGS::UDPSocket& udp)
{
    int n = 0;
    uint8_t buf[sizeof(DGS::EntityTransfer) * 2];
    std::string from; int port = 0;
    while (udp.receive(buf, sizeof(buf), from, port) > 0) ++n;
    return n;
}

static const char* kObserveToken = "s3cr3t-observe-token";

static void subscribe(DGS::UDPSocket& udp, const char* token = kObserveToken)
{
    DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(token);
    udp.send("127.0.0.1", kZoneUdp, hello.getRawData(), hello.getSize());
}

/// Pumps the viewer's socket for `ms`, feeding everything into the state, as the viewer's thread does.
///
/// ⚠️ IT RE-SUBSCRIBES WHILE IT PUMPS, because that is what the real viewer does — and because the
/// subscription is a LEASE, not a registration. The first version subscribed once at the start and
/// then ran phases lasting longer than the lease: the zone quite properly stopped feeding it halfway
/// through, the position froze, and it read as "the viewer does not decode movement". Staying
/// subscribed has to be an active act in the test too, or the test is not doing what the viewer does.
/// `keepAlive = false` is for the two cases that need the lease to lapse on purpose.
static void pump(DGS::UDPSocket& udp, DGS::ViewerState& st, int ms, bool keepAlive = true)
{
    const auto t0 = std::chrono::steady_clock::now();
    uint64_t lastHello = 0;
    uint8_t buf[sizeof(DGS::EntityTransfer) * 2];
    std::string from; int port = 0;
    while (std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - t0).count() < ms)
    {
        if (keepAlive && nowMs() - lastHello > 400) { lastHello = nowMs(); subscribe(udp); }
        const int n = udp.receive(buf, sizeof(buf), from, port);
        if (n > 0) st.onDatagram(buf, n, nowMs());
        st.expire(nowMs());
    }
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
    std::thread tv(fakeSimple, kValPort, std::ref(v));
    std::thread ts(fakeSimple, kSocialPort, std::ref(so));
    while (!h || !v || !so) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); g_done = true; th.join(); tv.join(); ts.join(); return 1; }
    if (pid == 0) {
        if (!std::getenv("VIEWER_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
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
        setenv("CHUNK_SIZE_X", "1000.0", 1);
        setenv("CHUNK_SIZE_Y", "1000.0", 1);
        setenv("CHUNK_SIZE_Z", "1000.0", 1);
        setenv("ENTITY_LEASE_MS", "1500", 1);
        setenv("OBSERVER_LEASE_MS", "1500", 1);
        setenv("DGS_OBSERVE_TOKEN", kObserveToken, 1);
        setenv("GAME_MODULE_SO", stubPath, 1);
        char tmpl[] = "/tmp/dgs_viewer_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    bool started = false;
    for (int i = 0; i < 300 && !started; ++i) {
        if (g_metrics.load() > 0) started = true;
        else std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    check(started, "the zone is up and reporting to the head");

    DGS::UDPSocket player, viewer;
    player.bind(0);
    viewer.bind(0);
    { timeval tv2{}; tv2.tv_usec = 50000;
      setsockopt(viewer.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv2, sizeof(tv2)); }

    DGS::ViewerState state(kChunkM, /*ttlMs*/ 1200);

    // The viewer knows the zone from the head's list; here it is handed the same thing directly, since
    // `head_routing_e2e` already covers that the head answers PKT_ZONE_LIST correctly.
    DGS::ZoneInfoPublic z{};
    z.chunkXMin = 0; z.chunkXMax = 100;
    z.chunkYMin = 0; z.chunkYMax = 100;
    z.chunkZMin = 0; z.chunkZMax = 100;
    std::snprintf(z.addr, sizeof(z.addr), "127.0.0.1");
    z.port = kZoneUdp;
    state.setZones(&z, 1);

    if (started)
    {
        // ══ (0) NOTHING is shown before subscribing ═══════════════════════════════════════════
        // The counter-proof for (1): if the zone broadcast to anyone who happened to be listening,
        // "the viewer sees the world" would say nothing about the subscription at all.
        for (int i = 0; i < 6; ++i) { sendPlayer(player, 9001, 10, i * 100.0f);
                                      std::this_thread::sleep_for(std::chrono::milliseconds(80)); }
        pump(viewer, state, 500, /*keepAlive*/ false);
        check(state.entityCount() == 0,
              "without subscribing, the viewer receives NOTHING (the feed is not open to all)");

        // ══ (0b) A WRONG TOKEN IS STILL NOTHING ═══════════════════════════════════════════════
        // The observer feed is every entity's position, ten times a second — the exact thing a
        // wallhack wants — and it used to be handed to anyone who sent one byte. Asking with the
        // wrong secret must be worth exactly as much as not asking at all. Phase (1) below, which
        // asks with the RIGHT one and does see the world, is this check's positive control: without
        // it, "sees nothing" would also pass on a zone whose broadcast was simply broken.
        for (int i = 0; i < 4; ++i) { subscribe(viewer, "wrong-token");
                                      sendPlayer(player, 9001, 10, 100.0f);
                                      std::this_thread::sleep_for(std::chrono::milliseconds(80)); }
        pump(viewer, state, 500, /*keepAlive*/ false);
        check(state.entityCount() == 0,
              "subscribing with the WRONG token gets nothing: the feed is authenticated");

        // ══ (1) Subscribing shows the world ═══════════════════════════════════════════════════
        subscribe(viewer);
        for (int i = 0; i < 8; ++i) { sendPlayer(player, 9001, 10, 100.0f);
                                      pump(viewer, state, 120); }
        check(state.entityCount() == 1, "after PKT_OBSERVE the viewer sees the entity that is there");

        auto ents = state.entities();
        check(!ents.empty() && ents[0].uuid == 9001, "with its uuid");
        check(!ents.empty() && !ents[0].ghost, "and as a REAL entity, not a ghost");
        check(!ents.empty() && state.zoneOf(ents[0]) == 0,
              "attributed to the zone that covers its chunk (that is what colours it)");

        const float x0 = ents.empty() ? 0.0f : ents[0].x;
        check(x0 > 10.0f && x0 < 11.0f,
              "at chunk 10 plus its offset inside the chunk (100 m of 1000 -> 10.1)");

        // ══ (2) AND IT PUT NOTHING INTO THE WORLD ═════════════════════════════════════════════
        // Checked against the NODE's own count, over its metrics to the head — not against anything
        // the viewer believes about itself. One player was sent; the node must serve exactly one.
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        check(g_activeAtNode.load() == 1,
              "the node still serves exactly ONE entity: observing does not create a player");

        // ══ (3) Movement is seen MOVING ═══════════════════════════════════════════════════════
        // A viewer that decoded the first datagram and then stopped would pass everything above.
        // 20 m per ~150 ms step = 133 m/s, comfortably under the 200 m/s the player declares, so S1
        // lets it through. In chunk space that is 0.02 per step over a 1000 m chunk.
        float xPrev = x0;
        int   moves = 0;
        for (int i = 1; i <= 6; ++i)
        {
            sendPlayer(player, 9001, 10, 100.0f + i * 20.0f);
            pump(viewer, state, 150);
            const auto e = state.entities();
            if (!e.empty() && e[0].x > xPrev + 0.005f) { ++moves; xPrev = e[0].x; }
        }
        std::printf("    the position advanced %d times · %.3f -> %.3f (chunk space)\n",
                    moves, x0, xPrev);
        check(moves >= 4, "the position ADVANCES as the player moves (it is not a frozen frame)");

        // ══ (3b) THE TICK RATE, pinned ════════════════════════════════════════════════════════
        // The zone promises a 100 ms tick, and for a long time it delivered 4.6 Hz — its per-tick
        // blocking waits stacked to ~220 ms — while its own `performance` metric reported 211 us of
        // work. NOTHING in the node's telemetry could show it; it took an external harness
        // (`tools/load_zone`) to see it, and a player felt it as 142 ms of latency instead of 44.
        // A number that only an external tool can see is a number that will rot, so it is pinned here.
        {
            drain(viewer);
            subscribe(viewer);
            const auto tr0 = std::chrono::steady_clock::now();
            int snapshots = 0;
            uint64_t rxBytes = 0;
            uint8_t rb[sizeof(DGS::EntityTransfer) * 2];
            std::string rfrom; int rport = 0;
            while (std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - tr0).count() < 2000)
            {
                sendPlayer(player, 9001, 10, 220.0f);
                subscribe(viewer);   // the lease is shorter than this window: renew, as the viewer does
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                int r;
                while ((r = viewer.receive(rb, sizeof(rb), rfrom, rport)) > 0)
                    { ++snapshots; rxBytes += (uint64_t)r; }
            }
            const double secs = std::chrono::duration<double>(
                                    std::chrono::steady_clock::now() - tr0).count();
            const double hz = snapshots / secs;
            std::printf("    broadcast rate with ONE entity: %.1f Hz (the tick promises 10)\n", hz);
            check(hz > 8.0,
                  "the zone broadcasts at its nominal 10 Hz, not at whatever its blocking waits allow");

            // ── And what each of those datagrams COSTS ──────────────────────────────────────────
            // Measured at the far end of a real zone, not on a struct in this process. The broadcast
            // used to send `sizeof(EntityTransfer)` — 4160 B, of which 4096 was the empty `data[]` —
            // to every client, every tick, with an N x N fan-out. `wire_test` pins the encoder; this
            // pins the thing that actually leaves the socket, which is what a player's line pays for.
            const double avg = snapshots > 0 ? (double)rxBytes / snapshots : 0.0;
            std::printf("    average datagram from the zone: %.0f B (the raw struct is %zu B)\n",
                        avg, sizeof(DGS::EntityTransfer));
            check(snapshots > 0 && avg < 256.0,
                  "the zone broadcasts the DECLARED payload, not the whole 4160-byte struct");
        }

        // ══ (4) What leaves DISAPPEARS ════════════════════════════════════════════════════════
        // The player stops reporting. Its lease expires at the node, the broadcast stops carrying it,
        // and the viewer's TTL has to drop it — otherwise it draws a player who is no longer there.
        // ⚠️ THE TWO LEASES STACK, and the first version waited for only one of them. The node keeps
        // broadcasting the entity until ITS lease expires (ENTITY_LEASE_MS = 1500), and every one of
        // those broadcasts refreshes the viewer's TTL — so the viewer's 1200 ms clock does not even
        // start until the node has given up. 2200 ms was not enough and the entity was still on screen,
        // which read as "the TTL does not work" when the arithmetic was simply wrong.
        subscribe(viewer);                       // stay subscribed; only the PLAYER goes quiet
        pump(viewer, state, 3400);
        check(state.entityCount() == 0,
              "a player that stops reporting DISAPPEARS from the viewer (it does not linger)");
        check(g_activeAtNode.load() == 0, "and the node stopped serving it too (they agree)");

        // ══ (5) The lease stops the feed ══════════════════════════════════════════════════════
        // The counter-proof for the subscription being a lease: the viewer stops asking, and the zone
        // has to stop sending. Without this a closed window would cost the zone bandwidth for ever.
        subscribe(viewer);
        for (int i = 0; i < 4; ++i) { sendPlayer(player, 9002, 20, 300.0f); pump(viewer, state, 150); }
        // From here on nothing renews the subscription: the lease has to do the rest.
        bool sees9002 = false;
        for (const auto& e : state.entities()) if (e.uuid == 9002) sees9002 = true;
        check(sees9002, "a new entity appears while the subscription is alive");

        // Stop re-subscribing and let the lease run out.
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));   // OBSERVER_LEASE_MS = 1500

        // ⚠️ DRAIN FIRST. While that sleep ran, nobody was reading, so the kernel queued every
        // datagram the zone sent BEFORE the lease expired. Counting without draining measured that
        // backlog and reported the lease as broken — the first version of this check failed for that
        // reason alone. Only what arrives AFTER the queue is empty says anything.
        const int backlog = drain(viewer);

        for (int i = 0; i < 5; ++i) { sendPlayer(player, 9002, 20, 300.0f);
                                      std::this_thread::sleep_for(std::chrono::milliseconds(80)); }
        const int received = drain(viewer);
        std::printf("    backlog from before the lease expired: %d · arriving after it: %d\n",
                    backlog, received);
        check(received == 0,
              "once the lease expires the zone STOPS feeding it (a closed viewer costs nothing)");

        // Counter-proof: subscribing again brings the feed straight back. Without it, "0 datagrams"
        // would be satisfied by a zone that had simply stopped broadcasting to anyone.
        subscribe(viewer);
        for (int i = 0; i < 4; ++i) { sendPlayer(player, 9002, 20, 300.0f);
                                      std::this_thread::sleep_for(std::chrono::milliseconds(120)); }
        const int again = drain(viewer);
        std::printf("    after re-subscribing: %d datagrams\n", again);
        check(again > 0, "and re-subscribing brings it back (the silence was the lease, not a dead zone)");
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    g_done = true;
    th.join(); tv.join(); ts.join();

    std::printf("\n== viewer_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
