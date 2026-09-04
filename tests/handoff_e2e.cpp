// ─────────────────────────────────────────────────────────────────────────────────────────────────
// handoff_e2e — an entity crossing a border must not be able to VANISH.
//
// The handoff was fire-and-forget. `checkAndTransfer` wrote the reassign and the state to the head and
// then erased the entity UNCONDITIONALLY, ignoring both `send` results. Three ways for an entity to
// stop existing anywhere at all:
//
//   · the head is down or reconnecting → both writes fail, and the entity is erased anyway;
//   · the head cannot route that chunk (`targetFD == -1`) → it drops the reassign right there, in
//     silence, and nobody is told;
//   · the forward to the new owner fails → same silence.
//
// Afterwards no zone owned it, no counter had moved, and the last thing in the log was
// "Transferring...". This is not a UDP problem — that link is TCP and always was. It is an ignored
// error and a missing acknowledgement.
//
// So the protocol got an ack (`EntityReassign.ack`) and the zone holds the entity until the head says
// it landed. What the three phases here check, and why each is the others' counter-proof:
//
//   A. ack = 1  → the zone RELEASES it. Without this, "the entity stays" would also pass on a zone
//                 that simply never hands anything off, which is not a fix, it is a different bug.
//   B. ack = 2  → the head could not route the chunk, so the zone KEEPS it. This is the case that used
//                 to lose the entity while every socket call reported success.
//   C. no ack   → the head is gone mid-handoff. The entity has to still be there when it comes back.
//
// Phase C is watched through the zone's own broadcast rather than its metrics, because the metrics go
// to the head and the head is the thing that is down.
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
#include <vector>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);
}

// Below the ephemeral range on purpose — see the note in `validator_e2e.cpp`.
static const int kHeadPort = 21651;
static const int kZoneUdp  = 21652;

static const float kChunkM = 1000.0f;
static const char* kToken  = "handoff-e2e-token";
static const uint32_t kUuid = 8100;

static std::atomic<bool> g_done{false};
static std::atomic<int>  g_activeAtNode{-1};
static std::atomic<int>  g_metrics{0};
static std::atomic<int>  g_reassignsSeen{0};
static std::atomic<int>  g_ackMode{1};     // what the fake head answers: 1 = routed, 2 = no owner
static std::atomic<bool> g_headAlive{true};

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/// A head that answers reassigns the way the real one does, and that can be told to die.
static void fakeHead(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kHeadPort)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;

    while (!g_done)
    {
        if (!g_headAlive) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }

        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }

        DGS::Command cmd{};
        cmd.chunkSizeX = kChunkM; cmd.chunkSizeY = kChunkM; cmd.chunkSizeZ = kChunkM;
        cmd.port = kZoneUdp;
        std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");
        DGS::Packet p; p.pack(cmd);
        s.send(fd, p.getRawData(), p.getSize());

        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 200000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        while (!g_done && g_headAlive)
        {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;
            if (n < 0)  continue;
            DGS::Packet r; r.setBuffer(buf, n);

            if (r.getType() == DGS::PKT_METRICS)
            {
                g_activeAtNode = (int)r.unpackServerMetrics().activeEntities;
                ++g_metrics;
            }
            else if (r.getType() == DGS::PKT_REASSIGN)
            {
                auto ra = r.unpackEntityReassign();
                if (ra.ack != 0) continue;          // our own answer bouncing back: ignore
                ++g_reassignsSeen;
                DGS::EntityReassign answer = ra;
                answer.ack = (uint8_t)g_ackMode.load();
                DGS::Packet pa; pa.pack(answer);
                s.send(fd, pa.getRawData(), pa.getSize());
            }
        }
        s.closeClient(fd);
    }
}

static void sendPlayer(DGS::UDPSocket& udp, int32_t cx, float x)
{
    DGS::EntityTransfer e{};
    e.uuid   = kUuid;
    e.type   = DGS::ENT_PLAYER;
    e.chunkX = cx; e.chunkY = 0; e.chunkZ = 0;
    e.pos[0] = x;
    e.stats.speed[0] = 100000.0f;   // S1 is not the subject here: never let it reject a step
    e.stats.health   = 55.0f;
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

static void subscribe(DGS::UDPSocket& udp)
{
    DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(kToken);
    udp.send("127.0.0.1", kZoneUdp, hello.getRawData(), hello.getSize());
}

/// @return how many distinct entities the zone is STILL broadcasting at the end of `ms`.
///
/// ⚠️ THE TTL AND THE DRAIN ARE BOTH LOAD-BEARING. The first version used the viewer's default 5 s TTL
/// and did not drain first, so a single datagram received at the start of the window kept the count at
/// 1 for the whole window — an entity that had been erased a moment later still read as present. The
/// counter-proof exposed it: with the fix reverted, phase C stayed GREEN. It was measuring "was it
/// there at some point", not "is it there now", which is the whole question.
static size_t watch(DGS::UDPSocket& udp, int ms)
{
    uint8_t buf[8192];
    std::string from; int port = 0;
    while (udp.receive(buf, sizeof(buf), from, port) > 0) {}   // discard anything already queued

    DGS::ViewerState st(kChunkM, /*ttlMs*/ 400);
    const uint64_t until = nowMs() + (uint64_t)ms;
    while (nowMs() < until)
    {
        subscribe(udp);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        int n;
        while ((n = udp.receive(buf, sizeof(buf), from, port)) > 0) st.onDatagram(buf, n, nowMs());
        st.expire(nowMs());
    }
    return st.entityCount();
}

static bool waitMetric(int want, int msLimit)
{
    const uint64_t until = nowMs() + (uint64_t)msLimit;
    while (nowMs() < until) {
        if (g_activeAtNode.load() == want) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char absZone[PATH_MAX];
    const char* zonePath = realpath((argc > 1) ? argv[1] : "./build/zone_node", absZone)
                           ? absZone : "./build/zone_node";

    std::atomic<bool> headReady{false};
    std::thread head(fakeHead, std::ref(headReady));
    while (!headReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    std::fflush(stdout);
    const pid_t zone = fork();
    if (zone < 0) { std::printf("[FAIL] fork\n"); g_done = true; head.join(); return 1; }
    if (zone == 0) {
        if (!std::getenv("HANDOFF_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
        setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
        setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
        setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
        setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
        setenv("VALIDATOR_TCP_PORT", "21659", 1);
        setenv("SOCIAL_HOST",        "127.0.0.1", 1);
        setenv("SOCIAL_TCP_PORT",    "21660", 1);
        setenv("PERSISTENCE_HOST",   "127.0.0.1", 1);   // resolvable and refused: no DNS stall
        setenv("PERSISTENCE_PORT",   "21661", 1);
        setenv("CHUNK_X_MIN", "0", 1);  setenv("CHUNK_X_MAX", "10", 1);
        setenv("CHUNK_Y_MIN", "0", 1);  setenv("CHUNK_Y_MAX", "10", 1);
        setenv("CHUNK_Z_MIN", "0", 1);  setenv("CHUNK_Z_MAX", "10", 1);
        setenv("CHUNK_SIZE_X", "1000.0", 1);
        setenv("CHUNK_SIZE_Y", "1000.0", 1);
        setenv("CHUNK_SIZE_Z", "1000.0", 1);
        setenv("ENTITY_LEASE_MS", "60000", 1);   // long: the GC is not the subject
        setenv("HANDOFF_RETRY_MS", "300", 1);
        setenv("ZONE_PERSIST_MS", "0", 1);       // persistence is not the subject either
        setenv("DGS_OBSERVE_TOKEN", kToken, 1);
        setenv("GAME_MODULE_SO", "", 1);
        char tmpl[] = "/tmp/dgs_handoff_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(zonePath, zonePath, (char*)nullptr);
        _exit(127);
    }

    DGS::UDPSocket player, viewer;
    player.bind(0); viewer.bind(0);
    { timeval tv{}; tv.tv_usec = 50000;
      setsockopt(player.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(viewer.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

    auto feedInside  = [&]{ for (int i = 0; i < 8; ++i) {
                                sendPlayer(player, 5, 100.0f + i);
                                std::this_thread::sleep_for(std::chrono::milliseconds(80)); } };
    auto pushOutside = [&](int rounds){ for (int i = 0; i < rounds; ++i) {
                                sendPlayer(player, 50, 100.0f + i);
                                std::this_thread::sleep_for(std::chrono::milliseconds(100)); } };

    // ══ A. The head routes it: the zone must LET GO ══════════════════════════════════════════════
    g_ackMode = 1;
    feedInside();
    check(waitMetric(1, 4000), "the zone serves the player while it is inside its region");

    const int reassignsBefore = g_reassignsSeen.load();
    pushOutside(6);
    const bool released = waitMetric(0, 5000);
    std::printf("    ack=1 (routed): reassigns seen by the head %d, zone now serving %d\n",
                g_reassignsSeen.load() - reassignsBefore, g_activeAtNode.load());
    check(g_reassignsSeen.load() > reassignsBefore, "crossing the border DOES ask the head to reassign");
    check(released, "and once the head confirms it is routed, the zone releases the entity");

    // ══ B. The head cannot route it: the zone must KEEP it ═══════════════════════════════════════
    // This is the case that used to lose the entity while every socket call reported success: the head
    // drops an unroutable reassign in silence and the ceding zone had already erased it.
    g_ackMode = 2;
    feedInside();
    check(waitMetric(1, 4000), "a second player is served from inside the region");

    pushOutside(8);
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    std::printf("    ack=2 (no zone covers that chunk): zone serving %d\n", g_activeAtNode.load());
    check(g_activeAtNode.load() == 1,
          "told nobody can take it, the zone KEEPS the entity instead of erasing it into nothing");

    // ══ C. The head disappears mid-handoff ═══════════════════════════════════════════════════════
    // No answer at all. Watched through the zone's own broadcast, because the metrics channel is the
    // very thing that is down.
    g_headAlive = false;
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    pushOutside(8);
    const size_t stillThere = watch(viewer, 1200);
    std::printf("    head down mid-handoff: the zone is still broadcasting %zu entities\n", stillThere);
    check(stillThere == 1,
          "with the head gone the entity SURVIVES (it used to be erased into nobody's hands)");

    // ...and when the head comes back and can route it, the handoff completes.
    g_ackMode = 1;
    g_headAlive = true;
    g_activeAtNode = -1;
    const bool completed = waitMetric(0, 12000);
    std::printf("    head back: zone serving %d\n", g_activeAtNode.load());
    check(completed, "and once the head is back the retry completes the handoff (at-least-once)");

    kill(zone, SIGTERM);
    waitpid(zone, nullptr, 0);
    g_done = true;
    g_headAlive = true;
    head.join();

    std::printf("\n== handoff_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
