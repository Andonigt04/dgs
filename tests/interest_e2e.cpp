// ─────────────────────────────────────────────────────────────────────────────────────────────────
// interest_e2e — a player is told about what is NEAR them.
//
// Every client used to be sent every entity, so a zone's egress grew as N x N. Measured with
// `tools/load_zone`: at 64 players that was 175 MB/s out of one zone and 21 Mbit/s down PER CLIENT
// before `dataSize` was honoured, and after it the wall moved from bytes to sheer datagram count —
// about 7 us of `sendto` each, 16384 of them per tick at 128 players, which put the tick 14 % over its
// budget. Both walls are the same mistake: telling everybody about everything.
//
// With a 500 m radius and players spread over a world, the same ramp reaches **256 players at nominal
// tick with 13 ms of loop**, where it used to break at 128.
//
// SIX CASES, because any one of them alone is satisfied by something broken:
//   A. far apart, radius set   → A must NOT be told about B.
//   B. far apart, radius 0     → A MUST be told about B. Without this, (A) also passes on a zone that
//                                broadcasts nothing at all, which is not interest management.
//   C. close together, radius set → A MUST be told about B. Without this, (A) also passes on a zone
//                                that only ever sends you yourself.
//   D. an OBSERVER sees both in every case. A viewer exists to see the whole zone; one that inherited
//      a player's interest radius would be a viewer that lies about the world.
//
//   E. the same with the UDP plane ENCRYPTED (`DGS_UDP_KEY`): a player and a viewer holding the key
//      still receive the world. The zone seals each frame ONCE and sends the same bytes to everybody,
//      which is what keeps the fan-out affordable — sealing per recipient cost +37 % of loop time.
//   F. and a listener WITHOUT the key gets nothing usable, so (E) is not just "it still works".
//
// And in every case A must be told about ITSELF: a player who stopped being told where they are is
// watching somebody else's world.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

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
#include <set>
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
static const int kHeadPort = 21691;
static const int kZoneUdp  = 21692;

static const float    kChunkM = 1000.0f;
static const char*    kToken  = "interest-e2e-token";
static const uint32_t kA = 6001, kB = 6002;

static std::atomic<bool> g_done{false};
static std::atomic<int>  g_active{-1};

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
            if (r.getType() == DGS::PKT_METRICS)
                g_active = (int)r.unpackServerMetrics().activeEntities;
        }
        s.closeClient(fd);
    }
}

static const char* g_udpKey = nullptr;   // set for the encrypted phase

static pid_t spawnZone(const char* path, const char* radius)
{
    std::fflush(stdout);
    const pid_t p = fork();
    if (p != 0) return p;
    if (!std::getenv("INTEREST_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
    setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
    setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
    setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
    setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
    setenv("VALIDATOR_TCP_PORT", "21698", 1);
    setenv("SOCIAL_HOST",        "127.0.0.1", 1);
    setenv("SOCIAL_TCP_PORT",    "21699", 1);
    setenv("PERSISTENCE_HOST",   "127.0.0.1", 1);
    setenv("PERSISTENCE_PORT",   "21697", 1);
    setenv("CHUNK_X_MIN", "0", 1); setenv("CHUNK_X_MAX", "100", 1);
    setenv("CHUNK_Y_MIN", "0", 1); setenv("CHUNK_Y_MAX", "100", 1);
    setenv("CHUNK_Z_MIN", "0", 1); setenv("CHUNK_Z_MAX", "100", 1);
    setenv("CHUNK_SIZE_X", "1000.0", 1);
    setenv("CHUNK_SIZE_Y", "1000.0", 1);
    setenv("CHUNK_SIZE_Z", "1000.0", 1);
    setenv("ENTITY_LEASE_MS", "60000", 1);
    setenv("ZONE_PERSIST_MS", "0", 1);
    setenv("INTEREST_RADIUS_M", radius, 1);
    setenv("DGS_OBSERVE_TOKEN", kToken, 1);
    setenv("GAME_MODULE_SO", "", 1);
    if (g_udpKey) setenv("DGS_UDP_KEY", g_udpKey, 1); else unsetenv("DGS_UDP_KEY");
    char tmpl[] = "/tmp/dgs_interest_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(path, path, (char*)nullptr);
    _exit(127);
}

static void stop(pid_t pid) { if (pid > 0) { kill(pid, SIGTERM); waitpid(pid, nullptr, 0); } }

static void send(DGS::UDPSocket& udp, uint32_t uuid, int32_t chunkX, float x)
{
    DGS::EntityTransfer e{};
    e.uuid = uuid; e.type = DGS::ENT_PLAYER;
    e.chunkX = chunkX; e.chunkY = 50; e.chunkZ = 50;
    e.pos[0] = x;
    e.stats.speed[0] = 100000.0f;   // S1 is not the subject
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

static void subscribe(DGS::UDPSocket& udp)
{
    DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(kToken);
    udp.send("127.0.0.1", kZoneUdp, hello.getRawData(), hello.getSize());
}

/// Which entity uuids this socket is told about over `ms`.
static std::set<uint32_t> heard(DGS::UDPSocket& udp, int ms)
{
    std::set<uint32_t> ids;
    uint8_t buf[8192];
    std::string from; int port = 0;
    const uint64_t until = nowMs() + (uint64_t)ms;
    while (nowMs() < until)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        int n;
        while ((n = udp.receive(buf, sizeof(buf), from, port)) > 0)
        {
            if (buf[0] != DGS::PKT_ENTITY_TRANSFER) continue;
            DGS::EntityTransfer e{};
            DGS::Packet p; p.setBuffer(buf, (size_t)n);
            if (p.tryUnpackEntityTransfer(e)) ids.insert(e.uuid);
        }
    }
    return ids;
}

/// Runs one scenario. @return what A heard, and (through `obsIds`) what an observer heard.
static std::set<uint32_t> scenario(const char* zonePath, const char* radius,
                                   int32_t chunkB, std::set<uint32_t>& obsIds)
{
    pid_t zone = spawnZone(zonePath, radius);

    DGS::UDPSocket a, b, obs;
    a.bind(0); b.bind(0); obs.bind(0);
    for (DGS::UDPSocket* s : { &a, &b, &obs }) {
        timeval tv{}; tv.tv_usec = 30000;
        setsockopt(s->getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    // Both players report until the node serves two entities.
    for (int i = 0; i < 60 && g_active.load() != 2; ++i) {
        send(a, kA, 50, 100.0f);
        send(b, kB, chunkB, 100.0f);
        subscribe(obs);
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
    }

    // Keep reporting while listening: an entity that stopped reporting would be purged, and "A was not
    // told about B" would then be true for the wrong reason.
    std::thread keep([&]{
        const uint64_t until = nowMs() + 1400;
        while (nowMs() < until) {
            send(a, kA, 50, 100.0f);
            send(b, kB, chunkB, 100.0f);
            subscribe(obs);
            std::this_thread::sleep_for(std::chrono::milliseconds(60));
        }
    });
    std::set<uint32_t> aIds = heard(a, 1200);
    obsIds = heard(obs, 200);
    keep.join();
    { std::set<uint32_t> more = heard(obs, 400); obsIds.insert(more.begin(), more.end()); }

    stop(zone);
    g_active = -1;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    return aIds;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);
    char abs[PATH_MAX];
    const char* zonePath = realpath((argc > 1) ? argv[1] : "./build/zone_node", abs)
                           ? abs : "./build/zone_node";

    std::atomic<bool> ready{false};
    std::thread head(fakeHead, std::ref(ready));
    while (!ready) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // ── A. Far apart (20 km), 500 m radius ──────────────────────────────────────────────────────
    {
        std::set<uint32_t> obs;
        const std::set<uint32_t> a = scenario(zonePath, "500", 70, obs);
        std::printf("    far apart, radius 500: A heard %zu entities, observer heard %zu\n",
                    a.size(), obs.size());
        check(a.count(kA) == 1, "A is always told about ITSELF");
        check(a.count(kB) == 0, "A · 20 km away, A is NOT told about B");
        check(obs.count(kA) && obs.count(kB),
              "D · an OBSERVER still sees both (a viewer is not given a player's blinkers)");
    }

    // ── B. Far apart, no radius: the counter-proof for (A) ──────────────────────────────────────
    {
        std::set<uint32_t> obs;
        const std::set<uint32_t> a = scenario(zonePath, "0", 70, obs);
        std::printf("    far apart, radius 0:   A heard %zu entities\n", a.size());
        check(a.count(kA) && a.count(kB) == 1,
              "B · with no radius A IS told about B (the zone was not simply silent)");
    }

    // ── C. Close together, 500 m radius: the counter-proof for the filter itself ────────────────
    {
        std::set<uint32_t> obs;
        const std::set<uint32_t> a = scenario(zonePath, "500", 50, obs);
        std::printf("    same chunk, radius 500: A heard %zu entities\n", a.size());
        check(a.count(kA) && a.count(kB) == 1,
              "C · in the same place A IS told about B (it does not just send you yourself)");
    }

    // ── D. The same, with the UDP game plane ENCRYPTED ──────────────────────────────────────────
    // The broadcast is sealed with AES-256-GCM now (`DGS_UDP_KEY`), and the zone seals each frame ONCE
    // and sends the same bytes to every recipient. That is the property worth pinning: if it ever
    // regressed to sealing per recipient the numbers would still look right and the cost would not.
    // A player and a viewer that hold the key must see exactly what they saw in clear.
    {
        g_udpKey = "interest-e2e-udp-key";
        setenv("DGS_UDP_KEY", g_udpKey, 1);          // for this process's own sockets too
        std::set<uint32_t> obs;
        const std::set<uint32_t> a = scenario(zonePath, "0", 70, obs);
        std::printf("    encrypted UDP, radius 0: A heard %zu entities, observer heard %zu\n",
                    a.size(), obs.size());
        check(a.count(kA) && a.count(kB) == 1,
              "D · with the UDP plane ENCRYPTED a player still receives the world");
        check(obs.count(kA) && obs.count(kB),
              "D · and so does an observer holding the key");
    }

    // ── E. A viewer WITHOUT the key gets nothing it can use ─────────────────────────────────────
    // The counter-proof for (D): without it, "the observer saw both" would not tell us the traffic was
    // encrypted at all.
    {
        g_udpKey = "interest-e2e-udp-key";
        unsetenv("DGS_UDP_KEY");                     // the ZONE has it; this process does not
        std::set<uint32_t> obs;
        const std::set<uint32_t> a = scenario(zonePath, "0", 70, obs);
        std::printf("    without the key: A heard %zu, observer heard %zu\n", a.size(), obs.size());
        check(a.empty() && obs.empty(),
              "E · without the key a listener gets nothing usable (it really was encrypted)");
        g_udpKey = nullptr;
    }

    g_done = true;
    head.join();

    std::printf("\n== interest_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
