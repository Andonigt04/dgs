// ─────────────────────────────────────────────────────────────────────────────────────────────────
// load_zone — how many players does ONE zone actually hold, and what breaks first.
//
// Everything in this project is measured except the one number anybody asks about, so this ramps a
// real `zone_node` with real clients until it stops keeping up, and prints what gave way.
//
// It drives the node through its front door: N UDP sockets, each sending an `EntityTransfer` at 20 Hz
// exactly as `Client::sendTransform` does, each receiving the zone's broadcast. A fake head collects
// the node's own `ServerMetrics`, so the node's view (entities served, bytes out, loop time) can be
// compared against the harness's view (datagrams sent, datagrams received). When the two disagree,
// the disagreement IS the result.
//
// ⚠️ WHAT THIS IS NOT MEASURING. Clients declare a large `maxSpeed` and move in small steps so the S1
// filter never rejects anything: the subject is throughput, not validation. And there is no validator
// running, so the zone is in fail-open — a verdict round trip would add its own cost and is measured
// separately by `net_degraded`.
//
// ⚠️ THE HARNESS MUST PROVE IT KEPT UP. "The zone lost datagrams" and "the harness could not send
// them" look identical from the node's side, so every row prints the send rate actually achieved
// against the 20 Hz per client it was aiming for. If that column sags, the row says nothing about the
// zone and is marked as such.
//
//   load_zone [zone-bin] [stub.so] [max-clients] [seconds-per-step]
//
// ── 1000 PLAYERS, AND TWICE THE LOAD ─────────────────────────────────────────────────────────────
// The default ramp reaches 64 and doubles; to go STRAIGHT to a population, `LOAD_MIN_N`. And "twice
// the load" is two different axes, which load different places, so they are two knobs:
//
//   LOAD_MIN_N=1000 ... 1000                 # 1000 players at 20 Hz
//   LOAD_HZ=40 LOAD_MIN_N=1000 ... 1000      # twice the RATE: doubles what comes IN
//   LOAD_MIN_N=2000 ... 2000                 # twice the PEOPLE: quadruples what goes OUT
//
// ⚠️ AT 1000 NOTHING COMES OF IT WITHOUT TWO MORE SETTINGS, and each has its own knob because each
// is a measured wall, not a preference:
//   · `LOAD_SPREAD_CHUNKS` — in a crowd, 1000 players are 10^6 datagrams per tick (the broadcast is
//     N²). It is not slow: it does not exist. `tests/load_model.cpp` says ~70 chunks are needed.
//   · `LOAD_DRAIN_MAX` — the zone empties its socket up to `ZONE_UDP_DRAIN_MAX` datagrams PER TICK
//     (256 by default), i.e. 2560/s for the whole zone: 128 players at 20 Hz. With the default, 1000
//     clients send eight times what can be drained and the queue only grows.
//
//   LOAD_SPREAD_CHUNKS=100 LOAD_DRAIN_MAX=4096 LOAD_INTEREST_M=500 \
//     LOAD_MIN_N=1000 ./build/load_zone ./build/zone_node ./build/stub_rules.so 1000 10
//
// ⚠️ AND THE DESCRIPTOR LIMIT HAS TO GO UP: 1000 clients are 1000 sockets and the default on almost
// every Linux is 1024, counting the ones already open. This raises it itself (setrlimit) and SAYS how
// far it got; if it cannot get there it trims the population and says so, instead of half failing.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "tests/metric.h"

#include <sys/resource.h>

#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <csignal>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

static const int kHeadPort = 21701;
static const int kValPort  = 21702;
static const int kSocPort  = 21703;
static const int kZoneUdp  = 21704;
static const float kChunkM = 1000.0f;

static std::atomic<bool> g_done{false};

// What the NODE says about itself, straight off its metrics to the head.
static std::atomic<int>      g_activeEntities{0};
static std::atomic<uint64_t> g_bytesTx{0};
static std::atomic<uint64_t> g_bytesRx{0};
static std::atomic<int>      g_loopUs{0};        // ServerMetrics::performance, in microseconds
static std::atomic<int>      g_metricCount{0};

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
        // ⚠️ DIAGNOSTIC MODE, and the reason it exists. The zone's tick measured 4.6 Hz at EVERY
        // population, N=1 included, so the ceiling was not load — it was blocking. Its per-tick waits
        // add up: 100 ms of `usleep` plus a 100 ms receive timeout on the HEAD socket plus 5 + 5 for
        // the validator and social plus 10 for the last empty UDP read = ~220 ms, which is the 4.5 Hz
        // measured. With LOAD_CHATTY_HEAD=1 the head sends something every 20 ms, so that 100 ms wait
        // returns immediately and the difference in tick rate is the cost of the blocking, isolated.
        const bool chatty = std::getenv("LOAD_CHATTY_HEAD") != nullptr;
        std::thread chatter;
        if (chatty) chatter = std::thread([&s, fd]{
            DGS::Packet ping; ping.pack(DGS::PKT_ZONE_LIST);   // the zone ignores it: only the wake matters
            while (!g_done) {
                s.send(fd, ping.getRawData(), ping.getSize());
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        });

        while (!g_done) {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;
            if (n < 0)  continue;
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() == DGS::PKT_METRICS) {
                const auto m = r.unpackServerMetrics();
                g_activeEntities = (int)m.activeEntities;
                g_bytesTx = m.bytesTx;
                g_bytesRx = m.bytesRx;
                g_loopUs  = (int)(m.performance * 1000.0f);   // it is filled in milliseconds
                ++g_metricCount;
            }
        }
        if (chatter.joinable()) chatter.join();
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

struct Client
{
    std::unique_ptr<DGS::UDPSocket> sock;
    uint32_t uuid = 0;
    int32_t  chunkX = 50;   // where in the world this player is (see LOAD_SPREAD_CHUNKS)
    float    x = 0.0f;
    uint64_t recvCount = 0;
    uint64_t recvBytes = 0;
    uint64_t selfCount = 0;   // its OWN entity coming back: one per tick, interest radius or not
};

int main(int argc, char** argv)
{
    std::signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX], absStub[PATH_MAX];
    const char* nodePath = realpath((argc > 1) ? argv[1] : "./build/zone_node", abs)
                           ? abs : "./build/zone_node";
    const char* stubPath = realpath((argc > 2) ? argv[2] : "./build/stub_rules.so", absStub)
                           ? absStub : "./build/stub_rules.so";
    int       maxClients = (argc > 3) ? std::atoi(argv[3]) : 64;
    const int stepSecs   = (argc > 4) ? std::atoi(argv[4]) : 4;
    auto envInt = [](const char* k, int d) { const char* v = std::getenv(k); return v ? std::atoi(v) : d; };
    const double clientHz  = (double)envInt("LOAD_HZ", 20);        // the rate of EACH player
    const int    minN      = envInt("LOAD_MIN_N", 1);              // where the ramp starts
    const int    interestM = envInt("LOAD_INTEREST_M", 0);         // the zone's interest radius
    const int    drainMax  = envInt("LOAD_DRAIN_MAX", 0);          // 0 = the node's default (256)
    // ⚠️ THE SPREAD HAS TO FIT INSIDE THE ZONE'S REGION, and it did not. Clients are placed in chunks
    // 50..50+spread−1 and the zone declared a fixed 0..100: with `LOAD_SPREAD_CHUNKS=300`, two thirds
    // of the players fell OUTSIDE and the node spent its tick trying to hand them off to a neighbour
    // that does not exist — 1643 "out of bounds" in a 10 s run. The row still said "2000 served" and
    // its loop was 35 % higher than the broadcast's, which is what it was believed to be measuring. A
    // harness that measures something else and does not say so is worse than no harness.
    const int    spread    = envInt("LOAD_SPREAD_CHUNKS", 0);
    const bool   verdict   = std::getenv("LOAD_VERDICT") != nullptr;

    // ── File descriptors ────────────────────────────────────────────────────────────────────────
    // One client = one socket. The usual soft limit is 1024 COUNTING what is already open, so at 1000
    // clients it runs out mid-ramp: the `bind` calls start failing and the harness measures a
    // population that is not the one it reports. It is raised to the hard limit (no root needed) and,
    // if that is still not enough, the population is TRIMMED and said out loud — measuring 1000 with
    // 900 sockets is not measuring 1000.
    {
        struct rlimit rl{};
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
            const rlim_t wanted = (rlim_t)maxClients + 128;        // + the harness's own and the node's
            if (rl.rlim_cur < wanted) {
                rl.rlim_cur = (rl.rlim_max == RLIM_INFINITY) ? wanted
                                                             : std::min<rlim_t>(wanted, rl.rlim_max);
                setrlimit(RLIMIT_NOFILE, &rl);
                getrlimit(RLIMIT_NOFILE, &rl);
            }
            std::printf("  descriptors: limit %llu (~%d needed)\n",
                        (unsigned long long)rl.rlim_cur, maxClients + 128);
            if (rl.rlim_cur < wanted) {
                const int fits = (int)rl.rlim_cur - 128;
                std::printf("  ⚠️  not enough: the population is trimmed from %d to %d "
                            "(raise the hard limit with `ulimit -Hn`)\n", maxClients, fits);
                if (fits < 1) return 1;
                maxClients = fits;
            }
        }
        // ⚠️ FLUSH BEFORE THE FORK. This is the first thing printed before `fork()`, and stdio's
        // buffer is COPIED into the child: the child's `freopen` on stdout flushes the inherited copy
        // on its way out, so the line came out twice. Harmless here and not harmless at all in a
        // harness whose whole job is reporting what it saw.
        std::fflush(stdout);
    }

    std::atomic<bool> h{false}, v{false}, so{false};
    std::thread th(fakeHead, std::ref(h));
    std::thread tv(fakeSimple, kValPort, std::ref(v));
    std::thread ts(fakeSimple, kSocPort, std::ref(so));
    while (!h || !v || !so) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pid = fork();
    if (pid < 0) { std::printf("fork failed\n"); return 1; }
    if (pid == 0) {
        std::freopen("/tmp/dgs_load_zone.log", "w", stdout);
        setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
        setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
        setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
        setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
        setenv("VALIDATOR_TCP_PORT", std::to_string(kValPort).c_str(), 1);
        setenv("SOCIAL_HOST",        "127.0.0.1", 1);
        setenv("SOCIAL_TCP_PORT",    std::to_string(kSocPort).c_str(), 1);
        setenv("CHUNK_X_MIN", "0", 1);
        // As far as the spread reaches (clients start at chunk 50), with slack.
        setenv("CHUNK_X_MAX", std::to_string(std::max(100, 50 + spread + 1)).c_str(), 1);
        setenv("CHUNK_Y_MIN", "0", 1); setenv("CHUNK_Y_MAX", "100", 1);
        setenv("CHUNK_Z_MIN", "0", 1); setenv("CHUNK_Z_MAX", "100", 1);
        setenv("CHUNK_SIZE_X", "1000.0", 1);
        setenv("CHUNK_SIZE_Y", "1000.0", 1);
        setenv("CHUNK_SIZE_Z", "1000.0", 1);
        setenv("ENTITY_LEASE_MS", "10000", 1);   // long: nobody must be purged mid-measurement
        setenv("GAME_MODULE_SO", stubPath, 1);
        // The two walls that have to be moved by hand at 1000 players (see the header). If they are
        // not asked for, the node keeps its defaults and the harness measures the default, which is
        // also a result.
        if (interestM > 0) setenv("INTEREST_RADIUS_M", std::to_string(interestM).c_str(), 1);
        if (drainMax  > 0) setenv("ZONE_UDP_DRAIN_MAX", std::to_string(drainMax).c_str(), 1);
        char tmpl[] = "/tmp/dgs_load_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    for (int i = 0; i < 400 && g_metricCount.load() == 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    if (g_metricCount.load() == 0) { std::printf("the zone never reported\n"); kill(pid, SIGTERM); return 1; }

    const size_t E = sizeof(DGS::EntityTransfer);
    std::printf("\n  ONE zone, real clients at 20 Hz, %d s per step.  EntityTransfer = %zu B\n", stepSecs, E);
    std::printf("  The zone broadcasts EVERY entity to EVERY client once per tick, so what each client\n");
    std::printf("  receives is N entities x the tick rate, and the zone's egress grows as N x N.\n\n");
    std::printf("  `snap/s` is the tick rate each client experiences: its own entity echoed back, once\n");
    std::printf("  10 (the 100 ms tick). It falling is the zone losing the ability to keep its clients\n");
    std::printf("  current, which is the thing that matters to a player.\n\n");
    std::printf("  %5s %7s %8s %8s %7s %9s %9s %8s %8s %8s\n",
                "N", "served", "sent/s", "want/s", "snap/s", "MB/s cli", "MB/s out", "lat p50", "lat p95", "loop");
    std::printf("  %5s %7s %8s %8s %7s %9s %9s %8s %8s %8s\n",
                "-----", "------", "-------", "-------", "------", "--------", "--------", "-------", "-------", "-------");

    std::vector<Client> clients;
    int firstBroken = -1;

    // The ramp starts where `LOAD_MIN_N` says (1 by default) and doubles, but the LAST step is
    // exactly `maxClients`: asking for 1000 must measure 1000, not 512 and then nothing.
    for (int n = (minN > 1 ? std::min(minN, maxClients) : 1); ; n = std::min(n * 2, maxClients))
    {
        // Grow the population; existing clients keep their identity so the zone is not rebuilt.
        while ((int)clients.size() < n)
        {
            Client c;
            c.sock = std::make_unique<DGS::UDPSocket>();
            c.sock->bind(0);
            // ⚠️ NON-BLOCKING, not a short timeout. With SO_RCVTIMEO of 1 ms, draining N empty sockets
            // cost N milliseconds per pass, and at N=32 that alone ate more than the 50 ms send period:
            // the harness fell to 448 datagrams/s of the 640 it was aiming for and the row had to be
            // thrown away. An empty read must cost nothing.
            const int fd = c.sock->getSocketFD();
            fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
            // And a big receive buffer, so a drop is the zone's or the kernel's queue, never ours.
            // ⚠️ BUT SHARED OUT AMONG THEM ALL. It was a flat 8 MiB per socket, which is fine with 64
            // clients (512 MiB of ceiling) and a bomb with 1000: on this machine `rmem_max` is 4 MiB,
            // so it would be up to 4 GiB of kernel queue with 6 GiB free — the harness would die of
            // OOM and the result would read "the zone cannot take 1000", the exact opposite of what
            // was measured. With a total budget, 1000 clients get 256 KiB each: a client receives ~13
            // entities of 62 B per tick, so 256 KiB is several seconds of slack.
            const int kRxBudget = 256 * 1024 * 1024;
            const int rcvbuf = std::max(256 * 1024, std::min(8 * 1024 * 1024,
                                        kRxBudget / std::max(1, maxClients)));
            setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
            c.uuid = 5000 + (uint32_t)clients.size();
            // ⚠️ WHERE THE PLAYERS STAND DECIDES WHAT INTEREST MANAGEMENT CAN DO, so it is a knob and
            // both cases get measured. Default 0 = everybody in one chunk, a scrum, which is the
            // scenario the original capacity table used and the one interest management cannot help.
            // LOAD_SPREAD_CHUNKS=N puts them across N chunks, kilometres apart, which is what a world
            // normally looks like.
            c.chunkX = spread > 0 ? (int32_t)(50 + (clients.size() % (size_t)spread)) : 50;
            c.x    = 100.0f + 3.0f * (float)clients.size();
            clients.push_back(std::move(c));
        }

        auto sendOne = [&](Client& c, float tag) {
            DGS::EntityTransfer e{};
            e.uuid   = c.uuid;
            e.type   = DGS::ENT_PLAYER;
            e.chunkX = c.chunkX; e.chunkY = 50; e.chunkZ = 50;
            e.pos[0] = c.x; e.pos[1] = 0.0f; e.pos[2] = 0.0f;
            e.stats.speed[0] = 5000.0f;      // S1 is not the subject: never let it reject a step
            e.stats.baseDMG  = tag;          // echo tag, untouched by the zone and by the rules stub
            DGS::Packet p; p.pack(e);   // the same wire the real client uses
            c.sock->send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
        };

        // Let the zone register everyone before measuring.
        for (int w = 0; w < 40 && g_activeEntities.load() < n; ++w) {
            for (auto& c : clients) sendOne(c, 0.0f);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        for (auto& c : clients) { c.recvCount = 0; c.recvBytes = 0; c.selfCount = 0; }
        const uint64_t txBefore = g_bytesTx.load();

        // ── The measurement window ──────────────────────────────────────────────────────────────
        std::vector<double> latencies;
        uint64_t sent = 0;
        uint8_t  buf[sizeof(DGS::EntityTransfer) * 2];
        std::string from; int port = 0;

        const auto t0 = std::chrono::steady_clock::now();
        uint64_t nextTick = 0;
        while (true)
        {
            const double elapsed = std::chrono::duration<double>(
                                       std::chrono::steady_clock::now() - t0).count();
            if (elapsed >= (double)stepSecs) break;

            // Each client's rate: 20 Hz nominal, `LOAD_HZ` for twice the load.
            if ((uint64_t)(elapsed * clientHz) >= nextTick)
            {
                ++nextTick;
                const float tag = (float)nowMs();
                for (auto& c : clients) { c.x += 0.5f; sendOne(c, tag); ++sent; }
            }

            // Drain everyone. The probe client's echoes give the latency.
            for (size_t i = 0; i < clients.size(); ++i)
            {
                Client& c = clients[i];
                for (int k = 0; k < 64; ++k)
                {
                    const int r = c.sock->receive(buf, sizeof(buf), from, port);
                    if (r <= 0) break;
                    c.recvCount++; c.recvBytes += (uint64_t)r;
                    // ⚠️ `snap/s` USED TO ASSUME EVERY CLIENT GETS EVERY ENTITY — it divided the
                    // datagram count by N twice. That was true when the zone told everybody about
                    // everything; with interest management a spread-out player receives only what is
                    // near them, so the old formula read 0.5 Hz and the harness printed "the zone is
                    // behind" about a zone that was ticking perfectly. A harness that measures a world
                    // the server no longer implements is worse than no harness.
                    //
                    // What is counted now is the client's OWN entity coming back, which is exactly one
                    // datagram per tick per client whatever the interest radius is.
                    if (buf[0] == DGS::PKT_ENTITY_TRANSFER)
                    {
                        DGS::EntityTransfer self{};
                        DGS::Packet sp; sp.setBuffer(buf, (size_t)r);
                        if (sp.tryUnpackEntityTransfer(self) && self.uuid == c.uuid) c.selfCount++;
                    }
                    if (i == 0 && buf[0] == DGS::PKT_ENTITY_TRANSFER)
                    {
                        DGS::EntityTransfer e{};
                        DGS::Packet ep; ep.setBuffer(buf, (size_t)r);
                        if (!ep.tryUnpackEntityTransfer(e)) continue;
                        if (e.uuid == clients[0].uuid && e.stats.baseDMG > 0.0f)
                        {
                            const double lat = (double)nowMs() - (double)e.stats.baseDMG;
                            if (lat >= 0.0 && lat < 60000.0) latencies.push_back(lat);
                        }
                    }
                }
            }
        }
        const double secs = std::chrono::duration<double>(
                                std::chrono::steady_clock::now() - t0).count();

        uint64_t rxBytes = 0, rxCount = 0;
        uint64_t selfTotal = 0;
        for (const auto& c : clients) { rxBytes += c.recvBytes; rxCount += c.recvCount; selfTotal += c.selfCount; }

        const double sentPerSec = (double)sent / secs;
        const double wantPerSec = clientHz * (double)n;
        const double measMBs    = (double)rxBytes / secs / 1e6;
        const double perCliMBs  = measMBs / (double)n;
        // Ticks per second as each client experiences them: its own entity echoed back, once per
        // tick. Independent of how many OTHER entities it is told about.
        const double snapsPerSec = (double)selfTotal / secs / (double)n;

        std::sort(latencies.begin(), latencies.end());
        const double p50 = latencies.empty() ? -1.0 : latencies[latencies.size() * 50 / 100];
        const double p95 = latencies.empty() ? -1.0
                          : latencies[std::min(latencies.size() - 1, latencies.size() * 95 / 100)];

        const int served = g_activeEntities.load();
        const bool harnessKeptUp = sentPerSec > wantPerSec * 0.9;
        // Broken = it stopped serving everyone, or clients get fewer than half the nominal snapshots,
        // or the echo latency passed a third of a second, which is where a player feels it.
        const bool zoneKeptUp = served >= n && snapsPerSec > 5.0 && p95 < 333.0;
        if (!zoneKeptUp && firstBroken < 0 && harnessKeptUp) firstBroken = n;

        std::printf("  %5d %7d %8.0f %8.0f %7.1f %9.3f %9.2f %8.0f %8.0f %7dus%s\n",
                    n, served, sentPerSec, wantPerSec, snapsPerSec, perCliMBs, measMBs,
                    p50, p95, g_loopUs.load(),
                    harnessKeptUp ? (zoneKeptUp ? "" : "   <- the zone is behind")
                                  : "   <- THE HARNESS is behind: row invalid");
        std::fflush(stdout);

        // EVERY step's figures, to the metric channel: that way a run of this reaches the job's
        // summary (or wherever it is collected) without anybody reading the table by eye. The key
        // carries the population, which is what tells one row from another.
        {
            char key[80];
            auto pub = [&](const char* what, double v, const char* unit) {
                std::snprintf(key, sizeof key, "%s_n%d", what, n);
                dgsMetric(key, v, unit);
            };
            pub("served", (double)served, "entities");
            pub("snapshots", snapsPerSec, "Hz");
            pub("egress", measMBs, "MB/s");
            pub("latency_p95", p95, "ms");
            pub("loop", (double)g_loopUs.load() / 1000.0, "ms");
            pub("harness_send", sentPerSec, "dg/s");
        }

        (void)txBefore;
        if (n >= maxClients) break;
    }

    std::printf("\n");
    if (firstBroken > 0)
        std::printf("  first population where the zone stops keeping up: N = %d\n", firstBroken);
    else
        std::printf("  the zone kept up to N = %d (nothing broke inside the range tested)\n", maxClients);
    std::printf("  the node's log is in /tmp/dgs_load_zone.log\n");

    // ── THE VERDICT (LOAD_VERDICT=1), for when this runs as a test ──────────────────────────────
    // ⚠️ IT DOES NOT FAIL BECAUSE THE ZONE IS LATE. At 1000 players it is KNOWN to be late — the model
    // says so and the measured table says so — and demanding the nominal tick would be a test born
    // broken, the kind that ends up commented out. What must stay true at any population, and is what
    // is watched here: that the node STAYS ALIVE and keeps serving everybody. Losing entities, dying
    // or going unresponsive are regressions; being slow is a figure.
    const int servedFinal = g_activeEntities.load();
    int nodeStatus = 0;
    const bool nodeAlive = (waitpid(pid, &nodeStatus, WNOHANG) == 0);
    bool bad = false;
    if (verdict) {
        std::printf("\n  verdict: the node %s · serving %d of %d\n",
                    nodeAlive ? "is still alive" : "HAS DIED", servedFinal, maxClients);
        if (!nodeAlive)                  { std::printf("  [FAIL] the zone_node did not survive the load\n"); bad = true; }
        else if (servedFinal < maxClients) { std::printf("  [FAIL] the zone stopped serving %d entities\n",
                                                         maxClients - servedFinal); bad = true; }
        else                               std::printf("  [ok]   the zone holds %d players without losing one\n",
                                                       maxClients);
        std::printf("\n== load_zone: %d OK · %d FAILED ==\n", bad ? 0 : 1, bad ? 1 : 0);
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    g_done = true;
    th.join(); tv.join(); ts.join();
    return bad ? 1 : 0;
}
