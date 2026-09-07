// ─────────────────────────────────────────────────────────────────────────────────────────────────
// fill_world — N players that populate a cluster that is ALREADY RUNNING.
//
// `load_zone` cannot do this. It stands up its own zone, its own head and its own validator, measures
// them and tears the lot down: it is a benchmark, and a benchmark that owns the world it measures is
// no use when what you want is to put sixty people into a cluster you are already looking at.
//
// This is the other half: it starts nothing, spawns nothing, and talks to a live head exactly the way
// a client does.
//
// ⚠️ IT ASKS THE HEAD WHICH ZONE TO TALK TO, and that is the whole reason it is worth writing rather
// than pointing sixty sockets at one UDP port. A cluster with two zone_nodes only shows what it is for
// when somebody CROSSES between them: the entity is handed over, the new owner promotes it and starts
// simulating, the old one drops its lease. A filler hard-wired to one port cannot cross anything, so
// it would populate the demo and hide the interesting part of it. Here every player announces the
// border to the zone it is leaving and then re-queries the head, exactly as `Client::sendTransform`
// does — so `FILL_CROSSERS` walks real players across a real border and the handoff is real.
//
// That order is not a detail: getting it backwards here produced 20 seconds of crossings and NOT ONE
// handoff, which is how the same bug was found in the real client.
//
// ⚠️ WHAT IT IS NOT. These are not the game: no rendering, no input, no prediction, no login. They are
// transforms on the wire at a fixed rate, which is what the zone sees of any player anyway. `speed[0]`
// is declared far above the speed they actually move at, so the validator's S1 filter never rejects a
// step — the subject here is a populated world, not anti-cheat, and a filler that got itself banned
// mid-recording would be its own kind of comedy.
//
//   fill_world [count] [seconds]        seconds = 0 -> until Ctrl+C
//
//   HEAD_SERVER_HOST / HEAD_SERVER_PORT   who to ask (default 127.0.0.1:42424)
//   FILL_SPREAD_CHUNKS   8      how many chunks to spread them across (1 = all in one scrum)
//   FILL_CHUNK_BASE      50     the first chunk on X
//   FILL_CHUNK_BASE_Y/_Z (=X)   the chunk on Y and Z. A real game does NOT spawn on the
//                               diagonal, so leaving these at the X base puts the crowd
//                               millions of metres away from the player
//   FILL_CROSSERS        0      how many of them walk across chunk borders, for the handoff
//   FILL_HZ              20     transforms per second per client, as the real client sends
//   FILL_SPEED_MPS       12     how fast the patrolling ones move
//   FILL_CROSS_SPEED_MPS 60     how fast the crossers move (a vehicle: a border every ~17 s)
//   FILL_CROSS_START_M   100    how far from the border a crosser starts (the first handoff)
//   FILL_PATROL_M        300    radius of the circle they walk
//   FILL_UUID_BASE       7000   so they cannot collide with your own two players
//   FILL_ANNOUNCE_MS     500    how long a crosser keeps reporting to the zone it is leaving
//   CHUNK_SIZE_M         1000   MUST match the cluster's CHUNK_SIZE_X
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
#include <fcntl.h>
#include <csignal>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

static uint64_t nowMs()
{
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static std::atomic<bool> g_stop{false};
static void onSignal(int) { g_stop = true; }

static int envInt(const char* n, int d)
{
    const char* v = std::getenv(n);
    return (v && *v) ? std::atoi(v) : d;
}
static float envFloat(const char* n, float d)
{
    const char* v = std::getenv(n);
    return (v && *v) ? (float)std::atof(v) : d;
}

struct Filler
{
    std::unique_ptr<DGS::UDPSocket> sock;
    uint32_t uuid   = 0;
    int32_t  chunkX = 0, chunkY = 0, chunkZ = 0;
    float    x = 0.0f, z = 0.0f;      // metres inside the chunk
    float    phase = 0.0f;            // where it is around its patrol circle
    bool     crosser = false;
    int      dir = 1;                 // crossers ping-pong instead of teleporting back
    uint64_t announceUntil = 0;       // keep talking to the OLD zone until this passes
    std::string zoneAddr;
    int      zonePort = 0;
    uint64_t recvCount = 0;
    int      queries = 0;
};

/// One TCP link to the head, shared. A zone query is a request/response, so it is serialised — and
/// they are RARE: a client only asks when it changes chunk. Sixty sockets to the head would be sixty
/// connections the head has to hold for something that happens once a minute per client.
struct HeadLink
{
    DGS::TCPSocket sock;
    std::mutex     mtx;
    bool           up = false;
};

static bool queryZone(HeadLink& head, Filler& f)
{
    std::lock_guard<std::mutex> g(head.mtx);
    if (!head.up) return false;

    DGS::ZoneQuery q{};
    q.uuid   = f.uuid;
    q.chunkX = f.chunkX; q.chunkY = f.chunkY; q.chunkZ = f.chunkZ;
    DGS::Packet p; p.pack(q);
    if (!head.sock.send(head.sock.getSocketFD(), p.getRawData(), p.getSize())) { head.up = false; return false; }

    uint8_t buf[512];
    const int n = head.sock.receive(head.sock.getSocketFD(), buf, sizeof(buf));
    if (n <= 0) { if (n == 0) head.up = false; return false; }

    DGS::Packet r; r.setBuffer(buf, (size_t)n);
    if (r.getType() != DGS::PKT_ZONE_RESPONSE) return false;

    const DGS::ZoneResponse zr = r.unpackZoneResponse();
    if (zr.port <= 0 || zr.addr[0] == '\0') return false;      // no zone covers that chunk
    f.zoneAddr = zr.addr;
    f.zonePort = zr.port;
    ++f.queries;
    return true;
}

int main(int argc, char** argv)
{
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT,  onSignal);
    std::signal(SIGTERM, onSignal);

    const int count   = (argc > 1) ? std::atoi(argv[1]) : 32;
    const int seconds = (argc > 2) ? std::atoi(argv[2]) : 0;

    const std::string headHost = std::getenv("HEAD_SERVER_HOST") ? std::getenv("HEAD_SERVER_HOST")
                                                                 : "127.0.0.1";
    const int   headPort  = envInt("HEAD_SERVER_PORT", 42424);
    const int   spread    = std::max(1, envInt("FILL_SPREAD_CHUNKS", 8));
    const int   base      = envInt("FILL_CHUNK_BASE", 50);
    // ⚠️ Y y Z TIENEN SU PROPIA BASE, y antes valian la de X. Con `FILL_CHUNK_BASE=146089` la multitud
    // acababa en el chunk (146089, 146089, 146089) mientras un juego real aparece en (146089, 3289,
    // 4190): ciento cuarenta y dos millones de metros de distancia. El cluster funcionaba, el cliente
    // recibia cero, y no habia nada que mirar porque nadie estaba cerca de nadie. Un juego NO aparece
    // en la diagonal. Su propio log dice donde esta: divide sus tres coordenadas por el tamano de chunk.
    const int   baseY     = envInt("FILL_CHUNK_BASE_Y", base);
    const int   baseZ     = envInt("FILL_CHUNK_BASE_Z", base);
    const int   crossers  = envInt("FILL_CROSSERS", 0);
    const int   hz        = std::max(1, envInt("FILL_HZ", 20));
    const float speed     = envFloat("FILL_SPEED_MPS", 12.0f);
    const float crossSpeed= envFloat("FILL_CROSS_SPEED_MPS", 60.0f);   // a vehicle, so a border comes soon
    const float patrol    = envFloat("FILL_PATROL_M", 300.0f);
    const uint32_t uuidB  = (uint32_t)envInt("FILL_UUID_BASE", 7000);
    const float chunkM    = envFloat("CHUNK_SIZE_M", 1000.0f);
    const int   announceMs= envInt("FILL_ANNOUNCE_MS", 500);

    if (count <= 0) { std::printf("nothing to do: count = %d\n", count); return 1; }

    HeadLink head;
    if (!head.sock.connect(headHost, headPort, 3000))
    {
        std::printf("no head at %s:%d — start the cluster first\n", headHost.c_str(), headPort);
        return 1;
    }
    { timeval tv{}; tv.tv_sec = 2;
      setsockopt(head.sock.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    head.up = true;

    std::vector<Filler> fillers;
    fillers.reserve((size_t)count);
    for (int i = 0; i < count; ++i)
    {
        Filler f;
        f.sock.reset(new DGS::UDPSocket());
        if (!f.sock->bind(0)) { std::printf("could not bind a socket for filler %d\n", i); return 1; }
        const int fd = f.sock->getSocketFD();
        fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

        f.uuid    = uuidB + (uint32_t)i;
        f.chunkX  = base + (i % spread);
        f.chunkY  = baseY;
        f.chunkZ  = baseZ;
        f.crosser = i < crossers;
        f.phase   = 6.2831853f * (float)i / (float)count;
        f.x       = chunkM * 0.5f + patrol * std::cos(f.phase);
        f.z       = chunkM * 0.5f + patrol * std::sin(f.phase);

        // ⚠️ A CROSSER STARTS NEAR THE BORDER, ON PURPOSE. Dropped in the middle of a 1 km chunk at a
        // walking pace it needs 83 seconds to reach the edge — measured, on a 14-second run where not
        // one of them crossed anything and the handoff the tool exists to show never happened. They
        // start `FILL_CROSS_START_M` from the edge and move at their own speed, so the first handoff
        // lands within a couple of seconds and then repeats on a period you can plan a shot around.
        if (f.crosser)
        {
            f.x = chunkM - std::min(chunkM * 0.9f, envFloat("FILL_CROSS_START_M", 100.0f))
                - (float)i * 4.0f;                     // fanned out, so they do not cross in lockstep
            f.z = chunkM * 0.5f;
        }
        fillers.push_back(std::move(f));
    }

    // Everyone asks once before moving: a filler with no zone has nowhere to send.
    int placed = 0;
    for (auto& f : fillers) if (queryZone(head, f)) ++placed;
    std::printf("fill_world: %d/%d players placed by the head · %d chunk%s from %d · %d crosser%s\n",
                placed, count, spread, spread == 1 ? "" : "s", base,
                crossers, crossers == 1 ? "" : "s");
    if (placed == 0)
    {
        std::printf("  the head gave no zone for chunk %d..%d — is a zone_node covering it?\n",
                    base, base + spread - 1);
        return 1;
    }
    std::printf("  Ctrl+C to stop.\n\n");

    const double dt = 1.0 / (double)hz;
    const auto   t0 = std::chrono::steady_clock::now();
    uint64_t ticks = 0, sent = 0, lastSent = 0;
    double   lastReport = 0.0;
    uint8_t  drain[4096];
    std::string from; int port = 0;

    while (!g_stop)
    {
        const double elapsed = std::chrono::duration<double>(
                                   std::chrono::steady_clock::now() - t0).count();
        if (seconds > 0 && elapsed >= (double)seconds) break;

        if ((double)ticks * dt > elapsed)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        ++ticks;

        for (auto& f : fillers)
        {
            if (f.crosser)
            {
                // Straight line across the world: this is the one that produces a handoff.
                f.x += crossSpeed * (float)f.dir * (float)dt;

                bool crossed = false;
                if (f.x >= chunkM)
                {
                    // Turn around at the far edge rather than wrapping: a 7 km jump back to the first
                    // chunk is a teleport, and a teleport is exactly what the validator's S1 filter
                    // exists to reject. A demo whose players cheat is a demo about the anti-cheat.
                    if (f.chunkX + 1 >= base + spread) { f.dir = -1; f.x = chunkM - 1.0f; }
                    else { f.x -= chunkM; f.chunkX += 1; crossed = true; }
                }
                else if (f.x < 0.0f)
                {
                    if (f.chunkX - 1 < base) { f.dir = 1; f.x = 1.0f; }
                    else { f.x += chunkM; f.chunkX -= 1; crossed = true; }
                }

                // ⚠️ IT TELLS THE ZONE IT IS LEAVING, and that is the whole handoff.
                // `checkAndTransfer` scans the entities a zone OWNS and transfers the ones whose chunk
                // has left its bounds — so the border crossing has to be reported to the zone we are
                // still talking to. Re-querying the head first and switching straight to the new zone
                // means the old zone never hears about it: it just stops getting updates and the lease
                // GC quietly drops the player. Which is exactly what `Client::sendTransform` used to
                // do — this tool is how that was found, and `client_handoff_e2e` is where it is now
                // pinned. Measured here first —
                // a first version did exactly that, and across 20 s of crossings not one zone logged a
                // handoff. So: announce here, keep reporting to the old owner for
                // `FILL_ANNOUNCE_MS`, and only then ask the head where to go next.
                if (crossed) f.announceUntil = nowMs() + (uint64_t)announceMs;
            }

            else
            {
                // A patrol circle: it moves visibly without ever leaving its chunk.
                f.phase += (speed / std::max(1.0f, patrol)) * (float)dt;
                f.x = chunkM * 0.5f + patrol * std::cos(f.phase);
                f.z = chunkM * 0.5f + patrol * std::sin(f.phase);
            }

            // The announcement window has elapsed: NOW ask the head where this player belongs.
            if (f.announceUntil != 0 && nowMs() >= f.announceUntil)
            {
                f.announceUntil = 0;
                queryZone(head, f);
            }

            if (f.zonePort <= 0) continue;

            DGS::EntityTransfer e{};
            e.uuid   = f.uuid;
            e.type   = DGS::ENT_PLAYER;
            e.chunkX = f.chunkX; e.chunkY = f.chunkY; e.chunkZ = f.chunkZ;
            e.pos[0] = f.x; e.pos[1] = 0.0f; e.pos[2] = f.z;
            e.angle  = (uint16_t)((int)(f.phase * 10430.0f) & 0xFFFF);
            e.stats.speed[0] = std::max(speed, crossSpeed) * 50.0f;   // far above the truth: S1 is not the subject
            e.stats.health   = 100.0f;
            DGS::Packet p; p.pack(e);
            f.sock->send(f.zoneAddr, f.zonePort, p.getRawData(), p.getSize());
            ++sent;

            // Drain what the zone sends back. Not to use it — to keep the socket buffer from filling
            // and to be able to say the world is actually talking to them.
            for (int k = 0; k < 32; ++k)
            {
                const int r = f.sock->receive(drain, sizeof(drain), from, port);
                if (r <= 0) break;
                ++f.recvCount;
            }
        }

        if (elapsed - lastReport >= 1.0)
        {
            uint64_t rx = 0; int queries = 0;
            for (const auto& f : fillers) { rx += f.recvCount; queries += f.queries; }
            // ⚠️ THE ACHIEVED RATE IS PRINTED, not the intended one. If this process cannot keep up,
            // the world it is showing is not the world you think you are recording — and that would
            // look exactly like a zone dropping people.
            std::printf("  t=%3.0fs  sent %5llu/s (target %d)  ·  received %llu  ·  zone queries %d\n",
                        elapsed,
                        (unsigned long long)(sent - lastSent),
                        hz * count, (unsigned long long)rx, queries);
            std::fflush(stdout);
            lastSent = sent; lastReport = elapsed;
        }
    }

    uint64_t rx = 0; int queries = 0;
    for (const auto& f : fillers) { rx += f.recvCount; queries += f.queries; }
    const double total = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - t0).count();
    std::printf("\n  stopped after %.1fs · %llu transforms sent (%.0f/s) · %llu datagrams received"
                " · %d zone queries\n",
                total, (unsigned long long)sent, (double)sent / (total > 0 ? total : 1),
                (unsigned long long)rx, queries);
    return 0;
}
