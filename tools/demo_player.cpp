// ─────────────────────────────────────────────────────────────────────────────────────────────────
// demo_player — something for the viewer to look at.
//
// A cluster with no players is a correct but empty picture, so there was no way to tell a working
// viewer from a broken one by looking at it. This walks N entities around a zone at a plausible speed,
// sending exactly what a real client sends: the raw `EntityTransfer` over UDP, at 20 Hz.
//
// It is a client, not a test hook: it goes through the same door as a player and gets the same
// treatment — the zone's S1 filter, the validator, the lease. If it moved faster than the speed it
// declares, S1 would throw it out, which is the point of declaring one.
//
//   demo_player [zone-host] [zone-udp-port] [count]        default 127.0.0.1 42420 8
//
// Env: DEMO_CHUNK_X / DEMO_CHUNK_Y / DEMO_CHUNK_Z  the chunk the crowd walks in (default 50,50,50)
//      DEMO_SPEED                                  m/s (default 40, and the steps stay under it)
//      DEMO_CHUNK_SIZE                             metres per chunk (default 1000)
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/types.h"

#include <csignal>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

static volatile std::sig_atomic_t g_stop = 0;
static void onSigint(int) { g_stop = 1; }

int main(int argc, char** argv)
{
    std::signal(SIGINT,  onSigint);
    std::signal(SIGTERM, onSigint);

    const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    const int         port = argc > 2 ? std::atoi(argv[2]) : 42420;
    const int         count = argc > 3 ? std::atoi(argv[3]) : 8;

    const int32_t cx = std::atoi(std::getenv("DEMO_CHUNK_X") ? std::getenv("DEMO_CHUNK_X") : "50");
    const int32_t cy = std::atoi(std::getenv("DEMO_CHUNK_Y") ? std::getenv("DEMO_CHUNK_Y") : "50");
    const int32_t cz = std::atoi(std::getenv("DEMO_CHUNK_Z") ? std::getenv("DEMO_CHUNK_Z") : "50");
    const float speed = (float)std::atof(std::getenv("DEMO_SPEED") ? std::getenv("DEMO_SPEED") : "40");
    const float chunkM = (float)std::atof(std::getenv("DEMO_CHUNK_SIZE")
                                          ? std::getenv("DEMO_CHUNK_SIZE") : "1000");

    DGS::UDPSocket udp;
    udp.bind(0);

    std::printf("[demo] %d entities walking in chunk (%d,%d,%d) -> %s:%d at %.0f m/s\n",
                count, cx, cy, cz, host.c_str(), port, speed);
    std::printf("[demo] Ctrl-C to stop\n");

    // Circles of different radii and phases, so the picture is obviously alive and each entity is
    // distinguishable. The step per tick stays well under `speed` so S1 lets every update through.
    const float dt     = 0.05f;                       // 20 Hz, like a real client
    const float radius = chunkM * 0.30f;
    float t = 0.0f;

    while (!g_stop)
    {
        for (int i = 0; i < count; ++i)
        {
            // Angular rate chosen so the tangential speed is a fraction of the declared maximum:
            // v = omega * r, and we want v ~= speed/2 so there is margin for jitter.
            const float r     = radius * (0.35f + 0.65f * (float)(i + 1) / (float)count);
            const float omega = (speed * 0.5f) / std::max(r, 1.0f);
            const float phase = (float)i * 6.2831853f / (float)count;

            DGS::EntityTransfer e{};
            e.uuid   = 1000 + (uint32_t)i;
            e.type   = DGS::ENT_PLAYER;
            e.chunkX = cx; e.chunkY = cy; e.chunkZ = cz;
            e.pos[0] = chunkM * 0.5f + r * std::cos(omega * t + phase);
            e.pos[1] = chunkM * 0.5f;
            e.pos[2] = chunkM * 0.5f + r * std::sin(omega * t + phase);
            e.angle  = (uint16_t)((std::fmod(omega * t + phase, 6.2831853f) / 6.2831853f) * 65535.0f);
            e.stats.speed[0] = speed;
            e.stats.health   = 100.0f;

            udp.send(host, port, (const uint8_t*)&e, sizeof(e));
        }

        t += dt;
        std::this_thread::sleep_for(std::chrono::milliseconds((int)(dt * 1000)));
    }

    std::printf("\n[demo] stopped\n");
    return 0;
}
