// ─────────────────────────────────────────────────────────────────────────────────────────────────
// split_resize_e2e — a SPLIT has to actually split something.
//
// THE BUG THIS PINS. `Orchestrator::trySplitDown` spawns a child owning the upper half of a zone's
// range and sends the parent `CMD_TRANSFER_SERVER` with its new xMax. `zone_node` had NO handler for
// PKT_COMMAND at all — grep for CMD_TRANSFER_SERVER in it returned nothing — and `xMax` was `const`,
// read from the environment at boot. The head does not remember the cut either: `updateNodeTopology`
// overwrites its record of a zone's box with whatever that zone reports in PKT_METRICS, which was the
// unchanged one.
//
// So after a split, parent [0..100] and child [51..100] both claimed the upper half forever, and
// `findTargetNode` returns the FIRST match by insertion order — the parent, registered first. The
// child received nothing. The cluster could scale up on paper and route exactly as if it had not, and
// nothing anywhere said so.
//
// `orchestrator_test` could not catch it: it forces DGS_ZONE_BIN=/bin/true and asserts the resize
// LEAVES over the socket, not that anybody obeys it. This test is the other side of that wire — a
// real `zone_node` process, and every assertion read off the wire rather than out of a log.
//
//   A. the zone reports its box, and after CMD_TRANSFER_SERVER it reports the SHRUNK one.
//   B. an entity in the ceded half is HANDED OFF — and the counter-proof is the same entity in the
//      same place before the resize, which must NOT be handed off. Without that half, "it asks for a
//      reassign" would also pass on a zone that hands off everything all the time.
//   C. un resize del head es AUTORITATIVO: tambien puede hacer crecer la caja. Sin eso la FUSION es
//      imposible (un superviviente que se queda la region de otro tiene que crecer), y lo unico que la
//      zona rechaza es un rango imposible. Que el head no reparta cajas solapadas se comprueba donde
//      corresponde: `orchestrator_test` 3e.
//   D. THE CUT IS NOT ALWAYS X. A split could only ever cut on X, so a zone became a thinner and
//      thinner slab and a crowd standing in one place could not be divided however many nodes were
//      thrown at it. A resize on Y must shrink Y, must leave X alone, and must hand off an entity that
//      the Y cut — and only the Y cut — has left outside.
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
#include <mutex>
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
static const int      kHeadPort = 21671;
static const int      kZoneUdp  = 21672;
static const float    kChunkM   = 1000.0f;
static const uint32_t kUuid     = 8300;

static const int32_t kBoxMax    = 100;   // the zone starts owning x = 0..100
static const int32_t kNewMax    = 50;    // and is told to shrink to 0..50
static const int32_t kCededChunk = 80;   // where the entity is: inside before, outside after

static std::atomic<bool>    g_done{false};
static std::atomic<int32_t> g_reportedXMax{-1};   // last xMax the zone told the head about
static std::atomic<int32_t> g_reportedYMax{-1};
static std::atomic<int>     g_metrics{0};
static std::atomic<int>     g_reassigns{0};       // reassign REQUESTS for kUuid
static std::atomic<int>     g_zoneFd{-1};
// The head's own socket, so the resize goes out on the SAME link the zone is reading. `TCPSocket`
// keeps per-connection state; writing from a different instance would not be the same connection.
// One reader (the head thread) plus one writer (main) is exactly the contract network.h documents.
static DGS::TCPSocket*      g_headSock = nullptr;

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/// A head that only does what this test needs: read metrics, ack reassigns, and resize on demand.
static void fakeHead(std::atomic<bool>& ready)
{
    static DGS::TCPSocket s;
    g_headSock = &s;
    if (!s.listen(kHeadPort)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;

    while (!g_done)
    {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        g_zoneFd = fd;

        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 200000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        while (!g_done)
        {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;
            if (n < 0)  continue;
            DGS::Packet r; r.setBuffer(buf, n);

            if (r.getType() == DGS::PKT_METRICS)
            {
                const auto sm = r.unpackServerMetrics();
                g_reportedXMax = sm.node.chunkXMax;
                g_reportedYMax = sm.node.chunkYMax;
                ++g_metrics;
            }
            else if (r.getType() == DGS::PKT_REASSIGN)
            {
                auto ra = r.unpackEntityReassign();
                if (ra.ack != 0) continue;             // our own answer bouncing back
                if (ra.entityUuid == kUuid) ++g_reassigns;
                // ack = 2 ("nobody covers that chunk") on purpose: the zone KEEPS the entity, so it
                // keeps asking. The subject here is whether it asks at all, and an entity released on
                // the first ack would make the "before" and "after" windows incomparable.
                DGS::EntityReassign answer = ra;
                answer.ack = 2;
                DGS::Packet pa; pa.pack(answer);
                s.send(fd, pa.getRawData(), pa.getSize());
            }
        }
        s.closeClient(fd);
        g_zoneFd = -1;
    }
}

static void sendResize(int32_t newMax, DGS::ResizeAxis axis = DGS::AXIS_X, int32_t newMin = 0)
{
    const int fd = g_zoneFd.load();
    if (fd < 0 || !g_headSock) return;
    DGS::Command cmd{};
    cmd.purpose    = DGS::CMD_TRANSFER_SERVER;
    cmd.chunkX     = newMax;
    cmd.chunkY     = newMin;   // el comando lleva el RANGO, no solo el tope: ver `sendResizeCommand`
    cmd.resizeAxis = axis;
    cmd.chunkSizeX = kChunkM; cmd.chunkSizeY = kChunkM; cmd.chunkSizeZ = kChunkM;
    cmd.port = kZoneUdp;
    std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");
    DGS::Packet p; p.pack(cmd);
    g_headSock->send(fd, p.getRawData(), p.getSize());
}

static void sendPlayer(DGS::UDPSocket& udp, int32_t cx, float x, int32_t cy = 0)
{
    DGS::EntityTransfer e{};
    e.uuid   = kUuid;
    e.type   = DGS::ENT_PLAYER;
    e.chunkX = cx; e.chunkY = cy; e.chunkZ = 0;
    e.pos[0] = x;
    e.stats.speed[0] = 100000.0f;   // S1 is not the subject: never let it reject a step
    e.stats.health   = 55.0f;
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

static bool waitMax(std::atomic<int32_t>& what, int32_t want, int msLimit)
{
    const uint64_t until = nowMs() + (uint64_t)msLimit;
    while (nowMs() < until)
    {
        if (what.load() == want) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

/// Feeds the entity for `ms` and returns how many reassign REQUESTS the head saw in that window.
static int feedAndCount(DGS::UDPSocket& udp, int32_t cx, int ms, int32_t cy = 0)
{
    const int before = g_reassigns.load();
    const uint64_t until = nowMs() + (uint64_t)ms;
    float x = 100.0f;
    while (nowMs() < until)
    {
        sendPlayer(udp, cx, x, cy);
        x += 1.0f;
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
    }
    return g_reassigns.load() - before;
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
        if (!std::getenv("SPLIT_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
        setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
        setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
        setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
        setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
        setenv("VALIDATOR_TCP_PORT", "21679", 1);
        setenv("SOCIAL_HOST",        "127.0.0.1", 1);
        setenv("SOCIAL_TCP_PORT",    "21680", 1);
        setenv("PERSISTENCE_HOST",   "127.0.0.1", 1);   // resolvable and refused: no DNS stall
        setenv("PERSISTENCE_PORT",   "21681", 1);
        setenv("CHUNK_X_MIN", "0", 1);  setenv("CHUNK_X_MAX", std::to_string(kBoxMax).c_str(), 1);
        // Y is deliberately WIDE: axis D cuts it, and a bound of 10 leaves nothing to cut.
        setenv("CHUNK_Y_MIN", "0", 1);   setenv("CHUNK_Y_MAX", "200", 1);
        setenv("CHUNK_Z_MIN", "-10", 1); setenv("CHUNK_Z_MAX", "10", 1);
        setenv("CHUNK_SIZE_X", "1000.0", 1);
        setenv("CHUNK_SIZE_Y", "1000.0", 1);
        setenv("CHUNK_SIZE_Z", "1000.0", 1);
        setenv("ENTITY_LEASE_MS", "60000", 1);   // long: the GC is not the subject
        // ⚠️ EL TECHO DE VELOCIDAD ES DEL SERVIDOR, y estas fases mueven entidades a sitios muy lejanos a
        // proposito (otro chunk, otra region) porque el sujeto es el traspaso o la lease, no S1. Antes
        // colaba porque S1 tenia una puerta de atras —`dt > 2 s -> pasa`— que se ha cerrado: esperar ya
        // no compra un teletransporte. Asi que el permiso se pide donde corresponde, al despliegue.
        setenv("CLIENT_MAX_SPEED_MPS", "1000000", 1);
        setenv("HANDOFF_RETRY_MS", "300", 1);
        setenv("ZONE_PERSIST_MS", "0", 1);
        setenv("ZONE_RESTORE", "0", 1);
        setenv("GAME_MODULE_SO", "", 1);
        char tmpl[] = "/tmp/dgs_split_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(zonePath, zonePath, (char*)nullptr);
        _exit(127);
    }

    DGS::UDPSocket player;
    player.bind(0);
    { timeval tv{}; tv.tv_usec = 50000;
      setsockopt(player.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

    // ══ A. the zone reports its box, then shrinks when told ═════════════════════════════════════
    check(waitMax(g_reportedXMax, kBoxMax, 8000), "the zone reports the box it was started with (xMax = 100)");

    // ══ B(counter-proof). The same entity, in the same chunk, BEFORE the resize ═════════════════
    // 80 is inside 0..100, so nothing may be handed off. Without this window, "it asks for a
    // reassign after the resize" would also pass on a zone that asks for one all the time.
    const int askedBefore = feedAndCount(player, kCededChunk, 1600);
    std::printf("    before the resize, chunk %d is INSIDE 0..%d: %d reassign requests\n",
                kCededChunk, kBoxMax, askedBefore);
    check(askedBefore == 0, "an entity inside the box is NOT handed off");

    // ══ A(cont). The resize ═════════════════════════════════════════════════════════════════════
    sendResize(kNewMax);
    const bool shrank = waitMax(g_reportedXMax, kNewMax, 8000);
    std::printf("    after CMD_TRANSFER_SERVER(xMax=%d) the zone reports xMax = %d\n",
                kNewMax, g_reportedXMax.load());
    check(shrank, "CMD_TRANSFER_SERVER shrinks the zone, and the head learns it from the metrics");

    // ══ B. The same entity is now outside, and must be handed off ═══════════════════════════════
    const int askedAfter = feedAndCount(player, kCededChunk, 2500);
    std::printf("    after the resize, chunk %d is OUTSIDE 0..%d: %d reassign requests\n",
                kCededChunk, kNewMax, askedAfter);
    check(askedAfter > 0, "an entity in the ceded half IS handed off — the split moves players");

    // ══ C. UN RESIZE ES AUTORITATIVO, TAMBIEN PARA CRECER ═══════════════════════════════════════
    // Esta fase decia lo contrario: "un resize solo puede encoger". Era una defensa razonable contra
    // que dos zonas reclamaran los mismos chunks... y hacia IMPOSIBLE la fusion, que es justo lo
    // contrario de un split: un superviviente que se queda la region de otro tiene que CRECER.
    // Medido antes de cambiarlo: cien milisegundos despues de fusionar, `findTargetNode` devolvia -1
    // para la region de la victima — el head anotaba la union en su topologia y la siguiente muestra
    // de metricas la reescribia con la caja que la zona seguia teniendo.
    //
    // Quien decide las cajas es el HEAD: es el unico que ve la topologia entera. Que no reparta cajas
    // solapadas se comprueba ALLI (`orchestrator_test` 3e: una union que no es un rectangulo no se
    // fusiona). Aqui la zona solo rechaza lo que no tiene sentido en si mismo.
    sendResize(kBoxMax + 100, DGS::AXIS_X, 0);
    const bool grew = waitMax(g_reportedXMax, kBoxMax + 100, 8000);
    std::printf("    tras CMD_TRANSFER_SERVER(x=[0..%d]) la zona reporta xMax = %d\n",
                kBoxMax + 100, g_reportedXMax.load());
    check(grew, "C · un resize del head tambien puede HACER CRECER la caja (sin esto no hay fusion)");

    // Lo unico que se rechaza es un rango imposible.
    sendResize(10, DGS::AXIS_X, 500);        // min 500 > max 10
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    std::printf("    tras un rango invertido [500..10] la zona sigue en xMax = %d\n",
                g_reportedXMax.load());
    check(g_reportedXMax.load() == kBoxMax + 100,
          "C · y un rango invertido se ignora: la zona no se queda sin region");

    // ══ D. The cut is not always X ══════════════════════════════════════════════════════════════
    // An entity at (chunk 10 on X, chunk 150 on Y) is inside on both axes: X was cut to 50 and Y is
    // still 0..200. Nothing may be handed off — that is the counter-proof for the window after it.
    // ⚠️ LET THE PREVIOUS PHASE DRAIN FIRST. The entity is the SAME uuid, and it is arriving from
    // chunk 80 on X — outside the zone's 0..50 — so a handoff for it is still being retried every
    // HANDOFF_RETRY_MS. Counting straight away catches that retry and reads it as "an entity inside
    // the box was handed off". Measured: it passed twice and then failed with exactly 1 stray
    // reassign. Feed it at the new, inside position until the zone stops asking, then start counting.
    feedAndCount(player, 10, 1500, 150);

    const int yBefore = feedAndCount(player, 10, 1600, 150);
    std::printf("    (x=10, y=150) with Y still 0..200: %d reassign requests\n", yBefore);
    check(yBefore == 0, "an entity inside on every axis is NOT handed off");

    sendResize(100, DGS::AXIS_Y);
    const bool shrankY = waitMax(g_reportedYMax, 100, 8000);
    std::printf("    after CMD_TRANSFER_SERVER(axis=Y, yMax=100): the zone reports yMax = %d, xMax = %d\n",
                g_reportedYMax.load(), g_reportedXMax.load());
    check(shrankY, "a resize on Y shrinks Y — the split is not stuck on one axis");
    // X es lo que la fase C dejo (0..200), no lo que valia al principio: lo que se mide aqui es que un
    // resize en Y NO toca X, no un numero concreto.
    check(g_reportedXMax.load() == kBoxMax + 100, "and it leaves X exactly where it was");

    const int yAfter = feedAndCount(player, 10, 2500, 150);
    std::printf("    (x=10, y=150) with Y now 0..100: %d reassign requests\n", yAfter);
    check(yAfter > 0, "and the entity the Y cut left outside IS handed off");

    g_done = true;
    kill(zone, SIGTERM);
    int st = 0; waitpid(zone, &st, 0);
    head.join();

    std::printf("\n%d/%d checks passed\n", g_pass, g_pass + g_fail);
    return g_fail == 0 ? 0 : 1;
}
