#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/auth.h"
#include "include/dgs/game_module.h"

#include <iostream>
#include <fstream>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <cmath>
#include <cstring>
#include <cstdlib>
#include <string>
#include <algorithm>
#include <map>
#include <dlfcn.h>
#include <csignal>
#include <csetjmp>
#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <poll.h>
#include <sys/socket.h>

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// Monotonic bandwidth counters for the node (accumulated since start-up, P3 §4.1).
// Incremented at the real send/receive points; the orchestrator derives the rate with Δ/EWMA.
static uint64_t g_bytesRx = 0;
static uint64_t g_bytesTx = 0;

// ------------------------------------------------------------------------------------------------
// The PROJECT's RULES MODULE loaded into the ZONE (P4, §3.6: C4 — the owning zone SIMULATES its
// entities). The DGS is generic: it does not know the physics; if the project ships
// lib<project>_rules.so it is loaded and `step` advances the entities THIS zone owns at a fixed tick
// (local physical authority); with no module (or a null / crashing `step`) the zone is just a
// forwarding chokepoint plus S1, and the world advances from client updates.
static const DGS::GameModule* z_mod   = nullptr;   // null → do not simulate
static DGS::WorldQuery        z_wq{};              // read-only world lent to the module
static DGS::ZoneHandle        z_zone  = nullptr;   // zone created by the module (authoritative state)

static volatile int g_draining = 0;   // §3.9: while DRAINING we claim no new ownership (we are retiring)

// Crash guard for the `.so` (§3.5): a SIGSEGV inside `step`/`validateMove` would kill the whole node.
static sigjmp_buf            z_sigJmp;
static volatile sig_atomic_t z_inModule = 0;
static std::atomic<bool>     z_susStep{false};

static void z_crashHandler(int sig)
{
    if (z_inModule)
    {
        z_inModule = 0;
        z_susStep.store(true);
        siglongjmp(z_sigJmp, 1);
    }
    signal(sig, SIG_DFL);
    raise(sig);
}

static void z_installCrashGuard()
{
    static uint8_t altStack[64 * 1024] __attribute__((aligned(16)));
    stack_t ss{};
    ss.ss_sp   = altStack;
    ss.ss_size = sizeof(altStack);
    sigaltstack(&ss, nullptr);

    struct sigaction sa{};
    sa.sa_flags   = SA_ONSTACK | SA_NODEFER;
    sa.sa_handler = z_crashHandler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGSEGV, &sa, nullptr);
    sigaction(SIGBUS,  &sa, nullptr);
    sigaction(SIGFPE,  &sa, nullptr);
    sigaction(SIGILL,  &sa, nullptr);
}

// Loads the module and creates the zone. Returns true if a usable `step` is available.
static void loadZoneModule()
{
    // Read-only world: chunk size in the host's units (coordinates arrive de-quantised by the host).
    float km = (float)std::atof(std::getenv("DGS_CHUNK_KM") ? std::getenv("DGS_CHUNK_KM") : "1.0");
    z_wq = DGS::WorldQuery{};
    z_wq.chunkSizeX = km; z_wq.chunkSizeY = km; z_wq.chunkSizeZ = km;

    const char* so = std::getenv("GAME_MODULE_SO") ? std::getenv("GAME_MODULE_SO") : "libharuka_rules.so";
    void* h = dlopen(so, RTLD_NOW);
    if (!h)
    {
        std::cout << "[ZoneNode] no rules module (" << so << "): " << dlerror()
                  << " -> no simulation (S1 only)" << std::endl;
        return;
    }
    auto entry = (const DGS::GameModule* (*)())dlsym(h, "dgs_game_module_v1");
    if (!entry || !(z_mod = entry()) || z_mod->abiVersion != DGS::GAME_MODULE_ABI)
    {
        std::cout << "[ZoneNode] invalid module / ABI != " << DGS::GAME_MODULE_ABI << " -> no simulation" << std::endl;
        dlclose(h); z_mod = nullptr; return;
    }
    if (!z_mod->step)
    {
        std::cout << "[ZoneNode] module has no `step` -> no simulation in this zone" << std::endl;
        dlclose(h); z_mod = nullptr; return;
    }
    if (z_mod->createZone) z_zone = z_mod->createZone(&z_wq);
    if (!z_zone)
    {
        std::cout << "[ZoneNode] createZone failed -> no simulation" << std::endl;
        dlclose(h); z_mod = nullptr; return;
    }
    std::cout << "[ZoneNode] rules module '" << (z_mod->name ? z_mod->name : "?")
              << "' ABI=" << z_mod->abiVersion << " -> zone simulation active" << std::endl;
}

float getRAM()
{
    long pages, rss;
    std::ifstream stat_file("/proc/self/statm");
    stat_file >> pages >> rss;
    stat_file.close();

    long total_bytes = rss * sysconf(_SC_PAGESIZE);
    float limit = 512.f * 1024.f * 1024.f;
    return (float)total_bytes / limit;
}

bool isNearBorder(const DGS::EntityTransfer& e,
                  int32_t xMin, int32_t xMax,
                  int32_t yMin, int32_t yMax,
                  int32_t zMin, int32_t zMax,
                  int32_t threshold)
{
    return e.chunkX <= xMin + threshold || e.chunkX >= xMax - threshold ||
           e.chunkY <= yMin + threshold || e.chunkY >= yMax - threshold ||
           e.chunkZ <= zMin + threshold || e.chunkZ >= zMax - threshold;
}

static void angleToQuat(uint16_t angle, float rot[4])
{
    float yaw     = angle * (3.14159265f / 32767.5f);
    float halfYaw = yaw * 0.5f;
    rot[0] = 0.0f;
    rot[1] = sinf(halfYaw);
    rot[2] = 0.0f;
    rot[3] = cosf(halfYaw);
}

// ⚠️ THE HANDOFF USED TO LOSE ENTITIES, AND IT DID IT IN SILENCE. This function sent the reassign and
// the state and then erased the entity UNCONDITIONALLY — both `send` results ignored. Three ways for
// an entity to cease to exist anywhere:
//   · the head is down or reconnecting: the writes fail, the entity is erased anyway;
//   · the head cannot route the chunk (`targetFD == -1`): it drops the reassign without a word;
//   · the forward to the new owner fails: same, nobody is told.
// Nothing owned it afterwards, no counter moved, and the logs said "Transferring...".
//
// It is now AT-LEAST-ONCE: the entity stays here, owned and simulated, until the head answers
// `ack = 1` ("routed, you may let go"). `ack = 2` means no zone covers that chunk, so keeping it is
// the only correct thing to do. Un-acked handoffs are re-sent on a throttle. The failure mode changed
// from "the entity vanishes" to "the entity lingers in the wrong zone for a moment", which is visible,
// recoverable, and bounded by the ordinary lease.
void checkAndTransfer(DGS::TCPSocket& tcp, std::vector<DGS::EntityTransfer>& entities,
                      std::map<uint32_t, uint64_t>& ownedUntil,
                      std::map<uint32_t, uint64_t>& handoffLastTry,
                      uint64_t retryMs,
                      int32_t xMin, int32_t xMax,
                      int32_t yMin, int32_t yMax,
                      int32_t zMin, int32_t zMax)
{
    for (auto it = entities.begin(); it != entities.end();)
    {
        bool out = it->chunkX < xMin || it->chunkX > xMax ||
                   it->chunkY < yMin || it->chunkY > yMax ||
                   it->chunkZ < zMin || it->chunkZ > zMax;

        if (out)
        {
            // Throttle: an un-acked handoff is retried, not spammed once per tick.
            const uint64_t now = nowMs();
            auto lt = handoffLastTry.find(it->uuid);
            if (lt != handoffLastTry.end() && now - lt->second < retryMs) { ++it; continue; }
            const bool first = (lt == handoffLastTry.end());
            handoffLastTry[it->uuid] = now;

            std::cout << "[ZoneNode] Entity " << it->uuid
                      << (first ? " out of bounds. Transferring..." : " handoff not acked yet, re-sending")
                      << std::endl;

            // §3.6 handoff: the entity crosses into a neighbouring zone → we cede AUTHORITY explicitly.
            // The head routes it to the new owner by chunk (PKT_REASSIGN), which promotes it (ghost→real).
            DGS::EntityReassign ra{};
            ra.entityUuid = it->uuid;
            ra.chunkX     = it->chunkX;
            ra.chunkY     = it->chunkY;
            ra.chunkZ     = it->chunkZ;
            ra.fromZone   = 0;   // the head fills it in if needed
            ra.toZone     = 0;   // 0 = let the head resolve it by chunk
            ra.ack = 0;   // a REQUEST; the head answers 1 (routed) or 2 (nobody covers that chunk)
            DGS::Packet pRa; pRa.pack(ra);
            const bool okRa = tcp.send(tcp.getSocketFD(), pRa.getRawData(), pRa.getSize());
            g_bytesTx += pRa.getSize();

            // The full state travels too (the new owner needs the complete EntityTransfer).
            DGS::Packet p;
            p.pack(*it);
            const bool okEnt = okRa && tcp.send(tcp.getSocketFD(), p.getRawData(), p.getSize());
            g_bytesTx += p.getSize();

            if (!okRa || !okEnt)
                std::cout << "[ZoneNode] handoff WRITE FAILED for uuid=" << it->uuid
                          << " (keeping it; will retry)" << std::endl;

            // WE KEEP IT. Ownership is only given up when the head says it landed somewhere.
            ++it;
        }
        else ++it;
    }
}

void emitGhostDeltas(DGS::TCPSocket& tcp,
                     const std::vector<DGS::EntityTransfer>& entities,
                     std::map<uint32_t, DGS::EntityTransfer>& lastSnapshot,
                     int32_t xMin, int32_t xMax,
                     int32_t yMin, int32_t yMax,
                     int32_t zMin, int32_t zMax,
                     int32_t threshold)
{
    for (const auto& e : entities)
    {
        if (!isNearBorder(e, xMin, xMax, yMin, yMax, zMin, zMax, threshold)) continue;

        DGS::GhostDelta delta{};
        delta.uuid   = e.uuid;
        delta.chunkX = e.chunkX;
        delta.chunkY = e.chunkY;
        delta.chunkZ = e.chunkZ;

        auto it = lastSnapshot.find(e.uuid);
        bool isNew = it == lastSnapshot.end();

        if (isNew ||
            fabsf(e.pos[0] - it->second.pos[0]) > 0.1f ||
            fabsf(e.pos[1] - it->second.pos[1]) > 0.1f ||
            fabsf(e.pos[2] - it->second.pos[2]) > 0.1f ||
            e.angle != it->second.angle)
        {
            delta.dirtyMask |= DGS::DIRTY_TRANSFORM;
            delta.pos[0] = e.pos[0];
            delta.pos[1] = e.pos[1];
            delta.pos[2] = e.pos[2];
            angleToQuat(e.angle, delta.rot);
        }

        if (isNew || std::memcmp(&e.stats, &it->second.stats, sizeof(DGS::Stats)) != 0)
        {
            delta.dirtyMask |= DGS::DIRTY_STATS;
            delta.stats = e.stats;
        }

        if (delta.dirtyMask == 0) continue;

        DGS::Packet p;
        p.pack(delta);
        tcp.send(tcp.getSocketFD(), p.getRawData(), p.getSize());
        g_bytesTx += p.getSize();
        lastSnapshot[e.uuid] = e;
    }

    for (auto it = lastSnapshot.begin(); it != lastSnapshot.end();)
    {
        bool exists = false;
        for (const auto& e : entities)
            if (e.uuid == it->first) { exists = true; break; }
        it = exists ? std::next(it) : lastSnapshot.erase(it);
    }
}

// ⚠️ THE BROADCAST USED TO SEND THE WHOLE STRUCT: `sizeof(EntityTransfer)`, 4160 bytes, of which 4096
// are the `data[]` blob that is EMPTY for a player who is only moving. `dataSize` exists precisely so
// that those bytes need not travel, and this path ignored it — while `Packet::pack(EntityTransfer)`,
// already used on the TCP paths, has always honoured it. The format was not missing; the UDP path was
// bypassing it with a raw memcpy of the struct.
//
// Measured, with `dataSize = 0`: 4160 B → 62 B, **67x less**. It matters because the fan-out is N x N
// — every client gets every entity, every tick — so the zone's egress at 64 players was 174 MB/s and
// 21 Mbit/s DOWN PER CLIENT, which rules out 64 players on a domestic line long before the CPU
// notices. It is the wire, not the simulation, that was the wall.
//
// It also removes a fragile rule: the receivers used to recognise an entity BY ITS EXACT SIZE. That
// is how `net_degraded` once silently dropped everything (a proxy truncated at 2048 and the validator,
// comparing sizes, discarded in silence). Now every datagram carries its `PacketType` in byte 0, the
// same discriminator the rest of the protocol uses.
//
// Serialising is done ONCE PER TICK, not once per client: the same snapshot goes to everybody, and
// packing it per recipient made the CPU cost N x N as well, for identical bytes.
// ⚠️ EVERY CLIENT USED TO GET EVERY ENTITY, so a zone's egress grew as N x N. Measured: at 64 players
// that was 175 MB/s out of one zone and **21 Mbit/s DOWN PER CLIENT** before `dataSize` was honoured,
// and 2.6 MB/s after — better, but still quadratic, and the load ramp showed the tick going over
// budget at 128 because of the sheer NUMBER of datagrams (about 7 us of `sendto` each, 16384 of them
// per tick). Bytes were the first wall; datagram count is the second, and both are the same mistake:
// telling everybody about everything.
//
// A player is sent what is NEAR them. `INTEREST_RADIUS_M` = 0 keeps the old behaviour, which is what
// the measurements compare against.
//
// It is worth being blunt about what this does NOT fix: a crowd standing in one place still sees each
// other, so a hundred players in a market square is still a hundred squared. Interest management buys
// scale for a world that is SPREAD OUT; it buys nothing for a scrum, and any number quoted for it has
// to say which of the two was measured.
struct BroadcastFrames
{
    struct Frame
    {
        std::vector<uint8_t> bytes;
        uint32_t uuid;
        float    gx, gy, gz;    // global position, for deciding who is near enough to care
    };
    std::vector<Frame> frames;
};

static BroadcastFrames buildBroadcast(const std::vector<DGS::EntityTransfer>& entities,
                                      const std::map<uint32_t, DGS::GhostDelta>& ghosts,
                                      float csX, float csY, float csZ)
{
    BroadcastFrames out;
    out.frames.reserve(entities.size() + ghosts.size());

    for (const auto& e : entities)
    {
        DGS::Packet p;
        p.pack(e);                       // honours dataSize, tags with PKT_ENTITY_TRANSFER
        // Sealed HERE, once, not once per recipient: the same snapshot goes to everybody, and
        // encrypting it N times cost +37 % of loop time at 256 players when it was done inside `send`.
        std::vector<uint8_t> wire;
        DGS::sealForUdp(p.getRawData(), p.getSize(), wire);
        out.frames.push_back({ std::move(wire),
                               e.uuid,
                               e.chunkX * csX + e.pos[0],
                               e.chunkY * csY + e.pos[1],
                               e.chunkZ * csZ + e.pos[2] });
    }

    for (const auto& [uuid, ghost] : ghosts)
    {
        DGS::Packet p;
        p.pack(ghost);
        std::vector<uint8_t> wire;
        DGS::sealForUdp(p.getRawData(), p.getSize(), wire);
        out.frames.push_back({ std::move(wire),
                               (uint32_t)ghost.uuid,
                               ghost.chunkX * csX + ghost.pos[0],
                               ghost.chunkY * csY + ghost.pos[1],
                               ghost.chunkZ * csZ + ghost.pos[2] });
    }
    return out;
}

/// Sends the already-serialised snapshot to one recipient.
///
/// `radiusM <= 0` means everything — which is what an OBSERVER gets: a viewer exists to see the whole
/// zone, and one that only saw a corner of it would be a viewer that lies.
void broadcastToClient(DGS::UDPSocket& udp,
                       const std::string& addr, int port,
                       const BroadcastFrames& snapshot,
                       float radiusM = 0.0f,
                       bool haveCentre = false,
                       float cx = 0, float cy = 0, float cz = 0,
                       uint32_t selfUuid = 0)
{
    const bool filtered = radiusM > 0.0f && haveCentre;
    const float r2 = radiusM * radiusM;

    for (const auto& f : snapshot.frames)
    {
        if (filtered && f.uuid != selfUuid)
        {
            // Its own entity always goes: a player who stopped being told where THEY are would be
            // watching someone else's world.
            const float dx = f.gx - cx, dy = f.gy - cy, dz = f.gz - cz;
            if (dx * dx + dy * dy + dz * dz > r2) continue;
        }
        udp.sendRaw(addr, port, f.bytes.data(), f.bytes.size());
        g_bytesTx += f.bytes.size();   // already the WIRE size: the frame was sealed when it was built
    }
}

struct PendingValidation
{
    uint64_t   deadlineMs;
    uint32_t   retries;
    DGS::EntityTransfer entity;
    float      maxSpeed;
    uint32_t   ownerZone;
    // ⚠️ UNA ACCION SOLO OCURRE SI SE ACEPTA, y esa es toda la diferencia con el movimiento. Una
    // posicion ya esta en `entities` cuando se pregunta —falla abierto, y un sitio mal aceptado se
    // corrige en el siguiente paquete—; una accion CREA algo, y lo creado no se deshace. Asi que la
    // entidad propuesta espera aqui y solo entra en el mundo con el veredicto bueno. Sin validador,
    // sin modulo o con el interruptor abierto, nunca entra: eso es fallar cerrado.
    bool       applyOnAccept = false;
    uint16_t   pendingAction  = 0;   ///< ActionKind, cuando `applyOnAccept`
    uint32_t   actor          = 0;   ///< a quien hay que contestarle
    uint32_t   clientReqId    = 0;   ///< el numero con el que EL reconoce su peticion
};

// Last known GLOBAL position of each entity (baseline for the S1 pre-check and for the REQ).
struct LastPosition
{
    float    gx, gy, gz;
    float    maxSpeed;
    uint64_t tsMs;
    // Metros de margen ACUMULADOS y todavia sin gastar. Ver `s1Plausible`.
    float    budgetM = 0.0f;
};

// Local S1 pre-check (generic, module-agnostic): a fast teleport/speed filter.
// It is the same computation as the validator's fallback, applied BEFORE sending the REQ so the
// network is not saturated. The sender here is the owning zone → this is a plausibility step, not an
// authority one.
/// ⚠️ ESTO JUZGABA LA CONEXION Y NO LA VELOCIDAD, y es el fallo mas injusto que tenia el sistema.
///
/// Era `maxDist = maxSpeed * dt + 1 m`, con `dt` medido entre las LLEGADAS de dos datagramas. Mientras
/// los paquetes llegan repartidos, bien. En cuanto se agolpan —y se agolpan en cuanto la maquina o la
/// red se ocupan— dos que llegan con un milisegundo de diferencia reciben `maxSpeed * 0.001 + 1 m` de
/// margen para un paso que el jugador dio en cincuenta milisegundos. Rechazado por un salto que no ha
/// dado. Medido en este repo: 0 de 20 pasos legitimos rechazados en maquina ociosa, 8 de 20 bajo la
/// suite entera. El mismo jugador y el mismo movimiento, distinto veredicto segun lo ocupado que
/// estuviera el SERVIDOR.
///
/// Ahora el margen se ACUMULA con el tiempo real en vez de calcularse por paquete: un cubo que se
/// llena a `maxSpeed` metros por segundo y del que cada paso aceptado gasta su distancia. Una racha de
/// cuatro paquetes pegados gasta del margen que se lleno durante los doscientos milisegundos de
/// silencio anterior, que es exactamente el tiempo que el jugador estuvo andando.
///
/// El cubo tiene TOPE, y ese tope es lo que impide que esto sea una barra libre: quien se queda quieto
/// diez segundos no acumula diez segundos de teletransporte. Media segundo de margen es lo mismo que
/// concedia el calculo viejo ante un hueco de medio segundo, asi que no se abre la mano — se reparte
/// mejor.
static bool s1Plausible(const DGS::EntityTransfer& e, float csX, float csY, float csZ,
                        LastPosition& last, float dt)
{
    // ⚠️ AQUI HABIA DOS PUERTAS DE ATRAS, y las dos eran `return true`.
    //
    //   · `dt <= 0` — cuatro datagramas leidos en el mismo milisegundo dan dt = 0, y saltaban el
    //     filtro entero. Como la referencia avanza con cada uno aceptado, sesenta pasos seguidos
    //     mueven al jugador trescientos metros en un milisegundo. No hay que entender el protocolo:
    //     basta con no dormir entre envios.
    //   · `dt > 2 s` — esperar tres segundos y teletransportarse a donde sea.
    //
    // Las dos desaparecen con el cubo: si no ha pasado tiempo, no se ha acumulado margen, y punto. Y
    // esperar mucho tampoco sirve porque el cubo tiene tope. Un filtro con escapes no es un filtro.
    static const float burstS = [] {
        const char* v = std::getenv("S1_BURST_S");
        const float f = v ? (float)std::atof(v) : 0.5f;
        return (f > 0.0f && f < 5.0f) ? f : 0.5f;
    }();

    const float dx = (e.chunkX * csX + e.pos[0]) - last.gx;
    const float dy = (e.chunkY * csY + e.pos[1]) - last.gy;
    const float dz = (e.chunkZ * csZ + e.pos[2]) - last.gz;
    const float dist = std::sqrt(dx*dx + dy*dy + dz*dz);

    last.budgetM += last.maxSpeed * (dt > 0.0f ? dt : 0.0f);
    const float cap = last.maxSpeed * burstS + 1.0f;   // el metro de holgura de siempre
    if (last.budgetM > cap) last.budgetM = cap;

    if (dist > last.budgetM) return false;
    last.budgetM -= dist;
    return true;
}

int main()
{
    // ⚠️ A NODE MUST NOT DIE BECAUSE A PEER HUNG UP. Writing to a socket whose other end has closed
    // raises SIGPIPE, and its default action is to KILL the process. No node installed this, and the
    // whole suite stayed green anyway: every test calls `signal(SIGPIPE, SIG_IGN)` before `fork()`, and
    // a child INHERITS an ignored disposition — so under CTest the nodes survived, and started from a
    // shell, systemd, Docker or `dgs run` they died the first time a peer disconnected.
    // Measured with the same binary and the same environment: parent ignoring SIGPIPE -> ran the full
    // 6 s; ordinary parent -> exit 141 (128 + SIGPIPE) within seconds of the head closing.
    // A closed peer is an ordinary event: `send` returns EPIPE and the reconnect paths handle it.
    std::signal(SIGPIPE, SIG_IGN);
    DGS::UDPSocket udp_zone_node;
    DGS::TCPSocket tcp_zone_node;

    std::vector<DGS::EntityTransfer>          entities;
    std::map<uint32_t, DGS::EntityTransfer>   lastSnapshot;
    std::map<uint32_t, DGS::GhostDelta>       ghostEntities;
    std::map<uint32_t, uint64_t>              ghostLastSeen;  // uuid → last time we saw its ghost (promotion TTL)
    std::map<uint32_t, std::pair<std::string, int>> clientMap; // uuid → {addr, port}
    std::map<uint32_t, uint64_t>              entityOwnedUntil; // uuid → until when I AM the owner (lease §3.6)
    std::map<uint32_t, uint64_t>              lastActiveSeen;   // uuid → last time the client reported (GC)
    std::map<uint32_t, uint64_t>              handoffLastTry;   // uuid → last handoff attempt (at-least-once)
    uint64_t lastStepMs = 0;

    const char* headHost  = std::getenv("HEAD_SERVER_HOST") ? std::getenv("HEAD_SERVER_HOST") : "head-server";
    int         headPort  = std::atoi(std::getenv("HEAD_SERVER_PORT") ? std::getenv("HEAD_SERVER_PORT") : "42424");
    const char* zoneAddr  = std::getenv("MY_POD_IP")        ? std::getenv("MY_POD_IP")        : "127.0.0.1";
    int         udpPort   = std::atoi(std::getenv("ZONE_UDP_PORT")    ? std::getenv("ZONE_UDP_PORT")    : "42425");
    int32_t     threshold = std::atoi(std::getenv("GHOST_THRESHOLD")  ? std::getenv("GHOST_THRESHOLD")  : "1");

    if (!udp_zone_node.bind(udpPort))
    {
        std::cerr << "[ZoneNode] Failed to bind UDP on port " << udpPort << std::endl;
        return 1;
    }
    std::cout << "[ZoneNode] UDP listening on :" << udpPort << std::endl;

    // SO_RCVTIMEO on UDP so the tick never blocks
    struct timeval tvUDP { 0, 10000 }; // 10ms
    setsockopt(udp_zone_node.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tvUDP, sizeof(tvUDP));

    auto connectToHead = [&]() -> bool {
        tcp_zone_node = DGS::TCPSocket();
        for (int attempt = 1; ; ++attempt)
        {
            if (tcp_zone_node.connect(headHost, headPort))
            {
                DGS::sendAuth(tcp_zone_node);   // the head will not register a zone without it
                struct timeval tvTCP { 0, 100000 };
                setsockopt(tcp_zone_node.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tvTCP, sizeof(tvTCP));
                std::cout << "[ZoneNode] Connected to HeadServer (attempt " << attempt << ")" << std::endl;
                return true;
            }
            std::cerr << "[ZoneNode] Retrying connection to HeadServer..." << std::endl;
            sleep(3);
        }
    };

    connectToHead();

    // ---- P2: validation request-ack against the VALIDATOR ----
    const char* validHost = std::getenv("VALIDATOR_HOST")     ? std::getenv("VALIDATOR_HOST")     : "validador";
    int         validPort = std::atoi(std::getenv("VALIDATOR_TCP_PORT") ? std::getenv("VALIDATOR_TCP_PORT") : "42428");
    DGS::TCPSocket tcp_validator;

    // BOUNDED AND SPACED retry. It used to reconnect on EVERY timeout with an unbounded `connect`, and
    // that froze the whole node: see the long note on `TCPSocket::connect`. Measured here: with the
    // arbiter accepting but never answering, the zone made 11 reconnections in 9 s, filled the
    // validator's accept queue, and the TWELFTH blocked for ~2 min. During that window the head received
    // NOT ONE metric nor status report — it went blind precisely when something was wrong.
    uint64_t lastValidatorTryMs = 0;
    const uint64_t VALIDATOR_RETRY_MS =
        (uint64_t)std::atoi(std::getenv("VALIDATOR_RETRY_MS") ? std::getenv("VALIDATOR_RETRY_MS") : "2000");
    // Wait between retries: grows on failure, resets on success. Without the growth an UNREACHABLE
    // validator (packets dropped, not refused) costs `VALIDATOR_CONNECT_MS` every `VALIDATOR_RETRY_MS`
    // forever — 25 % of the tick thrown away indefinitely.
    uint64_t validatorRetryMs = VALIDATOR_RETRY_MS;
    const uint64_t VALIDATOR_RETRY_MAX_MS = (uint64_t)std::atoi(
        std::getenv("VALIDATOR_RETRY_MAX_MS") ? std::getenv("VALIDATOR_RETRY_MAX_MS") : "30000");
    // 500 ms: a deadline that is actually short. This `connect` lives INSIDE the tick, so its worst case
    // is time the node spends serving nobody.
    const int VALIDATOR_CONNECT_MS =
        std::atoi(std::getenv("VALIDATOR_CONNECT_MS") ? std::getenv("VALIDATOR_CONNECT_MS") : "500");

    auto connectToValidator = [&]() -> bool {
        lastValidatorTryMs = nowMs();
        tcp_validator = DGS::TCPSocket();
        if (tcp_validator.connect(validHost, validPort, VALIDATOR_CONNECT_MS))
        {
            DGS::sendAuth(tcp_validator);   // first thing on the wire, before any request
            struct timeval tvVAL { 0, 5000 };   // 5 ms: does not stall the simulation tick
            setsockopt(tcp_validator.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tvVAL, sizeof(tvVAL));
            std::cout << "[ZoneNode] P2: connected to Validator " << validHost << ":" << validPort << std::endl;
            validatorRetryMs = VALIDATOR_RETRY_MS;
            return true;
        }
        validatorRetryMs = std::min(validatorRetryMs * 2, VALIDATOR_RETRY_MAX_MS);
        std::cout << "[ZoneNode] P2: Validator unavailable at " << validHost << ":" << validPort
                  << " (fail-open: local S1; next attempt in " << validatorRetryMs << " ms)" << std::endl;
        return false;
    };
    bool validated = connectToValidator();

    // ---- P7 (§3.7): the ZONE subscribes to the social node. The zone NEVER decides account matters:
    // it applies what the social node tells it (bans/permissions → banned players are blocked at entry).
    // The local chat channel is emitted by the zone through spatial interest; every other channel is
    // routed by the social node (subscription fan-out).
    const char* socialHost = std::getenv("SOCIAL_HOST") ? std::getenv("SOCIAL_HOST") : "social";
    int         socialPort = std::atoi(std::getenv("SOCIAL_TCP_PORT") ? std::getenv("SOCIAL_TCP_PORT") : "42430");
    DGS::TCPSocket tcp_social;
    if (tcp_social.connect(socialHost, socialPort))
    {
        DGS::sendAuth(tcp_social);
        struct timeval tvSOC { 0, 5000 };
        setsockopt(tcp_social.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tvSOC, sizeof(tvSOC));
        std::cout << "[ZoneNode] P7: subscribed to the social node " << socialHost << ":" << socialPort << std::endl;
    }
    else
        std::cout << "[ZoneNode] P7: social node unavailable at " << socialHost << ":"
                  << socialPort << " (no local bans)" << std::endl;

    z_installCrashGuard();       // crash containment for the .so (§3.5)
    loadZoneModule();            // the project's rules module (or no simulation)

    std::map<uint32_t, LastPosition>  lastPosition;       // uuid → baseline
    std::map<uint32_t, uint64_t>      lastChatAt;         // uuid → last LOCAL chat (anti-spam)
    std::map<uint32_t, PendingValidation> pendingValid;   // requestId -> in flight
    std::map<uint32_t, uint64_t>      lastReqMs;          // uuid → last REQ sent (throttle)

    // Read-only observers: "addr:port" → { endpoint, lease deadline }. Kept apart from `clientMap` on
    // purpose (see the PKT_OBSERVE branch below).
    struct ObserverEntry { std::pair<std::string, int> endpoint; uint64_t leaseUntilMs; };
    std::map<std::string, ObserverEntry> observers;
    const uint64_t OBSERVER_LEASE_MS =
        (uint64_t)std::atoi(std::getenv("OBSERVER_LEASE_MS") ? std::getenv("OBSERVER_LEASE_MS") : "5000");
    // Shared secret an observer must present. NO DEFAULT ON PURPOSE: a default would be published in
    // this file and would therefore be no secret, and an operator who never sets one would be running
    // with the feed wide open while believing it was protected. Unset means observers are refused.
    const std::string observeToken =
        std::getenv("DGS_OBSERVE_TOKEN") ? std::getenv("DGS_OBSERVE_TOKEN") : "";
    const size_t OBSERVER_MAX =
        (size_t)std::atoi(std::getenv("OBSERVER_MAX") ? std::getenv("OBSERVER_MAX") : "8");
    std::cout << "[ZoneNode] observers: "
              << (observeToken.empty() ? "DISABLED (DGS_OBSERVE_TOKEN unset)"
                                       : "token required, max " + std::to_string(OBSERVER_MAX))
              << std::endl;
    std::map<uint32_t, uint64_t>      bannedUntilMs;      // P7: uuid → blocked until (0 = forever)

    // How long an un-acked handoff waits before being re-sent. It is a retry, not a spin: the entity
    // stays owned and simulated here in the meantime.
    const uint64_t HANDOFF_RETRY_MS =
        (uint64_t)std::atoi(std::getenv("HANDOFF_RETRY_MS") ? std::getenv("HANDOFF_RETRY_MS") : "1000");

    // How far a player is told about. 0 = tell everybody about everything, which is what the capacity
    // numbers in the README were measured against.
    // Same as the social node's per-uuid limit: one person, one voice, at a human rate.
    const uint64_t LOCAL_CHAT_RATE_MS =
        (uint64_t)std::atoi(std::getenv("CHAT_RATE_MS") ? std::getenv("CHAT_RATE_MS") : "500");

    // El techo de velocidad de un JUGADOR, en metros por segundo, y es del servidor porque el cliente
    // no puede declarar el numero con el que se le juzga (ver la nota en la entrada UDP). Por defecto
    // 150: por encima de correr, saltar y caer —la gravedad llega a ~50 m/s en cinco segundos— y muy
    // por debajo de un teleport. Un juego con vehiculos tendra que subirlo, y ese es exactamente el
    // tipo de decision que pertenece al despliegue y no al jugador.
    const float CLIENT_MAX_SPEED_MPS =
        (float)std::atof(std::getenv("CLIENT_MAX_SPEED_MPS") ? std::getenv("CLIENT_MAX_SPEED_MPS") : "150");

    const float INTEREST_RADIUS_M =
        (float)std::atof(std::getenv("INTEREST_RADIUS_M") ? std::getenv("INTEREST_RADIUS_M") : "0");

    uint32_t reqSeq      = 1;
    uint32_t statSent    = 0;
    uint32_t statTimeout = 0;
    uint32_t statViolations = 0;
    uint32_t statRejected   = 0;   // violations caught by S1 (never even emitted)
    uint64_t lastStatusMs  = 0;
    uint8_t  cbState       = 0;    // 0=CLOSED, 1=OPEN (temporary eviction)
    uint64_t cbOpenUntil   = 0;
    uint32_t cbOpenCount   = 0;
    constexpr uint32_t CB_MAX_OPEN = 3;              // how many trips before we fail closed
    constexpr uint64_t CB_OPEN_MS  = 2000;           // how long it stays OPEN before retrying

    auto circuitBreakerOk = [&]() -> bool {
        if (!validated) return true;                       // fail-open: S1 only
        if (cbState == 1 && nowMs() < cbOpenUntil) return false;
        return true;
    };

    // ⚠️ THE TICK WAS NOT A TICK, IT WAS A QUEUE OF BLOCKING WAITS. Measured with `tools/load_zone`:
    // the zone delivered 4.6 complete snapshots a second where the design says 10, at EVERY population
    // — one idle player included — and its own `performance` metric reported 211 us of work, so nothing
    // in its telemetry could ever show it. The waits stacked up per tick: 100 ms of `usleep`, plus a
    // 100 ms receive timeout on the HEAD socket, plus 5 + 5 for the validator and the social node, plus
    // 10 for the last (empty) UDP read = ~220 ms. Isolated by making the fake head chatter every 20 ms,
    // which removes only that 100 ms wait: the rate went 4.6 -> 8.7 Hz and player latency 142 -> 37 ms.
    //
    // So: ONE `poll` decides which sockets have something, and only those are read. An empty socket now
    // costs nothing instead of its full timeout. `poll` before a blocking read also keeps `recvAll`
    // safe — a readable socket delivers, whereas making these sockets non-blocking would let a partial
    // read return EAGAIN mid-message and desynchronise the stream for good.
    // ⚠️ AND IT ASKS TLS TOO. A TLS record can carry several messages, so OpenSSL may be holding
    // decrypted bytes the kernel no longer reports as readable — `poll` says "nothing" and the node
    // sits on data it already has. It is the classic way a TLS port goes quiet under load, and it
    // would have looked exactly like the blocking-tick bug this gate was written to fix.
    auto readable = [](DGS::TCPSocket& s, int fd, int timeoutMs) -> bool {
        if (fd < 0) return false;
        if (s.pending(fd)) return true;                     // already decrypted and waiting
        pollfd p{ fd, POLLIN, 0 };
        if (!(::poll(&p, 1, timeoutMs) > 0 && (p.revents & POLLIN))) return false;
        // Readable at the kernel is not the same as readable at the application once TLS is on: a
        // TLS 1.3 `NewSessionTicket` arriving after the handshake looks exactly like data here, and
        // the blocking read that follows would wait for a message nobody sent. Ask the layer that
        // can tell the difference.
        return s.tlsEnabled() ? s.pending(fd) : true;
    };

    // The region this zone serves, and the size of a chunk. Read ONCE: they came from the environment
    // and were re-read on every tick — nine `getenv` + `atoi` per tick for values that cannot change
    // inside a process. Hoisting them is also what lets the restore below know what to ask for, which
    // is the whole point: a zone has to know its own region before anyone can hand it back.
    //
    // ⚠️ THE DEFAULT USED TO BE 0..100 IN EVERY AXIS, and with `CHUNK_SIZE_*` also defaulting to 1.0
    // that is a box of ONE HUNDRED METRES. A game that spawns its players anywhere else got an empty
    // ZoneResponse and no zone at all, silently. The default is now "the whole world", so the
    // orchestrator's model — start with one zone covering everything and let SPLIT subdivide it under
    // load — works without anyone having to guess where the spawn points are.
    //
    // Why ±1e6 chunks and not INT32_MIN..INT32_MAX: three places do arithmetic on these bounds that
    // would overflow at the extremes — `isNearBorder` computes `xMax - threshold`, the drain computes
    // `(xMin + xMax) / 2` and `xMax - xMin`, and the region hash computes `xMin * 31 + yMin * 17`.
    // ±1e6 keeps every one of those inside int32 and is still 2000000 km across at 1 km chunks, which
    // is 50x the circumference of the Earth.
    // ⚠️ THE MAXIMA ARE NOT const: CMD_TRANSFER_SERVER (a SPLIT) shrinks one of them at runtime, and
    // which one depends on the axis the orchestrator chose. See the PKT_COMMAND handler.
    int32_t       xMin = std::atoi(std::getenv("CHUNK_X_MIN") ? std::getenv("CHUNK_X_MIN") : "-1000000");
    int32_t       xMax = std::atoi(std::getenv("CHUNK_X_MAX") ? std::getenv("CHUNK_X_MAX") : "1000000");
    int32_t       yMin = std::atoi(std::getenv("CHUNK_Y_MIN") ? std::getenv("CHUNK_Y_MIN") : "-1000000");
    int32_t       yMax = std::atoi(std::getenv("CHUNK_Y_MAX") ? std::getenv("CHUNK_Y_MAX") : "1000000");
    int32_t       zMin = std::atoi(std::getenv("CHUNK_Z_MIN") ? std::getenv("CHUNK_Z_MIN") : "-1000000");
    int32_t       zMax = std::atoi(std::getenv("CHUNK_Z_MAX") ? std::getenv("CHUNK_Z_MAX") : "1000000");
    const float   csX  = (float)std::atof(std::getenv("CHUNK_SIZE_X") ? std::getenv("CHUNK_SIZE_X") : "1.0");
    const float   csY  = (float)std::atof(std::getenv("CHUNK_SIZE_Y") ? std::getenv("CHUNK_SIZE_Y") : "1.0");
    const float   csZ  = (float)std::atof(std::getenv("CHUNK_SIZE_Z") ? std::getenv("CHUNK_SIZE_Z") : "1.0");

    // ── RESTORE FROM PERSISTENCE ────────────────────────────────────────────────────────────────
    // A zone used to start EMPTY, always. Everything the cluster had ever written to Mongo was
    // unreachable — the persistence node only wrote, and nothing could read one entity back, let alone
    // a region. So a restart lost the world: not because the data was gone, but because nothing asked.
    //
    // This asks. One PKT_PERSIST_RANGE for exactly the chunks this zone serves, and the answer is a
    // stream of entities ended by a PKT_NONE. It is BEST EFFORT on purpose: a zone must come up with no
    // persistence node at all (that is how every test that does not care about it runs, and how a
    // cluster survives losing its database), so a failure here is a log line, not a refusal to start.
    //
    // Restored entities take an ORDINARY LEASE. They are not privileged: a player who does not come
    // back is purged by the same GC as anyone else once the lease runs out. The restore repopulates the
    // world, it does not pin it.
    // ── THE PERSISTENCE LINK ────────────────────────────────────────────────────────────────────
    // ⚠️ NOTHING HERE MAY RUN ON THE TICK THREAD. Not the connect, and above all not the name
    // resolution that precedes it: `connect`'s deadline covers the TCP handshake and nothing else, and
    // a `PERSISTENCE_HOST` that does not resolve cost **11.2 seconds** on this machine. Measured twice,
    // because I made the same mistake twice:
    //   · restoring inline before the main loop delayed the zone's FIRST TICK by those 11 s, which
    //     turned four unrelated end-to-end tests red by starving its metrics;
    //   · then the write-through's periodic reconnect did it again from INSIDE the loop, freezing the
    //     tick for 11 s every retry — `zone_policy_e2e` caught it as "the tick freezes", which is
    //     precisely the failure that test exists to catch, and `viewer_e2e` as a lease that would not
    //     expire. With `ZONE_PERSIST_MS=0` both went green, which is what identified it.
    // So every connect happens on a worker thread and the loop only ever touches a socket that is
    // already connected. A zone must start, and keep ticking, whether or not there is a database.
    const char* persHost = std::getenv("PERSISTENCE_HOST") ? std::getenv("PERSISTENCE_HOST") : "persistence";
    const int   persPort = std::atoi(std::getenv("PERSISTENCE_PORT") ? std::getenv("PERSISTENCE_PORT") : "42429");
    const bool  restoreEnabled = !(std::getenv("ZONE_RESTORE") &&
                                   std::string(std::getenv("ZONE_RESTORE")) == "0");

    DGS::TCPSocket tcp_persistence;
    bool     persistenceUp = false;
    uint64_t lastPersistMs = 0;
    uint64_t lastLinkTryMs = 0;

    std::mutex                        linkMtx;
    std::unique_ptr<DGS::TCPSocket>   linkReady;        // connected by the worker, taken by the loop
    std::vector<DGS::EntityTransfer>  restorePending;   // what the region query brought back
    std::atomic<bool>                 linkPending{false};
    bool                              restoreMerged = !restoreEnabled;
    std::thread                       linkThread;

    // Connects on a worker thread and, the first time, asks for this zone's region. The socket is left
    // connected and handed to the loop; the loop never resolves or connects anything itself.
    auto spawnLink = [&](bool doRestore) {
        if (linkPending.load()) return;
        if (linkThread.joinable()) linkThread.join();
        linkPending = true;
        lastLinkTryMs = nowMs();
        linkThread = std::thread([&, doRestore]() {
            auto sock = std::unique_ptr<DGS::TCPSocket>(new DGS::TCPSocket());
            if (sock->connect(persHost, persPort, 500)) DGS::sendAuth(*sock);
            else
            {
                if (doRestore)
                    std::cout << "[ZoneNode] no persistence at " << persHost << ":" << persPort
                              << " -> starting with an empty world" << std::endl;
                linkPending = false;
                return;
            }

            if (doRestore)
            {
                DGS::PersistRange range{};
                range.chunkXMin = xMin; range.chunkXMax = xMax;
                range.chunkYMin = yMin; range.chunkYMax = yMax;
                range.chunkZMin = zMin; range.chunkZMax = zMax;
                range.limit     = (uint32_t)std::atoi(std::getenv("ZONE_RESTORE_MAX")
                                                      ? std::getenv("ZONE_RESTORE_MAX") : "1024");

                DGS::Packet req; req.packPersistRange(range);
                sock->send(sock->getSocketFD(), req.getRawData(), req.getSize());

                // Read answers until the PKT_NONE terminator or the deadline. The terminator is what
                // lets "this region is empty" be an ANSWER instead of a timeout. One receive is one
                // packet: `TCPSocket` length-prefixes every message.
                const int restoreMs = std::atoi(std::getenv("ZONE_RESTORE_MS")
                                                ? std::getenv("ZONE_RESTORE_MS") : "2000");
                uint8_t buf[8192];
                const uint64_t deadline = nowMs() + (uint64_t)restoreMs;
                bool done = false;
                std::vector<DGS::EntityTransfer> got;

                while (!done && nowMs() < deadline)
                {
                    pollfd pfd{ sock->getSocketFD(), POLLIN, 0 };
                    if (!sock->pending(sock->getSocketFD()))
                    {
                        if (::poll(&pfd, 1, 50) <= 0 || !(pfd.revents & POLLIN)) continue;
                        if (sock->tlsEnabled() && !sock->pending(sock->getSocketFD())) continue;
                    }
                    const int n = sock->receive(sock->getSocketFD(), buf, sizeof(buf));
                    if (n <= 0) break;

                    {
                        DGS::Packet p; p.setBuffer(buf, (size_t)n);
                        if (p.getType() == DGS::PKT_NONE) { done = true; break; }

                        DGS::EntityTransfer e{};
                        if (!p.tryUnpackEntityTransfer(e)) continue;

                        // Trust the bounds we ASKED for, not the answer: a document outside this zone
                        // belongs to a neighbour, and claiming it would give one entity two owners —
                        // the single thing this architecture exists to prevent.
                        if (e.chunkX < xMin || e.chunkX > xMax ||
                            e.chunkY < yMin || e.chunkY > yMax ||
                            e.chunkZ < zMin || e.chunkZ > zMax) continue;

                        got.push_back(e);
                    }
                }
                std::cout << "[ZoneNode] restored " << got.size() << " entities from persistence"
                          << (done ? "" : " (answer incomplete: deadline or closed connection)")
                          << std::endl;
                { std::lock_guard<std::mutex> lk(linkMtx); restorePending = std::move(got); }
            }

            { std::lock_guard<std::mutex> lk(linkMtx); linkReady = std::move(sock); }
            linkPending = false;
        });
    };

    if (!restoreEnabled) std::cout << "[ZoneNode] restore disabled (ZONE_RESTORE=0)" << std::endl;
    spawnLink(restoreEnabled);

    while (true)
    {
        const auto tickStart = std::chrono::steady_clock::now();

        // The connected socket, and the restored world, arrive here — whenever the worker got round to
        // them. Taking them costs a mutex and nothing else: no resolving, no connecting, no waiting.
        {
            std::lock_guard<std::mutex> lk(linkMtx);
            if (linkReady)
            {
                tcp_persistence = std::move(*linkReady);
                linkReady.reset();
                persistenceUp = true;
            }
        }

        if (!restoreMerged && !linkPending.load())
        {
            restoreMerged = true;

            std::vector<DGS::EntityTransfer> incoming;
            { std::lock_guard<std::mutex> lk(linkMtx); incoming = std::move(restorePending); }

            const uint64_t lease = (uint64_t)std::atoi(std::getenv("ENTITY_LEASE_MS")
                                                       ? std::getenv("ENTITY_LEASE_MS") : "3000");
            int merged = 0, skipped = 0;
            for (const auto& e : incoming)
            {
                // A client already talking about this entity is newer than anything stored. Restoring
                // over it would drag a connected player back to their last save.
                if (lastActiveSeen.count(e.uuid)) { ++skipped; continue; }

                entities.push_back(e);
                entityOwnedUntil[e.uuid] = nowMs() + lease;
                lastActiveSeen[e.uuid]   = nowMs();
                lastPosition[e.uuid] = { e.chunkX * csX + e.pos[0],
                                         e.chunkY * csY + e.pos[1],
                                         e.chunkZ * csZ + e.pos[2],
                                         e.stats.speed[0], nowMs() };
                ++merged;
            }
            if (merged > 0 || skipped > 0)
                std::cout << "[ZoneNode] merged " << merged << " restored entities"
                          << (skipped ? " (" + std::to_string(skipped) + " already live, kept)" : "")
                          << std::endl;
        }

        // Receive UDP from clients — DRAIN the socket, do not take one datagram per tick.
        //
        // ⚠️ THIS USED TO READ EXACTLY ONE DATAGRAM PER TICK, and the tick is 100 ms. That is a ceiling
        // of TEN datagrams a second for the WHOLE zone, shared by every player in it: a single client
        // at 20 Hz already overruns it, and everything past the ceiling piles up in the socket buffer
        // and arrives late until it is dropped. Found from the other end — the viewer showed an entity
        // frozen in place while the client was plainly moving, because the zone was still chewing
        // through datagrams from seconds earlier.
        //
        // ⚠️ AND THE `continue`s INSIDE MEANT "SKIP THE WHOLE TICK". They sat in a bare block inside the
        // main loop, so a banned player or one datagram rejected by S1 cost that tick its simulation
        // step, its broadcast, its ghost deltas, its validation timeouts and its metrics — for
        // everybody. A cheater sending junk could hold the zone's other players still. Inside this loop
        // they mean what they always read as: skip THIS datagram, take the next.
        //
        // Bounded on purpose: a flood must not let ingest starve the rest of the tick.
        const int UDP_DRAIN_MAX =
            std::atoi(std::getenv("ZONE_UDP_DRAIN_MAX") ? std::getenv("ZONE_UDP_DRAIN_MAX") : "256");
        for (int drained = 0; drained < UDP_DRAIN_MAX; ++drained)
        {
            uint8_t udpBuf[sizeof(DGS::EntityTransfer)];
            std::string clientAddr;
            int clientPort = 0;
            int udpBytes = udp_zone_node.receive(udpBuf, sizeof(udpBuf), clientAddr, clientPort);
            if (udpBytes <= 0) break;               // nothing left waiting: on with the tick
            g_bytesRx += (uint64_t)udpBytes;

            // ⚠️ QUE LLEGA, cuando hay que averiguar por que algo no pasa. Un datagrama de un tipo
            // que esta cadena no reconoce se ignora en silencio, asi que "no llego" y "llego y no lo
            // entendi" se ven exactamente igual desde fuera. Con `ZONE_TRACE_UDP=1` se distinguen.
            // Apagado por defecto: son diez lineas por segundo y por jugador.
            {
                static const bool traceUdp = std::getenv("ZONE_TRACE_UDP") != nullptr;
                if (traceUdp)
                    std::cout << "[ZoneNode] udp tipo=" << (int)udpBuf[0] << " " << udpBytes
                              << " bytes de " << clientAddr << ":" << clientPort << std::endl;
            }

            // READ-ONLY OBSERVER (viewer / ops tooling). A PKT_OBSERVE datagram subscribes the sender
            // to the same stream the zone already broadcasts to its clients. It is deliberately NOT
            // routed through the client path: an observer never lands in `clientMap`, never gets a
            // uuid, never takes a lease and never becomes an entity — subscribing must not be a way to
            // put something into the world. And it is a LEASE, refreshed by re-sending: a viewer that
            // closes its window stops being fed instead of collecting 10 datagrams a second for ever.
            //
            // ⚠️ IT USED TO BE ONE UNAUTHENTICATED BYTE. Anyone who could reach this UDP port got the
            // position of every entity in the zone ten times a second — the exact feed a wallhack
            // needs, handed over on request, and with no way to tell afterwards that it had happened.
            // Now it carries a token and the zone FAILS CLOSED: with `DGS_OBSERVE_TOKEN` unset there
            // are no observers at all, so forgetting to configure it cannot leave the feed open.
            //
            // What this does NOT do, and it must be said: the token travels in clear in a datagram.
            // It closes "anyone who can reach the port", not "anyone who can read the wire" — on an
            // untrusted path it is sniffable and replayable. The real answer is DTLS or a private
            // network for the observer plane; this is the floor, not the ceiling.
            if (udpBytes >= 1 && udpBuf[0] == DGS::PKT_OBSERVE)
            {
                if (observeToken.empty()) { statRejected++; continue; }   // fail closed

                std::string offered;
                try {
                    DGS::Packet op;
                    op.setBuffer(udpBuf, (size_t)udpBytes);
                    op.unpackPacketType();
                    offered = op.readString();
                } catch (const std::exception&) { statRejected++; continue; }

                // Length first, then a comparison that does not return early on the first wrong byte.
                bool ok = offered.size() == observeToken.size();
                if (ok) {
                    unsigned char diff = 0;
                    for (size_t i = 0; i < offered.size(); ++i)
                        diff |= (unsigned char)(offered[i] ^ observeToken[i]);
                    ok = (diff == 0);
                }
                const std::string key = clientAddr + ":" + std::to_string(clientPort);
                if (!ok)
                {
                    statRejected++;
                    std::cout << "[ZoneNode] observer REJECTED (bad token) " << key << std::endl;
                    continue;
                }
                // A cap even for authenticated observers: each one multiplies the zone's egress by a
                // whole extra client, so a leaked token must not turn into an amplifier.
                if (!observers.count(key) && observers.size() >= OBSERVER_MAX)
                {
                    statRejected++;
                    std::cout << "[ZoneNode] observer REJECTED (limit " << OBSERVER_MAX << ") "
                              << key << std::endl;
                    continue;
                }
                if (!observers.count(key))
                    std::cout << "[ZoneNode] observer subscribed " << key << std::endl;
                observers[key] = { { clientAddr, clientPort }, nowMs() + OBSERVER_LEASE_MS };
            }
            // ── LOCAL CHAT (§3.7 `CHAT_LOCAL`) ──────────────────────────────────────────────────
            // ⚠️ THIS CHANNEL WAS IMPLEMENTED BY NOBODY. The social node returns early for it with the
            // comment "the owning zone emits it", and the zone had zero mentions of chat: a message on
            // the local channel went nowhere at all. It belongs here because "local" means WHO IS NEAR
            // YOU, and the zone is the only process that knows — it is already deciding exactly that,
            // every tick, for the entity broadcast.
            //
            // So it travels the plane it belongs to: the client's UDP link, sealed like everything else
            // on it, filtered by the same interest radius, and never touching the social node or the
            // head. Nothing else in the system has to grow a subscription model for it.
            else if (udpBuf[0] == DGS::PKT_CHAT)
            {
                DGS::ChatMessage cm{};
                try {
                    DGS::Packet cp;
                    cp.setBuffer(udpBuf, (size_t)udpBytes);
                    cm = cp.unpackChatMessage();
                } catch (const std::exception&) { statRejected++; continue; }

                if (cm.channel != DGS::CHAT_LOCAL) { statRejected++; continue; }   // not ours

                // The ban list this zone already keeps for movement applies to speech as well: a
                // banned player who cannot enter the world has no business being heard in it.
                auto bIt = bannedUntilMs.find(cm.uuid);
                if (bIt != bannedUntilMs.end() && (bIt->second == 0 || nowMs() < bIt->second))
                { statRejected++; continue; }

                // Same rate as the social node's, and for the same reason: one uuid must not be able
                // to make the zone fan out faster than a person can type.
                const uint64_t nowChat = nowMs();
                auto lc = lastChatAt.find(cm.uuid);
                if (lc != lastChatAt.end() && nowChat - lc->second < LOCAL_CHAT_RATE_MS)
                { statRejected++; continue; }
                lastChatAt[cm.uuid] = nowChat;

                // Where the speaker is. Without a position we cannot say who is near them, and
                // shouting to the whole zone would be the opposite of what this channel means.
                auto sp = lastPosition.find(cm.uuid);
                if (sp == lastPosition.end()) { statRejected++; continue; }

                cm.timestampMs = (uint32_t)nowChat;
                DGS::Packet outChat; outChat.pack(cm);

                // Sealed ONCE for everybody, exactly as the entity broadcast is: the alternative is
                // encrypting the same sentence once per listener.
                std::vector<uint8_t> sealed;
                DGS::sealForUdp(outChat.getRawData(), outChat.getSize(), sealed);

                int heard = 0;
                for (const auto& [ouuid, endpoint] : clientMap)
                {
                    if (ouuid == cm.uuid) continue;              // you do not need to be told what you said
                    auto op = lastPosition.find(ouuid);
                    if (op == lastPosition.end()) continue;
                    if (INTEREST_RADIUS_M > 0.0f)
                    {
                        const float dx = op->second.gx - sp->second.gx;
                        const float dy = op->second.gy - sp->second.gy;
                        const float dz = op->second.gz - sp->second.gz;
                        if (dx*dx + dy*dy + dz*dz > INTEREST_RADIUS_M * INTEREST_RADIUS_M) continue;
                    }
                    udp_zone_node.sendRaw(endpoint.first, endpoint.second, sealed.data(), sealed.size());
                    g_bytesTx += DGS::udpWireSize(outChat.getSize());
                    ++heard;
                }
                std::cout << "[ZoneNode] local chat uuid=" << cm.uuid << " heard by " << heard
                          << std::endl;
            }
            // A client's position update. Recognised by its TYPE BYTE, not by its size: the size rule
            // could not survive `dataSize` being honoured, and it was never sound anyway — a truncated
            // datagram simply stopped matching and was dropped in silence (see `net_degraded`).
            else if (udpBuf[0] == DGS::PKT_ACTION)
            {
                // ⚠️ AQUI EMPIEZA LO QUE EL JUGADOR PIDE, y hasta hoy no habia por donde pedir nada.
                // El validador distingue `kind=0` (movimiento, falla abierto suavizado) de `kind=1`
                // (accion, falla CERRADO) desde hace tiempo, y `action_e2e` lo cubre — pero la zona
                // ponia `kind = 0` a pelo en todas sus peticiones, asi que la ruta segura no la
                // alcanzaba ningun juego. Esto la conecta.
                DGS::ActionRequest a{};
                DGS::Packet ap;
                ap.setBuffer(udpBuf, (size_t)udpBytes);
                // ⚠️ UNA ACCION DESCARTADA NO PUEDE SER MUDA. Todas las salidas de aqui eran un
                // `continue` sin una linea, asi que "el objeto no aparece" no distinguia entre no
                // haber llegado, no entenderse, no estar permitida y no poder preguntar al validador.
                // Me costo una tarde de sondas averiguar cual de las cuatro era.
                if (!ap.tryUnpackActionRequest(a))
                {
                    std::cout << "[ZoneNode] ACCION RECHAZADA: datagrama ilegible (" << udpBytes
                              << " bytes)" << std::endl;
                    statRejected++; continue;
                }

                // ⚠️ APUNTAR DE DONDE VIENE, o no hay a quien contestarle. La direccion de un cliente
                // solo se registraba al recibir su ENTIDAD, y una peticion no es una entidad: quien
                // pedia algo sin haberse movido antes no existia para el mapa de clientes, asi que el
                // veredicto no tenia destino. Medido: el rechazo no llegaba nunca.
                clientMap[a.actor] = { clientAddr, clientPort };

                // Un baneado no pide nada.
                auto banA = bannedUntilMs.find(a.actor);
                if (banA != bannedUntilMs.end() && (banA->second == 0 || nowMs() < banA->second))
                {
                    std::cout << "[ZoneNode] ACCION RECHAZADA: " << a.actor << " esta baneado"
                              << std::endl;
                    statRejected++; continue;
                }

                if (a.action != DGS::ACTION_PLACE &&
                    a.action != DGS::ACTION_DROP &&
                    a.action != DGS::ACTION_REMOVE &&
                    a.action != DGS::ACTION_MOVE)
                {
                    std::cout << "[ZoneNode] ACCION RECHAZADA: verbo " << a.action << " desconocido"
                              << std::endl;
                    statRejected++; continue;
                }
                // DROP y PLACE CREAN; REMOVE y MOVE operan sobre algo que ya existe.
                const bool creates = (a.action == DGS::ACTION_PLACE || a.action == DGS::ACTION_DROP);

                // ⚠️ SOBRE ALGO QUE EXISTE Y QUE ES DEL MUNDO. REMOVE y MOVE llevan un uuid elegido
                // por quien pide —no hay otra forma de decir cual—, asi que el servidor comprueba
                // ANTES de preguntarle a nadie que ese objeto existe aqui y que es un objeto del
                // mundo. Sin esto, un uuid inventado se convierte en una peticion valida sobre la
                // entidad de otro: bastaria con acertar un numero para borrar a un jugador.
                DGS::EntityTransfer* victim = nullptr;
                if (!creates)
                {
                    for (auto& ent : entities)
                        if (ent.uuid == a.target) { victim = &ent; break; }
                    if (!victim || !(victim->state & DGS::STATE_WORLD_OWNED))
                    {
                        std::cout << "[ZoneNode] ACCION RECHAZADA: " << a.target
                                  << (victim ? " no es un objeto del mundo" : " no existe aqui")
                                  << std::endl;
                        statRejected++;
                        continue;
                    }
                }

                // ⚠️ EL UUID LO PONE EL SERVIDOR. Si lo eligiera quien pide, acertar un numero
                // bastaria para sobrescribir el objeto de otro. Se deriva del sitio y de lo que es,
                // asi que pedir dos veces lo mismo en el mismo punto no crea gemelos, y el bit alto
                // separa los objetos del mundo del espacio de los jugadores.
                uint64_t h = 1469598103934665603ull;
                auto mix = [&h](const void* p, size_t n) {
                    const uint8_t* b = (const uint8_t*)p;
                    for (size_t i = 0; i < n; ++i) { h ^= b[i]; h *= 1099511628211ull; }
                };
                const int32_t qx = (int32_t)std::llround(a.pos[0] * 10.0f);
                const int32_t qz = (int32_t)std::llround(a.pos[2] * 10.0f);
                mix(&a.chunkX, sizeof(a.chunkX)); mix(&a.chunkY, sizeof(a.chunkY));
                mix(&a.chunkZ, sizeof(a.chunkZ)); mix(&qx, sizeof(qx)); mix(&qz, sizeof(qz));
                if (a.dataSize) mix(a.data, a.dataSize);
                const uint32_t newUuid = 0x80000000u | (uint32_t)(h & 0x7FFFFFFFu);

                // Lo que se PROPONE. En PLACE es un objeto nuevo; en REMOVE y MOVE es el que ya
                // existe, con el cambio pedido — el validador tiene que ver sobre que decide.
                DGS::EntityTransfer prop{};
                if (!creates)
                {
                    prop = *victim;
                    if (a.action == DGS::ACTION_MOVE)
                    {
                        prop.chunkX = a.chunkX; prop.chunkY = a.chunkY; prop.chunkZ = a.chunkZ;
                        prop.pos[0] = a.pos[0]; prop.pos[1] = a.pos[1]; prop.pos[2] = a.pos[2];
                        prop.angle  = a.angle;
                    }
                }
                else
                {
                prop.uuid   = newUuid;
                prop.type   = DGS::ENT_ITEM;
                prop.state  = DGS::STATE_WORLD_OWNED;   // lo pone el SERVIDOR, no el cliente
                prop.chunkX = a.chunkX; prop.chunkY = a.chunkY; prop.chunkZ = a.chunkZ;
                prop.pos[0] = a.pos[0]; prop.pos[1] = a.pos[1]; prop.pos[2] = a.pos[2];
                prop.angle  = a.angle;
                prop.dataSize = a.dataSize;
                if (a.dataSize) std::memcpy(prop.data, a.data, a.dataSize);
                }

                // Sin validador alcanzable no se coloca nada. Es lo contrario del movimiento, y a
                // proposito: una posicion mal aceptada se corrige sola en el siguiente paquete, un
                // objeto creado de mas se queda.
                if (!circuitBreakerOk())
                {
                    std::cout << "[ZoneNode] ACCION RECHAZADA (sin validador): actor=" << a.actor
                              << std::endl;
                    statRejected++;
                    continue;
                }

                // ⚠️ EL BLOB QUE EL MODULO ESPERA, no el que a mi me venia bien. El ABI lo tiene
                // escrito: `validateAction(zone, actor, blob, n, world)` con el blob = `ActionHeader`
                // + el payload del juego. Y el validador pasa `r.entityUuid` como ACTOR — asi que ese
                // campo lleva QUIEN pide, no el objeto propuesto, o el modulo no puede escribir una
                // sola regla sobre quien hace las cosas.
                //
                // La entidad que se VALIDA y la que se APLICA no son la misma: la primera lleva el
                // blob de la accion, la segunda el payload limpio del juego (el modelo). Mezclarlas
                // haria que la mesa que acaba en el mundo llevara dentro la cabecera de la peticion.
                DGS::EntityTransfer forValidation = prop;
                {
                    DGS::ActionHeader hdr{};
                    hdr.verb   = a.action;                 // mismos numeros que ActionVerb
                    hdr.flags  = 0;
                    hdr.target = a.target;
                    hdr.at[0]  = prop.chunkX * csX + prop.pos[0];
                    hdr.at[1]  = prop.chunkY * csY + prop.pos[1];
                    hdr.at[2]  = prop.chunkZ * csZ + prop.pos[2];
                    hdr.amount = 1.0f;

                    const uint16_t hdrN = (uint16_t)sizeof(hdr);
                    const uint16_t payN = (uint16_t)std::min<size_t>(a.dataSize,
                                              DGS::MAX_ENTITY_DATA - hdrN);
                    std::memcpy(forValidation.data, &hdr, hdrN);
                    if (payN) std::memcpy(forValidation.data + hdrN, a.data, payN);
                    forValidation.dataSize = (uint16_t)(hdrN + payN);
                }

                DGS::ValidateRequest req{};
                req.requestId  = reqSeq++;
                req.entityUuid = a.actor;    // el ABI lo recibe como `actor`
                req.ownerZone  = (uint32_t)(xMin * 31 + yMin * 17 + zMin);
                req.moduleId   = 0;
                req.kind       = 1;          // ACCION: la ruta que falla cerrado
                req.entity     = forValidation;
                req.maxSpeed   = 0.0f;
                req.dtSeconds  = 0.0f;

                DGS::Packet pReq; pReq.pack(req);
                if (tcp_validator.send(tcp_validator.getSocketFD(), pReq.getRawData(), pReq.getSize()))
                {
                    g_bytesTx += pReq.getSize();
                    PendingValidation pv{};
                    pv.deadlineMs    = nowMs() + 500;
                    pv.entity        = prop;
                    pv.ownerZone     = req.ownerZone;
                    pv.applyOnAccept = true;
                    pv.pendingAction = a.action;
                    pv.actor         = a.actor;
                    pv.clientReqId   = a.requestId;
                    pendingValid[req.requestId] = pv;
                    statSent++;
                    std::cout << "[ZoneNode] ACCION " << a.action << " pedida por " << a.actor
                              << " -> validando (uuid " << prop.uuid << ")" << std::endl;
                }
                else
                {
                    std::cout << "[ZoneNode] ACCION RECHAZADA: no se pudo enviar al validador"
                              << std::endl;
                    statRejected++;
                }
                continue;
            }
            else if (udpBuf[0] == DGS::PKT_ENTITY_TRANSFER)
            {
                DGS::EntityTransfer e{};
                DGS::Packet ep;
                ep.setBuffer(udpBuf, (size_t)udpBytes);
                if (!ep.tryUnpackEntityTransfer(e))
                {
                    // Malformed, truncated, or a lying `dataSize`. It costs this datagram and nothing
                    // else: a node must not die on what a stranger sends it.
                    statRejected++;
                    continue;
                }
                clientMap[e.uuid] = { clientAddr, clientPort };

                // ⚠️ LO QUE UN CLIENTE DECLARA SOBRE SI MISMO NO VALE NADA, Y HASTA AQUI VALIA TODO.
                // Este datagrama viene por el enlace UDP de un JUGADOR (los traspasos entre zonas van
                // por TCP), y llegaba entero: tipo de entidad, bits de estado y stats, tal cual, a
                // creer. Tres cosas se creian que no se pueden creer:
                //
                //   · `stats.speed` — que es EL NUMERO CON EL QUE SE LE JUZGA. S1 comprueba
                //     `maxDist = maxSpeed * dt + 1 m` contra el ultimo valor recibido, o sea que el
                //     anti-trampas le preguntaba al sospechoso cual era el limite de velocidad. Un
                //     cliente con `maxSpeed = 1e9` no era rechazado jamas.
                //   · `type` — un jugador podia declararse ENT_ITEM y dejar de ser tratado como quien
                //     se mueve.
                //   · `state` — y con el `STATE_WORLD_OWNED`, el bit que EXIME del recolector de
                //     leases. Un cliente podia sembrar el mundo de objetos que no caducan nunca.
                //
                // Quien entro por aqui ES un jugador, y el techo de velocidad es del SERVIDOR. Lo que
                // el juego tenga de suyo —vida, dano, lo que sea, que cambia de un juego a otro y no
                // tiene por que parecerse a `Stats`— viaja en `data`, que es opaco a proposito y que
                // este nodo no interpreta.
                e.type  = DGS::ENT_PLAYER;
                e.state = (DGS::EntityState)(e.state & ~DGS::STATE_WORLD_OWNED);
                e.stats.speed[0] = e.stats.speed[1] = e.stats.speed[2] = CLIENT_MAX_SPEED_MPS;

                // ---- P7 (§3.7): the zone applies what the social node decides (it NEVER decides).
                // A banned account → ENTRY is blocked (the client keeps sending positions, but the zone
                // neither serves nor simulates them). The zone does NOT judge: it applies the ban.
                auto banIt = bannedUntilMs.find(e.uuid);
                if (banIt != bannedUntilMs.end() && (banIt->second == 0 || nowMs() < banIt->second))
                {
                    statRejected++;
                    continue;   // banned: not propagated
                }

                // ---- P2: local S1 pre-check + validation request ----
                uint64_t now = nowMs();
                auto pit = lastPosition.find(e.uuid);
                if (pit != lastPosition.end())
                {
                    float dt = (now - pit->second.tsMs) / 1000.0f;
                    if (!s1Plausible(e, csX, csY, csZ, pit->second, dt))
                    {
                        // S1 travellers (teleport / impossible speed): discarded in the zone.
                        statRejected++;
                        std::cout << "[ZoneNode] S1 blocked uuid=" << e.uuid << std::endl;
                        continue;   // neither propagated nor sent for validation
                    }

                    // Breaker closed and a validator present: ask for a verdict.
                    auto lastReq = lastReqMs.find(e.uuid);
                    bool shouldAsk = circuitBreakerOk() &&
                                     (lastReq == lastReqMs.end() || (now - lastReq->second) >= 30);
                    if (shouldAsk)
                    {
                        DGS::ValidateRequest req{};
                        req.requestId  = reqSeq++;
                        req.entityUuid = e.uuid;
                        req.ownerZone  = (uint32_t)(xMin * 31 + yMin * 17 + zMin);
                        req.moduleId   = 0;       // project-defined (SAME via GAME_MODULE_SO)
                        req.kind       = 0;       // move
                        req.entity     = e;
                        req.lastGX     = pit->second.gx;
                        req.lastGY     = pit->second.gy;
                        req.lastGZ     = pit->second.gz;
                        req.maxSpeed   = pit->second.maxSpeed;
                        req.dtSeconds  = dt;

                        DGS::Packet pReq; pReq.pack(req);
                        if (tcp_validator.send(tcp_validator.getSocketFD(), pReq.getRawData(), pReq.getSize()))
                        {
                            g_bytesTx += pReq.getSize();
                            pendingValid[req.requestId] = { now + 500, 0, e, pit->second.maxSpeed, req.ownerZone };
                            lastReqMs[e.uuid] = now;
                            statSent++;
                        }
                    }
                }

                // ALWAYS update the baseline (fail-open included) with the last accepted state.
                //
                // ⚠️ SIN CONSERVAR EL PRESUPUESTO, ESTA LINEA DESHACIA EL ARREGLO DE S1. Asignar la
                // struct entera pone `budgetM` a su valor por defecto, o sea a CERO, en cada paquete
                // aceptado — y el cubo que se llena con el tiempo real se vaciaba antes de servir para
                // nada. Medido: el mismo 12 contra 6 que antes del arreglo, con el arreglo puesto.
                {
                    const float keepBudget = lastPosition.count(e.uuid) ? lastPosition[e.uuid].budgetM : 0.0f;
                    lastPosition[e.uuid] = {
                        e.chunkX * csX + e.pos[0],
                        e.chunkY * csY + e.pos[1],
                        e.chunkZ * csZ + e.pos[2],
                        e.stats.speed[0],
                        now,
                        keepBudget
                    };
                }

                // Update or add the entity (§3.9: while DRAINING we stop claiming entities — the
                // orchestrator's topology already routes them to the survivor).
                if (!g_draining)
                {
                    bool found = false;
                    for (auto& existing : entities)
                    {
                        if (existing.uuid == e.uuid)
                        {
                            // ⚠️ `existing = e` USED TO WIPE THE ENTITY'S PAYLOAD TWENTY TIMES A SECOND.
                            // Every update replaced the whole struct, and an ordinary movement update
                            // carries `dataSize = 0` — that is the entire point of `dataSize`, the 67x
                            // saving this system's capacity rests on: a player who is only moving does
                            // not re-send four kilobytes of inventory. So the blob survived exactly one
                            // tick. Measured: a client sent a 64-byte inventory, the zone broadcast
                            // `dataSize = 64`, and one second of ordinary transforms later it broadcast
                            // **0**.
                            //
                            // `dataSize == 0` means "I have nothing new to say about my payload", not
                            // "my payload is empty". Everything else in the update still replaces what
                            // was there — position, stats, state — because that IS what those packets
                            // are for.
                            //
                            // ⚠️ AND THERE IS NO WAY TO CLEAR A PAYLOAD. With this reading a client
                            // cannot send an empty one on purpose; it would need a signal of its own.
                            // Nothing needs that today, and inventing one nobody uses would be worse
                            // than writing down that it is missing.
                            const uint16_t keepSize = e.dataSize == 0 ? existing.dataSize : 0;
                            uint8_t keep[DGS::MAX_ENTITY_DATA];
                            if (keepSize) std::memcpy(keep, existing.data, keepSize);

                            existing = e;

                            if (keepSize)
                            {
                                existing.dataSize = keepSize;
                                std::memcpy(existing.data, keep, keepSize);
                            }
                            found = true;
                            break;
                        }
                    }
                    if (!found) entities.push_back(e);

                    // §3.6: I AM the owning zone of this entity (I cover its chunk) while it reports.
                    entityOwnedUntil[e.uuid] = now + (uint64_t)std::atoi(std::getenv("ENTITY_LEASE_MS") ? std::getenv("ENTITY_LEASE_MS") : "3000");
                    lastActiveSeen[e.uuid]   = now;
                }
            }
        }

        // Receive the Validator's ACK (request-ack P2)
        if (validated)
        {
            DGS::Packet ackP;
            uint8_t ackBuf[128];
            int av = readable(tcp_validator, tcp_validator.getSocketFD(), 0)
                     ? tcp_validator.receive(tcp_validator.getSocketFD(), ackBuf, sizeof(ackBuf))
                     : -1;
            if (av > 0) g_bytesRx += (uint64_t)av;
            if (av > 0)
            {
                ackP.setBuffer(ackBuf, av);
                if (ackP.getType() == DGS::PKT_VALIDATE_ACK)
                {
                    auto ack_ = ackP.unpackValidateAck();
                    auto it = pendingValid.find(ack_.requestId);
                    if (it != pendingValid.end())
                    {
                        uint32_t uuid = it->second.entity.uuid;
                        const bool     it2Apply  = it->second.applyOnAccept;
                        const uint16_t it2Action = it->second.pendingAction;
                        const uint32_t it2Actor  = it->second.actor;
                        const uint32_t it2ReqId  = it->second.clientReqId;
                        const DGS::EntityTransfer appliedEntity = it->second.entity;
                        pendingValid.erase(it);

                        // ⚠️ CONTESTARLE. Una peticion sin respuesta no es una peticion: el cliente
                        // pedia colocar o tirar algo, lo ponia ya en su pantalla —prediccion optimista,
                        // que es lo correcto para que se sienta inmediato— y NUNCA se enteraba de si
                        // habia ocurrido. Si el servidor decia que no, el objeto se quedaba en su
                        // mundo y en el de nadie mas. Dos mundos distintos y ni un mensaje.
                        if (it2Apply)
                        {
                            auto cli = clientMap.find(it2Actor);
                            if (cli != clientMap.end())
                            {
                                DGS::ActionAck ack{};
                                ack.requestId = it2ReqId;
                                ack.action    = it2Action;
                                ack.accepted  = (ack_.verdict == 0) ? 0 : 1;
                                ack.uuid      = (ack_.verdict == 0) ? 0u : appliedEntity.uuid;
                                DGS::Packet pAck; pAck.pack(ack);
                                std::vector<uint8_t> sealedAck;
                                DGS::sealForUdp(pAck.getRawData(), pAck.getSize(), sealedAck);
                                udp_zone_node.sendRaw(cli->second.first, cli->second.second,
                                                      sealedAck.data(), sealedAck.size());
                                g_bytesTx += DGS::udpWireSize(pAck.getSize());
                            }
                        }
                        if (ack_.verdict == 0)
                        {
                            std::cout << "[ZoneNode] VALIDATOR: VIOLATION uuid=" << uuid
                                      << " weight=" << ack_.weight << std::endl;
                            statViolations++;
                            // Evict from the local registry
                            for (auto ite = entities.begin(); ite != entities.end();)
                                if (ite->uuid == uuid) ite = entities.erase(ite);
                                else ++ite;
                            lastPosition.erase(uuid);
                        }
                        else
                        {
                            // Una accion pendiente: AHORA existe, y no antes.
                            if (it2Apply)
                            {
                                if (it2Action == DGS::ACTION_REMOVE)
                                {
                                    for (auto ie = entities.begin(); ie != entities.end();)
                                        if (ie->uuid == appliedEntity.uuid) ie = entities.erase(ie);
                                        else ++ie;
                                    lastPosition.erase(appliedEntity.uuid);
                                    std::cout << "[ZoneNode] ACCION ACEPTADA: destruido uuid="
                                              << appliedEntity.uuid << std::endl;
                                }
                                else
                                {
                                    // PLACE crea; MOVE reemplaza al que ya estaba por la version
                                    // movida. Los dos acaban igual: la entidad que el validador
                                    // aprobo, y ninguna otra.
                                    bool replaced = false;
                                    for (auto& ie : entities)
                                        if (ie.uuid == appliedEntity.uuid) { ie = appliedEntity; replaced = true; break; }
                                    if (!replaced) entities.push_back(appliedEntity);
                                    // Un lease normal, no `now + 0`. El bit es lo que lo mantiene
                                    // vivo y lo que lo hace persistible; el lease solo tiene que no
                                    // nacer caducado, o toda comprobacion de propiedad dice que no.
                                    entityOwnedUntil[appliedEntity.uuid] = nowMs() + (uint64_t)std::atoi(
                                        std::getenv("ENTITY_LEASE_MS") ? std::getenv("ENTITY_LEASE_MS") : "3000");
                                    std::cout << "[ZoneNode] ACCION ACEPTADA: "
                                              << (replaced ? "movido" : "creado") << " uuid="
                                              << appliedEntity.uuid << std::endl;
                                }
                            }
                            // Good verdict → close the breaker.
                            // ⚠️ AND RESET THE TRIP COUNTER. Without this `cbOpenCount` only ever grew:
                            // 3 timeouts across the whole life of the process were enough to exhaust it
                            // FOREVER, and from then on the branch that OPENS the circuit never ran
                            // again — the node fell straight through to fail-open and reconnected in a
                            // loop. Measured: 11 unanswered validations in a row and the head still saw
                            // `state = 1` (ok). The counter must measure CONSECUTIVE failures, not ones
                            // accumulated since start-up.
                            cbState = 0; cbOpenUntil = 0; cbOpenCount = 0;
                        }
                    }
                }
            }
        }

        // HALF-OPEN: probe the arbiter again every so often, and NOT only when we gave up on it.
        //
        // Without this `validated` stayed false FOREVER: nothing else in the node ever set it back to
        // true, so a validator that recovered was never used again and the head saw `state = 0` for the
        // rest of the process's life. A breaker that cannot close again is not a breaker, it's a fuse.
        //
        // ⚠️ AND `!validated` WAS NOT THE ONLY WAY TO GET STUCK. A `connect` that lands in a listening
        // socket's ACCEPT QUEUE succeeds even though nobody ever accepts it: from this side the socket
        // looks perfectly healthy, `validated` stays true, and every `send` on it fails silently. The
        // zone then sat with the breaker OPEN reporting `state = 2` — honestly, but for ever: no
        // verdict could arrive to close it, no timeout could accumulate because no request ever left,
        // and the probe above never ran because `validated` was true. Measured: `reqSent` frozen at 67
        // and `state = 2` at t = 20, 25 and 30 s, in 2 runs out of 3.
        //
        // So a breaker that has been open past its window is also a reason to re-dial. "Connected" is
        // not the same as "working", and only re-dialling can tell them apart.
        const bool stuckOpen = validated && cbState == 1 && nowMs() >= cbOpenUntil;
        if ((!validated || stuckOpen) && nowMs() - lastValidatorTryMs >= validatorRetryMs)
        {
            validated = connectToValidator();
            if (validated) { cbState = 0; cbOpenUntil = 0; cbOpenCount = 0; }
        }

        // P7 (§3.7): receive from the social node (bans/permissions). The zone only APPLIES.
        {
            uint8_t socBuf[8192];
            int sv = readable(tcp_social, tcp_social.getSocketFD(), 0)
                     ? tcp_social.receive(tcp_social.getSocketFD(), socBuf, sizeof(socBuf))
                     : -1;
            if (sv > 0)
            {
                DGS::Packet sp;
                sp.setBuffer(socBuf, sv);
                if (sp.getType() == DGS::PKT_ACCOUNT)
                {
                    auto a = sp.unpackAccountAction();
                    if (a.action == DGS::ACC_BAN)
                        bannedUntilMs[a.targetUuid] = a.durationS ? nowMs() + (uint64_t)a.durationS * 1000 : 0;
                    else if (a.action == DGS::ACC_UNBAN)
                        bannedUntilMs.erase(a.targetUuid);
                    std::cout << "[ZoneNode] P7: account ban/permissions uuid=" << a.targetUuid
                              << " action=" << (int)a.action << std::endl;
                }
            }
        }

        // Timeout/expiry of pending REQs + resend (capped backoff)
        if (!pendingValid.empty())
        {
            uint64_t now = nowMs();
            for (auto it = pendingValid.begin(); it != pendingValid.end();)
            {
                if (now >= it->second.deadlineMs)
                {
                    if (it->second.retries < 2)
                    {
                        DGS::ValidateRequest req{};
                        req.requestId  = reqSeq++;
                        req.entityUuid = it->second.entity.uuid;
                        req.ownerZone  = it->second.ownerZone;
                        req.moduleId   = 0;
                        req.kind       = 0;
                        req.entity     = it->second.entity;
                        req.maxSpeed   = it->second.maxSpeed;
                        req.dtSeconds  = 0;   // retry: no trustworthy dt
                        DGS::Packet pReq; pReq.pack(req);
                        if (tcp_validator.send(tcp_validator.getSocketFD(), pReq.getRawData(), pReq.getSize()))
                        {
                            g_bytesTx += pReq.getSize();
                            it->second.retries++;
                            it->second.deadlineMs = now + 500;
                            ++it;
                            continue;
                        }
                        statTimeout++;
                    }
                    else
                    {
                        statTimeout++;
                        // Reconnect ONLY once the spacing has elapsed. Without this guard it reconnected
                        // on every single timeout; and the `sleep(3)` that used to live here was another
                        // tick freezer: three seconds with no metrics, no heartbeat and no broadcast to
                        // clients. The loop already sleeps 100 ms per turn, so the spacing needs to block
                        // nobody.
                        if (nowMs() - lastValidatorTryMs >= validatorRetryMs)
                            validated = connectToValidator();
                    }
                    // A verdict that never arrives is operational information, not noise: without this
                    // line a hung validator is indistinguishable from a healthy one in the logs.
                    std::cout << "[ZoneNode] validation UNANSWERED uuid=" << it->second.entity.uuid
                              << " (timeouts=" << statTimeout << " trips=" << cbOpenCount + 1
                              << ")" << std::endl;
                    cbOpenCount++;
                    if (cbOpenCount >= CB_MAX_OPEN) { cbState = 0; validated = false; }   // fail-closed → fail-open by exhaustion
                    else
                    {
                        // ⚠️ THIS LINE WAS MISSING, AND WITHOUT IT THE CIRCUIT BREAKER WAS DEAD CODE.
                        // `cbState` was declared, read in `circuitBreakerOk()` and set to 0 in two
                        // places — but NEVER to 1. Consequence: `circuitBreakerOk()` could never return
                        // false, `ValidatorStatus::state` could never be 2, and the head never learned
                        // that the arbiter had stopped answering. Measured before the fix: with the
                        // validator accepting and staying silent for 9 s, the zone kept reporting "ok".
                        cbState     = 1;
                        cbOpenUntil = now + CB_OPEN_MS;
                    }
                    it = pendingValid.erase(it);
                }
                else ++it;
            }
        }

        // Receive TCP from the HeadServer
        {
            uint8_t tcpBuf[8192];
            // The head is the one that cost half the tick rate: 100 ms of waiting, every tick, for a
            // socket that is silent almost all the time.
            int bytes = readable(tcp_zone_node, tcp_zone_node.getSocketFD(), 0)
                        ? tcp_zone_node.receive(tcp_zone_node.getSocketFD(), tcpBuf, sizeof(tcpBuf))
                        : -1;
            if (bytes > 0) g_bytesRx += (uint64_t)bytes;
            if (bytes == 0)
            {
                std::cerr << "[ZoneNode] HeadServer closed the connection. Reconnecting..." << std::endl;
                connectToHead();
            }
            if (bytes > 0)
            {
                DGS::Packet pRecv;
                pRecv.setBuffer(tcpBuf, bytes);

                switch (pRecv.getType())
                {
                    case DGS::PKT_ENTITY_TRANSFER:
                    {
                        DGS::EntityTransfer e{};
                        // A malformed packet costs the packet, not the node: the throwing decode
                        // would take the whole zone down with an uncaught exception, and this one
                        // arrives over the network from a peer we do not control.
                        if (!pRecv.tryUnpackEntityTransfer(e)) break;

                        // Handoff PROMOTION (P4 §3.6): this entity crossed the border into MY zone.
                        // If we were already projecting it as a ghost (seeing it while its owner
                        // simulated it), we promote it to a REAL entity: drop the ghost so it is not
                        // duplicated.
                        bool wasGhost = ghostEntities.count(e.uuid) != 0;
                        ghostEntities.erase(e.uuid);
                        ghostLastSeen.erase(e.uuid);

                        // Do not duplicate: if we already held it as a real entity, just update it.
                        bool found = false;
                        for (auto& existing : entities)
                            if (existing.uuid == e.uuid) { existing = e; found = true; break; }
                        if (!found) entities.push_back(e);
                        std::cout << "[ZoneNode] Handoff: entity " << e.uuid
                                  << " promoted to real (was a ghost? " << (wasGhost ? "yes" : "no")
                                  << ")" << std::endl;

                        // §3.6: on receiving the reassigned entity, I AM the new owner (lease).
                        entityOwnedUntil[e.uuid] = nowMs() + (uint64_t)std::atoi(
                            std::getenv("ENTITY_LEASE_MS") ? std::getenv("ENTITY_LEASE_MS") : "3000");
                        lastActiveSeen[e.uuid]   = nowMs();
                        break;
                    }
                    case DGS::PKT_GHOST_DELTA:
                    {
                        auto ghost = pRecv.unpackGhostDelta();

                        // Do not shadow: if this uuid is ALREADY a real entity of ours (handoff in
                        // flight) the ghost is stale → ignore it rather than clobber authoritative data.
                        bool isReal = false;
                        for (const auto& ent : entities)
                            if (ent.uuid == ghost.uuid) { isReal = true; break; }
                        if (isReal) break;

                        ghostEntities[ghost.uuid] = ghost;
                        ghostLastSeen[ghost.uuid] = nowMs();
                        break;
                    }
                    case DGS::PKT_COMMAND:
                    {
                        // ⚠️ THIS HANDLER DID NOT EXIST, AND WITHOUT IT THE SPLIT WAS A NO-OP.
                        // `trySplitDown` spawns a child owning [midHigh..xMax] and sends the parent
                        // `CMD_TRANSFER_SERVER` with the new xMax. Nothing here read it: `xMax` was
                        // `const`, taken from the environment at boot. And the head does not remember
                        // the cut either — `updateNodeTopology` overwrites its record of the box with
                        // whatever the zone reports in PKT_METRICS, which was the unchanged one.
                        //
                        // So parent [0..100] and child [51..100] both claimed 51..100 forever, and
                        // `findTargetNode` returns the FIRST match by insertion order — the parent,
                        // registered first. The child received nothing at all. The cluster could scale
                        // up on paper and route exactly as if it had not.
                        //
                        // `orchestrator_test` did not catch it because it forces DGS_ZONE_BIN=/bin/true
                        // and asserts that the resize LEAVES over the socket, not that anyone obeys it.
                        //
                        // Shrinking is enough to fix it: everything else already follows. The entities
                        // now outside the box are handed off by `checkAndTransfer` on the next tick
                        // (which is what makes the split actually move players), the ghost border moves
                        // with `isNearBorder`, and the head learns the new box from the very next
                        // PKT_METRICS because that is where it reads it from.
                        auto cmd = pRecv.unpackCommand();
                        if (cmd.purpose != DGS::CMD_TRANSFER_SERVER) break;

                        // WHICH bound is being cut. A split used to be able to cut on X and nothing
                        // else, so a zone could only ever become a thinner slab and a crowd standing
                        // in one place could not be divided however many nodes were thrown at it.
                        const char  name   = (cmd.resizeAxis == DGS::AXIS_Y) ? 'Y'
                                           : (cmd.resizeAxis == DGS::AXIS_Z) ? 'Z' : 'X';
                        int32_t&    bound  = (cmd.resizeAxis == DGS::AXIS_Y) ? yMax
                                           : (cmd.resizeAxis == DGS::AXIS_Z) ? zMax : xMax;
                        int32_t&    low    = (cmd.resizeAxis == DGS::AXIS_Y) ? yMin
                                           : (cmd.resizeAxis == DGS::AXIS_Z) ? zMin : xMin;
                        const int32_t newMax = cmd.chunkX;   // rango nuevo EN ESE EJE
                        const int32_t newMin = cmd.chunkY;

                        // ⚠️ ANTES ESTO SOLO DEJABA ENCOGER, Y ASI LA FUSION ERA IMPOSIBLE. La regla
                        // era "un resize solo puede encoger", puesta como defensa contra que dos zonas
                        // reclamaran los mismos chunks. Pero un superviviente que se queda la region
                        // de otro tiene que CRECER, y sin poder hacerlo el mundo de la victima se
                        // quedaba sin dueno: medido, `findTargetNode` devolvia -1 cien milisegundos
                        // despues de fusionar, porque el head reescribe su copia de la caja con la que
                        // reporta la zona y la zona seguia con la suya.
                        //
                        // Quien decide las cajas es el HEAD: es el unico que ve la topologia entera, y
                        // es el que ahora se niega a fusionar dos cajas cuya union no sea un rectangulo
                        // (ver `mergeableAxis`). Aqui solo se rechaza lo que no tiene sentido en si
                        // mismo, un rango invertido.
                        if (newMin > newMax)
                        {
                            std::cout << "[ZoneNode] RESIZE " << name << "[" << newMin << ".." << newMax
                                      << "] RECHAZADO: rango invertido" << std::endl;
                            break;
                        }

                        std::cout << "[ZoneNode] RESIZE: " << name << " [" << low << ".." << bound
                                  << "] -> [" << newMin << ".." << newMax << "]" << std::endl;
                        low   = newMin;
                        bound = newMax;
                        break;
                    }
                    case DGS::PKT_REASSIGN:
                    {
                        auto ra = pRecv.unpackEntityReassign();

                        // An ANSWER to a handoff WE asked for, not an entity being given to us.
                        // ack=1: it reached its new owner, we may finally let go.
                        // ack=2: no zone covers that chunk — the head could not route it. Letting go
                        //        would erase the entity from the world, so we keep it and keep trying.
                        if (ra.ack != 0)
                        {
                            const uint32_t uuid = (uint32_t)ra.entityUuid;
                            if (ra.ack == 1)
                            {
                                for (auto eit = entities.begin(); eit != entities.end();)
                                    if (eit->uuid == uuid) eit = entities.erase(eit); else ++eit;
                                entityOwnedUntil.erase(uuid);
                                handoffLastTry.erase(uuid);
                                std::cout << "[ZoneNode] handoff ACKED for " << uuid
                                          << " -> released" << std::endl;
                            }
                            else
                            {
                                std::cout << "[ZoneNode] handoff for " << uuid
                                          << " has NO OWNER (head could not route the chunk)"
                                          << " -> keeping it" << std::endl;
                            }
                            break;
                        }

                        // §3.6: the head confirms we are the new owning zone. If we already have the
                        // entity (its EntityTransfer arrived first) we renew the lease; otherwise we
                        // wait for the full state.
                        auto itEnt = std::find_if(entities.begin(), entities.end(),
                                                  [&](const DGS::EntityTransfer& e){ return e.uuid == ra.entityUuid; });
                        if (itEnt != entities.end())
                        {
                            entityOwnedUntil[ra.entityUuid] = nowMs() + (uint64_t)std::atoi(
                                std::getenv("ENTITY_LEASE_MS") ? std::getenv("ENTITY_LEASE_MS") : "3000");
                            std::cout << "[ZoneNode] REASSIGN: we now own " << ra.entityUuid << std::endl;
                        }
                        else
                        {
                            std::cout << "[ZoneNode] REASSIGN " << ra.entityUuid
                                      << " received before the full state" << std::endl;
                        }
                        break;
                    }
                    case DGS::PKT_DRAIN:
                    {
                        // §3.9: the orchestrator asks us to drain (we are merging / scaling down).
                        auto lc = pRecv.unpackZoneLifecycle();
                        g_draining = 1;
                        std::cout << "[ZoneNode] DRAIN requested (requestId=" << lc.requestId
                                  << ") -> no longer claiming entities" << std::endl;

                        // Serialise the region's authoritative state and hand it over: the module
                        // extracts it with `serializeRegion` → head → the survivor folds it in with
                        // `mergeRegion`.
                        if (z_mod && z_mod->serializeRegion && z_zone)
                        {
                            double center[3] = {
                                ((xMin + xMax) / 2.0) * csX * 1000.0,
                                ((yMin + yMax) / 2.0) * csY * 1000.0,
                                ((zMin + zMax) / 2.0) * csZ * 1000.0
                            };
                            double span = (double)std::max(xMax - xMin, std::max(yMax - yMin, zMax - zMin));
                            double radius = (span / 2.0) * csX * 1000.0;

                            DGS::ZoneRegion reg{};
                            size_t n = z_mod->serializeRegion(z_zone, center, radius, reg.data, sizeof(reg.data));
                            if (n > sizeof(reg.data))
                            {
                                std::cout << "[ZoneNode] Region excede cap (" << n
                                          << "b > " << sizeof(reg.data) << "b) -> no blob" << std::endl;
                            }
                            else
                            {
                                reg.chunkX  = (xMin + xMax) / 2;
                                reg.chunkY  = (yMin + yMax) / 2;
                                reg.chunkZ  = (zMin + zMax) / 2;
                                reg.srcZone = (uint32_t)(xMin * 31 + yMin * 17 + zMin);
                                reg.size    = (uint32_t)n;
                                DGS::Packet pReg; pReg.pack(reg);
                                tcp_zone_node.send(tcp_zone_node.getSocketFD(), pReg.getRawData(), pReg.getSize());
                                g_bytesTx += pReg.getSize();
                            }
                        }

                        // Cede ownership of what we serve: publish our state to the head so the
                        // orchestrator's topology reroutes it to the surviving neighbour.
                        for (auto it = entityOwnedUntil.begin(); it != entityOwnedUntil.end();)
                        {
                            for (auto& ent : entities)
                                if (ent.uuid == it->first)
                                {
                                    DGS::Packet pEnt; pEnt.pack(ent);
                                    tcp_zone_node.send(tcp_zone_node.getSocketFD(), pEnt.getRawData(), pEnt.getSize());
                                    g_bytesTx += pEnt.getSize();
                                    break;
                                }
                            entityOwnedUntil.erase(it);   // we surrender the lease
                            it = entityOwnedUntil.begin();
                        }

                        // Confirm: ack=1 with the same requestId.
                        DGS::ZoneLifecycle resp{ lc.requestId, 1 };
                        DGS::Packet pAck; pAck.pack(resp);
                        tcp_zone_node.send(tcp_zone_node.getSocketFD(), pAck.getRawData(), pAck.getSize());
                        g_bytesTx += pAck.getSize();
                        break;
                    }
                    case DGS::PKT_DELETE_ZONE:
                    {
                        // §3.9: draining finished → authoritatively destroy the module's zone and exit.
                        auto lc = pRecv.unpackZoneLifecycle();
                        std::cout << "[ZoneNode] DELETE_ZONE (requestId=" << lc.requestId
                                  << ") -> destroying the module zone and exiting" << std::endl;
                        if (z_mod && z_mod->destroyZone) z_mod->destroyZone(z_zone);
                        z_zone = nullptr;
                        exit(0);
                    }
                    case DGS::PKT_ZONE_REGION:
                    {
                        // §3.9 Merge/handoff: the ceding neighbour sends us its region's serialised
                        // state → we fold it into OUR zone with mergeRegion.
                        auto region = pRecv.unpackZoneRegion();
                        if (z_mod && z_mod->mergeRegion && z_zone)
                        {
                            int r = z_mod->mergeRegion(z_zone, region.data, region.size);
                            std::cout << "[ZoneNode] MERGE region srcZone=" << region.srcZone
                                      << " bytes=" << region.size << " -> " << (r == 0 ? "ok" : "fail")
                                      << std::endl;
                        }
                        else
                        {
                            std::cout << "[ZoneNode] PKT_ZONE_REGION with no module/mergeRegion -> ignoring blob ("
                                      << region.size << "b)" << std::endl;
                        }
                        break;
                    }
                    default: break;
                }
            }
        }

        auto start = std::chrono::high_resolution_clock::now();

        // Ghost TTL cleanup: if a ghost gets no update within GHOST_TTL_MS and was not promoted (its
        // owner stopped emitting it: the entity left our neighbourhood or the source vanished), it is
        // dropped so we do not project ownerless phantom entities.
        {
            uint64_t now = nowMs();
            uint64_t ghostTTL = (uint64_t)std::atoi(std::getenv("GHOST_TTL_MS") ? std::getenv("GHOST_TTL_MS") : "3000");
            for (auto it = ghostLastSeen.begin(); it != ghostLastSeen.end();)
            {
                if (now - it->second > ghostTTL)
                {
                    ghostEntities.erase(it->first);
                    it = ghostLastSeen.erase(it);
                }
                else ++it;
            }
        }

        // ── WRITE-THROUGH TO PERSISTENCE ────────────────────────────────────────────────────────
        // ⚠️ NOTHING IN A RUNNING CLUSTER EVER WROTE AN ENTITY TO MONGO. The persistence node's write
        // path was reachable only by sending it a raw PKT_ENTITY_TRANSFER over TCP, and in the live
        // chain nobody does: the zone sends the validator a PKT_VALIDATE_REQ, which the validator
        // answers with an ACK and forwards nowhere, and the `cache_node` that would have relayed one had no
        // client in the whole repository. Verified by running head + validator + persistence + zone +
        // four players for five seconds: `Entity stored` 0, documents in Mongo 0. The write path and
        // the read path were BOTH orphaned; restoring from an empty database would have been theatre.
        //
        // The zone is the right writer: it is the OWNER, so it is the only node that knows the current
        // state. Periodic, not per update — at 10 Hz per entity a zone of 64 players would be 640
        // upserts a second to describe a world that changes far more slowly than it is observed.
        // `ZONE_PERSIST_MS` = 0 turns it off.
        {
            const uint64_t now = nowMs();
            const uint64_t persistEvery = (uint64_t)std::atoi(
                std::getenv("ZONE_PERSIST_MS") ? std::getenv("ZONE_PERSIST_MS") : "10000");

            if (persistEvery > 0 && now - lastPersistMs >= persistEvery)
            {
                lastPersistMs = now;

                // A dropped connection is ordinary: ask a worker to reconnect and carry on ticking.
                // Doing it here would put a name resolution back on the tick — the exact bug this file
                // records twice above.
                if (!persistenceUp) spawnLink(/*doRestore*/ false);

                if (persistenceUp)
                {
                    int written = 0;
                    for (const auto& e : entities)
                    {
                        // Only what we OWN. Writing a ghost would mean persisting a neighbour's
                        // entity from our stale projection of it — two writers for one state.
                        //
                        // ⚠️ SALVO LOS OBJETOS DEL MUNDO, y sin esta linea no se guardaba ni uno. Un
                        // jugador demuestra que es suyo reportandose; una piedra tirada no reporta
                        // nada — para eso existe `STATE_WORLD_OWNED`. Pero el GC lo exime por el BIT y
                        // este escritor decidia por el LEASE, asi que las dos mitades del sistema
                        // discrepaban sobre quien es el dueno de una piedra: el recolector la dejaba
                        // vivir y la persistencia la ignoraba. Resultado exacto de "puse algo y
                        // manana no estaba", pero por dentro: nunca llego a la base de datos.
                        //
                        // La regla es UNA: si lleva el bit, la posee la zona que cubre su chunk. Aqui
                        // y en el GC, o vuelven a discrepar.
                        const bool worldOwned = (e.state & DGS::STATE_WORLD_OWNED) != 0;
                        if (!worldOwned)
                        {
                            auto ownIt = entityOwnedUntil.find(e.uuid);
                            if (ownIt == entityOwnedUntil.end() || now >= ownIt->second) continue;
                        }

                        DGS::Packet p; p.pack(e);
                        if (!tcp_persistence.send(tcp_persistence.getSocketFD(),
                                                  p.getRawData(), p.getSize()))
                        {
                            persistenceUp = false;
                            std::cout << "[ZoneNode] persistence link lost while writing" << std::endl;
                            break;
                        }
                        g_bytesTx += p.getSize();
                        ++written;
                    }
                    if (written > 0)
                        std::cout << "[ZoneNode] persisted " << written << " owned entities" << std::endl;
                }
            }
        }

        // Entity GC (§3.5/§3.6): if an entity I own stops reporting past its lease I purge it, so we
        // do not serve them forever (a leak of `entities`/`lastPosition`/ownership in long sessions).
        {
            uint64_t now = nowMs();
            uint64_t lease = (uint64_t)std::atoi(std::getenv("ENTITY_LEASE_MS") ? std::getenv("ENTITY_LEASE_MS") : "3000");
            for (auto it = lastActiveSeen.begin(); it != lastActiveSeen.end();)
            {
                if (now - it->second > lease)
                {
                    // ⚠️ A CRATE IS NOT A PLAYER WHO LEFT. The lease exists so a disconnected client
                    // stops being served; applied to everything it also means the world cannot contain
                    // an object, because nothing reports a crate on the ground. `STATE_WORLD_OWNED`
                    // says "the world vouches for this one" and the GC leaves it alone — measured
                    // before this existed: three items placed once were gone 3 s later, restore or no
                    // restore.
                    bool worldOwned = false;
                    for (const auto& ent : entities)
                        if (ent.uuid == it->first) { worldOwned = (ent.state & DGS::STATE_WORLD_OWNED) != 0; break; }
                    if (worldOwned) { ++it; continue; }

                    entityOwnedUntil.erase(it->first);
                    lastPosition.erase(it->first);
                    lastSnapshot.erase(it->first);
                    // remove it from `entities` if still there (owner died / left the world)
                    for (auto eit = entities.begin(); eit != entities.end();)
                        if (eit->uuid == it->first) eit = entities.erase(eit);
                        else ++eit;
                    it = lastActiveSeen.erase(it);
                }
                else ++it;
            }
        }

        // Local SIMULATION (§3.6, C4): the owning zone advances ITS entities with module->step at a
        // fixed tick. Only what is under our lease is simulated; the rest is broadcast as ghosts. If the
        // module crashes (crash guard) it is marked suspect and simulation stops without killing the node.
        {
            uint64_t now  = nowMs();
            uint64_t stepMs = (uint64_t)std::atoi(std::getenv("ZONE_STEP_MS") ? std::getenv("ZONE_STEP_MS") : "100");
            if (now - lastStepMs >= stepMs && z_mod && z_zone && !z_susStep.load())
            {
                lastStepMs = now;
                float dt = stepMs / 1000.0f;
                for (auto& e : entities)
                {
                    auto ownIt = entityOwnedUntil.find(e.uuid);
                    if (ownIt == entityOwnedUntil.end() || now >= ownIt->second) continue;
                    if (sigsetjmp(z_sigJmp, 1) == 0)
                    {
                        z_inModule = 1;
                        z_mod->step(z_zone, &e, dt, &z_wq);
                        z_inModule = 0;
                    }
                    else
                    {
                        z_inModule = 0;
                        std::cout << "[ZoneNode] CRASH in the module during step -> suspending local simulation" << std::endl;
                        break;
                    }
                }
            }
        }

        checkAndTransfer(tcp_zone_node, entities, entityOwnedUntil, handoffLastTry, HANDOFF_RETRY_MS,
                         xMin, xMax, yMin, yMax, zMin, zMax);
        emitGhostDeltas(tcp_zone_node, entities, lastSnapshot, xMin, xMax, yMin, yMax, zMin, zMax, threshold);

        // ⚠️ CADA CUANTO SE SIMULA Y CADA CUANTO SE ENVIA SON DOS DECISIONES, Y ESTABAN PEGADAS.
        // La difusion vivia DENTRO del tick, asi que subir la simulacion de 10 a 60 Hz multiplico por
        // seis el trafico hacia todos los clientes sin que nadie lo pidiera — y el ancho de banda es
        // justo el recurso escaso aqui, no la CPU (el tick cuesta 0,095 ms). Mas Hz debe decidir lo
        // FLUIDA que va la simulacion; cuantos datos salen por el cable es otro mando.
        //
        // El defecto son los 100 ms de siempre: separar los dos relojes no debe cambiar por su cuenta
        // lo que sale por el cable, o el cambio deja de ser un refactor y se convierte en una decision
        // de producto tomada de tapadillo. Subirlo es lo UNICO que hace que un vecino se mueva fluido
        // sin suavizar en el cliente, y cuesta ancho de banda lineal: es una decision, no un defecto.
        // `ZONE_BROADCAST_MS` = 0 lo devuelve a "cada tick".
        static uint64_t lastBroadcastMs = 0;
        const uint64_t  bcastEveryMs = (uint64_t)std::atoi(
            std::getenv("ZONE_BROADCAST_MS") ? std::getenv("ZONE_BROADCAST_MS") : "100");
        const bool toca = (bcastEveryMs == 0) || (nowMs() - lastBroadcastMs >= bcastEveryMs);
        if (toca) lastBroadcastMs = nowMs();

        // Broadcast to every connected client, from ONE serialisation of the snapshot, and to each
        // one only what is near them.
        const BroadcastFrames snapshot = buildBroadcast(entities, ghostEntities, csX, csY, csZ);
        for (const auto& [uuid, endpoint] : clientMap)
        {
            if (!toca) break;
            // The recipient's own position is the centre of its interest. Until we have one — the
            // very first tick after it appears — it is sent everything, because guessing where
            // somebody is in order to decide what they may see is worse than a moment of extra data.
            bool haveCentre = false;
            float cx = 0, cy = 0, cz = 0;
            for (const auto& f : snapshot.frames)
                if (f.uuid == uuid) { cx = f.gx; cy = f.gy; cz = f.gz; haveCentre = true; break; }

            broadcastToClient(udp_zone_node, endpoint.first, endpoint.second, snapshot,
                              INTEREST_RADIUS_M, haveCentre, cx, cy, cz, uuid);
        }

        // ...and to the observers, from the SAME snapshot: a viewer that showed something different
        // from what the players are being sent would be worse than no viewer at all.
        {
            const uint64_t now = nowMs();
            for (auto it = observers.begin(); it != observers.end();)
            {
                if (now >= it->second.leaseUntilMs)
                {
                    std::cout << "[ZoneNode] observer lease expired " << it->first << std::endl;
                    it = observers.erase(it);
                    continue;
                }
                // ⚠️ Y AL MISMO RITMO QUE LOS JUGADORES. Esto se quedo fuera del mando de difusion, asi
                // que un visor recibia CADA TICK mientras los clientes recibian cada 100 ms: a 60 Hz,
                // seis veces mas datagramas de los que nadie le habia pedido. Ademas de contradecir lo
                // que dice el parrafo de arriba, ahogaba a quien mirase — medido con el banco, que se
                // quedaba colgado drenando un socket que se llenaba mas rapido de lo que lo vaciaba.
                // La caducidad del lease se sigue mirando cada tick: eso es barato y es correccion.
                if (toca)
                    broadcastToClient(udp_zone_node, it->second.endpoint.first,
                                      it->second.endpoint.second, snapshot);
                ++it;
            }
        }

        DGS::ServerMetrics metrics{};
        metrics.ramUsage       = getRAM();
        metrics.node.chunkXMin = xMin; metrics.node.chunkXMax = xMax;
        metrics.node.chunkYMin = yMin; metrics.node.chunkYMax = yMax;
        metrics.node.chunkZMin = zMin; metrics.node.chunkZMax = zMax;
        std::strncpy(metrics.node.addr, zoneAddr, sizeof(metrics.node.addr) - 1);
        metrics.node.port = udpPort;

        // WHERE the load is, one coarse histogram per axis over this zone's own box. The orchestrator
        // has never had this: it knew the box and one entity COUNT, so a split could only cut the box
        // down the middle — and a crowd standing in chunk 3 of a 0..100 zone got a cut at 50 that left
        // them all on one side. See ServerMetrics for what it deliberately does not capture.
        //
        // Bucketing is over the CURRENT box, which the same packet carries, so the head can invert it
        // without agreeing on anything else. A box of one chunk collapses to bucket 0, which is right:
        // there is nothing to divide.
        {
            auto bucket = [](int32_t v, int32_t lo, int32_t hi) -> uint32_t {
                const int64_t span = (int64_t)hi - (int64_t)lo + 1;
                if (span <= 1) return 0;
                int64_t b = ((int64_t)v - (int64_t)lo) * (int64_t)DGS::MAX_SPLIT_BUCKETS / span;
                if (b < 0) b = 0;
                if (b >= (int64_t)DGS::MAX_SPLIT_BUCKETS) b = DGS::MAX_SPLIT_BUCKETS - 1;
                return (uint32_t)b;
            };
            for (const auto& e : entities)
            {
                // Saturating: a bucket with more than 65535 entities in it is already far past
                // anything a zone can serve, and wrapping would point the cut at the wrong place.
                uint16_t& bx = metrics.popX[bucket(e.chunkX, xMin, xMax)];
                uint16_t& by = metrics.popY[bucket(e.chunkY, yMin, yMax)];
                uint16_t& bz = metrics.popZ[bucket(e.chunkZ, zMin, zMax)];
                if (bx < 0xFFFF) ++bx;
                if (by < 0xFFFF) ++by;
                if (bz < 0xFFFF) ++bz;
            }
        }
        metrics.startTimeS      = (uint64_t)std::time(nullptr);
        metrics.bytesRx         = g_bytesRx;
        metrics.bytesTx         = g_bytesTx;
        metrics.failedTransfers = statTimeout;
        metrics.activeEntities  = (uint32_t)entities.size();

        // Validation STATUS heartbeat towards the head (P2) — every 5 s.
        if (nowMs() - lastStatusMs > 5000)
        {
            DGS::ValidatorStatus st{};
            st.state      = (int8_t)(validated ? (cbState == 1 ? 2 : 1) : 0);  // 0=no validator, 1=ok, 2=breaker open
            st.reqSent    = statSent;
            st.reqTimeout = statTimeout;
            st.bytesRecv  = g_bytesRx;
            st.failedTransfers = statViolations;
            st.activeEntities  = (uint32_t)entities.size();
            st.timestampMs     = nowMs();
            DGS::Packet pSt; pSt.pack(st);
            tcp_zone_node.send(tcp_zone_node.getSocketFD(), pSt.getRawData(), pSt.getSize());
            g_bytesTx += pSt.getSize();
            lastStatusMs = nowMs();
        }

        auto end = std::chrono::high_resolution_clock::now();
        metrics.performance = std::chrono::duration<float, std::milli>(end - start).count();

        // ⚠️ LA TELEMETRIA NO VA AL RITMO DE LA SIMULACION. Esto se enviaba UNA VEZ POR TICK, asi que
        // subir el tick de 10 a 60 Hz habria multiplicado por seis el trafico hacia el head y el
        // tamaño de sus logs sin decir nada nuevo: el orquestador decide con medias moviles sobre
        // decenas de segundos, y 10 muestras por segundo ya son mas de las que mira. Cada cuanto se
        // MIDE y cada cuanto se SIMULA son preguntas distintas.
        {
            static uint64_t lastMetricsMs = 0;
            const uint64_t everyMs = (uint64_t)std::atoi(
                std::getenv("ZONE_METRICS_MS") ? std::getenv("ZONE_METRICS_MS") : "100");
            if (nowMs() - lastMetricsMs >= everyMs)
            {
                lastMetricsMs = nowMs();
                DGS::Packet p;
                p.pack(metrics);
                g_bytesTx += p.getSize();
                if (!tcp_zone_node.send(tcp_zone_node.getSocketFD(), p.getRawData(), p.getSize()))
                {
                    std::cerr << "[ZoneNode] Connection with HeadServer lost. Reconnecting..." << std::endl;
                    connectToHead();
                }
            }
        }

        // A FIXED PERIOD, not a fixed pause. `usleep()` slept the whole budget ON TOP of everything the
        // tick had already spent, so the period was always longer than the design assumes — and grew
        // with load. Sleeping only the remainder makes the tick a real rate, and when the work
        // genuinely overruns the budget it now shows up as a rate drop instead of silently stretching.
        //
        // ⚠️ 60 Hz, NO 10. El periodo era de 100 ms "por diseño", y el diseño no lo sostenia ninguna
        // medida: el tick cuesta 0,095 ms de MEDIANA y 0,17 en el p99 — el 0,1 % del presupuesto que
        // tenia. Lo que costaba esa decision se veia en la pantalla del jugador: la posicion de un
        // vecino llegaba diez veces por segundo, asi que alguien andando a 5 m/s daba zancadas de
        // medio metro, y eso se lee como latencia del servidor aunque el servidor conteste en 2 ms.
        // A 16,6 ms el mismo trabajo ocupa el 0,6 % y la posicion llega seis veces mas a menudo.
        // Lo que sube seis veces es el ancho de banda de difusion, no la CPU; el radio de interes es
        // lo que acota eso, no el reloj.
        {
            const int64_t spentUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                        std::chrono::steady_clock::now() - tickStart).count();
            const int64_t budgetUs = (int64_t)std::atoi(
                std::getenv("ZONE_TICK_US") ? std::getenv("ZONE_TICK_US") : "16666");
            if (spentUs < budgetUs) usleep((useconds_t)(budgetUs - spentUs));
        }
    }

    return 0;
}
