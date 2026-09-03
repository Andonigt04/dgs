#include "include/dgs/network.h"
#include "include/dgs/packet.h"
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

void checkAndTransfer(DGS::TCPSocket& tcp, std::vector<DGS::EntityTransfer>& entities,
                      std::map<uint32_t, uint64_t>& ownedUntil,
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
            std::cout << "[ZoneNode] Entity " << it->uuid << " out of bounds. Transferring..." << std::endl;

            // §3.6 handoff: the entity crosses into a neighbouring zone → we cede AUTHORITY explicitly.
            // The head routes it to the new owner by chunk (PKT_REASSIGN), which promotes it (ghost→real).
            DGS::EntityReassign ra{};
            ra.entityUuid = it->uuid;
            ra.chunkX     = it->chunkX;
            ra.chunkY     = it->chunkY;
            ra.chunkZ     = it->chunkZ;
            ra.fromZone   = 0;   // the head fills it in if needed
            ra.toZone     = 0;   // 0 = let the head resolve it by chunk
            DGS::Packet pRa; pRa.pack(ra);
            tcp.send(tcp.getSocketFD(), pRa.getRawData(), pRa.getSize());
            g_bytesTx += pRa.getSize();

            // The full state travels too (the new owner needs the complete EntityTransfer).
            DGS::Packet p;
            p.pack(*it);
            tcp.send(tcp.getSocketFD(), p.getRawData(), p.getSize());
            g_bytesTx += p.getSize();

            // We stop owning it: it is no longer ours.
            ownedUntil.erase(it->uuid);
            it = entities.erase(it);
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

// Sends every entity and ghost to one client
void broadcastToClient(DGS::UDPSocket& udp,
                       const std::string& addr, int port,
                       const std::vector<DGS::EntityTransfer>& entities,
                       const std::map<uint32_t, DGS::GhostDelta>& ghosts)
{
    for (const auto& e : entities)
    {
        const uint8_t* raw = reinterpret_cast<const uint8_t*>(&e);
        udp.send(addr, port, raw, sizeof(DGS::EntityTransfer));
        g_bytesTx += sizeof(DGS::EntityTransfer);
    }

    for (const auto& [uuid, ghost] : ghosts)
    {
        DGS::Packet p;
        p.pack(ghost);
        udp.send(addr, port, p.getRawData(), p.getSize());
        g_bytesTx += p.getSize();
    }
}

struct PendingValidation
{
    uint64_t   deadlineMs;
    uint32_t   retries;
    DGS::EntityTransfer entity;
    float      maxSpeed;
    uint32_t   ownerZone;
};

// Last known GLOBAL position of each entity (baseline for the S1 pre-check and for the REQ).
struct LastPosition
{
    float    gx, gy, gz;
    float    maxSpeed;
    uint64_t tsMs;
};

// Local S1 pre-check (generic, module-agnostic): a fast teleport/speed filter.
// It is the same computation as the validator's fallback, applied BEFORE sending the REQ so the
// network is not saturated. The sender here is the owning zone → this is a plausibility step, not an
// authority one.
static bool s1Plausible(const DGS::EntityTransfer& e, float csX, float csY, float csZ,
                        const LastPosition& last, float dt)
{
    if (dt <= 0 || dt > 2.f) return true;
    float dx = (e.chunkX * csX + e.pos[0]) - last.gx;
    float dy = (e.chunkY * csY + e.pos[1]) - last.gy;
    float dz = (e.chunkZ * csZ + e.pos[2]) - last.gz;
    float maxDist = (last.maxSpeed * dt) + 1.0f;   // 1 m of slack
    return (dx*dx + dy*dy + dz*dz) <= (maxDist * maxDist);
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
    std::map<uint32_t, PendingValidation> pendingValid;   // requestId → en vuelo
    std::map<uint32_t, uint64_t>      lastReqMs;          // uuid → last REQ sent (throttle)

    // Read-only observers: "addr:port" → { endpoint, lease deadline }. Kept apart from `clientMap` on
    // purpose (see the PKT_OBSERVE branch below).
    struct ObserverEntry { std::pair<std::string, int> endpoint; uint64_t leaseUntilMs; };
    std::map<std::string, ObserverEntry> observers;
    const uint64_t OBSERVER_LEASE_MS =
        (uint64_t)std::atoi(std::getenv("OBSERVER_LEASE_MS") ? std::getenv("OBSERVER_LEASE_MS") : "5000");
    std::map<uint32_t, uint64_t>      bannedUntilMs;      // P7: uuid → blocked until (0 = forever)

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

    while (true)
    {
        int32_t xMin = std::atoi(std::getenv("CHUNK_X_MIN") ? std::getenv("CHUNK_X_MIN") : "0");
        int32_t xMax = std::atoi(std::getenv("CHUNK_X_MAX") ? std::getenv("CHUNK_X_MAX") : "100");
        int32_t yMin = std::atoi(std::getenv("CHUNK_Y_MIN") ? std::getenv("CHUNK_Y_MIN") : "0");
        int32_t yMax = std::atoi(std::getenv("CHUNK_Y_MAX") ? std::getenv("CHUNK_Y_MAX") : "100");
        int32_t zMin = std::atoi(std::getenv("CHUNK_Z_MIN") ? std::getenv("CHUNK_Z_MIN") : "0");
        int32_t zMax = std::atoi(std::getenv("CHUNK_Z_MAX") ? std::getenv("CHUNK_Z_MAX") : "100");
        float csX = (float)std::atof(std::getenv("CHUNK_SIZE_X") ? std::getenv("CHUNK_SIZE_X") : "1.0");
        float csY = (float)std::atof(std::getenv("CHUNK_SIZE_Y") ? std::getenv("CHUNK_SIZE_Y") : "1.0");
        float csZ = (float)std::atof(std::getenv("CHUNK_SIZE_Z") ? std::getenv("CHUNK_SIZE_Z") : "1.0");

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

            // READ-ONLY OBSERVER (viewer / ops tooling). A 1-byte PKT_OBSERVE datagram subscribes the
            // sender to the same stream the zone already broadcasts to its clients. It is deliberately
            // NOT routed through the client path: an observer never lands in `clientMap`, never gets a
            // uuid, never takes a lease and never becomes an entity — subscribing must not be a way to
            // put something into the world. And it is a LEASE, refreshed by re-sending: a viewer that
            // closes its window stops being fed instead of collecting 10 datagrams a second for ever.
            if (udpBytes >= 1 && udpBuf[0] == DGS::PKT_OBSERVE)
            {
                const std::string key = clientAddr + ":" + std::to_string(clientPort);
                if (!observers.count(key))
                    std::cout << "[ZoneNode] observer subscribed " << key << std::endl;
                observers[key] = { { clientAddr, clientPort }, nowMs() + OBSERVER_LEASE_MS };
            }
            else if (udpBytes == sizeof(DGS::EntityTransfer))
            {
                DGS::EntityTransfer e;
                std::memcpy(&e, udpBuf, sizeof(e));
                clientMap[e.uuid] = { clientAddr, clientPort };

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
                lastPosition[e.uuid] = {
                    e.chunkX * csX + e.pos[0],
                    e.chunkY * csY + e.pos[1],
                    e.chunkZ * csZ + e.pos[2],
                    e.stats.speed[0],
                    now
                };

                // Update or add the entity (§3.9: while DRAINING we stop claiming entities — the
                // orchestrator's topology already routes them to the survivor).
                if (!g_draining)
                {
                    bool found = false;
                    for (auto& existing : entities)
                    {
                        if (existing.uuid == e.uuid) { existing = e; found = true; break; }
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
            int av = tcp_validator.receive(tcp_validator.getSocketFD(), ackBuf, sizeof(ackBuf));
            if (av > 0) g_bytesRx += (uint64_t)av;
            if (av > 0)
            {
                ackP.setBuffer(ackBuf, av);
                if (ackP.getType() == DGS::PKT_VALIDATE_ACK)
                {
                    auto ack = ackP.unpackValidateAck();
                    auto it = pendingValid.find(ack.requestId);
                    if (it != pendingValid.end())
                    {
                        uint32_t uuid = it->second.entity.uuid;
                        pendingValid.erase(it);
                        if (ack.verdict == 0)
                        {
                            std::cout << "[ZoneNode] VALIDATOR: VIOLATION uuid=" << uuid
                                      << " weight=" << ack.weight << std::endl;
                            statViolations++;
                            // Evict from the local registry
                            for (auto ite = entities.begin(); ite != entities.end();)
                                if (ite->uuid == uuid) ite = entities.erase(ite);
                                else ++ite;
                            lastPosition.erase(uuid);
                        }
                        else
                        {
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

        // HALF-OPEN: when we are in fail-open by exhaustion, probe the arbiter again every so often.
        // Without this `validated` stayed false FOREVER: nothing else in the node ever set it back to
        // true, so a validator that recovered was never used again and the head saw `state = 0` for the
        // rest of the process's life. A breaker that cannot close again is not a breaker, it's a fuse.
        if (!validated && nowMs() - lastValidatorTryMs >= validatorRetryMs)
        {
            validated = connectToValidator();
            if (validated) { cbState = 0; cbOpenUntil = 0; cbOpenCount = 0; }
        }

        // P7 (§3.7): receive from the social node (bans/permissions). The zone only APPLIES.
        {
            uint8_t socBuf[8192];
            int sv = tcp_social.receive(tcp_social.getSocketFD(), socBuf, sizeof(socBuf));
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
            int bytes = tcp_zone_node.receive(tcp_zone_node.getSocketFD(), tcpBuf, sizeof(tcpBuf));
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
                        auto e = pRecv.unpackEntityTransfer();

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
                    case DGS::PKT_REASSIGN:
                    {
                        auto ra = pRecv.unpackEntityReassign();
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
                        std::cout << "[ZoneNode] DRAIN solicitado (requestId=" << lc.requestId
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

        // Entity GC (§3.5/§3.6): if an entity I own stops reporting past its lease I purge it, so we
        // do not serve them forever (a leak of `entities`/`lastPosition`/ownership in long sessions).
        {
            uint64_t now = nowMs();
            uint64_t lease = (uint64_t)std::atoi(std::getenv("ENTITY_LEASE_MS") ? std::getenv("ENTITY_LEASE_MS") : "3000");
            for (auto it = lastActiveSeen.begin(); it != lastActiveSeen.end();)
            {
                if (now - it->second > lease)
                {
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

        checkAndTransfer(tcp_zone_node, entities, entityOwnedUntil, xMin, xMax, yMin, yMax, zMin, zMax);
        emitGhostDeltas(tcp_zone_node, entities, lastSnapshot, xMin, xMax, yMin, yMax, zMin, zMax, threshold);

        // Broadcast to every connected client
        for (const auto& [uuid, endpoint] : clientMap)
            broadcastToClient(udp_zone_node, endpoint.first, endpoint.second, entities, ghostEntities);

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
                broadcastToClient(udp_zone_node, it->second.endpoint.first,
                                  it->second.endpoint.second, entities, ghostEntities);
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

        DGS::Packet p;
        p.pack(metrics);
        g_bytesTx += p.getSize();
        if (!tcp_zone_node.send(tcp_zone_node.getSocketFD(), p.getRawData(), p.getSize()))
        {
            std::cerr << "[ZoneNode] Connection with HeadServer lost. Reconnecting..." << std::endl;
            connectToHead();
        }

        usleep(100000);
    }

    return 0;
}
