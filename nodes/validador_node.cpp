#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/types.h"
#include "include/dgs/game_module.h"   // ABI of the per-project RULES MODULE (dlopen)

#include <sys/epoll.h>
#include <cstring>
#include <cmath>
#include <map>
#include <set>
#include <iostream>
#include <dlfcn.h>
#include <cstdlib>
#include <csignal>
#include <csetjmp>
#include <atomic>

static constexpr float SCALE       = 1000.0f;

struct LastKnown
{
    float    gx, gy, gz;
    uint64_t timestamp_ms;
    float    maxSpeed;
};

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// ------------------------------------------------------------------------------------------------
// The PROJECT's RULES MODULE (hot-loaded). The DGS is GENERIC: it knows neither the physics nor the
// game's structures (inventory, casting, world editing) — it only carries bytes and DELEGATES the
// semantics to the module, which the project ships as a .so (the same code the client uses to
// predict). Only the MOVEMENT verb here; the rest (validateAction, …) grow on the same contract
// without touching this node. With no module the historical `validate()` is used as a fallback.
static const DGS::GameModule* g_module = nullptr;   // null → generic fallback
static DGS::WorldQuery        g_wq{};               // read-only world state for the module
static DGS::ZoneHandle        g_zone = nullptr;     // v4: zone created by the module (authoritative state)

// ------------------------------------------------------------------------------------------------
// Module crash containment (§3.5). The project's .so is third-party code: a SIGSEGV inside
// validateMove/step would kill the whole node. Guard: the call into the module is wrapped in
// sigsetjmp; the fatal-signal handler, if the signal lands INSIDE the module (g_inModule),
// siglongjmps back → the process stays alive and the module is marked SUSPECT (generic fallback from
// then on). Signals outside the module are re-raised with the default behaviour (a real crash).
static sigjmp_buf              g_sigJmp;
static volatile sig_atomic_t   g_inModule = 0;
static std::atomic<bool>       g_moduleSuspicious{false};

static void crashHandler(int sig)
{
    if (g_inModule)
    {
        g_inModule = 0;
        g_moduleSuspicious.store(true);
        siglongjmp(g_sigJmp, 1);
    }
    signal(sig, SIG_DFL);
    raise(sig);
}

static void installCrashGuard()
{
    static uint8_t altStack[64 * 1024] __attribute__((aligned(16)));
    stack_t ss{};
    ss.ss_sp   = altStack;
    ss.ss_size = sizeof(altStack);
    sigaltstack(&ss, nullptr);

    struct sigaction sa{};
    sa.sa_flags   = SA_ONSTACK | SA_NODEFER;
    sa.sa_handler = crashHandler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGSEGV, &sa, nullptr);
    sigaction(SIGBUS,  &sa, nullptr);
    sigaction(SIGFPE,  &sa, nullptr);
    sigaction(SIGILL,  &sa, nullptr);
}

// Calls g_module->validateMove under the crash guard. Returns true = legal movement.
static bool moduleValidateMove(const DGS::MoveSample* s)
{
    if (sigsetjmp(g_sigJmp, 1) == 0)
    {
        g_inModule = 1;
        int r = g_module->validateMove(g_zone, s, &g_wq);
        g_inModule = 0;
        return r != 0;
    }
    std::cout << "[Validator] CRASH in the rules module -> suspect, generic fallback" << std::endl;
    return false;   // conservative: the movement is discarded
}

// Historical GENERIC fallback: speed/teleport only (no terrain; the DGS does not know the world).
static bool validateFallback(const DGS::EntityTransfer& e, const LastKnown& last, float csX, float csY, float csZ)
{
    float dt = (nowMs() - last.timestamp_ms) / 1000.0f;
    if (dt <= 0 || dt > 2.f) return true;

    float dx = (e.chunkX * csX + e.pos[0]) - last.gx;
    float dy = (e.chunkY * csY + e.pos[1]) - last.gy;
    float dz = (e.chunkZ * csZ + e.pos[2]) - last.gz;
    float distSq = dx*dx + dy*dy + dz*dz;

    float maxDist = (last.maxSpeed * dt) + (SCALE / 1000.0f);
    return distSq <= (maxDist * maxDist);
}

// Validates a REQUEST (P2): the owning zone sent its PREDICTED state plus the client's claim. The
// validator uses the SAME path as UDP/TCP (project module or generic fallback) to issue the verdict.
// Returns true = legal movement.
static bool validateMoveRequest(const DGS::ValidateRequest& r, float csX, float csY, float csZ)
{
    if (!g_module || !g_module->validateMove || g_moduleSuspicious.load())
    {
        DGS::EntityTransfer e = r.entity;
        LastKnown last{ r.lastGX, r.lastGY, r.lastGZ, r.dtSeconds <= 0 ? 0 : (uint64_t)(nowMs() - (uint64_t)(r.dtSeconds * 1000)), r.maxSpeed };
        return validateFallback(e, last, csX, csY, csZ);
    }

    DGS::MoveSample s{};
    s.now       = &r.entity;
    s.lastGX    = r.lastGX;
    s.lastGY    = r.lastGY;
    s.lastGZ    = r.lastGZ;
    s.maxSpeed  = r.maxSpeed;
    s.dtSeconds = r.dtSeconds;
    return moduleValidateMove(&s);
}

// P7 (§2.3, §3.7): validates an ACTION (kind=1, critical verbs: destroy/place/ACT_TRANSFER). The
// economy and the destructive verbs are FAIL-CLOSED: no module, a suspect module or a null
// validateAction → REJECTED (the economy is never client-authoritative). With a module the verdict is
// the project's, via `validateAction` over the OPAQUE blob carried in `data[0..dataSize)` (the DGS
// does not interpret it). ACT_TRANSFER (guild economy) comes in through exactly this path.
static bool validateActionRequest(const DGS::ValidateRequest& r)
{
    if (!g_module || !g_module->validateAction || g_moduleSuspicious.load())
        return false;   // fail-closed: no verdict → reject (guild bank, loot, trade)

    // Opaque action blob (ActionHeader + the game's payload) carried in data[].
    return g_module->validateAction(g_zone, (uint32_t)r.entityUuid,
                                    r.entity.data, r.entity.dataSize, &g_wq) == 1;
}

// The SINGLE point of movement validation: if the project module is present, it builds the MoveSample
// and delegates to it (the same rules as the client); otherwise it falls back to the generic path.
// Returns true = legal movement.
static bool validateMoveDGS(const DGS::EntityTransfer& e, const LastKnown& last, float csX, float csY, float csZ)
{
    if (!g_module || !g_module->validateMove || g_moduleSuspicious.load())
        return validateFallback(e, last, csX, csY, csZ);

    DGS::MoveSample s{};
    s.now       = &e;
    s.lastGX    = last.gx;
    s.lastGY    = last.gy;
    s.lastGZ    = last.gz;
    s.maxSpeed  = last.maxSpeed;
    s.dtSeconds = (nowMs() - last.timestamp_ms) / 1000.0f;
    return moduleValidateMove(&s);
}

// Loads the project's module (GAME_MODULE_SO, default "libharuka_rules.so") and prepares the
// WorldQuery. The planet (for anti-noclip) is only enabled if the operator provisions it through the
// environment — while the head server does not propagate it, g_wq.planetRadius = 0 and the module
// validates SPEED ONLY (like the fallback). chunkSize does always arrive in the initial Command.
static void loadGameModule(float csX, float csY, float csZ)
{
    g_wq = DGS::WorldQuery{};
    g_wq.chunkSizeX = csX; g_wq.chunkSizeY = csY; g_wq.chunkSizeZ = csZ;
    // OPTIONAL planet from the environment (local anti-noclip testing). Needs GLOBAL position in metres.
    if (const char* r = std::getenv("GAME_PLANET_RADIUS")) {
        g_wq.planetRadius    = std::atof(r);
        g_wq.seed            = (uint32_t)std::atol(std::getenv("GAME_SEED")            ? std::getenv("GAME_SEED")            : "0");
        g_wq.reliefStrength  = (float)   std::atof(std::getenv("GAME_RELIEF")          ? std::getenv("GAME_RELIEF")          : "1.0");
        g_wq.profile         = (int32_t) std::atol(std::getenv("GAME_PROFILE")         ? std::getenv("GAME_PROFILE")         : "0");
    }

    const char* so = std::getenv("GAME_MODULE_SO") ? std::getenv("GAME_MODULE_SO") : "libharuka_rules.so";
    void* h = dlopen(so, RTLD_NOW);
    if (!h) { std::cout << "[Validator] no rules module (" << so << "): " << dlerror()
                        << " -> fallback generico" << std::endl; return; }

    auto entry = (const DGS::GameModule* (*)())dlsym(h, "dgs_game_module_v1");
    if (!entry) { std::cout << "[Validator] " << so << " has no dgs_game_module_v1 -> fallback" << std::endl; dlclose(h); return; }

    const DGS::GameModule* m = entry();
    if (!m || m->abiVersion != DGS::GAME_MODULE_ABI) {
        std::cout << "[Validator] module ABI != " << DGS::GAME_MODULE_ABI << " -> fallback" << std::endl;
        dlclose(h); return;
    }
    g_module = m;   // the .so stays loaded for the life of the process (no dlclose)

    // v4: create the module's zone (authoritative state per zone, not globals). If the module does NOT
    // expose createZone (an incomplete/corrupt v4 module) we do not use it to validate → generic fallback.
    if (m->createZone)
    {
        g_zone = m->createZone(&g_wq);
        if (!g_zone)
        {
            std::cout << "[Validator] createZone returned null -> generic fallback" << std::endl;
            g_module = nullptr;
            return;
        }
    }
    else
    {
        std::cout << "[Validator] module has no createZone (incomplete v4) -> generic fallback" << std::endl;
        g_module = nullptr;
        return;
    }

    std::cout << "[Validator] rules module '" << (m->name ? m->name : "?") << "' ABI=" << m->abiVersion
              << (g_wq.planetRadius > 1.0 ? " (with terrain)" : " (speed only)") << std::endl;
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
    DGS::UDPSocket udpSocket;
    DGS::TCPSocket tcpSocket;
    DGS::TCPSocket headServer;
    DGS::TCPSocket persistence;

    int         udpPort      = std::atoi(std::getenv("VALIDADOR_UDP_PORT")  ? std::getenv("VALIDADOR_UDP_PORT")  : "42427");
    int         tcpPort      = std::atoi(std::getenv("VALIDADOR_TCP_PORT")  ? std::getenv("VALIDADOR_TCP_PORT")  : "42428");
    const char* headHost     = std::getenv("HEAD_SERVER_HOST")               ? std::getenv("HEAD_SERVER_HOST")               : "head-server";
    int         headPort     = std::atoi(std::getenv("HEAD_SERVER_PORT")     ? std::getenv("HEAD_SERVER_PORT")     : "42424");
    const char* persHost     = std::getenv("PERSISTENCE_HOST")               ? std::getenv("PERSISTENCE_HOST")               : "persistence";
    int         persPort     = std::atoi(std::getenv("PERSISTENCE_PORT")     ? std::getenv("PERSISTENCE_PORT")     : "42429");

    // Threshold for the minimum-dt discard (see the block in the UDP path). Configurable because it
    // depends on the client's cadence: it has to sit comfortably below it.
    const uint64_t minDtMs = (uint64_t)std::atoi(std::getenv("VALIDADOR_MIN_DT_MS")
                                                 ? std::getenv("VALIDADOR_MIN_DT_MS") : "5");

    if (!udpSocket.bind(udpPort))           { std::cerr << "[Validator] UDP error on port "  << udpPort  << std::endl; return 1; }
    if (!tcpSocket.listen(tcpPort))         { std::cerr << "[Validator] TCP error on port "  << tcpPort  << std::endl; return 1; }
    if (!headServer.connect(headHost, headPort))  { std::cerr << "[Validator] Failed to connect to HeadServer" << std::endl; return 1; }
    if (!persistence.connect(persHost, persPort)) { std::cerr << "[Validator] Failed to connect to Persistence" << std::endl; return 1; }

    uint8_t cmdBuf[512];
    int cmdBytes = headServer.receive(headServer.getSocketFD(), cmdBuf, sizeof(cmdBuf));
    if (cmdBytes <= 0) { std::cerr << "[Validator] No initial Command received" << std::endl; return 1; }

    DGS::Packet cmdPacket;
    cmdPacket.setBuffer(cmdBuf, cmdBytes);
    DGS::Command cmd = cmdPacket.unpackCommand();

    float csX = cmd.chunkSizeX;
    float csY = cmd.chunkSizeY;
    float csZ = cmd.chunkSizeZ;

    std::cout << "[Validator] ChunkSize=(" << csX << ", " << csY << ", " << csZ << ") m" << std::endl;
    std::cout << "[Validator] UDP:42427  TCP:42428  Persistence:42429" << std::endl;

    installCrashGuard();                  // crash containment for the .so (§3.5)
    loadGameModule(csX, csY, csZ);        // the project's rules (same code as the client) or fallback

    int epollFD = epoll_create1(0);
    epoll_event ev;
    ev.events = EPOLLIN;

    ev.data.fd = udpSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, udpSocket.getSocketFD(), &ev);

    ev.data.fd = tcpSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, tcpSocket.getSocketFD(), &ev);

    epoll_event events[64];
    std::set<int> cacheFDs;
    std::map<uint32_t, LastKnown> lastKnown;

    while (true)
    {
        int n = epoll_wait(epollFD, events, 64, -1);
        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;

            if (fd == udpSocket.getSocketFD())
            {
                uint8_t buffer[sizeof(DGS::EntityTransfer)];
                std::string ip; int port;
                int bytes = udpSocket.receive(buffer, sizeof(buffer), ip, port);
                if (bytes != sizeof(DGS::EntityTransfer)) continue;

                DGS::EntityTransfer e{};
                std::memcpy(&e, buffer, sizeof(e));

                auto it = lastKnown.find(e.uuid);
                if (it != lastKnown.end())
                {
                    // ⚠️ MINIMUM-dt DISCARD — the defence against UDP REORDERING.
                    //
                    // UDP reorders routinely on the real internet. When an old sample arrives AFTER a
                    // newer one it carries a large distance and a near-zero `dt`: exactly the signature
                    // of a teleport. Measured through a degrading proxy (`net_degraded`), that produced
                    // **13 false violations out of 14 reordering events** — an honest player on a bad
                    // route accused of cheating, one by one.
                    //
                    // The sample is neither judged NOR used to update the baseline. Both halves matter:
                    //   · not judging it removes the false positive;
                    //   · not updating `lastKnown` denies a cheater any benefit — their state does not
                    //     advance, so their next sample is measured against the GOOD baseline.
                    // Which means flooding with back-to-back samples buys no distance: it throws it away.
                    //
                    // The threshold sits WELL below any real cadence (20 Hz = 50 ms, 100 Hz = 10 ms)
                    // so legitimate traffic is never discarded.
                    const uint64_t dtMs = nowMs() - it->second.timestamp_ms;
                    if (dtMs < minDtMs) continue;

                    if (!validateMoveDGS(e, it->second, csX, csY, csZ))
                    {
                        std::cout << "[Validator] VIOLATION detected (UDP) uuid=" << e.uuid << std::endl;
                        continue;
                    }
                }

                lastKnown[e.uuid] = {
                    e.chunkX * csX + e.pos[0],
                    e.chunkY * csY + e.pos[1],
                    e.chunkZ * csZ + e.pos[2],
                    nowMs(),
                    e.stats.speed[0]
                };
            }
            else if (fd == tcpSocket.getSocketFD())
            {
                int newFD = tcpSocket.accept();
                if (newFD < 0) continue;
                cacheFDs.insert(newFD);
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                std::cout << "[Validator] Cache connected FD=" << newFD << std::endl;
            }
            else if (cacheFDs.count(fd))
            {
                uint8_t buffer[8192];
                int bytes = tcpSocket.receive(fd, buffer, sizeof(buffer));
                if (bytes <= 0)
                {
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    tcpSocket.closeClient(fd);
                    cacheFDs.erase(fd);
                    continue;
                }

                DGS::Packet p;
                p.setBuffer(buffer, bytes);

                if (p.getType() == DGS::PKT_VALIDATE_REQ)
                {
                    // P2: request-ack from the zones (ownerZone predicts, the validator arbitrates).
                    DGS::ValidateRequest req = p.unpackValidateRequest();
                    // kind=0 movement → validateMove; kind=1 action (destroy/place/ACT_TRANSFER) →
                    // validateAction, FAIL-CLOSED (P7 §2.3/§3.7): with no verdict from the module it is
                    // rejected.
                    bool ok = (req.kind == 0) ? validateMoveRequest(req, csX, csY, csZ)
                                              : validateActionRequest(req);

                    DGS::ValidateAck ack{};
                    ack.requestId = req.requestId;
                    ack.verdict   = ok ? 1 : 0;
                    // weight: intensity of the suspicion (only when it is a violation)
                    ack.weight    = ok ? 0 : 1;
                    if (!ok)
                    {
                        std::cout << "[Validator] VIOLATION detected (REQ) uuid=" << req.entityUuid
                                  << " zone=" << req.ownerZone << " kind=" << (int)req.kind << std::endl;
                    }
                    DGS::Packet ackPacket;
                    ackPacket.pack(ack);
                    tcpSocket.send(fd, ackPacket.getRawData(), ackPacket.getSize());
                    continue;
                }

                auto e = p.unpackEntityTransfer();

                auto it = lastKnown.find(e.uuid);
                if (it != lastKnown.end() && !validateMoveDGS(e, it->second, csX, csY, csZ))
                {
                    std::cout << "[Validator] VIOLATION detected (TCP) uuid=" << e.uuid << std::endl;
                    continue;
                }

                lastKnown[e.uuid] = {
                    e.chunkX * csX + e.pos[0],
                    e.chunkY * csY + e.pos[1],
                    e.chunkZ * csZ + e.pos[2],
                    nowMs(),
                    e.stats.speed[0]
                };

                persistence.send(persistence.getSocketFD(), p.getRawData(), p.getSize());
            }
        }
    }

    return 0;
}
