#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/types.h"
#include "include/dgs/game_module.h"   // ABI del MÓDULO DE REGLAS por proyecto (dlopen)

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
// MÓDULO DE REGLAS del PROYECTO (cargado en caliente). El DGS es GENÉRICO: NO conoce la física ni las
// estructuras del juego (inventario, casting, edición de mundo) — solo transporta bytes y DELEGA la
// semántica en el módulo, que el proyecto entrega como .so (mismo código que el cliente usa para
// predecir). Aquí solo el verbo de MOVIMIENTO; los demás (validateAction, ...) crecen sobre el mismo
// contrato sin tocar este nodo. Si no hay módulo, se usa el `validate()` histórico como fallback.
static const DGS::GameModule* g_module = nullptr;   // null → fallback genérico
static DGS::WorldQuery        g_wq{};               // estado de mundo de solo-lectura para el módulo
static DGS::ZoneHandle        g_zone = nullptr;     // v4: zona creada por el módulo (estado autoritativo)

// ------------------------------------------------------------------------------------------------
// Contención de crashes del módulo (§3.5). El .so del proyecto es código de terceros: un SIGSEGV dentro
// de validateMove/step mataría el nodo entero. Guard: la llamada al módulo se envuelve en sigsetjmp; el
// manejador de señales fatales, si la señal cae DENTRO del módulo (g_inModule), hace siglongjmp de vuelta
// → el proceso sigue vivo y el módulo queda marcado como SOSPECHOSO (fallback genérico desde entonces).
// Las señales fuera del módulo se re-lanzan con el comportamiento por defecto (crash real).
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

// Llama a g_module->validateMove bajo el crash-guard. Devuelve true = movimiento legal.
static bool moduleValidateMove(const DGS::MoveSample* s)
{
    if (sigsetjmp(g_sigJmp, 1) == 0)
    {
        g_inModule = 1;
        int r = g_module->validateMove(g_zone, s, &g_wq);
        g_inModule = 0;
        return r != 0;
    }
    std::cout << "[Validador] CRASH del modulo de reglas -> sospechoso, fallback generico" << std::endl;
    return false;   // conservador: el movimiento se descarta
}

// Fallback GENÉRICO histórico: solo velocidad/teleport (sin terreno; el DGS no conoce el mundo).
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

// Valida un REQUEST (P2): la zona dueña envió su estado PREDICHO + la afirmación del cliente. El
// validador usa el MISMO camino que UDP/TCP (módulo del proyecto o fallback genérico) para emitir el
// veredicto. Devuelve true = movimiento legal.
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

// Punto ÚNICO de validación de movimiento: si hay módulo del proyecto, arma el MoveSample y delega en
// él (mismas reglas que el cliente); si no, cae al fallback genérico. Devuelve true = movimiento legal.
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

// Carga el módulo del proyecto (GAME_MODULE_SO, def. "libharuka_rules.so") y prepara el WorldQuery.
// El planeta (para el anti-noclip) solo se activa si el operador lo provisiona por entorno — mientras
// el head-server no lo propague, g_wq.planetRadius=0 y el módulo valida SOLO velocidad (como el
// fallback). El chunkSize sí viene siempre en el Command inicial.
static void loadGameModule(float csX, float csY, float csZ)
{
    g_wq = DGS::WorldQuery{};
    g_wq.chunkSizeX = csX; g_wq.chunkSizeY = csY; g_wq.chunkSizeZ = csZ;
    // Planeta OPCIONAL por entorno (pruebas locales del anti-noclip). Requiere pos GLOBAL en metros.
    if (const char* r = std::getenv("GAME_PLANET_RADIUS")) {
        g_wq.planetRadius    = std::atof(r);
        g_wq.seed            = (uint32_t)std::atol(std::getenv("GAME_SEED")            ? std::getenv("GAME_SEED")            : "0");
        g_wq.reliefStrength  = (float)   std::atof(std::getenv("GAME_RELIEF")          ? std::getenv("GAME_RELIEF")          : "1.0");
        g_wq.profile         = (int32_t) std::atol(std::getenv("GAME_PROFILE")         ? std::getenv("GAME_PROFILE")         : "0");
    }

    const char* so = std::getenv("GAME_MODULE_SO") ? std::getenv("GAME_MODULE_SO") : "libharuka_rules.so";
    void* h = dlopen(so, RTLD_NOW);
    if (!h) { std::cout << "[Validador] sin modulo de reglas (" << so << "): " << dlerror()
                        << " -> fallback generico" << std::endl; return; }

    auto entry = (const DGS::GameModule* (*)())dlsym(h, "dgs_game_module_v1");
    if (!entry) { std::cout << "[Validador] " << so << " sin dgs_game_module_v1 -> fallback" << std::endl; dlclose(h); return; }

    const DGS::GameModule* m = entry();
    if (!m || m->abiVersion != DGS::GAME_MODULE_ABI) {
        std::cout << "[Validador] ABI del modulo != " << DGS::GAME_MODULE_ABI << " -> fallback" << std::endl;
        dlclose(h); return;
    }
    g_module = m;   // el .so queda cargado toda la vida del proceso (no dlclose)

    // v4: crear la zona del módulo (estado autoritativo por zona, no globals). Si el módulo NO expone
    // createZone (módulo v4 incompleto/corrupto), no lo usamos para validar → fallback genérico.
    if (m->createZone)
    {
        g_zone = m->createZone(&g_wq);
        if (!g_zone)
        {
            std::cout << "[Validador] createZone devolvio null -> fallback generico" << std::endl;
            g_module = nullptr;
            return;
        }
    }
    else
    {
        std::cout << "[Validador] modulo sin createZone (v4 incompleto) -> fallback generico" << std::endl;
        g_module = nullptr;
        return;
    }

    std::cout << "[Validador] modulo de reglas '" << (m->name ? m->name : "?") << "' ABI=" << m->abiVersion
              << (g_wq.planetRadius > 1.0 ? " (con terreno)" : " (solo velocidad)") << std::endl;
}

int main()
{
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

    if (!udpSocket.bind(udpPort))           { std::cerr << "[Validador] Error UDP:"  << udpPort  << std::endl; return 1; }
    if (!tcpSocket.listen(tcpPort))         { std::cerr << "[Validador] Error TCP:"  << tcpPort  << std::endl; return 1; }
    if (!headServer.connect(headHost, headPort))  { std::cerr << "[Validador] Error conectando HeadServer" << std::endl; return 1; }
    if (!persistence.connect(persHost, persPort)) { std::cerr << "[Validador] Error conectando Persistence" << std::endl; return 1; }

    uint8_t cmdBuf[512];
    int cmdBytes = headServer.receive(headServer.getSocketFD(), cmdBuf, sizeof(cmdBuf));
    if (cmdBytes <= 0) { std::cerr << "[Validador] No se recibio Command inicial" << std::endl; return 1; }

    DGS::Packet cmdPacket;
    cmdPacket.setBuffer(cmdBuf, cmdBytes);
    DGS::Command cmd = cmdPacket.unpackCommand();

    float csX = cmd.chunkSizeX;
    float csY = cmd.chunkSizeY;
    float csZ = cmd.chunkSizeZ;

    std::cout << "[Validador] ChunkSize=(" << csX << ", " << csY << ", " << csZ << ") km" << std::endl;
    std::cout << "[Validador] UDP:42427  TCP:42428  Persistence:42429" << std::endl;

    installCrashGuard();                  // contención de crashes del .so (§3.5)
    loadGameModule(csX, csY, csZ);        // reglas del proyecto (mismo código que el cliente) o fallback

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
                if (it != lastKnown.end() && !validateMoveDGS(e, it->second, csX, csY, csZ))
                {
                    std::cout << "[Validador] VIOLATION detectada (UDP) uuid=" << e.uuid << std::endl;
                    continue;
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
                std::cout << "[Validador] Cache conectado FD=" << newFD << std::endl;
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
                    // P2: request-ack de las zonas (ownerZone predice, validador queda de árbitro).
                    DGS::ValidateRequest req = p.unpackValidateRequest();
                    bool ok = (req.kind == 0) ? validateMoveRequest(req, csX, csY, csZ) : false;

                    DGS::ValidateAck ack{};
                    ack.requestId = req.requestId;
                    ack.verdict   = ok ? 1 : 0;
                    // weight: intensidad de la sospecha (solo si es violación)
                    ack.weight    = ok ? 0 : 1;
                    if (!ok)
                    {
                        std::cout << "[Validador] VIOLATION detectada (REQ) uuid=" << req.entityUuid
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
                    std::cout << "[Validador] VIOLATION detectada (TCP) uuid=" << e.uuid << std::endl;
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
