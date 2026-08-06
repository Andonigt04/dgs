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

// Contadores monotónicos de ancho de banda del nodo (acumulados desde arranque, P3 §4.1).
// Se incrementan en los puntos reales de envío/recepción; el orquestador deriva la tasa con Δ/EWMA.
static uint64_t g_bytesRx = 0;
static uint64_t g_bytesTx = 0;

// ------------------------------------------------------------------------------------------------
// MÓDULO DE REGLAS del PROYECTO cargado en la ZONA (P4, §3.6: C4 — la zona dueña SIMULA sus entidades).
// El DGS es genérico: no conoce la física; si el proyecto entrega lib<_project>_rules.so se carga y
// `step` avanza las entidades que ESTA zona posee a tick fijo (autoridad física local); si no hay módulo
// (o su `step` es null / quiebra), la zona queda como cuello de reenvío + S1 y el mundo va por los
// updates del cliente.
static const DGS::GameModule* z_mod   = nullptr;   // null → no simular
static DGS::WorldQuery        z_wq{};              // mundo de solo-lectura prestado al módulo
static DGS::ZoneHandle        z_zone  = nullptr;   // zona creada por el módulo (estado autoritativo)

static volatile int g_draining = 0;   // §3.9: en DRAINING no reclamamos ownership nuevo (estamos cesando)

// Crash-guard del `.so` (§3.5): un SIGSEGV dentro de `step`/`validateMove` mataría el nodo entero.
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

// Carga el módulo y crea la zona. Devuelve true si hay `step` utilizable.
static void loadZoneModule()
{
    // Mundo de solo-lectura: tamaño de chunk en km (las coordenadas llegan des-cuantizadas por el host).
    float km = (float)std::atof(std::getenv("DGS_CHUNK_KM") ? std::getenv("DGS_CHUNK_KM") : "1.0");
    z_wq = DGS::WorldQuery{};
    z_wq.chunkSizeX = km; z_wq.chunkSizeY = km; z_wq.chunkSizeZ = km;

    const char* so = std::getenv("GAME_MODULE_SO") ? std::getenv("GAME_MODULE_SO") : "libharuka_rules.so";
    void* h = dlopen(so, RTLD_NOW);
    if (!h)
    {
        std::cout << "[ZoneNode] sin modulo de reglas (" << so << "): " << dlerror()
                  << " -> sin simulacion (solo S1)" << std::endl;
        return;
    }
    auto entry = (const DGS::GameModule* (*)())dlsym(h, "dgs_game_module_v1");
    if (!entry || !(z_mod = entry()) || z_mod->abiVersion != DGS::GAME_MODULE_ABI)
    {
        std::cout << "[ZoneNode] modulo invalido/ABI != " << DGS::GAME_MODULE_ABI << " -> sin simulacion" << std::endl;
        dlclose(h); z_mod = nullptr; return;
    }
    if (!z_mod->step)
    {
        std::cout << "[ZoneNode] modulo sin `step` -> sin simulacion en la zona" << std::endl;
        dlclose(h); z_mod = nullptr; return;
    }
    if (z_mod->createZone) z_zone = z_mod->createZone(&z_wq);
    if (!z_zone)
    {
        std::cout << "[ZoneNode] createZone fallo -> sin simulacion" << std::endl;
        dlclose(h); z_mod = nullptr; return;
    }
    std::cout << "[ZoneNode] modulo de reglas '" << (z_mod->name ? z_mod->name : "?")
              << "' ABI=" << z_mod->abiVersion << " -> simulacion de la zona activa" << std::endl;
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
            std::cout << "[ZoneNode] Entidad " << it->uuid << " fuera de limites. Transfiriendo..." << std::endl;

            // §3.6 handoff: la entidad cruza a una zona vecina → cedemos la AUTORIDAD explícitamente.
            // El head la enruta a la nueva dueña por chunk (PKT_REASSIGN), que la promoverá (ghost→real).
            DGS::EntityReassign ra{};
            ra.entityUuid = it->uuid;
            ra.chunkX     = it->chunkX;
            ra.chunkY     = it->chunkY;
            ra.chunkZ     = it->chunkZ;
            ra.fromZone   = 0;   // lo rellena el head si hace falta
            ra.toZone     = 0;   // 0 = el head la resuelve por chunk
            DGS::Packet pRa; pRa.pack(ra);
            tcp.send(tcp.getSocketFD(), pRa.getRawData(), pRa.getSize());
            g_bytesTx += pRa.getSize();

            // El estado completo también viaja (la nueva dueña necesita el EntityTransfer completo).
            DGS::Packet p;
            p.pack(*it);
            tcp.send(tcp.getSocketFD(), p.getRawData(), p.getSize());
            g_bytesTx += p.getSize();

            // Dejamos de poseerla: ya no es nuestra.
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

// Envía todas las entidades y ghosts a un cliente
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

// Última posición GLOBAL conocida de cada entidad (baseline del pre-check S1 y del REQ).
struct LastPosition
{
    float    gx, gy, gz;
    float    maxSpeed;
    uint64_t tsMs;
};

// Pre-chequeo local S1 (genérico, module-agnóstico): filtro rápido de teleport/velocidad.
// Es el mismo cálculo que el fallback del validador, aplicado ANTES de mandar el REQ para no
// saturar la red. Quien manda soy yo (la zona dueña) → es un paso de plausibilidad, no de autoridad.
static bool s1Plausible(const DGS::EntityTransfer& e, float csX, float csY, float csZ,
                        const LastPosition& last, float dt)
{
    if (dt <= 0 || dt > 2.f) return true;
    float dx = (e.chunkX * csX + e.pos[0]) - last.gx;
    float dy = (e.chunkY * csY + e.pos[1]) - last.gy;
    float dz = (e.chunkZ * csZ + e.pos[2]) - last.gz;
    float maxDist = (last.maxSpeed * dt) + 1.0f;   // margen de 1 m
    return (dx*dx + dy*dy + dz*dz) <= (maxDist * maxDist);
}

int main()
{
    DGS::UDPSocket udp_zone_node;
    DGS::TCPSocket tcp_zone_node;

    std::vector<DGS::EntityTransfer>          entities;
    std::map<uint32_t, DGS::EntityTransfer>   lastSnapshot;
    std::map<uint32_t, DGS::GhostDelta>       ghostEntities;
    std::map<uint32_t, uint64_t>              ghostLastSeen;  // uuid → última vez que vimos su ghost (TTL promoción)
    std::map<uint32_t, std::pair<std::string, int>> clientMap; // uuid → {addr, port}
    std::map<uint32_t, uint64_t>              entityOwnedUntil; // uuid → hasta cuándo SOY el dueño (lease §3.6)
    std::map<uint32_t, uint64_t>              lastActiveSeen;   // uuid → última vez que el cliente reportó (GC)
    uint64_t lastStepMs = 0;

    const char* headHost  = std::getenv("HEAD_SERVER_HOST") ? std::getenv("HEAD_SERVER_HOST") : "head-server";
    int         headPort  = std::atoi(std::getenv("HEAD_SERVER_PORT") ? std::getenv("HEAD_SERVER_PORT") : "42424");
    const char* zoneAddr  = std::getenv("MY_POD_IP")        ? std::getenv("MY_POD_IP")        : "127.0.0.1";
    int         udpPort   = std::atoi(std::getenv("ZONE_UDP_PORT")    ? std::getenv("ZONE_UDP_PORT")    : "42425");
    int32_t     threshold = std::atoi(std::getenv("GHOST_THRESHOLD")  ? std::getenv("GHOST_THRESHOLD")  : "1");

    if (!udp_zone_node.bind(udpPort))
    {
        std::cerr << "[ZoneNode] Error al hacer bind UDP en puerto " << udpPort << std::endl;
        return 1;
    }
    std::cout << "[ZoneNode] UDP escuchando en :" << udpPort << std::endl;

    // SO_RCVTIMEO en UDP para no bloquear el tick
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
                std::cout << "[ZoneNode] Conectado a HeadServer (intento " << attempt << ")" << std::endl;
                return true;
            }
            std::cerr << "[ZoneNode] Reintentando conexion a HeadServer..." << std::endl;
            sleep(3);
        }
    };

    connectToHead();

    // ---- P2: request-ack de validación contra el VALIDADOR ----
    const char* validHost = std::getenv("VALIDATOR_HOST")     ? std::getenv("VALIDATOR_HOST")     : "validador";
    int         validPort = std::atoi(std::getenv("VALIDATOR_TCP_PORT") ? std::getenv("VALIDATOR_TCP_PORT") : "42428");
    DGS::TCPSocket tcp_validator;

    auto connectToValidator = [&]() -> bool {
        tcp_validator = DGS::TCPSocket();
        if (tcp_validator.connect(validHost, validPort))
        {
            struct timeval tvVAL { 0, 5000 };   // 5ms: no bloquea el tick de simulación
            setsockopt(tcp_validator.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tvVAL, sizeof(tvVAL));
            std::cout << "[ZoneNode] P2: conectado al Validador " << validHost << ":" << validPort << std::endl;
            return true;
        }
        std::cout << "[ZoneNode] P2: Validador no disponible en " << validHost << ":" << validPort
                  << " (fail-open: S1 local)" << std::endl;
        return false;
    };
    bool validated = connectToValidator();
    if (!validated) sleep(3);   // reintento el siguiente ciclo

    // ---- P7 (§3.7): suscripción de la ZONA al nodo social. La zona NUNCA decide cuenta: aplica lo que
    // el nodo social le dice (bans/permisos → bloquea entrada de baneados). El canal local del chat lo
    // emite la zona por interés espacial; los demás canales los enruta el social (fan-out por suscripción).
    const char* socialHost = std::getenv("SOCIAL_HOST") ? std::getenv("SOCIAL_HOST") : "social";
    int         socialPort = std::atoi(std::getenv("SOCIAL_TCP_PORT") ? std::getenv("SOCIAL_TCP_PORT") : "42430");
    DGS::TCPSocket tcp_social;
    if (tcp_social.connect(socialHost, socialPort))
    {
        struct timeval tvSOC { 0, 5000 };
        setsockopt(tcp_social.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tvSOC, sizeof(tvSOC));
        std::cout << "[ZoneNode] P7: suscrito al nodo social " << socialHost << ":" << socialPort << std::endl;
    }
    else
        std::cout << "[ZoneNode] P7: nodo social no disponible en " << socialHost << ":"
                  << socialPort << " (sin bans locales)" << std::endl;

    z_installCrashGuard();       // contención de crashes del .so (§3.5)
    loadZoneModule();            // módulo de reglas del proyecto (o sin simulación)

    std::map<uint32_t, LastPosition>  lastPosition;       // uuid → baseline
    std::map<uint32_t, PendingValidation> pendingValid;   // requestId → en vuelo
    std::map<uint32_t, uint64_t>      lastReqMs;          // uuid → último REQ enviado (throttle)
    std::map<uint32_t, uint64_t>      bannedUntilMs;      // P7: uuid → hasta cuándo bloqueado (0=siempre)

    uint32_t reqSeq      = 1;
    uint32_t statSent    = 0;
    uint32_t statTimeout = 0;
    uint32_t statViolations = 0;
    uint32_t statRejected   = 0;   // violaciones atrapadas por S1 (ni siquiera se emiten)
    uint64_t lastStatusMs  = 0;
    uint8_t  cbState       = 0;    // 0=CLOSED, 1=OPEN (evicción temporal)
    uint64_t cbOpenUntil   = 0;
    uint32_t cbOpenCount   = 0;
    constexpr uint32_t CB_MAX_OPEN = 3;              // cuántas veces abrimos antes de fallar-cerrado

    auto circuitBreakerOk = [&]() -> bool {
        if (!validated) return true;                       // fail-open: solo S1
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

        // Recibir UDP de clientes
        {
            uint8_t udpBuf[sizeof(DGS::EntityTransfer)];
            std::string clientAddr;
            int clientPort = 0;
            int udpBytes = udp_zone_node.receive(udpBuf, sizeof(udpBuf), clientAddr, clientPort);
            if (udpBytes > 0) g_bytesRx += (uint64_t)udpBytes;

            if (udpBytes == sizeof(DGS::EntityTransfer))
            {
                DGS::EntityTransfer e;
                std::memcpy(&e, udpBuf, sizeof(e));
                clientMap[e.uuid] = { clientAddr, clientPort };

                // ---- P7 (§3.7): la zona aplica lo que el nodo social decide (NUNCA lo decide ella).
                // Cuenta baneada → se bloquea la ENTRADA (el cliente sigue mandando posiciones, pero la
                // zona no las sirve ni las simula). La zona NO juzga: solo aplica el ban recibido.
                auto banIt = bannedUntilMs.find(e.uuid);
                if (banIt != bannedUntilMs.end() && (banIt->second == 0 || nowMs() < banIt->second))
                {
                    statRejected++;
                    continue;   // baneado: no se propaga
                }

                // ---- P2: pre-chequeo local S1 + request de validación ----
                uint64_t now = nowMs();
                auto pit = lastPosition.find(e.uuid);
                if (pit != lastPosition.end())
                {
                    float dt = (now - pit->second.tsMs) / 1000.0f;
                    if (!s1Plausible(e, csX, csY, csZ, pit->second, dt))
                    {
                        // travellers S1 (teleport/velocidad imposible): se descarta en la zona.
                        statRejected++;
                        std::cout << "[ZoneNode] S1 bloqueo uuid=" << e.uuid << std::endl;
                        continue;   // no se propaga ni se pide validar
                    }

                    // Sin circuit breaker abierto y con validador: pedir veredicto.
                    auto lastReq = lastReqMs.find(e.uuid);
                    bool shouldAsk = circuitBreakerOk() &&
                                     (lastReq == lastReqMs.end() || (now - lastReq->second) >= 30);
                    if (shouldAsk)
                    {
                        DGS::ValidateRequest req{};
                        req.requestId  = reqSeq++;
                        req.entityUuid = e.uuid;
                        req.ownerZone  = (uint32_t)(xMin * 31 + yMin * 17 + zMin);
                        req.moduleId   = 0;       // definido por proyecto (SAME via GAME_MODULE_SO)
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

                // Actualizar baseline SIEMPRE (incluso en fail-open) con el último estado aceptado.
                lastPosition[e.uuid] = {
                    e.chunkX * csX + e.pos[0],
                    e.chunkY * csY + e.pos[1],
                    e.chunkZ * csZ + e.pos[2],
                    e.stats.speed[0],
                    now
                };

                // Actualizar o añadir entidad (§3.9: en DRAINING dejamos de reclamar entidades — la
                // topología del orquestador ya las enruta al superviviente).
                if (!g_draining)
                {
                    bool found = false;
                    for (auto& existing : entities)
                    {
                        if (existing.uuid == e.uuid) { existing = e; found = true; break; }
                    }
                    if (!found) entities.push_back(e);

                    // §3.6: SOY la zona dueña de esta entidad (cubre su chunk) mientras reporte actividad.
                    entityOwnedUntil[e.uuid] = now + (uint64_t)std::atoi(std::getenv("ENTITY_LEASE_MS") ? std::getenv("ENTITY_LEASE_MS") : "3000");
                    lastActiveSeen[e.uuid]   = now;
                }
            }
        }

        // Recibir ACK del Validador (request-ack P2)
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
                            std::cout << "[ZoneNode] VALIDADOR: VIOLATION uuid=" << uuid
                                      << " weight=" << ack.weight << std::endl;
                            statViolations++;
                            // Evictar del registro local
                            for (auto ite = entities.begin(); ite != entities.end();)
                                if (ite->uuid == uuid) ite = entities.erase(ite);
                                else ++ite;
                            lastPosition.erase(uuid);
                        }
                        else
                        {
                            // veredicto OK → cierra contacto en el circuito
                            cbState = 0; cbOpenUntil = 0;
                        }
                    }
                }
            }
        }

        // P7 (§3.7): recibir del nodo social (bans/permisos). La zona solo APLICA lo que el social decide.
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
                    std::cout << "[ZoneNode] P7: ban/permisos de cuenta uuid=" << a.targetUuid
                              << " action=" << (int)a.action << std::endl;
                }
            }
        }

        // Timeout/expiración de REQ pendientes + reenvío (backoff cap)
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
                        req.dtSeconds  = 0;   // reintento: sin dt fiable
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
                        validated = connectToValidator();   // reconexión
                        if (!validated) sleep(3);
                    }
                    cbOpenCount++;
                    if (cbOpenCount >= CB_MAX_OPEN) { cbState = 0; validated = false; }   // fail-closed→fail-open por agotamiento
                    it = pendingValid.erase(it);
                }
                else ++it;
            }
        }

        // Recibir TCP del HeadServer
        {
            uint8_t tcpBuf[8192];
            int bytes = tcp_zone_node.receive(tcp_zone_node.getSocketFD(), tcpBuf, sizeof(tcpBuf));
            if (bytes > 0) g_bytesRx += (uint64_t)bytes;
            if (bytes == 0)
            {
                std::cerr << "[ZoneNode] HeadServer cerro la conexion. Reconectando..." << std::endl;
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

                        // PROMOCIÓN de handoff (P4 §3.6): esta entidad cruzó el borde hacia MI zona.
                        // Si ya la estábamos proyectando como ghost (la veíamos mientras el dueño la
                        // simulaba), la promovemos a entidad REAL: quitarla de ghost para no duplicarla.
                        bool wasGhost = ghostEntities.count(e.uuid) != 0;
                        ghostEntities.erase(e.uuid);
                        ghostLastSeen.erase(e.uuid);

                        // No duplicar: si ya la teníamos como entidad real, solo actualizar.
                        bool found = false;
                        for (auto& existing : entities)
                            if (existing.uuid == e.uuid) { existing = e; found = true; break; }
                        if (!found) entities.push_back(e);
                        std::cout << "[ZoneNode] Handoff: entidad " << e.uuid
                                  << " promovida a real (era ghost? " << (wasGhost ? "si" : "no")
                                  << ")" << std::endl;

                        // §3.6: al recibir la entidad reasignada, SOY el nuevo dueño (lease).
                        entityOwnedUntil[e.uuid] = nowMs() + (uint64_t)std::atoi(
                            std::getenv("ENTITY_LEASE_MS") ? std::getenv("ENTITY_LEASE_MS") : "3000");
                        lastActiveSeen[e.uuid]   = nowMs();
                        break;
                    }
                    case DGS::PKT_GHOST_DELTA:
                    {
                        auto ghost = pRecv.unpackGhostDelta();

                        // No ensombrecer: si esta uuid YA es una entidad real nuestra (handoff en curso),
                        // el ghost es obsoleto → ignorarlo en vez de machacar dato autoritativo.
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
                        // §3.6: el head nos confirma que somos la nueva zona dueña. Si ya tenemos la
                        // entidad (llegó el EntityTransfer antes), renovamos el lease; si no, esperamos
                        // el estado completo.
                        auto itEnt = std::find_if(entities.begin(), entities.end(),
                                                  [&](const DGS::EntityTransfer& e){ return e.uuid == ra.entityUuid; });
                        if (itEnt != entities.end())
                        {
                            entityOwnedUntil[ra.entityUuid] = nowMs() + (uint64_t)std::atoi(
                                std::getenv("ENTITY_LEASE_MS") ? std::getenv("ENTITY_LEASE_MS") : "3000");
                            std::cout << "[ZoneNode] REASSIGN: somos dueños de " << ra.entityUuid << std::endl;
                        }
                        else
                        {
                            std::cout << "[ZoneNode] REASSIGN " << ra.entityUuid
                                      << " recibido antes que el estado completo" << std::endl;
                        }
                        break;
                    }
                    case DGS::PKT_DRAIN:
                    {
                        // §3.9: el orquestador nos pide drenar (vamos a fusionar/escalar-down).
                        auto lc = pRecv.unpackZoneLifecycle();
                        g_draining = 1;
                        std::cout << "[ZoneNode] DRAIN solicitado (requestId=" << lc.requestId
                                  << ") -> dejo de reclamar entidades" << std::endl;

                        // Serializar el estado autoritativo de la región y cederlo: el módulo extrae
                        // `serializeRegion` → head → superviviente lo incorpora con `mergeRegion`.
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
                                          << "b > " << sizeof(reg.data) << "b) -> sin blob" << std::endl;
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

                        // Ceder la propiedad de lo que servimos: difundir nuestro estado al head para que
                        // la topología del orquestador lo reenrute al vecino superviviente.
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
                            entityOwnedUntil.erase(it);   // rendimos el lease
                            it = entityOwnedUntil.begin();
                        }

                        // Confirmar: ack=1 con el mismo requestId.
                        DGS::ZoneLifecycle resp{ lc.requestId, 1 };
                        DGS::Packet pAck; pAck.pack(resp);
                        tcp_zone_node.send(tcp_zone_node.getSocketFD(), pAck.getRawData(), pAck.getSize());
                        g_bytesTx += pAck.getSize();
                        break;
                    }
                    case DGS::PKT_DELETE_ZONE:
                    {
                        // §3.9: drenaje terminado → destruir autoritativamente la zona del módulo y salir.
                        auto lc = pRecv.unpackZoneLifecycle();
                        std::cout << "[ZoneNode] DELETE_ZONE (requestId=" << lc.requestId
                                  << ") -> destruyendo zona del modulo y saliendo" << std::endl;
                        if (z_mod && z_mod->destroyZone) z_mod->destroyZone(z_zone);
                        z_zone = nullptr;
                        exit(0);
                    }
                    case DGS::PKT_ZONE_REGION:
                    {
                        // §3.9 Fusión/traspaso: el vecino que cede nos manda el estado serializado de su
                        // región → lo incorporamos a NUESTRA zona con mergeRegion.
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
                            std::cout << "[ZoneNode] PKT_ZONE_REGION sin modulo/mergeRegion -> ignoro blob ("
                                      << region.size << "b)" << std::endl;
                        }
                        break;
                    }
                    default: break;
                }
            }
        }

        auto start = std::chrono::high_resolution_clock::now();

        // Limpieza TTL de ghosts: si un ghost no recibe actualización en GHOST_TTL_MS y no se promovió
        // (el dueño dejó de emitirlo: la entidad salió de nuestra vecindad o el source desapareció),
        // se descarta para no proyectar entidades fantasma sin dueño.
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

        // GC de entidades (§3.5/§3.6): si una entidad que poseo deja de reportar más allá del lease,
        // la purgo — su dueño volará con la tira de contador — para no servirlas para siempre
        // (fuga de `entities`/`lastPosition`/ownership en sesiones largas).
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
                    // quitarla de `entities` si sigue (owner cayó / salió del mundo)
                    for (auto eit = entities.begin(); eit != entities.end();)
                        if (eit->uuid == it->first) eit = entities.erase(eit);
                        else ++eit;
                    it = lastActiveSeen.erase(it);
                }
                else ++it;
            }
        }

        // SIMULACIÓN local (§3.6, C4): la zona dueña avanza SUS entidades con module->step a tick fijo.
        // Solo se simula lo que está bajo nuestro lease; el resto se difunde como ghost. Si el módulo
        // quiebra (crash-guard) se marca sospechoso y se deja de simular sin matar el nodo.
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
                        std::cout << "[ZoneNode] CRASH del modulo en step -> suspendo simulacion local" << std::endl;
                        break;
                    }
                }
            }
        }

        checkAndTransfer(tcp_zone_node, entities, entityOwnedUntil, xMin, xMax, yMin, yMax, zMin, zMax);
        emitGhostDeltas(tcp_zone_node, entities, lastSnapshot, xMin, xMax, yMin, yMax, zMin, zMax, threshold);

        // Broadcast a todos los clientes conectados
        for (const auto& [uuid, endpoint] : clientMap)
            broadcastToClient(udp_zone_node, endpoint.first, endpoint.second, entities, ghostEntities);

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

        // Heartbeat de ESTADO de validación hacia el head (P2) — cadencia 5 s.
        if (nowMs() - lastStatusMs > 5000)
        {
            DGS::ValidatorStatus st{};
            st.state      = (int8_t)(validated ? (cbState == 1 ? 2 : 1) : 0);  // 0=sin validador,1=ok,2=circuito abierto
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
            std::cerr << "[ZoneNode] Conexion con HeadServer perdida. Reconectando..." << std::endl;
            connectToHead();
        }

        usleep(100000);
    }

    return 0;
}
