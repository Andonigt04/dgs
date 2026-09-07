// ─────────────────────────────────────────────────────────────────────────────────────────────────
// client_claims_e2e — lo que un cliente dice SOBRE SI MISMO no puede valer nada, y valia todo.
//
// El enlace UDP de un jugador llegaba entero a la zona y se creia entero. Tres campos del
// `EntityTransfer` los rellena el cliente y los tres son decisiones que no le corresponden:
//
//   · `stats.speed` — que es EL NUMERO CON EL QUE SE LE JUZGA. El filtro S1 comprueba
//     `maxDist = maxSpeed * dt + 1 m` contra el ultimo valor RECIBIDO, asi que el anti-trampas le
//     preguntaba al sospechoso cual era el limite. Con `maxSpeed = 1e9` no se rechazaba un solo salto.
//   · `type` — un jugador podia declararse otra cosa.
//   · `state`, y con el `STATE_WORLD_OWNED`: el bit que EXIME del recolector de leases. Un cliente
//     podia sembrar el mundo de objetos que no caducan nunca.
//
// Las tres fases miden lo mismo desde fuera, por el broadcast de la zona, que es lo unico que un
// tercero puede observar. Y cada una lleva su contraprueba dentro, porque "rechazado" y "no llego"
// se parecen demasiado:
//
//   A. un cliente que declara una velocidad absurda NO puede teletransportarse — y la contraprueba es
//      un paso pequeño del MISMO cliente, que si tiene que pasar. Sin ella, "no se movio" tambien lo
//      pasaria una zona que rechaza todo.
//   B. lo que la zona difunde lleva el techo del SERVIDOR, no el que mando el cliente.
//   C. un cliente que se marca STATE_WORLD_OWNED no consigue el bit.
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
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);
}

// Por debajo del rango efimero a proposito — ver la nota de `validador_e2e.cpp`.
static const int      kHeadPort = 21961;
static const int      kZoneUdp  = 21963;
static const char*    kToken    = "claims-e2e-token";
static const int32_t  kChunk    = 5;
static const float    kServerMax = 150.0f;   // el techo que se le pone a la zona
static const uint32_t kUuid    = 8500;

static std::atomic<bool> g_done{false};

static uint64_t nowMs()
{
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/// Un head que solo acepta la conexion de la zona: aqui no se enruta nada.
static void fakeHead(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kHeadPort)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;

    while (!g_done)
    {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 200000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) { const int n = s.receive(fd, buf, sizeof(buf)); if (n == 0) break; }
        s.closeClient(fd);
    }
}

static pid_t spawnZone(const char* bin)
{
    std::fflush(stdout);
    const pid_t p = fork();
    if (p != 0) return p;
    if (!std::getenv("CLAIMS_VERBOSE")) std::freopen("/dev/null", "w", stdout);
    setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
    setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
    setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
    setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
    setenv("VALIDATOR_TCP_PORT", "21969", 1);
    setenv("SOCIAL_HOST",        "127.0.0.1", 1);
    setenv("SOCIAL_TCP_PORT",    "21968", 1);
    setenv("ZONE_RESTORE",       "0", 1);
    setenv("ZONE_PERSIST_MS",    "0", 1);
    setenv("CHUNK_X_MIN", "0", 1); setenv("CHUNK_X_MAX", "100", 1);
    setenv("CHUNK_Y_MIN", "0", 1); setenv("CHUNK_Y_MAX", "100", 1);
    setenv("CHUNK_Z_MIN", "0", 1); setenv("CHUNK_Z_MAX", "100", 1);
    setenv("CHUNK_SIZE_X", "1000.0", 1);
    setenv("CHUNK_SIZE_Y", "1000.0", 1);
    setenv("CHUNK_SIZE_Z", "1000.0", 1);
    setenv("ENTITY_LEASE_MS", "60000", 1);   // largo: el GC no es el sujeto
    setenv("CLIENT_MAX_SPEED_MPS", "150", 1);
    setenv("DGS_OBSERVE_TOKEN", kToken, 1);
    setenv("GAME_MODULE_SO", "", 1);
    char tmpl[] = "/tmp/dgs_claims_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(bin, bin, (char*)nullptr);
    _exit(127);
}

/// Manda una entidad declarando lo que el cliente quiera: velocidad, tipo y bits de estado.
static void claim(DGS::UDPSocket& udp, float x, float speed,
                  DGS::EntityType type, uint32_t state)
{
    DGS::EntityTransfer e{};
    e.uuid   = kUuid;
    e.type   = type;
    e.state  = (DGS::EntityState)state;
    e.chunkX = kChunk; e.chunkY = kChunk; e.chunkZ = kChunk;
    e.pos[0] = x;
    e.stats.speed[0] = e.stats.speed[1] = e.stats.speed[2] = speed;
    e.stats.health   = 70.0f;
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

static void subscribe(DGS::UDPSocket& udp)
{
    DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(kToken);
    udp.send("127.0.0.1", kZoneUdp, hello.getRawData(), hello.getSize());
}

/// Lo ULTIMO que la zona difunde sobre esa entidad. Es lo unico que un tercero puede observar, y por
/// eso es donde se mide: no lo que el cliente dijo, sino lo que el mundo acabo creyendo.
static bool lastBroadcast(DGS::UDPSocket& obs, int ms, DGS::EntityTransfer& out)
{
    bool got = false;
    uint8_t buf[8192];
    std::string from; int port = 0;
    const uint64_t until = nowMs() + (uint64_t)ms;
    while (nowMs() < until)
    {
        subscribe(obs);
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
        int n;
        while ((n = obs.receive(buf, sizeof(buf), from, port)) > 0)
        {
            if (n < 1 || buf[0] != DGS::PKT_ENTITY_TRANSFER) continue;
            DGS::Packet p; p.setBuffer(buf, (size_t)n);
            DGS::EntityTransfer e{};
            if (!p.tryUnpackEntityTransfer(e)) continue;
            if (e.uuid != kUuid) continue;
            out = e; got = true;
        }
    }
    return got;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX];
    const char* zoneBin = realpath((argc > 1) ? argv[1] : "./build/zone_node", abs)
                          ? abs : "./build/zone_node";

    std::atomic<bool> ready{false};
    std::thread head(fakeHead, std::ref(ready));
    while (!ready) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t zone = spawnZone(zoneBin);
    std::this_thread::sleep_for(std::chrono::milliseconds(700));

    DGS::UDPSocket player, obs;
    player.bind(0); obs.bind(0);
    { timeval tv{}; tv.tv_usec = 50000;
      setsockopt(player.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(obs.getSocketFD(),    SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

    // ══ A. la velocidad que el cliente declara no le compra distancia ═══════════════════════════
    // Se planta, y desde ahi da un paso IMPOSIBLE declarando que puede. Entre los dos envios pasan
    // ~100 ms: a 150 m/s el servidor le concede 15 m mas el metro de holgura.
    claim(player, 100.0f, 1e9f, DGS::ENT_PLAYER, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    claim(player, 900.0f, 1e9f, DGS::ENT_PLAYER, 0);   // 800 m de golpe
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    DGS::EntityTransfer seen{};
    const bool any = lastBroadcast(obs, 1200, seen);
    std::printf("    declara maxSpeed=1e9 y salta 800 m -> el mundo le cree en x=%.1f\n",
                any ? seen.pos[0] : -1.0f);
    check(any && seen.pos[0] < 500.0f,
          "un salto imposible NO se acepta por mucho que el cliente declare que puede");

    // CONTRAPRUEBA: un paso normal del MISMO cliente si tiene que pasar. Sin esto, la comprobacion de
    // arriba tambien la pasaria una zona que simplemente rechaza todo lo que le llega.
    claim(player, 105.0f, 1e9f, DGS::ENT_PLAYER, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    DGS::EntityTransfer step{};
    const bool moved = lastBroadcast(obs, 1200, step);
    std::printf("    CONTRAPRUEBA, paso normal de 5 m -> x=%.1f\n", moved ? step.pos[0] : -1.0f);
    check(moved && std::fabs(step.pos[0] - 105.0f) < 1.0f,
          "y un paso normal SI pasa: no es que la zona rechace todo");

    // ══ B. el techo que viaja es el del SERVIDOR ════════════════════════════════════════════════
    std::printf("    el cliente declaro %.0f m/s · el mundo difunde %.0f m/s\n", 1e9f, step.stats.speed[0]);
    check(std::fabs(step.stats.speed[0] - kServerMax) < 0.5f,
          "la zona reemplaza la velocidad declarada por la suya (CLIENT_MAX_SPEED_MPS)");

    // ══ C. un cliente no se concede el bit que le exime del recolector ══════════════════════════
    claim(player, 106.0f, 1e9f, DGS::ENT_ITEM, DGS::STATE_WORLD_OWNED);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    DGS::EntityTransfer sneaky{};
    const bool got = lastBroadcast(obs, 1200, sneaky);
    std::printf("    se declara ENT_ITEM + WORLD_OWNED -> el mundo lo ve como tipo %d, state %u\n",
                got ? (int)sneaky.type : -1, got ? (unsigned)sneaky.state : 0u);
    check(got && (sneaky.state & DGS::STATE_WORLD_OWNED) == 0,
          "STATE_WORLD_OWNED enviado por un cliente se le quita: no puede sembrar objetos eternos");
    check(got && sneaky.type == DGS::ENT_PLAYER,
          "y quien entra por el enlace de un jugador ES un jugador, lo declare como lo declare");

    g_done = true;
    kill(zone, SIGTERM);
    int st = 0; waitpid(zone, &st, 0);
    head.join();

    std::printf("\n%d/%d comprobaciones\n", g_pass, g_pass + g_fail);
    return g_fail == 0 ? 0 : 1;
}
