// ─────────────────────────────────────────────────────────────────────────────────────────────────
// place_action_e2e — poner una mesa es PEDIRLO, no hacerlo.
//
// Antes un cliente creaba objetos del mundo el mismo: mandaba un `EntityTransfer` con
// `STATE_WORLD_OWNED` —el bit que exime del recolector de leases— y la zona se lo creia. O sea que un
// cliente modificado podia sembrar el mundo de objetos que no caducan nunca. La zona ya no acepta ese
// bit por el enlace de un jugador (`client_claims_e2e`), y esto es lo que lo sustituye: el cliente
// PIDE, el validador decide, y solo entonces el SERVIDOR crea la entidad.
//
// La ruta ya existia y no la alcanzaba nadie: el validador distingue `kind=0` (movimiento, falla
// abierto suavizado) de `kind=1` (accion, falla CERRADO) desde hace tiempo, pero la zona ponia
// `kind = 0` en todas sus peticiones. Un movimiento mal aceptado se corrige en el siguiente paquete;
// un objeto creado de mas se queda.
//
// Las cuatro fases, y cada una es la que impide leer mal a las otras:
//
//   A. la regla ACEPTA  -> el objeto aparece, con su payload y con el bit puesto POR EL SERVIDOR.
//   B. la regla RECHAZA -> no aparece. Sin esto, (A) tambien lo pasaria una zona que coloca todo lo
//      que le piden sin preguntar, que es exactamente el agujero que se esta cerrando.
//   C. sin validador    -> no aparece. "Rechazado" y "no habia a quien preguntar" tienen que acabar
//      igual, o el fail-closed seria una promesa y no una conducta.
//   D. el uuid lo pone el SERVIDOR. El cliente no lo manda: si lo eligiera, acertar un numero
//      bastaria para sobrescribir la mesa de otro.
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
#include <cmath>
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

static const int      kHeadPort = 21981;
static const int      kZoneUdp  = 21983;
static const int      kValTcp   = 21985;
static const char*    kToken    = "place-e2e-token";
static const int32_t  kChunk    = 7;
static const char*    kKind     = "assets/models/table.glb";

static std::atomic<bool> g_done{false};

static uint64_t nowMs()
{
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

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

static pid_t spawnValidator(const char* bin, const char* so, const char* verdict)
{
    std::fflush(stdout);
    const pid_t p = fork();
    if (p != 0) return p;
    if (!std::getenv("PLACE_VERBOSE")) std::freopen("/dev/null", "w", stdout);
    setenv("VALIDADOR_TCP_PORT", std::to_string(kValTcp).c_str(), 1);
    setenv("VALIDADOR_UDP_PORT", "21986", 1);
    setenv("HEAD_SERVER_HOST", "127.0.0.1", 1);
    setenv("HEAD_SERVER_PORT", std::to_string(kHeadPort).c_str(), 1);
    setenv("PERSISTENCE_HOST", "127.0.0.1", 1);
    setenv("PERSISTENCE_PORT", "21987", 1);
    setenv("GAME_MODULE_SO", so, 1);
    if (verdict) setenv("STUB_ACTION_VERDICT", verdict, 1);
    else         unsetenv("STUB_ACTION_VERDICT");
    char tmpl[] = "/tmp/dgs_place_v_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(bin, bin, (char*)nullptr);
    _exit(127);
}

static pid_t spawnZone(const char* bin)
{
    std::fflush(stdout);
    const pid_t p = fork();
    if (p != 0) return p;
    if (!std::getenv("PLACE_VERBOSE")) std::freopen("/dev/null", "w", stdout);
    setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
    setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
    setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
    setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
    setenv("VALIDATOR_TCP_PORT", std::to_string(kValTcp).c_str(), 1);
    setenv("SOCIAL_HOST",        "127.0.0.1", 1);
    setenv("SOCIAL_TCP_PORT",    "21988", 1);
    setenv("PERSISTENCE_HOST",   "127.0.0.1", 1);
    setenv("PERSISTENCE_PORT",   "21987", 1);
    setenv("ZONE_RESTORE", "0", 1);
    setenv("ZONE_PERSIST_MS", "0", 1);
    setenv("CHUNK_X_MIN", "0", 1); setenv("CHUNK_X_MAX", "100", 1);
    setenv("CHUNK_Y_MIN", "0", 1); setenv("CHUNK_Y_MAX", "100", 1);
    setenv("CHUNK_Z_MIN", "0", 1); setenv("CHUNK_Z_MAX", "100", 1);
    setenv("CHUNK_SIZE_X", "1000.0", 1);
    setenv("CHUNK_SIZE_Y", "1000.0", 1);
    setenv("CHUNK_SIZE_Z", "1000.0", 1);
    setenv("ENTITY_LEASE_MS", "60000", 1);
    setenv("DGS_OBSERVE_TOKEN", kToken, 1);
    setenv("GAME_MODULE_SO", "", 1);   // la zona no simula: quien decide la accion es el validador
    char tmpl[] = "/tmp/dgs_place_z_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(bin, bin, (char*)nullptr);
    _exit(127);
}

/// Pide colocar algo. Fijate en lo que NO lleva: ningun uuid. Quien pide no elige la identidad.
/// @return el `requestId` con el que se reconocera la respuesta.
static uint32_t g_reqSeq = 100;
static uint32_t askAction(DGS::UDPSocket& udp, uint32_t actor, uint16_t kind, uint32_t target, float x)
{
    DGS::ActionRequest a{};
    a.requestId = g_reqSeq++;
    a.actor  = actor;
    a.action = kind;
    a.target = target;
    a.chunkX = kChunk; a.chunkY = kChunk; a.chunkZ = kChunk;
    a.pos[0] = x; a.pos[1] = 0.0f; a.pos[2] = 500.0f;
    a.dataSize = (uint16_t)std::strlen(kKind);
    std::memcpy(a.data, kKind, a.dataSize);
    DGS::Packet p; p.pack(a);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
    return a.requestId;
}

/// Espera el veredicto de una peticion. -1 = no llego ninguno.
///
/// ⚠️ ES LA MITAD QUE FALTABA. Sin respuesta, un juego que coloca al instante —y hace bien, o
/// construir se sentiria a cien milisegundos— no puede DESHACERLO cuando el servidor dice que no: el
/// objeto se queda en su pantalla y en la de nadie mas. Dos mundos distintos y ni un mensaje.
static int waitVerdict(DGS::UDPSocket& udp, uint32_t reqId, int ms)
{
    uint8_t buf[8192]; std::string from; int port = 0;
    const uint64_t until = nowMs() + (uint64_t)ms;
    while (nowMs() < until)
    {
        const int n = udp.receive(buf, sizeof(buf), from, port);
        if (n <= 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        if (buf[0] != DGS::PKT_ACTION_ACK) continue;
        DGS::Packet p; p.setBuffer(buf, (size_t)n);
        DGS::ActionAck ack{};
        if (!p.tryUnpackActionAck(ack) || ack.requestId != reqId) continue;
        return (int)ack.accepted;
    }
    return -1;
}

/// Lo que la zona difunde: el objeto del mundo que haya, con su uuid, su payload y su bit.
static bool findPlaced(DGS::UDPSocket& obs, int ms, uint32_t& uuid, std::string& payload, bool& owned)
{
    bool got = false;
    uint8_t buf[8192]; std::string from; int port = 0;
    while (obs.receive(buf, sizeof(buf), from, port) > 0) {}   // vaciar el pasado
    const uint64_t until = nowMs() + (uint64_t)ms;
    while (nowMs() < until)
    {
        DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(kToken);
        obs.send("127.0.0.1", kZoneUdp, hello.getRawData(), hello.getSize());
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        int n;
        while ((n = obs.receive(buf, sizeof(buf), from, port)) > 0)
        {
            if (n < 1 || buf[0] != DGS::PKT_ENTITY_TRANSFER) continue;
            DGS::Packet p; p.setBuffer(buf, (size_t)n);
            DGS::EntityTransfer e{};
            if (!p.tryUnpackEntityTransfer(e)) continue;
            if (e.type != DGS::ENT_ITEM) continue;
            uuid = e.uuid; owned = (e.state & DGS::STATE_WORLD_OWNED) != 0;
            payload.assign((const char*)e.data, e.dataSize);
            got = true;
        }
    }
    return got;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);
    char b1[PATH_MAX], b2[PATH_MAX], b3[PATH_MAX];
    const char* valBin  = realpath((argc > 1) ? argv[1] : "./build/validador_node", b1) ? b1 : "./build/validador_node";
    const char* zoneBin = realpath((argc > 2) ? argv[2] : "./build/zone_node", b2) ? b2 : "./build/zone_node";
    const char* soPath  = realpath((argc > 3) ? argv[3] : "./build/stub_rules_actions.so", b3) ? b3 : "./build/stub_rules_actions.so";

    std::atomic<bool> ready{false};
    std::thread head(fakeHead, std::ref(ready));
    while (!ready) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    DGS::UDPSocket player, obs;
    player.bind(0); obs.bind(0);
    { timeval tv{}; tv.tv_usec = 50000;
      setsockopt(player.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(obs.getSocketFD(),    SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

    // ══ A. la regla ACEPTA ══════════════════════════════════════════════════════════════════════
    pid_t val  = spawnValidator(valBin, soPath, nullptr);      // sin STUB_ACTION_VERDICT -> acepta
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    pid_t zone = spawnZone(zoneBin);
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    // ⚠️ EL VERBO ES PARAMETRIZABLE PARA PODER APUNTAR AL MODULO REAL. Con el modulo de juguete da
    // igual cual sea: acepta lo que este bien formado. Con `libharuka_rules.so`, ACT_PLACE se va a la
    // geometria de construccion (necesita `PlaceAction` y catalogo) y ACT_DROP no — asi que para
    // preguntarle al modulo REAL "¿por que rechazas un objeto tirado?" hay que poder mandarle el verbo
    // de tirar contra un entorno controlado, sin cluster y sin nadie moviendose.
    const uint16_t verb = std::getenv("PLACE_E2E_VERB")
                        ? (uint16_t)std::atoi(std::getenv("PLACE_E2E_VERB"))
                        : (uint16_t)DGS::ACTION_PLACE;
    std::printf("    verbo bajo prueba: %u\n", verb);
    const uint32_t reqOk = askAction(player, 4242, verb, 0, 500.0f);
    uint32_t uuid = 0; std::string payload; bool owned = false;
    const bool placed = findPlaced(obs, 2000, uuid, payload, owned);
    std::printf("    la regla acepta -> aparece: %s · uuid %u · payload \"%s\"\n",
                placed ? "SI" : "NO", uuid, payload.c_str());
    check(placed, "A · con la regla aceptando, el objeto EXISTE en el mundo");
    check(placed && payload == kKind, "A · y llega con su payload: el mundo sabe QUE es");
    check(placed && owned, "A · con STATE_WORLD_OWNED puesto por el SERVIDOR, no por quien lo pidio");
    check(placed && (uuid & 0x80000000u) != 0,
          "D · y con un uuid que asigna el servidor (el cliente no manda ninguno)");

    // El que lo pidio se entera de que ocurrio, y con que identidad.
    const int vOk = waitVerdict(player, reqOk, 1500);
    std::printf("    veredicto que le llega al que pidio: %d (1=si, 0=no, -1=ninguno)\n", vOk);
    check(vOk == 1, "F · y el que lo pidio RECIBE el veredicto: la peticion tiene respuesta");

    // ══ E. MOVER y DESTRUIR: lo mismo, sobre algo que ya existe ═════════════════════════════════
    // Un uuid inventado no puede convertirse en una peticion valida sobre la entidad de otro: el
    // servidor comprueba que el objetivo exista AQUI y que sea un objeto del mundo antes de preguntar.
    askAction(player, 4242, DGS::ACTION_MOVE, uuid, 800.0f);
    uint32_t um = 0; std::string pm; bool om = false;
    findPlaced(obs, 1500, um, pm, om);
    DGS::EntityTransfer moved{};
    {   // leer su posicion: el broadcast la lleva
        uint8_t buf[8192]; std::string from; int port = 0;
        const uint64_t until = nowMs() + 1200;
        while (nowMs() < until) {
            DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(kToken);
            obs.send("127.0.0.1", kZoneUdp, hello.getRawData(), hello.getSize());
            std::this_thread::sleep_for(std::chrono::milliseconds(80));
            int n;
            while ((n = obs.receive(buf, sizeof(buf), from, port)) > 0) {
                if (n < 1 || buf[0] != DGS::PKT_ENTITY_TRANSFER) continue;
                DGS::Packet p; p.setBuffer(buf, (size_t)n);
                DGS::EntityTransfer e{};
                if (p.tryUnpackEntityTransfer(e) && e.uuid == uuid) moved = e;
            }
        }
    }
    std::printf("    MOVE aceptado -> x=%.1f (estaba en 500)\n", moved.pos[0]);
    check(std::fabs(moved.pos[0] - 800.0f) < 1.0f,
          "E · MOVE mueve el objeto que ya existia, y no crea otro");

    // Un objetivo que no existe no es una peticion: se rechaza antes de molestar al validador.
    askAction(player, 4242, DGS::ACTION_REMOVE, 0xDEADBEEF, 0.0f);
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    uint32_t uz = 0; std::string pz; bool oz = false;
    const bool still = findPlaced(obs, 1200, uz, pz, oz);
    check(still && uz == uuid, "E · un uuid inventado no borra nada (el objetivo tiene que existir)");

    askAction(player, 4242, DGS::ACTION_REMOVE, uuid, 0.0f);
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    uint32_t ug = 0; std::string pg; bool og = false;
    const bool gone = !findPlaced(obs, 1500, ug, pg, og);
    std::printf("    REMOVE aceptado -> sigue ahi: %s\n", gone ? "NO" : "SI");
    check(gone, "E · y REMOVE lo destruye de verdad");

    g_done = true; kill(zone, SIGTERM); kill(val, SIGTERM);
    int st = 0; waitpid(zone, &st, 0); waitpid(val, &st, 0);
    head.join();

    // ══ B. la regla RECHAZA ═════════════════════════════════════════════════════════════════════
    // La contraprueba de (A): sin esto, "aparece" tambien lo pasaria una zona que coloca todo lo que
    // le piden sin preguntar a nadie — que es justo el agujero que esto cierra.
    g_done = false;
    std::atomic<bool> ready2{false};
    std::thread head2(fakeHead, std::ref(ready2));
    while (!ready2) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    val  = spawnValidator(valBin, soPath, "0");                // la regla dice NO
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    zone = spawnZone(zoneBin);
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    const uint32_t reqNo = askAction(player, 4242, verb, 0, 600.0f);
    uint32_t u2 = 0; std::string p2; bool o2 = false;
    const bool placed2 = findPlaced(obs, 2000, u2, p2, o2);
    std::printf("    la regla rechaza -> aparece: %s\n", placed2 ? "SI" : "NO");
    check(!placed2, "B · con la regla rechazando NO se crea nada (la decision es del servidor)");

    // ⚠️ Y EL RECHAZO TAMBIEN SE CUENTA. Es el caso que importa: un cliente que solo se entera de los
    // "si" no puede deshacer nada, que es exactamente el estado en el que estaba.
    const int vNo = waitVerdict(player, reqNo, 1500);
    std::printf("    veredicto de la peticion rechazada: %d\n", vNo);
    check(vNo == 0, "F · y el RECHAZO tambien se le comunica (no solo los si)");

    g_done = true; kill(zone, SIGTERM); kill(val, SIGTERM);
    waitpid(zone, &st, 0); waitpid(val, &st, 0);
    head2.join();

    // ══ C. sin validador ════════════════════════════════════════════════════════════════════════
    // "Rechazado" y "no habia a quien preguntar" tienen que acabar igual, o el fail-closed seria una
    // promesa en un comentario y no una conducta.
    g_done = false;
    std::atomic<bool> ready3{false};
    std::thread head3(fakeHead, std::ref(ready3));
    while (!ready3) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    zone = spawnZone(zoneBin);                                  // ningun validador levantado
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    askAction(player, 4242, verb, 0, 700.0f);
    uint32_t u3 = 0; std::string p3; bool o3 = false;
    const bool placed3 = findPlaced(obs, 2000, u3, p3, o3);
    std::printf("    sin validador -> aparece: %s\n", placed3 ? "SI" : "NO");
    check(!placed3, "C · sin validador tampoco: falla CERRADO, como el movimiento no hace");

    g_done = true; kill(zone, SIGTERM); waitpid(zone, &st, 0);
    head3.join();

    std::printf("\n%d/%d comprobaciones\n", g_pass, g_pass + g_fail);
    return g_fail == 0 ? 0 : 1;
}
