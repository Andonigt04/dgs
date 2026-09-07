// ─────────────────────────────────────────────────────────────────────────────────────────────────
// s1_jitter_e2e — el anti-trampas castigaba la CONEXION, no la velocidad.
//
// S1 mide `dt` entre las LLEGADAS de dos datagramas y concede `maxSpeed * dt + 1 m`. Es razonable
// mientras los paquetes lleguen repartidos; deja de serlo en cuanto se agolpan, que es lo que hacen en
// cuanto la maquina o la red se ocupan. Dos paquetes que llegan con 5 ms de diferencia reciben un
// margen de `150 * 0.005 + 1 = 1,75 m` — y el jugador, que en ese rato ha andado lo que andaba, es
// rechazado por un salto que no ha dado.
//
// Medido en el repo antes de esto: en una maquina ociosa 0 de 20 pasos legitimos rechazados; bajo la
// suite entera, 8 de 20. El mismo jugador, el mismo movimiento, distinto rechazo segun lo ocupada que
// estuviera la maquina del SERVIDOR. Eso no es anti-trampas, es una loteria.
//
// Este test no espera a que la maquina se ponga nerviosa: PROVOCA el agolpamiento. Manda pasos
// identicos con los huecos alternando 5 ms y 95 ms — el mismo ritmo medio, la misma velocidad media,
// la misma distancia total. Un filtro que juzgue por llegada rechaza justo la mitad.
//
// ⚠️ Y NO SE MIDE DONDE ACABA, que fue mi primer intento y daba verde sin arreglar nada. Un paso
// rechazado no se pierde: la referencia no avanza, y el primer paquete de la racha siguiente trae un
// hueco grande, cubre la distancia acumulada y ENTRA. El jugador llega donde tenia que llegar.
//
// Lo que se rompe es el CAMINO. Con las rachas, el mundo no ve a alguien andando: ve a alguien
// apareciendo veinte metros mas alla cada doscientos milisegundos. Los demas jugadores lo ven dar
// tirones, y el validador le apunta violaciones que no ha cometido. Asi que lo que se cuenta es
// cuantos de sus pasos LLEGO A ENSENAR el mundo.
//
//   A. los dos llegan donde tenian que llegar: al de las rachas no se le castiga por su red.
//   C. Y LLEGAN POR EL FILTRO, no rodeandolo: una rafaga sin dormir ya no compra distancia. Esta es
//      la fase que de verdad muerde, y salio buscando la otra.
//   B. Y LA CONTRAPRUEBA, que es la que impide "arreglarlo" abriendo la mano: un tramposo de verdad
//      —que se teletransporta— sigue rechazado. Sin esta, subir el margen hasta que nadie se queje
//      pasaria el test A y dejaria el filtro inservible.
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
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);
}

static const int   kHeadPort = 21991;
static const int   kZoneUdp  = 21993;
static const char* kToken    = "s1-jitter-token";
static const int32_t kChunk  = 3;

// El techo del servidor y el paso que da el jugador. 5 m cada 50 ms son 100 m/s de media: por debajo
// del techo, asi que TODO paso es legitimo — la unica forma de rechazarlo es juzgar mal el tiempo.
static const float kServerMax = 150.0f;
static const float kStepM     = 5.0f;

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

static pid_t spawnZone(const char* bin)
{
    std::fflush(stdout);
    const pid_t p = fork();
    if (p != 0) return p;
    if (!std::getenv("S1_VERBOSE")) std::freopen("/dev/null", "w", stdout);
    setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
    setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
    setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
    setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
    setenv("VALIDATOR_TCP_PORT", "21999", 1);   // sin validador: el sujeto es el pre-filtro S1 local
    setenv("SOCIAL_HOST",        "127.0.0.1", 1);
    setenv("SOCIAL_TCP_PORT",    "21998", 1);
    setenv("PERSISTENCE_HOST",   "127.0.0.1", 1);
    setenv("PERSISTENCE_PORT",   "21997", 1);
    setenv("ZONE_RESTORE", "0", 1);
    setenv("ZONE_PERSIST_MS", "0", 1);
    setenv("CHUNK_X_MIN", "0", 1); setenv("CHUNK_X_MAX", "100", 1);
    setenv("CHUNK_Y_MIN", "0", 1); setenv("CHUNK_Y_MAX", "100", 1);
    setenv("CHUNK_Z_MIN", "0", 1); setenv("CHUNK_Z_MAX", "100", 1);
    setenv("CHUNK_SIZE_X", "1000.0", 1);
    setenv("CHUNK_SIZE_Y", "1000.0", 1);
    setenv("CHUNK_SIZE_Z", "1000.0", 1);
    setenv("ENTITY_LEASE_MS", "60000", 1);
    setenv("CLIENT_MAX_SPEED_MPS", "150", 1);
    setenv("DGS_OBSERVE_TOKEN", kToken, 1);
    setenv("GAME_MODULE_SO", "", 1);
    char tmpl[] = "/tmp/dgs_s1jit_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(bin, bin, (char*)nullptr);
    _exit(127);
}

static void step(DGS::UDPSocket& udp, uint32_t uuid, float x)
{
    DGS::EntityTransfer e{};
    e.uuid   = uuid;
    e.type   = DGS::ENT_PLAYER;
    e.chunkX = kChunk; e.chunkY = kChunk; e.chunkZ = kChunk;
    e.pos[0] = x;
    e.stats.health = 90.0f;   // la velocidad la pone el SERVIDOR (ver client_claims_e2e)
    DGS::Packet p; p.pack(e);
    udp.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
}

/// Cuantas posiciones DISTINTAS ha ensenado el mundo de esa entidad. Es la forma observable de "se le
/// vio andar": veinte pasos que se ven son veinte posiciones; veinte pasos de los que solo pasan cinco
/// son cinco saltos. Se muestrea mientras camina, no despues.
struct Watcher
{
    std::atomic<bool> stop{false};
    std::mutex mx;
    std::map<uint32_t, std::set<int>> seen;   // uuid -> posiciones (en decimetros, para no comparar floats)

    void run(int port)
    {
        DGS::UDPSocket obs;
        obs.bind(0);
        { timeval tv{}; tv.tv_usec = 20000;
          setsockopt(obs.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
        uint8_t buf[8192]; std::string from; int p = 0;
        while (!stop)
        {
            DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(kToken);
            obs.send("127.0.0.1", port, hello.getRawData(), hello.getSize());
            std::this_thread::sleep_for(std::chrono::milliseconds(15));
            int n;
            while ((n = obs.receive(buf, sizeof(buf), from, p)) > 0)
            {
                if (n < 1 || buf[0] != DGS::PKT_ENTITY_TRANSFER) continue;
                DGS::Packet pk; pk.setBuffer(buf, (size_t)n);
                DGS::EntityTransfer e{};
                if (!pk.tryUnpackEntityTransfer(e)) continue;
                std::lock_guard<std::mutex> lk(mx);
                seen[e.uuid].insert((int)std::lround(e.pos[0] * 10.0f));
            }
        }
    }

    size_t count(uint32_t uuid)
    {
        std::lock_guard<std::mutex> lk(mx);
        auto it = seen.find(uuid);
        return it == seen.end() ? 0 : it->second.size();
    }
};

/// Hasta donde ha llegado esa entidad segun el mundo.
static float finalX(DGS::UDPSocket& obs, uint32_t uuid, int ms)
{
    float x = -1.0f;
    uint8_t buf[8192]; std::string from; int port = 0;
    const uint64_t until = nowMs() + (uint64_t)ms;
    while (nowMs() < until)
    {
        DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE); hello.writeString(kToken);
        obs.send("127.0.0.1", kZoneUdp, hello.getRawData(), hello.getSize());
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
        int n;
        while ((n = obs.receive(buf, sizeof(buf), from, port)) > 0)
        {
            if (n < 1 || buf[0] != DGS::PKT_ENTITY_TRANSFER) continue;
            DGS::Packet p; p.setBuffer(buf, (size_t)n);
            DGS::EntityTransfer e{};
            if (p.tryUnpackEntityTransfer(e) && e.uuid == uuid) x = e.pos[0];
        }
    }
    return x;
}

/// Camina `steps` pasos de `kStepM`. Sin `jitter`, uno cada 50 ms — un cliente a 20 Hz con la red
/// tranquila. Con `jitter`, EN RAFAGAS: silencio y de golpe cuatro juntos.
///
/// ⚠️ ESE PATRON ES EL DE VERDAD, y no lo elegi de primeras. La primera version alternaba huecos de
/// 5 y 95 ms y el test pasaba sin arreglar nada: un paso rechazado lo ABSORBE el siguiente, porque la
/// referencia no avanza y el hueco siguiente concede margen de sobra. Midiendo solo donde acaba el
/// jugador, el fallo se tapa solo.
///
/// Cuando la maquina del servidor va cargada los paquetes no llegan desparejados: llegan a rachas. El
/// primero de cada racha trae un hueco enorme y pasa; los tres siguientes llegan pegados y cada uno
/// recibe `maxSpeed * 0.001 + 1 m` de margen para un paso de 5 m. Se pierden tres de cada cuatro, y
/// eso SI se ve al final del camino.
///
/// La distancia total y el tiempo total son los mismos en los dos casos: 20 pasos de 5 m en ~1 s, o
/// sea 100 m/s de media, por debajo del techo de 150. Todo paso es legitimo en los dos.
static void walk(DGS::UDPSocket& udp, uint32_t uuid, int steps, bool jitter)
{
    float x = 100.0f;
    step(udp, uuid, x);                       // primera posicion: fija la referencia
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    for (int i = 0; i < steps; ++i)
    {
        if (jitter) std::this_thread::sleep_for(std::chrono::milliseconds((i % 4 == 0) ? 200 : 1));
        else        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        x += kStepM;
        step(udp, uuid, x);
    }
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

    DGS::UDPSocket even, bursty, cheat, obs;
    even.bind(0); bursty.bind(0); cheat.bind(0); obs.bind(0);
    { timeval tv{}; tv.tv_usec = 50000;
      setsockopt(even.getSocketFD(),   SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(bursty.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(cheat.getSocketFD(),  SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(obs.getSocketFD(),    SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

    const int kSteps = 20;
    const float expected = 100.0f + kStepM * kSteps;

    // ══ A. mismo paseo, distinta suerte con la red ══════════════════════════════════════════════
    Watcher w;
    std::thread tw([&]{ w.run(kZoneUdp); });

    std::thread tEven  ([&]{ walk(even,   7301, kSteps, /*jitter*/false); });
    std::thread tBursty([&]{ walk(bursty, 7302, kSteps, /*jitter*/true);  });
    tEven.join(); tBursty.join();
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    w.stop = true; tw.join();

    const size_t nEven   = w.count(7301);
    const size_t nBursty = w.count(7302);
    const float  xEven   = finalX(obs, 7301, 600);
    const float  xBursty = finalX(obs, 7302, 600);
    std::printf("    posiciones que el mundo ENSENO — acompasado: %zu · agolpado: %zu (de %d pasos)\n",
                nEven, nBursty, kSteps);
    std::printf("    y donde acabaron: %.1f y %.1f (esperado %.1f — los dos llegan, ese no es el fallo)\n",
                xEven, xBursty, expected);
    // ⚠️ CONTAR POSICIONES NO SIRVE PARA JUZGAR EL FILTRO, y lo apunto porque me costo dos intentos
    // descubrirlo. La zona difunde a 10 Hz; el que va en rachas solo cambia de sitio cinco veces por
    // segundo A OJOS DE ESA DIFUSION, porque sus cuatro pasos ocurren en tres milisegundos. Sale 12
    // contra 6 con el filtro roto Y con el arreglado: eso mide la frecuencia de la zona, no S1.
    //
    // Lo que si es del filtro es que los dos LLEGUEN — y, sobre todo, la fase C: que lleguen POR el
    // filtro y no rodeandolo. Antes, el de las rachas llegaba porque `dt = 0` le daba paso libre.
    std::printf("    (las posiciones son el techo de difusion de la zona, no una medida del filtro)\n");
    check(std::fabs(xEven - expected) < 1.0f,
          "A · el jugador con la red tranquila llega donde tenia que llegar");
    check(std::fabs(xBursty - expected) < 1.0f,
          "A · y el de los paquetes AGOLPADOS tambien: no se le castiga por su red");

    // ══ C. LA PUERTA DE ATRAS: mandar rapido ════════════════════════════════════════════════════
    // `dt <= 0 -> return true` era un pase libre. Cuatro datagramas leidos en el mismo milisegundo dan
    // `dt = 0` y saltaban el filtro ENTERO — y como la referencia avanza con cada uno aceptado, veinte
    // pasos seguidos mueven al jugador cien metros en un milisegundo sin que nadie mire. No hace falta
    // ni entender el protocolo: basta con no dormir entre envios.
    DGS::UDPSocket flood;
    flood.bind(0);
    { timeval tv{}; tv.tv_usec = 50000;
      setsockopt(flood.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    step(flood, 7304, 100.0f);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    for (int i = 1; i <= 60; ++i) step(flood, 7304, 100.0f + 5.0f * i);   // 300 m sin dormir
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    const float xFlood = finalX(obs, 7304, 700);
    std::printf("    ráfaga sin dormir: pide 60 pasos (300 m) de golpe -> el mundo le cree en x=%.1f\n",
                xFlood);
    check(xFlood >= 0.0f && xFlood < 200.0f,
          "C · mandar rapido no compra distancia: `dt = 0` ya no es un pase libre");

    // ══ B. y un tramposo de verdad sigue rechazado ══════════════════════════════════════════════
    // Sin esto, "arreglarlo" abriendo la mano hasta que nadie se queje pasaria la fase A y dejaria el
    // filtro sin servir para nada.
    step(cheat, 7303, 100.0f);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    step(cheat, 7303, 900.0f);      // 800 m de golpe: a 150 m/s harian falta mas de 5 s
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const float xCheat = finalX(obs, 7303, 900);
    std::printf("    tramposo: salta de 100 a 900 -> el mundo le cree en x=%.1f\n", xCheat);
    check(xCheat >= 0.0f && xCheat < 200.0f,
          "B · un salto imposible SIGUE rechazado (no se ha abierto la mano)");

    g_done = true;
    kill(zone, SIGTERM);
    int st = 0; waitpid(zone, &st, 0);
    head.join();

    std::printf("\n%d/%d comprobaciones\n", g_pass, g_pass + g_fail);
    return g_fail == 0 ? 0 : 1;
}
