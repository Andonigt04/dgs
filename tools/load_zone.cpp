// ─────────────────────────────────────────────────────────────────────────────────────────────────
// load_zone — how many players does ONE zone actually hold, and what breaks first.
//
// Everything in this project is measured except the one number anybody asks about, so this ramps a
// real `zone_node` with real clients until it stops keeping up, and prints what gave way.
//
// It drives the node through its front door: N UDP sockets, each sending an `EntityTransfer` at 20 Hz
// exactly as `Client::sendTransform` does, each receiving the zone's broadcast. A fake head collects
// the node's own `ServerMetrics`, so the node's view (entities served, bytes out, loop time) can be
// compared against the harness's view (datagrams sent, datagrams received). When the two disagree,
// the disagreement IS the result.
//
// ⚠️ WHAT THIS IS NOT MEASURING. Clients declare a large `maxSpeed` and move in small steps so the S1
// filter never rejects anything: the subject is throughput, not validation. And there is no validator
// running, so the zone is in fail-open — a verdict round trip would add its own cost and is measured
// separately by `net_degraded`.
//
// ⚠️ THE HARNESS MUST PROVE IT KEPT UP. "The zone lost datagrams" and "the harness could not send
// them" look identical from the node's side, so every row prints the send rate actually achieved
// against the 20 Hz per client it was aiming for. If that column sags, the row says nothing about the
// zone and is marked as such.
//
//   load_zone [zone-bin] [stub.so] [max-clients] [seconds-per-step]
//
// ── 1000 JUGADORES, Y EL DOBLE DE CARGA ──────────────────────────────────────────────────────────
// La rampa por defecto llega a 64 y dobla; para ir DIRECTO a una poblacion, `LOAD_MIN_N`. Y «el
// doble de carga» son dos ejes distintos, que cargan sitios distintos y por eso son dos mandos:
//
//   LOAD_MIN_N=1000 ... 1000                 # 1000 jugadores a 20 Hz
//   LOAD_HZ=40 LOAD_MIN_N=1000 ... 1000      # el doble de RITMO: dobla lo que ENTRA
//   LOAD_MIN_N=2000 ... 2000                 # el doble de GENTE: cuadruplica lo que SALE
//
// ⚠️ A 1000 NO SALE NADA SI NO SE PONEN DOS COSAS MAS, y las dos tienen su propio mando porque las
// dos son paredes medidas, no manias:
//   · `LOAD_SPREAD_CHUNKS` — amontonados, 1000 jugadores son 10^6 datagramas por tick (la difusion
//     es N²). No es lento: no existe. `tests/load_model.cpp` dice que hacen falta ~87 chunks.
//   · `LOAD_DRAIN_MAX` — la zona vacia su socket hasta `ZONE_UDP_DRAIN_MAX` datagramas POR TICK
//     (256 por defecto), o sea 2 560/s para la zona entera: 128 jugadores a 20 Hz. Con el defecto,
//     1000 clientes mandan 8 veces lo que se puede drenar y la cola solo crece.
//
//   LOAD_SPREAD_CHUNKS=100 LOAD_DRAIN_MAX=4096 LOAD_INTEREST_M=500 \
//     LOAD_MIN_N=1000 ./build/load_zone ./build/zone_node ./build/stub_rules.so 1000 10
//
// ⚠️ Y HAY QUE SUBIR EL LIMITE DE DESCRIPTORES: 1000 clientes son 1000 sockets y el defecto de casi
// todo Linux es 1024 contando los que ya hay abiertos. Esto lo sube solo (setrlimit) y DICE hasta
// donde ha podido; si no llega, recorta la poblacion y lo canta en vez de fallar a medias.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "tests/metric.h"

#include <sys/resource.h>

#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <csignal>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

static const int kHeadPort = 21701;
static const int kValPort  = 21702;
static const int kSocPort  = 21703;
static const int kZoneUdp  = 21704;
static const float kChunkM = 1000.0f;

static std::atomic<bool> g_done{false};

// What the NODE says about itself, straight off its metrics to the head.
static std::atomic<int>      g_activeEntities{0};
static std::atomic<uint64_t> g_bytesTx{0};
static std::atomic<uint64_t> g_bytesRx{0};
static std::atomic<int>      g_loopUs{0};        // ServerMetrics::performance, in microseconds
static std::atomic<int>      g_metricCount{0};

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void fakeHead(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kHeadPort)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;
    while (!g_done) {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        DGS::Command cmd{};
        cmd.chunkSizeX = kChunkM; cmd.chunkSizeY = kChunkM; cmd.chunkSizeZ = kChunkM;
        cmd.port = kZoneUdp;
        std::snprintf(cmd.addr, sizeof(cmd.addr), "127.0.0.1");
        DGS::Packet p; p.pack(cmd);
        s.send(fd, p.getRawData(), p.getSize());
        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 300000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        // ⚠️ DIAGNOSTIC MODE, and the reason it exists. The zone's tick measured 4.6 Hz at EVERY
        // population, N=1 included, so the ceiling was not load — it was blocking. Its per-tick waits
        // add up: 100 ms of `usleep` plus a 100 ms receive timeout on the HEAD socket plus 5 + 5 for
        // the validator and social plus 10 for the last empty UDP read = ~220 ms, which is the 4.5 Hz
        // measured. With LOAD_CHATTY_HEAD=1 the head sends something every 20 ms, so that 100 ms wait
        // returns immediately and the difference in tick rate is the cost of the blocking, isolated.
        const bool chatty = std::getenv("LOAD_CHATTY_HEAD") != nullptr;
        std::thread chatter;
        if (chatty) chatter = std::thread([&s, fd]{
            DGS::Packet ping; ping.pack(DGS::PKT_ZONE_LIST);   // the zone ignores it: only the wake matters
            while (!g_done) {
                s.send(fd, ping.getRawData(), ping.getSize());
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        });

        while (!g_done) {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;
            if (n < 0)  continue;
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() == DGS::PKT_METRICS) {
                const auto m = r.unpackServerMetrics();
                g_activeEntities = (int)m.activeEntities;
                g_bytesTx = m.bytesTx;
                g_bytesRx = m.bytesRx;
                g_loopUs  = (int)(m.performance * 1000.0f);   // it is filled in milliseconds
                ++g_metricCount;
            }
        }
        if (chatter.joinable()) chatter.join();
        s.closeClient(fd);
    }
}

static void fakeSimple(int port, std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(port)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;
    while (!g_done) {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        uint8_t buf[4096];
        timeval tv{}; tv.tv_usec = 200000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) { if (s.receive(fd, buf, sizeof(buf)) == 0) break; }
        s.closeClient(fd);
    }
}

struct Client
{
    std::unique_ptr<DGS::UDPSocket> sock;
    uint32_t uuid = 0;
    int32_t  chunkX = 50;   // where in the world this player is (see LOAD_SPREAD_CHUNKS)
    float    x = 0.0f;
    uint64_t recvCount = 0;
    uint64_t recvBytes = 0;
    uint64_t selfCount = 0;   // its OWN entity coming back: one per tick, interest radius or not
};

int main(int argc, char** argv)
{
    std::signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX], absStub[PATH_MAX];
    const char* nodePath = realpath((argc > 1) ? argv[1] : "./build/zone_node", abs)
                           ? abs : "./build/zone_node";
    const char* stubPath = realpath((argc > 2) ? argv[2] : "./build/stub_rules.so", absStub)
                           ? absStub : "./build/stub_rules.so";
    int       maxClients = (argc > 3) ? std::atoi(argv[3]) : 64;
    const int stepSecs   = (argc > 4) ? std::atoi(argv[4]) : 4;
    auto envInt = [](const char* k, int d) { const char* v = std::getenv(k); return v ? std::atoi(v) : d; };
    const double hzCliente = (double)envInt("LOAD_HZ", 20);        // el ritmo de CADA jugador
    const int    minN      = envInt("LOAD_MIN_N", 1);              // donde empieza la rampa
    const int    interestM = envInt("LOAD_INTEREST_M", 0);         // radio de interes de la zona
    const int    drainMax  = envInt("LOAD_DRAIN_MAX", 0);          // 0 = el defecto del nodo (256)
    // ⚠️ EL REPARTO TIENE QUE CABER EN LA REGION DE LA ZONA, y no cabia. Los clientes se colocan en
    // los chunks 50..50+spread−1 y la zona declaraba 0..100 fijo: con `LOAD_SPREAD_CHUNKS=300`, dos
    // tercios de los jugadores caian FUERA y el nodo se pasaba el tick intentando traspasarlos a un
    // vecino que no existe — 1643 «out of bounds» en una corrida de 10 s. La fila seguia diciendo
    // «2000 servidos» y su bucle era un 35 % mas alto que el de la difusion, que es lo que se creia
    // estar midiendo. Un banco que mide otra cosa y no lo dice es peor que no tenerlo.
    const int    spread    = envInt("LOAD_SPREAD_CHUNKS", 0);
    const bool   verdict   = std::getenv("LOAD_VERDICT") != nullptr;

    // ── Descriptores ────────────────────────────────────────────────────────────────────────────
    // Un cliente = un socket. El limite blando tipico es 1024 CONTANDO lo que ya hay abierto, asi
    // que a 1000 clientes se agota a mitad de la rampa: los `bind` empiezan a fallar y el banco mide
    // una poblacion que no es la que dice. Se sube al limite duro (no hace falta root) y, si aun asi
    // no llega, se RECORTA la poblacion y se dice — medir 1000 con 900 sockets no es medir 1000.
    {
        struct rlimit rl{};
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
            const rlim_t querido = (rlim_t)maxClients + 128;       // + los del propio banco y el nodo
            if (rl.rlim_cur < querido) {
                rl.rlim_cur = (rl.rlim_max == RLIM_INFINITY) ? querido
                                                             : std::min<rlim_t>(querido, rl.rlim_max);
                setrlimit(RLIMIT_NOFILE, &rl);
                getrlimit(RLIMIT_NOFILE, &rl);
            }
            std::printf("  descriptores: limite %llu (hacen falta ~%d)\n",
                        (unsigned long long)rl.rlim_cur, maxClients + 128);
            if (rl.rlim_cur < querido) {
                const int cabe = (int)rl.rlim_cur - 128;
                std::printf("  ⚠️  no llega: la poblacion se recorta de %d a %d "
                            "(sube el limite duro con `ulimit -Hn`)\n", maxClients, cabe);
                if (cabe < 1) return 1;
                maxClients = cabe;
            }
        }
    }

    std::atomic<bool> h{false}, v{false}, so{false};
    std::thread th(fakeHead, std::ref(h));
    std::thread tv(fakeSimple, kValPort, std::ref(v));
    std::thread ts(fakeSimple, kSocPort, std::ref(so));
    while (!h || !v || !so) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pid = fork();
    if (pid < 0) { std::printf("fork failed\n"); return 1; }
    if (pid == 0) {
        std::freopen("/tmp/dgs_load_zone.log", "w", stdout);
        setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
        setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
        setenv("HEAD_SERVER_PORT",   std::to_string(kHeadPort).c_str(), 1);
        setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
        setenv("VALIDATOR_TCP_PORT", std::to_string(kValPort).c_str(), 1);
        setenv("SOCIAL_HOST",        "127.0.0.1", 1);
        setenv("SOCIAL_TCP_PORT",    std::to_string(kSocPort).c_str(), 1);
        setenv("CHUNK_X_MIN", "0", 1);
        // Hasta donde llega el reparto (los clientes empiezan en el chunk 50), con holgura.
        setenv("CHUNK_X_MAX", std::to_string(std::max(100, 50 + spread + 1)).c_str(), 1);
        setenv("CHUNK_Y_MIN", "0", 1); setenv("CHUNK_Y_MAX", "100", 1);
        setenv("CHUNK_Z_MIN", "0", 1); setenv("CHUNK_Z_MAX", "100", 1);
        setenv("CHUNK_SIZE_X", "1000.0", 1);
        setenv("CHUNK_SIZE_Y", "1000.0", 1);
        setenv("CHUNK_SIZE_Z", "1000.0", 1);
        setenv("ENTITY_LEASE_MS", "10000", 1);   // long: nobody must be purged mid-measurement
        setenv("GAME_MODULE_SO", stubPath, 1);
        // Las dos paredes que a 1000 jugadores hay que mover a mano (ver la cabecera). Si no se
        // piden, el nodo se queda con sus defectos y el banco mide el defecto, que tambien vale.
        if (interestM > 0) setenv("INTEREST_RADIUS_M", std::to_string(interestM).c_str(), 1);
        if (drainMax  > 0) setenv("ZONE_UDP_DRAIN_MAX", std::to_string(drainMax).c_str(), 1);
        char tmpl[] = "/tmp/dgs_load_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    for (int i = 0; i < 400 && g_metricCount.load() == 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    if (g_metricCount.load() == 0) { std::printf("the zone never reported\n"); kill(pid, SIGTERM); return 1; }

    const size_t E = sizeof(DGS::EntityTransfer);
    std::printf("\n  ONE zone, real clients at 20 Hz, %d s per step.  EntityTransfer = %zu B\n", stepSecs, E);
    std::printf("  The zone broadcasts EVERY entity to EVERY client once per tick, so what each client\n");
    std::printf("  receives is N entities x the tick rate, and the zone's egress grows as N x N.\n\n");
    std::printf("  `snap/s` is the tick rate each client experiences: its own entity echoed back, once\n");
    std::printf("  10 (the 100 ms tick). It falling is the zone losing the ability to keep its clients\n");
    std::printf("  current, which is the thing that matters to a player.\n\n");
    std::printf("  %5s %7s %8s %8s %7s %9s %9s %8s %8s %8s\n",
                "N", "served", "sent/s", "want/s", "snap/s", "MB/s cli", "MB/s out", "lat p50", "lat p95", "loop");
    std::printf("  %5s %7s %8s %8s %7s %9s %9s %8s %8s %8s\n",
                "-----", "------", "-------", "-------", "------", "--------", "--------", "-------", "-------", "-------");

    std::vector<Client> clients;
    int firstBroken = -1;

    // La rampa empieza donde diga `LOAD_MIN_N` (1 por defecto) y dobla, pero el ULTIMO escalon es
    // exactamente `maxClients`: pidiendo 1000 hay que medir 1000, no 512 y luego nada.
    for (int n = (minN > 1 ? std::min(minN, maxClients) : 1); ; n = std::min(n * 2, maxClients))
    {
        // Grow the population; existing clients keep their identity so the zone is not rebuilt.
        while ((int)clients.size() < n)
        {
            Client c;
            c.sock = std::make_unique<DGS::UDPSocket>();
            c.sock->bind(0);
            // ⚠️ NON-BLOCKING, not a short timeout. With SO_RCVTIMEO of 1 ms, draining N empty sockets
            // cost N milliseconds per pass, and at N=32 that alone ate more than the 50 ms send period:
            // the harness fell to 448 datagrams/s of the 640 it was aiming for and the row had to be
            // thrown away. An empty read must cost nothing.
            const int fd = c.sock->getSocketFD();
            fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
            // And a big receive buffer, so a drop is the zone's or the kernel's queue, never ours.
            // ⚠️ PERO REPARTIDO ENTRE TODOS. Eran 8 MiB fijos por socket, lo cual esta muy bien con 64
            // clientes (512 MiB de tope) y es una bomba con 1000: en esta maquina `rmem_max` son 4 MiB,
            // asi que serian hasta 4 GiB de cola de kernel con 6 GiB libres — el banco moriria por OOM
            // y el resultado seria «la zona no aguanta 1000», que es justo lo contrario de lo medido.
            // Con un presupuesto total, a 1000 clientes tocan 256 KiB cada uno: un cliente recibe ~13
            // entidades de 62 B por tick, o sea que 256 KiB son varios segundos de holgura.
            const int kPresupuestoRx = 256 * 1024 * 1024;
            const int rcvbuf = std::max(256 * 1024, std::min(8 * 1024 * 1024,
                                        kPresupuestoRx / std::max(1, maxClients)));
            setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
            c.uuid = 5000 + (uint32_t)clients.size();
            // ⚠️ WHERE THE PLAYERS STAND DECIDES WHAT INTEREST MANAGEMENT CAN DO, so it is a knob and
            // both cases get measured. Default 0 = everybody in one chunk, a scrum, which is the
            // scenario the original capacity table used and the one interest management cannot help.
            // LOAD_SPREAD_CHUNKS=N puts them across N chunks, kilometres apart, which is what a world
            // normally looks like.
            c.chunkX = spread > 0 ? (int32_t)(50 + (clients.size() % (size_t)spread)) : 50;
            c.x    = 100.0f + 3.0f * (float)clients.size();
            clients.push_back(std::move(c));
        }

        auto sendOne = [&](Client& c, float tag) {
            DGS::EntityTransfer e{};
            e.uuid   = c.uuid;
            e.type   = DGS::ENT_PLAYER;
            e.chunkX = c.chunkX; e.chunkY = 50; e.chunkZ = 50;
            e.pos[0] = c.x; e.pos[1] = 0.0f; e.pos[2] = 0.0f;
            e.stats.speed[0] = 5000.0f;      // S1 is not the subject: never let it reject a step
            e.stats.baseDMG  = tag;          // echo tag, untouched by the zone and by the rules stub
            DGS::Packet p; p.pack(e);   // the same wire the real client uses
            c.sock->send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
        };

        // Let the zone register everyone before measuring.
        for (int w = 0; w < 40 && g_activeEntities.load() < n; ++w) {
            for (auto& c : clients) sendOne(c, 0.0f);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        for (auto& c : clients) { c.recvCount = 0; c.recvBytes = 0; c.selfCount = 0; }
        const uint64_t txBefore = g_bytesTx.load();

        // ── The measurement window ──────────────────────────────────────────────────────────────
        std::vector<double> latencies;
        uint64_t sent = 0;
        uint8_t  buf[sizeof(DGS::EntityTransfer) * 2];
        std::string from; int port = 0;

        const auto t0 = std::chrono::steady_clock::now();
        uint64_t nextTick = 0;
        while (true)
        {
            const double elapsed = std::chrono::duration<double>(
                                       std::chrono::steady_clock::now() - t0).count();
            if (elapsed >= (double)stepSecs) break;

            // El ritmo de cada cliente: 20 Hz nominal, `LOAD_HZ` para el doble de carga.
            if ((uint64_t)(elapsed * hzCliente) >= nextTick)
            {
                ++nextTick;
                const float tag = (float)nowMs();
                for (auto& c : clients) { c.x += 0.5f; sendOne(c, tag); ++sent; }
            }

            // Drain everyone. The probe client's echoes give the latency.
            for (size_t i = 0; i < clients.size(); ++i)
            {
                Client& c = clients[i];
                for (int k = 0; k < 64; ++k)
                {
                    const int r = c.sock->receive(buf, sizeof(buf), from, port);
                    if (r <= 0) break;
                    c.recvCount++; c.recvBytes += (uint64_t)r;
                    // ⚠️ `snap/s` USED TO ASSUME EVERY CLIENT GETS EVERY ENTITY — it divided the
                    // datagram count by N twice. That was true when the zone told everybody about
                    // everything; with interest management a spread-out player receives only what is
                    // near them, so the old formula read 0.5 Hz and the harness printed "the zone is
                    // behind" about a zone that was ticking perfectly. A harness that measures a world
                    // the server no longer implements is worse than no harness.
                    //
                    // What is counted now is the client's OWN entity coming back, which is exactly one
                    // datagram per tick per client whatever the interest radius is.
                    if (buf[0] == DGS::PKT_ENTITY_TRANSFER)
                    {
                        DGS::EntityTransfer self{};
                        DGS::Packet sp; sp.setBuffer(buf, (size_t)r);
                        if (sp.tryUnpackEntityTransfer(self) && self.uuid == c.uuid) c.selfCount++;
                    }
                    if (i == 0 && buf[0] == DGS::PKT_ENTITY_TRANSFER)
                    {
                        DGS::EntityTransfer e{};
                        DGS::Packet ep; ep.setBuffer(buf, (size_t)r);
                        if (!ep.tryUnpackEntityTransfer(e)) continue;
                        if (e.uuid == clients[0].uuid && e.stats.baseDMG > 0.0f)
                        {
                            const double lat = (double)nowMs() - (double)e.stats.baseDMG;
                            if (lat >= 0.0 && lat < 60000.0) latencies.push_back(lat);
                        }
                    }
                }
            }
        }
        const double secs = std::chrono::duration<double>(
                                std::chrono::steady_clock::now() - t0).count();

        uint64_t rxBytes = 0, rxCount = 0;
        uint64_t selfTotal = 0;
        for (const auto& c : clients) { rxBytes += c.recvBytes; rxCount += c.recvCount; selfTotal += c.selfCount; }

        const double sentPerSec = (double)sent / secs;
        const double wantPerSec = hzCliente * (double)n;
        const double measMBs    = (double)rxBytes / secs / 1e6;
        const double perCliMBs  = measMBs / (double)n;
        // Ticks per second as each client experiences them: its own entity echoed back, once per
        // tick. Independent of how many OTHER entities it is told about.
        const double snapsPerSec = (double)selfTotal / secs / (double)n;

        std::sort(latencies.begin(), latencies.end());
        const double p50 = latencies.empty() ? -1.0 : latencies[latencies.size() * 50 / 100];
        const double p95 = latencies.empty() ? -1.0
                          : latencies[std::min(latencies.size() - 1, latencies.size() * 95 / 100)];

        const int served = g_activeEntities.load();
        const bool harnessKeptUp = sentPerSec > wantPerSec * 0.9;
        // Broken = it stopped serving everyone, or clients get fewer than half the nominal snapshots,
        // or the echo latency passed a third of a second, which is where a player feels it.
        const bool zoneKeptUp = served >= n && snapsPerSec > 5.0 && p95 < 333.0;
        if (!zoneKeptUp && firstBroken < 0 && harnessKeptUp) firstBroken = n;

        std::printf("  %5d %7d %8.0f %8.0f %7.1f %9.3f %9.2f %8.0f %8.0f %7dus%s\n",
                    n, served, sentPerSec, wantPerSec, snapsPerSec, perCliMBs, measMBs,
                    p50, p95, g_loopUs.load(),
                    harnessKeptUp ? (zoneKeptUp ? "" : "   <- the zone is behind")
                                  : "   <- THE HARNESS is behind: row invalid");
        std::fflush(stdout);

        // Las cifras de CADA escalon, al canal de metricas: asi una corrida de esto sube al resumen
        // del job (o a donde se recoja) sin que nadie tenga que leer la tabla a ojo. La clave lleva
        // la poblacion, que es lo que distingue una fila de otra.
        {
            char key[80];
            auto pub = [&](const char* que, double v, const char* unidad) {
                std::snprintf(key, sizeof key, "%s_n%d", que, n);
                dgsMetric(key, v, unidad);
            };
            pub("servidos", (double)served, "entidades");
            pub("snapshots", snapsPerSec, "Hz");
            pub("egreso", measMBs, "MB/s");
            pub("latencia_p95", p95, "ms");
            pub("bucle", (double)g_loopUs.load() / 1000.0, "ms");
            pub("banco_envio", sentPerSec, "dg/s");
        }

        (void)txBefore;
        if (n >= maxClients) break;
    }

    std::printf("\n");
    if (firstBroken > 0)
        std::printf("  first population where the zone stops keeping up: N = %d\n", firstBroken);
    else
        std::printf("  the zone kept up to N = %d (nothing broke inside the range tested)\n", maxClients);
    std::printf("  the node's log is in /tmp/dgs_load_zone.log\n");

    // ── EL VEREDICTO (LOAD_VERDICT=1), para cuando esto corre como test ──────────────────────────
    // ⚠️ NO FALLA PORQUE LA ZONA VAYA TARDE. A 1000 jugadores se SABE que va tarde —el modelo lo
    // dice y la tabla medida tambien—, asi que exigir el tick nominal seria un test que nace roto y
    // que se acabaria comentando. Lo que si tiene que seguir siendo verdad a cualquier poblacion, y
    // es lo que se vigila aqui: que el nodo SIGA VIVO y siga sirviendo a todo el mundo. Perder
    // entidades, morirse o dejar de responder son regresiones; ir lento es una cifra.
    const int servedFinal = g_activeEntities.load();
    int nodeStatus = 0;
    const bool nodeAlive = (waitpid(pid, &nodeStatus, WNOHANG) == 0);
    bool malo = false;
    if (verdict) {
        std::printf("\n  veredicto: el nodo %s · sirviendo %d de %d\n",
                    nodeAlive ? "sigue vivo" : "SE HA MUERTO", servedFinal, maxClients);
        if (!nodeAlive)                  { std::printf("  [FAIL] el zone_node no sobrevivio a la carga\n"); malo = true; }
        else if (servedFinal < maxClients) { std::printf("  [FAIL] la zona dejo de servir a %d entidades\n",
                                                         maxClients - servedFinal); malo = true; }
        else                               std::printf("  [ok]   la zona aguanta %d jugadores sin perder a nadie\n",
                                                       maxClients);
        std::printf("\n== load_zone: %d OK · %d FAILED ==\n", malo ? 0 : 1, malo ? 1 : 0);
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    g_done = true;
    th.join(); tv.join(); ts.join();
    return malo ? 1 : 0;
}
