// ─────────────────────────────────────────────────────────────────────────────────────────────────
// client_handoff_e2e — does the REAL client ever cause a handoff?
//
// It did not, and that made the authority handoff unreachable code in the shipped product. `handoff_e2e`
// covers the mechanism thoroughly — REASSIGN, at-least-once retry, ghost promotion, the lease released —
// and it passes. It drives the zone the way the PROTOCOL works: it reports the out-of-bounds chunk to
// the zone that owns the entity, which is the only thing that can start a handoff (`checkAndTransfer`
// walks the entities a zone owns and cedes the ones whose chunk has left its bounds).
//
// `Client::sendTransform` did the opposite. On a chunk change it asked the head first and then sent to
// the NEW zone, so the old owner never heard the player had left: it simply stopped receiving updates
// and the lease GC dropped them a few seconds later. No REASSIGN, no promotion, no lease released —
// and, worse than any of those, NO SERVER-SIDE STATE TRANSFERRED. The new zone rebuilt the entity out
// of the client's own datagram, which quietly makes the client the source of truth for its own stats
// every time it crosses a border.
//
// Measured before the fix, with this exact harness: "out of bounds. Transferring" x0, "handoff ACKED"
// x0, "promoted to real" x0. Nothing was broken; nothing was reached.
//
// ⚠️ THIS TEST IS NOT `handoff_e2e` WITH A DIFFERENT NAME. That one asks "does the handoff work when
// somebody triggers it". This one asks "does anybody trigger it", which is a question about the client
// and can only be answered by driving the real `DGS::Client` — hence the login API, a real
// `head_server_node` to route the reassignment, and two real `zone_node`s to be the two owners.
//
//   A. a player crossing from zone A into zone B causes the handoff, end to end.
//   B. THE COUNTER-PROOF: a player who never leaves zone A causes none. Without it, (A) would also
//      pass on a zone that logged "out of bounds" for everything that moved.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/client.h"
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <httplib.h>

#include <sys/wait.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
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

static const int kApiPort   = 21851;
static const int kZoneAUdp  = 21861;
static const int kZoneBUdp  = 21862;
static const uint32_t kCrosser = 4242;   // walks from chunk 0 into chunk 1
static const uint32_t kStayer  = 4243;   // never leaves chunk 0
static const uint32_t kSprinter = 4244;  // declares a speed and then uses it (phase C)
static const uint32_t kSilent   = 4245;  // moves just as fast having declared nothing

static const char* kZoneALog = "/tmp/dgs_clihandoff_a.log";
static const char* kZoneBLog = "/tmp/dgs_clihandoff_b.log";

static pid_t spawnZone(const char* bin, int udpPort, int xMin, int xMax, const char* log)
{
    std::fflush(stdout);   // see the note in persistence_e2e: fork duplicates an unflushed buffer
    const pid_t p = fork();
    if (p != 0) return p;
    std::freopen(log, "w", stdout);
    std::freopen("/dev/null", "w", stderr);
    setenv("ZONE_UDP_PORT",      std::to_string(udpPort).c_str(), 1);
    setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
    setenv("HEAD_SERVER_PORT",   "42424", 1);
    setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
    setenv("VALIDATOR_TCP_PORT", "21899", 1);
    setenv("SOCIAL_HOST",        "127.0.0.1", 1);
    setenv("SOCIAL_TCP_PORT",    "21898", 1);
    setenv("ZONE_RESTORE",       "0", 1);
    setenv("ZONE_PERSIST_MS",    "0", 1);
    setenv("CHUNK_X_MIN", std::to_string(xMin).c_str(), 1);
    setenv("CHUNK_X_MAX", std::to_string(xMax).c_str(), 1);
    setenv("CHUNK_Y_MIN", "0", 1); setenv("CHUNK_Y_MAX", "100", 1);
    setenv("CHUNK_Z_MIN", "0", 1); setenv("CHUNK_Z_MAX", "100", 1);
    setenv("CHUNK_SIZE_X", "1000.0", 1);
    // ⚠️ EL TECHO DE VELOCIDAD ES DEL SERVIDOR, y este test cruza fronteras a propósito dando saltos
    // que ningún jugador daría. Antes bastaba con que el cliente declarara una velocidad enorme —que
    // es justo el agujero que `client_claims_e2e` cierra: el anti-trampas preguntaba al sospechoso—,
    // así que ahora lo declara quien debe, el despliegue.
    setenv("CLIENT_MAX_SPEED_MPS", "1000000", 1);
    setenv("CHUNK_SIZE_Y", "1000.0", 1);
    setenv("CHUNK_SIZE_Z", "1000.0", 1);
    setenv("ENTITY_LEASE_MS", "60000", 1);   // long: a purge must not be mistaken for a handoff
    setenv("GAME_MODULE_SO", "", 1);
    char tmpl[] = "/tmp/dgs_clihandoff_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(bin, bin, (char*)nullptr);
    _exit(127);
}

static pid_t spawnHead(const char* bin)
{
    std::fflush(stdout);
    const pid_t p = fork();
    if (p != 0) return p;
    std::freopen("/dev/null", "w", stdout);
    std::freopen("/dev/null", "w", stderr);
    char tmpl[] = "/tmp/dgs_clihandoff_h_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(bin, bin, (char*)nullptr);
    _exit(127);
}

/// How many lines of `path` contain `needle`. -1 if the file is not there at all, which is a different
/// failure from "it is there and says nothing" and must not be confused with zero.
static int countIn(const char* path, const char* needle)
{
    std::FILE* f = std::fopen(path, "r");
    if (!f) return -1;
    int n = 0; char line[1024];
    while (std::fgets(line, sizeof(line), f)) if (std::strstr(line, needle)) ++n;
    std::fclose(f);
    return n;
}

/// Drives one player, one update every `everyMs`, as a game loop does.
///
/// ⚠️ THE INTERVAL IS A PARAMETER BECAUSE PHASE C DEPENDS ON IT, and that is a fact about the server,
/// not about this file. S1's allowance is `maxSpeed * dt + 1 m` with `dt` taken from the ARRIVAL TIMES
/// of consecutive datagrams — so when the machine is busy and two 50 ms updates land 1 ms apart, the
/// allowance collapses to the slack no matter how fast the player is entitled to move. At 50 ms this
/// test rejected 0 of 20 fast steps on an idle box and 10 of 20 under the full suite: a scheduling
/// outcome dressed up as an assertion. At 150 ms the scheduler would have to be 130 ms late to
/// reproduce it, and the margin becomes an enforceable property instead of a lucky one.
static void walk(DGS::Client& c, uint32_t uuid, int32_t chunkX, float fromX, float toX, int steps,
                 int everyMs = 50)
{
    const float rot[4] = { 0, 0, 0, 1 };
    for (int i = 0; i < steps; ++i)
    {
        const float t = steps > 1 ? (float)i / (float)(steps - 1) : 0.0f;
        const float pos[3] = { fromX + (toX - fromX) * t, 0.0f, 500.0f };
        c.sendState(uuid, chunkX, 0, 0, pos, rot);
        std::this_thread::sleep_for(std::chrono::milliseconds(everyMs));
    }
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);
    char ah[PATH_MAX], az[PATH_MAX];
    const char* headArg = (argc > 1) ? argv[1] : "./build/head_server_node";
    const char* zoneArg = (argc > 2) ? argv[2] : "./build/zone_node";
    const char* headBin = realpath(headArg, ah) ? ah : headArg;
    const char* zoneBin = realpath(zoneArg, az) ? az : zoneArg;

    httplib::Server api;
    api.Post("/api/auth/login", [](const httplib::Request&, httplib::Response& res) {
        res.set_content(R"({"token":"t"})", "application/json");
    });
    std::thread tApi([&]{ api.listen("127.0.0.1", kApiPort); });
    for (int i = 0; i < 300 && !api.is_running(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    check(api.is_running(), "the fake login API is up");

    const pid_t head = spawnHead(headBin);
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    // ⚠️ ZONE A OWNS EXACTLY ONE CHUNK. With a wide range the first border a player reaches is one of
    // its own, and crossing it is not a handoff at all — the test would be green about nothing.
    const pid_t zoneA = spawnZone(zoneBin, kZoneAUdp, 0, 0, kZoneALog);
    const pid_t zoneB = spawnZone(zoneBin, kZoneBUdp, 1, 5, kZoneBLog);
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));

    DGS::Client client;
    const bool up = client.connect("127.0.0.1", 42424, "u", "p", "127.0.0.1", kApiPort);
    check(up, "the real client logs in and is routed to a zone");

    if (up)
    {
        // ⚠️ EVERYONE HERE CREEPS, and that is not fussiness. The zone's S1 filter allows
        // `maxSpeed * dt + 1 m` per update, and a transform carries whatever `Stats` the client put in
        // it — which was nothing. A first version of this test moved 27 m per step and every packet
        // was discarded with "S1 blocked", so the handoff never had a chance to happen and the test
        // was measuring the anti-cheat. Phases A and B move under a metre per update so they hold
        // whatever the client does with stats; phase C is where the speed is the subject.

        // ══ B (first, so its silence is not just the run ending) ══════════════════════════════════
        // A player who never leaves chunk 0. Zone A owns them from start to finish.
        walk(client, kStayer, 0, 100.0f, 110.0f, 30);

        // ══ A. Across the border, from zone A into zone B ═════════════════════════════════════════
        walk(client, kCrosser, 0, 995.0f, 999.5f, 12);
        walk(client, kCrosser, 1,   0.5f,  20.0f, 40);
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));

        // ══ C. DOS JUGADORES, EL MISMO PASEO, EL MISMO TRATO ════════════════════════════════════
        // ⚠️ ESTA FASE MEDIA UN CONTRATO QUE YA NO EXISTE, dos veces. Primero decia "quien declara
        // 400 m/s pasa y quien no declara nada es rechazado" — o sea que los DERECHOS de un jugador
        // salian de lo que el jugador dijera de si mismo, que es el agujero que cierra
        // `client_claims_e2e`. Y ahora ni siquiera se puede declarar: `sendStats` ya no existe, porque
        // las tres llamadas del cliente eran el mismo paquete con un campo distinto relleno.
        //
        // Lo que queda es lo que debe ser cierto: dos jugadores que hacen exactamente lo mismo reciben
        // exactamente lo mismo. No es tautologico — se rompe si algo del estado de un uuid se filtra
        // al otro, que es como empezo todo esto.
        //
        // Los dos andan ~9 m cada 50 ms (180 m/s), por debajo del techo del servidor.
        walk(client, kSilent, 1, 500.0f, 680.0f, 20, 150);

        // El segundo se asienta primero: S1 compara contra la ultima posicion aceptada, asi que el
        // primer paso rapido de alguien que acaba de aparecer es un teletransporte y se rechaza por el
        // motivo correcto.
        walk(client, kSprinter, 1, 0.0f, 5.0f, 12, 50);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        walk(client, kSprinter, 1, 5.0f, 185.0f, 20, 150);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    client.disconnect();
    kill(zoneA, SIGTERM); kill(zoneB, SIGTERM); kill(head, SIGTERM);
    waitpid(zoneA, nullptr, 0); waitpid(zoneB, nullptr, 0); waitpid(head, nullptr, 0);
    api.stop(); tApi.join();

    char needle[64];
    std::snprintf(needle, sizeof(needle), "Entity %u out of bounds", kCrosser);
    const int transferring = countIn(kZoneALog, needle);
    const int acked        = countIn(kZoneALog, "handoff ACKED");
    const int promoted     = countIn(kZoneBLog, "Handoff: entity");

    std::snprintf(needle, sizeof(needle), "Entity %u out of bounds", kStayer);
    const int stayerMoved  = countIn(kZoneALog, needle);

    std::snprintf(needle, sizeof(needle), "S1 blocked uuid=%u", kSprinter);
    const int sprinterBlocked = countIn(kZoneBLog, needle);
    std::snprintf(needle, sizeof(needle), "S1 blocked uuid=%u", kSilent);
    const int silentBlocked   = countIn(kZoneBLog, needle);

    std::printf("    zone A (the one it left):  transfers %d · acked %d\n", transferring, acked);
    std::printf("    zone B (the one it entered): promotions %d\n", promoted);
    std::printf("    the player who never crossed: transfers %d\n", stayerMoved);

    check(transferring > 0, "A · a real client crossing a border makes its zone CEDE it");
    check(acked > 0,        "A · and the head routes it: the handoff is acknowledged");
    check(promoted > 0,     "A · and the receiving zone promotes it to a real entity it owns");
    check(stayerMoved == 0, "B · a player who never leaves causes NO handoff (it is not logging noise)");

    std::printf("    S1 rejections: player who DECLARED 400 m/s -> %d · player who declared nothing -> %d\n",
                sprinterBlocked, silentBlocked);
    // ⚠️ TWO PLAYERS, THE SAME MOVEMENT, ONE DIFFERENCE. Both walk 9 m every 150 ms; only one of them
    // declared a speed. S1 allows `maxSpeed * dt + 1 m`, so for the silent player the allowance is the
    // 1 m of slack and every step is rejected, while for the sprinter it is ~61 m and none should be.
    // The interval is what makes this enforceable rather than lucky — see the note on `walk`: S1 takes
    // `dt` from arrival times, and at 50 ms a busy machine bunched the packets enough to reject 10 of
    // 20 legitimate steps. That jitter sensitivity is S1's own and is written down under
    // \"What is missing\"; it is not what this phase is measuring.
    // ⚠️ ESTA FASE MEDIA UN CONTRATO QUE YA NO EXISTE, y su desaparicion es el arreglo. Antes decia:
    // quien declara 400 m/s pasa y quien no declara nada es rechazado. O sea que los DERECHOS de un
    // jugador salian de lo que el jugador dijera de si mismo — y `stats.speed` es literalmente el
    // numero con el que S1 le juzga (`maxDist = maxSpeed * dt + 1 m`). El anti-trampas preguntaba al
    // sospechoso cual era el limite: con `maxSpeed = 1e9` no se rechazaba un salto jamas.
    //
    // La zona ya no se cree nada de eso (ver la entrada UDP de `zone_node`): el techo es del servidor
    // (`CLIENT_MAX_SPEED_MPS`). Asi que lo que hay que medir aqui es lo contrario de antes — que los
    // dos reciban EL MISMO TRATO haciendo el MISMO movimiento, declaren lo que declaren. Con el codigo
    // viejo esta comprobacion se pone roja: eran 0 rechazos contra 15+.
    //
    // Que el techo se APLIQUE de verdad, y que un cliente no pueda subirselo, se mide en
    // `client_claims_e2e` contra una zona con un techo realista y con su contraprueba dentro.
    std::printf("    mismo paseo, uno declara 400 m/s y el otro nada: %d vs %d rechazos\n",
                sprinterBlocked, silentBlocked);
    check(sprinterBlocked == silentBlocked,
          "C · declarar o no declarar da EXACTAMENTE el mismo trato (el techo es del servidor)");
    check(sprinterBlocked <= 2,
          "C · y el paseo legitimo pasa: no es que se rechace a los dos por igual");

    std::printf("\n== client_handoff_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
