// ─────────────────────────────────────────────────────────────────────────────────────────────────
// client_world_e2e — can a player see anybody else?
//
// The answer was no, and nothing said so. `DGS::Client` used its UDP socket to SEND and nothing else:
// `recvLoop` read the TCP link to the head, and in a real cluster the head routes entity transfers to
// ZONES, not to clients. So `pollEntities()` and `pollGhosts()` — which is what an engine renders other
// players from — were fed by nothing. A player connected, was routed, sent their position, and stood
// in an empty world.
//
// It was invisible from inside the test suite because every other test stands up its own fake: the
// harness sends the client an entity over TCP and the client duly reports it, which pins the plumbing
// and not the path. Measured against one live zone at one moment, before the fix: the real client sent
// 60 transforms and heard back **0 entities and 0 ghosts**, while `fill_world` — a raw UDP socket
// speaking the same protocol, at the same time, to the same zone — sent 320 and received **316
// datagrams**. The datagrams were arriving. Nobody read them.
//
// What is checked here is what a player would notice:
//   A. somebody standing next to you SHOWS UP.
//   B. THE COUNTER-PROOF: somebody four kilometres away does NOT — so (A) is the client reading its
//      socket, not the zone shouting everything at everybody. It also pins interest management from
//      the only side that matters, the receiving one.
//   C. and you hear YOURSELF, which is not a bug but is a trap: the zone broadcasts the sender's own
//      entity back, so a game that spawns a remote avatar for every uuid it polls will duplicate its
//      own player. Pinned so that it is a documented property rather than a surprise.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/client.h"
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <httplib.h>

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <sys/wait.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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

static const int kApiPort = 21871;
static const int kZoneUdp = 21881;
static const float kChunkM = 1000.0f;

static const uint32_t kMe   = 5100;   // the real DGS::Client
static const uint32_t kNear = 5101;   // 20 m away, inside the interest radius
static const uint32_t kFar  = 5102;   // four chunks away, outside it

static const char* kGroupPass  = "world-e2e-group";
static const char* kMasterPass = "world-e2e-master";

static std::string toHex(const unsigned char* b, int n)
{
    static const char* hex = "0123456789abcdef";
    std::string s;
    s.reserve((size_t)n * 2);
    for (int i = 0; i < n; ++i) { s += hex[b[i] >> 4]; s += hex[b[i] & 0xF]; }
    return s;
}

/// What the login hands out, computed the way `tools/fake_login_api` computes it — which is the way
/// the SERVERS derive it. If these two ever disagree the client gets a key nothing will accept, and
/// the only symptom is silence, so they are written the same way on purpose.
static std::string groupKeyHex()
{
    unsigned char k[32];
    SHA256((const unsigned char*)kGroupPass, std::strlen(kGroupPass), k);
    return toHex(k, 32);
}

static std::string sessionKeyHex(uint32_t session)
{
    unsigned char mk[32];
    SHA256((const unsigned char*)kMasterPass, std::strlen(kMasterPass), mk);
    unsigned char out[32];
    unsigned int len = 0;
    HMAC(EVP_sha256(), mk, 32, (const unsigned char*)&session, sizeof(session), out, &len);
    return len == 32 ? toHex(out, 32) : std::string();
}

static pid_t spawnZone(const char* bin)
{
    std::fflush(stdout);   // see the note in persistence_e2e: fork duplicates an unflushed buffer
    const pid_t p = fork();
    if (p != 0) return p;
    std::freopen("/tmp/dgs_cliworld_zone.log", "w", stdout);
    std::freopen("/dev/null", "w", stderr);
    setenv("ZONE_UDP_PORT",      std::to_string(kZoneUdp).c_str(), 1);
    setenv("HEAD_SERVER_HOST",   "127.0.0.1", 1);
    setenv("HEAD_SERVER_PORT",   "42424", 1);
    setenv("VALIDATOR_HOST",     "127.0.0.1", 1);
    setenv("VALIDATOR_TCP_PORT", "21889", 1);
    setenv("SOCIAL_HOST",        "127.0.0.1", 1);
    setenv("SOCIAL_TCP_PORT",    "21888", 1);
    setenv("ZONE_RESTORE",       "0", 1);
    setenv("ZONE_PERSIST_MS",    "0", 1);
    setenv("CHUNK_X_MIN", "0", 1); setenv("CHUNK_X_MAX", "5", 1);
    setenv("CHUNK_Y_MIN", "0", 1); setenv("CHUNK_Y_MAX", "5", 1);
    setenv("CHUNK_Z_MIN", "0", 1); setenv("CHUNK_Z_MAX", "5", 1);
    setenv("CHUNK_SIZE_X", "1000.0", 1);
    setenv("CHUNK_SIZE_Y", "1000.0", 1);
    setenv("CHUNK_SIZE_Z", "1000.0", 1);
    // ⚠️ THE RADIUS IS WHAT MAKES (B) MEAN ANYTHING. It is 0 by default — everybody hears everybody —
    // and with that the "far" player would arrive too, and correctly.
    setenv("INTEREST_RADIUS_M", "500", 1);
    setenv("ENTITY_LEASE_MS", "60000", 1);
    setenv("GAME_MODULE_SO", "", 1);
    // ⚠️ THE GAME PLANE IS ENCRYPTED IN THIS TEST, and the client is given NO key in its environment:
    // it gets both from the login response. The zone holds the group key (it seals its broadcast once
    // with it, for everybody) and the master (it derives any client's session key to open uplinks).
    setenv("DGS_UDP_KEY",    kGroupPass,  1);
    setenv("DGS_UDP_MASTER", kMasterPass, 1);
    char tmpl[] = "/tmp/dgs_cliworld_XXXXXX";
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
    char tmpl[] = "/tmp/dgs_cliworld_h_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(bin, bin, (char*)nullptr);
    _exit(127);
}

/// Another player, driven the way `fill_world` drives one: a plain UDP socket sending transforms at
/// the zone's front door. Not a `DGS::Client`, on purpose — the subject is what OUR client RECEIVES,
/// and a second client would double the thing under test.
/// ⚠️ THE EXTRAS SEAL WITH WHATEVER THIS PROCESS HOLDS, which after the login is the session the API
/// issued — they share the client's process, so they share its keys. That is fine here: they are
/// stand-ins for other players and the subject is what OUR client RECEIVES, not how they authenticated.
/// The zone opens them either way, because it holds the master.
struct Extra
{
    DGS::UDPSocket sock;
    uint32_t uuid;
    int32_t  chunkX;
    float    x;

    /// Drains whatever the zone sent us. @return how many chat messages arrived.
    int pollChat()
    {
        uint8_t buf[8192]; std::string from; int port = 0;
        int n = 0;
        for (;;)
        {
            const int r = sock.receive(buf, sizeof(buf), from, port);
            if (r <= 0) break;
            DGS::Packet p; p.setBuffer(buf, (size_t)r);
            if (p.getType() == DGS::PKT_CHAT) ++n;
        }
        return n;
    }

    void send(float step)
    {
        DGS::EntityTransfer e{};
        e.uuid   = uuid;
        e.type   = DGS::ENT_PLAYER;
        e.chunkX = chunkX; e.chunkY = 0; e.chunkZ = 0;
        e.pos[0] = x; e.pos[1] = 0.0f; e.pos[2] = 500.0f;
        e.stats.speed[0] = 200.0f;   // S1 is not the subject here: never let it reject a step
        e.stats.health   = 90.0f;
        DGS::Packet p; p.pack(e);
        sock.send("127.0.0.1", kZoneUdp, p.getRawData(), p.getSize());
        x += step;
    }
};

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
        // The two keys a player needs: the GROUP key to open the zone's broadcast, and their own
        // session key to seal what they send. Neither is in this process's environment.
        const std::string body =
            std::string(R"({"token":"t","session":1,"udpKey":")") + sessionKeyHex(1) +
            R"(","groupKey":")" + groupKeyHex() + R"("})";
        res.set_content(body, "application/json");
    });
    std::thread tApi([&]{ api.listen("127.0.0.1", kApiPort); });
    for (int i = 0; i < 300 && !api.is_running(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    check(api.is_running(), "the fake login API is up");

    const pid_t head = spawnHead(headBin);
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    const pid_t zone = spawnZone(zoneBin);
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));

    DGS::Client client;
    const bool up = client.connect("127.0.0.1", 42424, "u", "p", "127.0.0.1", kApiPort);
    check(up, "the real client logs in and is routed to a zone");

    std::set<uint32_t> seen;
    size_t datagrams = 0;

    if (up)
    {
        Extra near{ {}, kNear, 0, 520.0f };   // 20 m from us
        Extra far { {}, kFar,  4, 500.0f };   // four chunks away: ~4 km
        near.sock.bind(0);
        far.sock.bind(0);
        { timeval tv{}; tv.tv_usec = 50000;
          setsockopt(near.sock.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
          setsockopt(far.sock.getSocketFD(),  SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

        const float rot[4] = { 0, 0, 0, 1 };
        for (int i = 0; i < 60; ++i)
        {
            // Under a metre per step: S1 sees maxSpeed 0 for a client that has not declared any, and
            // its allowance is then the 1 m of slack. Movement is not the subject here.
            const float pos[3] = { 500.0f + 0.3f * (float)i, 0.0f, 500.0f };
            client.sendState(kMe, 0, 0, 0, pos, rot);
            near.send(0.3f);
            far.send(0.3f);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            for (const auto& e : client.pollEntities()) { seen.insert(e.uuid); ++datagrams; }
        }
    }

    std::printf("    the client received %zu entity updates, from %zu distinct uuids\n",
                datagrams, seen.size());

    check(datagrams > 0, "A · the client READS the zone's broadcast at all (it used to read none)");
    check(seen.count(kNear) == 1, "A · a player standing 20 m away shows up in pollEntities()");
    check(seen.count(kFar) == 0,
          "B · a player 4 km away does NOT (so it is reading, not being shouted at)");
    check(seen.count(kMe) == 1,
          "C · and you hear YOURSELF: the zone echoes the sender, so a game must skip its own uuid");

    // ══ E. Proximity chat ════════════════════════════════════════════════════════════════════════
    // ⚠️ `CHAT_LOCAL` WAS IMPLEMENTED BY NOBODY. The social node returns early for it — "the owning
    // zone emits it" — and the zone had zero mentions of chat: a message on that channel went nowhere
    // at all. It belongs to the zone because "local" means who is NEAR you, which is the one thing the
    // zone knows and the social node must never have to learn.
    //
    // So it takes the same wire as a position, and the same filter: the neighbour 20 m away hears it,
    // the player 4 km away does not. That second half is the counter-proof — without it, "chat works"
    // would also pass on a zone shouting every word at everybody, which is what the head used to do.
    if (up)
    {
        Extra near2{ {}, kNear, 0, 560.0f };
        Extra far2 { {}, kFar,  4, 560.0f };
        near2.sock.bind(0); far2.sock.bind(0);
        { timeval tv{}; tv.tv_usec = 50000;
          setsockopt(near2.sock.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
          setsockopt(far2.sock.getSocketFD(),  SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

        // They have to be in the world, and it has to know where: the zone will not carry a voice for
        // somebody whose position it does not have.
        const float rot[4] = { 0, 0, 0, 1 };
        for (int i = 0; i < 12; ++i)
        {
            const float pos[3] = { 520.0f + 0.2f * (float)i, 0.0f, 500.0f };
            client.sendState(kMe, 0, 0, 0, pos, rot);
            near2.send(0.2f); far2.send(0.2f);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            near2.pollChat(); far2.pollChat();   // drain the entity traffic
        }

        client.sendChat(kMe, "me", "can you hear me", DGS::CHAT_LOCAL);

        int heardNear = 0, heardFar = 0;
        for (int i = 0; i < 20; ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            heardNear += near2.pollChat();
            heardFar  += far2.pollChat();
        }
        std::printf("    local chat: the neighbour heard %d, the player 4 km away heard %d\n",
                    heardNear, heardFar);
        check(heardNear >= 1, "E · proximity chat reaches the player standing next to you");
        check(heardFar == 0,  "E · and NOT the one four kilometres away (same radius as the world)");
    }

    // ══ D. Where the keys came from ══════════════════════════════════════════════════════════════
    // ⚠️ EVERYTHING ABOVE HAPPENED ON AN ENCRYPTED PLANE, and this process was never given a key: no
    // `DGS_UDP_KEY`, no `DGS_UDP_SESSION_KEY` in its environment. The zone seals its broadcast and
    // refuses anything whose tag does not check out, so the only way (A) could have passed is that the
    // client adopted what the login handed it — which is the plumbing that did not exist.
    //
    // The counter-proof is the same client with the keys forgotten: it must go deaf. Without it, "the
    // login works" would also pass on a build where the encryption had quietly switched itself off.
    check(std::getenv("DGS_UDP_KEY") == nullptr && std::getenv("DGS_UDP_SESSION_KEY") == nullptr,
          "D · the client process holds NO key of its own (the environment is empty)");

    if (up)
    {
        // ⚠️ DRAIN FIRST. The queues are still holding what the earlier phases produced, and counting
        // those as "heard after forgetting the keys" is measuring the past. It cost a red run.
        client.pollEntities(); client.pollGhosts(); client.pollChats();

        DGS::setUdpSessionKey(0, "");   // forget the group key the login gave us
        DGS::setUdpSessionKey(1, "");

        Extra near2{ {}, kNear, 0, 560.0f };
        near2.sock.bind(0);
        std::set<uint32_t> deaf;
        for (int i = 0; i < 30; ++i)
        {
            near2.send(0.3f);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            for (const auto& e : client.pollEntities()) deaf.insert(e.uuid);
        }
        std::printf("    with the keys forgotten the client hears %zu uuids\n", deaf.size());
        check(deaf.empty(),
              "D · and forgetting them makes it DEAF (so the keys were what made it work)");
    }

    client.disconnect();
    kill(zone, SIGTERM); kill(head, SIGTERM);
    waitpid(zone, nullptr, 0); waitpid(head, nullptr, 0);
    api.stop(); tApi.join();

    std::printf("\n== client_world_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
