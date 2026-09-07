// ─────────────────────────────────────────────────────────────────────────────────────────────────
// `DGS::Client` — the other half of the protocol, and nothing named it.
//
// Everything else in this suite tests the servers. But the client is what decides WHERE a player's
// packets go: it logs in over HTTP, asks the head which zone covers its chunk, and then sends UDP to
// whatever address that answer contained. If any link in that chain is wrong, a player is simply not
// in the world — and no server-side test would notice, because the packets never arrive.
//
// The whole chain is stood up for real: an httplib API, a fake head over TCP and a fake zone over UDP.
//
//     [fake API] --200/401--> [Client] --ZoneQuery--> [fake head]
//                                     <--ZoneResponse (addr:port)--'
//                                     --EntityTransfer (UDP)--> [fake zone at THAT port]
//
// Two properties carry most of the weight, and each needs its opposite to mean anything:
//
//   · the login GATES the session — a rejected login must not open a connection to the head at all;
//   · the zone query is CACHED per chunk — re-querying every frame would hammer the head, and never
//     re-querying would leave the player talking to the wrong node after crossing a border.
//
// The second one is checked in both directions: same chunk → no new query; new chunk → exactly one
// more, AND the datagrams actually move to the port the second answer named. Counting queries alone
// would pass on a client that asked and then ignored the reply.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/client.h"
#include "include/dgs/packet.h"

#include <httplib.h>

#include <sys/socket.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);
}

// Below the ephemeral range on purpose — see the note in `validator_e2e.cpp`.
static const int kApiPort   = 21601;
static const int kHeadPort  = 21602;
static const int kZoneAPort = 21603;   // the zone the first query points at
static const int kZoneBPort = 21604;   // the zone the second query points at
static const int kSocialPort = 21605;  // chat lives here now, NOT on the head

static std::atomic<bool> g_done{false};

// ── Fake login API ──────────────────────────────────────────────────────────────────────────────
static std::atomic<bool> g_loginOk{true};      // flipped by the test to reject a login
static std::atomic<int>  g_loginCalls{0};

// ── Fake head ───────────────────────────────────────────────────────────────────────────────────
static std::atomic<int>  g_zoneQueries{0};     // how many ZoneQuery packets reached the head
static std::atomic<int>  g_chatsAtHead{0};     // must stay 0: chat left the head
static std::atomic<int>  g_chatsAtSocial{0};
static std::atomic<int>  g_socialFd{-1};
static std::atomic<int>  g_answerPort{kZoneAPort};   // which zone the head points the client at
static std::atomic<int>  g_headFd{-1};         // so the test can push packets down to the client

static DGS::TCPSocket g_wire;                  // only for send/receive over an arbitrary fd

/// A stand-in for `social_node`: accepts one connection, counts the chat it is sent, and can push one
/// back. Chat used to go to the head, which fanned it out to EVERY connection it held — measured at 96
/// chatters taking a zone query from 0.07 ms to 43.6 ms with the head at 11% of one core. That is what
/// this port exists to keep off the orchestrator.
static void fakeSocial(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kSocialPort)) { ready = true; return; }
    { timeval tv{}; tv.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    ready = true;
    while (!g_done)
    {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        g_socialFd = fd;
        { timeval tv{}; tv.tv_usec = 200000; setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
        uint8_t buf[8192];
        while (!g_done)
        {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;
            if (n < 0)  continue;
            DGS::Packet p; p.setBuffer(buf, (size_t)n);
            if (p.getType() == DGS::PKT_CHAT) ++g_chatsAtSocial;
        }
        s.closeClient(fd);
        g_socialFd = -1;
    }
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
        g_headFd = fd;
        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 200000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n == 0) break;          // the client hung up
            if (n < 0)  continue;       // just the read deadline
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() == DGS::PKT_ZONE_QUERY) {
                ++g_zoneQueries;
                DGS::ZoneResponse resp{};
                std::snprintf(resp.addr, sizeof(resp.addr), "127.0.0.1");
                resp.port = g_answerPort.load();
                DGS::Packet p; p.pack(resp);
                s.send(fd, p.getRawData(), p.getSize());
            }
            else if (r.getType() == DGS::PKT_CHAT) ++g_chatsAtHead;
        }
        g_headFd = -1;
        s.closeClient(fd);
    }
}

// ── Fake zones (UDP) ────────────────────────────────────────────────────────────────────────────
struct ZoneSink
{
    std::atomic<int>      count{0};
    std::atomic<uint32_t> lastUuid{0};
    std::atomic<uint16_t> lastAngle{0};
    std::atomic<int>      lastChunkX{-999999};
    std::atomic<uint16_t> lastDataSize{0};
    std::atomic<float>    lastSpeed{0.0f};
};
static ZoneSink g_zoneA, g_zoneB;

static std::atomic<bool> g_zoneABound{false}, g_zoneBBound{false};

static void fakeZone(int port, ZoneSink* sink, std::atomic<bool>& ready, std::atomic<bool>* bound)
{
    DGS::UDPSocket u;
    // ⚠️ A fake that silently fails to bind turns "the packet never arrived" into a finding about the
    // client when it is a finding about the test. It has to say so.
    if (!u.bind(port)) { ready = true; return; }
    *bound = true;
    { timeval tv{}; tv.tv_usec = 100000;
      setsockopt(u.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    ready = true;
    uint8_t buf[sizeof(DGS::EntityTransfer) * 2];
    std::string ip; int p = 0;
    while (!g_done) {
        const int n = u.receive(buf, sizeof(buf), ip, p);
        if (n <= 0 || buf[0] != DGS::PKT_ENTITY_TRANSFER) continue;
        DGS::EntityTransfer e{};
        DGS::Packet ep; ep.setBuffer(buf, (size_t)n);
        if (!ep.tryUnpackEntityTransfer(e)) continue;
        sink->lastUuid     = e.uuid;
        sink->lastAngle    = e.angle;
        sink->lastChunkX   = e.chunkX;
        sink->lastDataSize = e.dataSize;
        sink->lastSpeed    = e.stats.speed[0];
        ++sink->count;
    }
}

/// Waits up to `msLimit` for a counter to reach `target`. @return whether it did.
static bool waitFor(std::atomic<int>& c, int target, int msLimit)
{
    const auto t0 = std::chrono::steady_clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - t0).count() < msLimit) {
        if (c.load() >= target) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

int main()
{
    signal(SIGPIPE, SIG_IGN);

    httplib::Server api;
    api.Post("/api/auth/login", [](const httplib::Request&, httplib::Response& res) {
        ++g_loginCalls;
        if (g_loginOk) res.set_content(R"({"token":"t"})", "application/json");
        else           res.status = 401;
    });
    std::thread tApi([&]{ api.listen("127.0.0.1", kApiPort); });
    for (int i = 0; i < 200 && !api.is_running(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

    std::atomic<bool> h{false}, za{false}, zb{false};
    std::thread tHead(fakeHead, std::ref(h));
    std::thread tZa(fakeZone, kZoneAPort, &g_zoneA, std::ref(za), &g_zoneABound);
    std::thread tZb(fakeZone, kZoneBPort, &g_zoneB, std::ref(zb), &g_zoneBBound);
    std::atomic<bool> so{false};
    std::thread tSo(fakeSocial, std::ref(so));
    while (!so) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    setenv("SOCIAL_HOST", "127.0.0.1", 1);
    setenv("SOCIAL_TCP_PORT", std::to_string(kSocialPort).c_str(), 1);
    while (!h || !za || !zb) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    check(api.is_running(), "the fake login API is up");
    check(g_zoneABound.load() && g_zoneBBound.load(),
          "both fake zones actually bound their UDP ports (or nothing below would mean anything)");

    // ══ (1) A REJECTED LOGIN GATES EVERYTHING ═════════════════════════════════════════════════
    // Not just "connect returns false": it must not reach the head at all. A client that opened the
    // session anyway would be an unauthenticated player in the world.
    {
        g_loginOk = false;
        const int queriesBefore = g_zoneQueries.load();
        DGS::Client c;
        const bool ok = c.connect("127.0.0.1", kHeadPort, "u", "bad", "127.0.0.1", kApiPort);
        check(!ok, "a rejected login makes connect() fail");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        check(g_zoneQueries.load() == queriesBefore,
              "and the client never even asks the head for a zone (the login is a gate, not a warning)");
        check(!c.isConnected(), "the session is not marked as connected");
    }

    // ══ (2) LOGIN → ZONE DISCOVERY → PLAY ═════════════════════════════════════════════════════
    // The counter-proof for (1), and the chain that matters: the head names a zone and the client's
    // datagrams end up at THAT port — not at a default, not at the head.
    g_loginOk    = true;
    g_answerPort = kZoneAPort;

    DGS::Client c;
    const bool ok = c.connect("127.0.0.1", kHeadPort, "u", "good", "127.0.0.1", kApiPort);
    check(ok, "with a valid login, connect() succeeds");
    check(c.isConnected(), "and the session is live");
    check(g_loginCalls.load() >= 2, "both logins actually went through the API");

    if (ok) {
        check(waitFor(g_zoneQueries, 1, 2000), "the client asks the head which zone covers its chunk");

        const float pos[3] = { 1.0f, 2.0f, 3.0f };
        const float idRot[4] = { 0.0f, 0.0f, 0.0f, 1.0f };   // identity: yaw = 0

        c.sendState(7001, 0, 0, 0, pos, idRot);
        check(waitFor(g_zoneA.count, 1, 2000),
              "its position lands on the zone the head named (not anywhere else)");
        check(g_zoneA.lastUuid.load() == 7001, "carrying the right uuid");
        check(g_zoneB.count.load() == 0, "and NOT on the other zone");

        // ── (3) The query is cached per chunk ────────────────────────────────────────────────
        // Re-querying on every frame would turn a movement update into a round trip to the head.
        const int queriesAfterFirst = g_zoneQueries.load();
        for (int i = 0; i < 20; ++i) c.sendState(7001, 0, 0, 0, pos, idRot);
        check(waitFor(g_zoneA.count, 21, 3000), "21 updates in the same chunk all arrive");
        check(g_zoneQueries.load() == queriesAfterFirst,
              "and NONE of them re-queries the head (the zone is cached per chunk)");

        // ── (4) Crossing a border: ANNOUNCE, then re-query, and the traffic MOVES ────────────
        // ⚠️ THE ORDER CHANGED, AND THIS TEST USED TO PIN THE WRONG ONE. It asserted that the client
        // re-queries the head the instant its chunk changes — which it did, and which is exactly why
        // the authority handoff was unreachable code: the zone that OWNED the player never heard they
        // had left, because the very next datagram went to somebody else. Only the owner can start a
        // handoff (`checkAndTransfer`), so the entity was not ceded, it was quietly GC'd, and no
        // server-side state moved with it. See `client_handoff_e2e`, which measures that end to end.
        //
        // So the client now ANNOUNCES first: it keeps reporting to the zone it is leaving, carrying the
        // NEW chunk, for `ZONE_ANNOUNCE_MS`, and only then asks the head. Both halves are pinned below,
        // because "it announces" without "it eventually moves" would be a client stuck in the past.
        g_answerPort = kZoneBPort;
        const int aBeforeCross = g_zoneA.count.load();
        c.sendState(7001, 5, 0, 0, pos, idRot);
        check(waitFor(g_zoneA.count, aBeforeCross + 1, 2000),
              "crossing a border first TELLS THE ZONE BEING LEFT (that is what starts the handoff)");
        check(g_zoneA.lastChunkX.load() == 5,
              "and what it tells it is the NEW chunk (which is what puts the entity out of bounds)");
        check(g_zoneQueries.load() == queriesAfterFirst,
              "and it has NOT asked the head yet: authority moves when the servers agree, not before");

        // Now let the announcement window run out. The client keeps sending, as a game loop does.
        for (int i = 0; i < 12 && g_zoneQueries.load() == queriesAfterFirst; ++i) {
            c.sendState(7001, 5, 0, 0, pos, idRot);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        check(waitFor(g_zoneQueries, queriesAfterFirst + 1, 3000),
              "and once the announcement is done it DOES re-query the head");
        // Counting queries is not enough: a client that asked and ignored the answer would pass. The
        // observable that matters is which socket the next datagram lands on.
        check(waitFor(g_zoneB.count, 1, 3000),
              "and the update goes to the NEW zone (the answer is used, not just requested)");
        check(g_zoneB.lastChunkX.load() == 5, "with the new chunk in it");

        const int aBefore = g_zoneA.count.load();
        const int bBefore = g_zoneB.count.load();
        c.sendState(7001, 5, 0, 0, pos, idRot);
        check(waitFor(g_zoneB.count, bBefore + 1, 2000) && g_zoneA.count.load() == aBefore,
              "and it stays there: the old zone stops receiving");

        // ── (5) The yaw encoding is a real encoding ──────────────────────────────────────────
        // `sendTransform` packs the quaternion's yaw into a uint16. A stub returning a constant would
        // satisfy every check above, so the mapping itself is pinned: yaw = 0 sits in the middle of
        // the range (that is what the +π offset is for) and a different yaw gives a different code.
        const uint16_t angleIdentity = g_zoneB.lastAngle.load();
        check(angleIdentity > 32000 && angleIdentity < 33500,
              "a yaw of 0 encodes to the MIDDLE of the uint16 range (the +pi offset is applied)");

        const float yaw90[4] = { 0.0f, 0.70710678f, 0.0f, 0.70710678f };   // +90 deg about Y
        const int b2 = g_zoneB.count.load();
        c.sendState(7001, 5, 0, 0, pos, yaw90);
        check(waitFor(g_zoneB.count, b2 + 1, 2000), "a rotated update arrives");
        check(g_zoneB.lastAngle.load() != angleIdentity,
              "and a different yaw encodes to a DIFFERENT angle (it is not a constant)");

        // ── (6) EL PAYLOAD VIAJA CON EL ESTADO, y no por un canal propio ─────────────────────
        // ⚠️ ESTA FASE PROBABA TRES LLAMADAS QUE YA NO EXISTEN. `sendTransform`, `sendStats` y
        // `sendInventory` mandaban el MISMO paquete con un campo distinto relleno, y tenerlas
        // separadas costaba que cada una tuviera que acordarse de lo que las otras habian dicho — de
        // ahi salieron dos bugs: unos `Stats` en blanco que borraban la velocidad declarada, y una
        // posicion a cero que teletransportaba al jugador al origen de su chunk.
        //
        // Ahora es una sola, y las stats del juego ya no viajan en `Stats` —que es la forma de UN
        // juego metida en el protocolo— sino dentro del payload opaco, junto al inventario. Lo que se
        // comprueba aqui es lo unico que el DGS promete de ese payload: que llega con su TAMANO, no
        // los cuatro kilobytes enteros.
        const int b4 = g_zoneB.count.load();
        const uint8_t inv[] = { 1, 2, 3, 4, 5, 6, 7 };
        c.sendState(7001, 5, 0, 0, pos, idRot, inv, sizeof(inv));
        check(waitFor(g_zoneB.count, b4 + 1, 2000) && g_zoneB.lastDataSize.load() == sizeof(inv),
              "el payload opaco viaja con el estado, y con su tamano (dataSize, no 4 KB)");

        // ── (7) Chat goes to the SOCIAL node — not the head, not the zone ────────────────────
        // ⚠️ THIS TEST USED TO PIN THE OPPOSITE, and it was green the whole time chat was costing the
        // orchestrator its day job. The head's handler fanned every message out to every connection it
        // held — players, and also every zone and validator, which dropped them — with no channel, no
        // rate limit and no ban check, on the single thread that also routes authority handoffs.
        // Measured on loopback: 96 chatters took a zone query from 0.07 ms to 43.6 ms while the head
        // sat at 11% of one core. It was never CPU; it was head-of-line blocking, the answer arriving
        // behind everyone else's conversation on the same stream.
        const int b5 = g_zoneB.count.load();
        c.sendChat(7001, "andoni", "hola");
        check(waitFor(g_chatsAtSocial, 1, 2000), "chat is sent to the SOCIAL node");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        check(g_chatsAtHead.load() == 0,
              "and NOT to the head (the orchestrator is out of the chat path)");
        check(g_zoneB.count.load() == b5, "and NOT to the zone over UDP (they are different planes)");

        // ── (8) The receive loop sorts what arrives, and polling DRAINS ──────────────────────
        // If `poll*` did not drain, the engine would replay the same entities every frame forever.
        const int fd = g_headFd.load();
        if (fd >= 0) {
            DGS::EntityTransfer e{}; e.uuid = 4242; e.chunkX = 5;
            DGS::Packet pe; pe.pack(e);
            g_wire.send(fd, pe.getRawData(), pe.getSize());

            // The chat comes back on the SOCIAL link, which is where it lives now.
            DGS::ChatMessage cm{}; cm.uuid = 9; std::snprintf(cm.text, sizeof(cm.text), "eco");
            DGS::Packet pc; pc.pack(cm);
            const int sfd = g_socialFd.load();
            if (sfd >= 0) g_wire.send(sfd, pc.getRawData(), pc.getSize());

            std::vector<DGS::EntityTransfer> ents;
            std::vector<DGS::ChatMessage>    chats;
            for (int i = 0; i < 100 && (ents.empty() || chats.empty()); ++i) {
                if (ents.empty())  { auto v = c.pollEntities(); if (!v.empty()) ents = v; }
                if (chats.empty()) { auto v = c.pollChats();    if (!v.empty()) chats = v; }
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            check(ents.size() == 1 && ents[0].uuid == 4242,
                  "an entity arriving from the head lands in pollEntities()");
            check(chats.size() == 1 && chats[0].uuid == 9,
                  "and a chat from the SOCIAL link lands in pollChats() (two sockets, one inbox)");
            check(c.pollEntities().empty() && c.pollChats().empty(),
                  "a second poll comes back EMPTY: polling drains, it does not replay");
        } else {
            check(false, "the head still holds the client's connection");
        }

        c.disconnect();
        check(!c.isConnected(), "disconnect() stops the session cleanly");
    }

    g_done = true;
    api.stop();
    tApi.join();
    tSo.join(); tHead.join(); tZa.join(); tZb.join();

    std::printf("\n== client_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
