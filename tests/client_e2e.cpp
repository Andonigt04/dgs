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

static std::atomic<bool> g_done{false};

// ── Fake login API ──────────────────────────────────────────────────────────────────────────────
static std::atomic<bool> g_loginOk{true};      // flipped by the test to reject a login
static std::atomic<int>  g_loginCalls{0};

// ── Fake head ───────────────────────────────────────────────────────────────────────────────────
static std::atomic<int>  g_zoneQueries{0};     // how many ZoneQuery packets reached the head
static std::atomic<int>  g_chatsAtHead{0};
static std::atomic<int>  g_answerPort{kZoneAPort};   // which zone the head points the client at
static std::atomic<int>  g_headFd{-1};         // so the test can push packets down to the client

static DGS::TCPSocket g_wire;                  // only for send/receive over an arbitrary fd

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
        if (n != (int)sizeof(DGS::EntityTransfer)) continue;
        DGS::EntityTransfer e;
        std::memcpy(&e, buf, sizeof(e));
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

        c.sendTransform(7001, 0, 0, 0, pos, idRot);
        check(waitFor(g_zoneA.count, 1, 2000),
              "its position lands on the zone the head named (not anywhere else)");
        check(g_zoneA.lastUuid.load() == 7001, "carrying the right uuid");
        check(g_zoneB.count.load() == 0, "and NOT on the other zone");

        // ── (3) The query is cached per chunk ────────────────────────────────────────────────
        // Re-querying on every frame would turn a movement update into a round trip to the head.
        const int queriesAfterFirst = g_zoneQueries.load();
        for (int i = 0; i < 20; ++i) c.sendTransform(7001, 0, 0, 0, pos, idRot);
        check(waitFor(g_zoneA.count, 21, 3000), "21 updates in the same chunk all arrive");
        check(g_zoneQueries.load() == queriesAfterFirst,
              "and NONE of them re-queries the head (the zone is cached per chunk)");

        // ── (4) Crossing a border re-queries — and the traffic MOVES ─────────────────────────
        // Counting queries is not enough: a client that asked and ignored the answer would pass. The
        // observable that matters is which socket the next datagram lands on.
        g_answerPort = kZoneBPort;
        c.sendTransform(7001, 5, 0, 0, pos, idRot);
        check(waitFor(g_zoneQueries, queriesAfterFirst + 1, 3000),
              "moving to another chunk DOES re-query the head");
        check(waitFor(g_zoneB.count, 1, 3000),
              "and the update goes to the NEW zone (the answer is used, not just requested)");
        check(g_zoneB.lastChunkX.load() == 5, "with the new chunk in it");

        const int aBefore = g_zoneA.count.load();
        c.sendTransform(7001, 5, 0, 0, pos, idRot);
        check(waitFor(g_zoneB.count, 2, 2000) && g_zoneA.count.load() == aBefore,
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
        c.sendTransform(7001, 5, 0, 0, pos, yaw90);
        check(waitFor(g_zoneB.count, b2 + 1, 2000), "a rotated update arrives");
        check(g_zoneB.lastAngle.load() != angleIdentity,
              "and a different yaw encodes to a DIFFERENT angle (it is not a constant)");

        // ── (6) Stats and inventory travel by the same route ─────────────────────────────────
        const int b3 = g_zoneB.count.load();
        DGS::Stats st{}; st.speed[0] = 12.5f; st.health = 80.0f;
        c.sendStats(7001, st);
        check(waitFor(g_zoneB.count, b3 + 1, 2000) &&
              g_zoneB.lastSpeed.load() > 12.0f && g_zoneB.lastSpeed.load() < 13.0f,
              "sendStats reaches the current zone with the stats intact");

        const int b4 = g_zoneB.count.load();
        const uint8_t inv[] = { 1, 2, 3, 4, 5, 6, 7 };
        c.sendInventory(7001, inv, sizeof(inv));
        check(waitFor(g_zoneB.count, b4 + 1, 2000) && g_zoneB.lastDataSize.load() == sizeof(inv),
              "sendInventory carries its opaque payload SIZE (dataSize, not the whole 4 KB)");

        // ── (7) Chat goes over TCP to the head, not UDP to the zone ──────────────────────────
        const int b5 = g_zoneB.count.load();
        c.sendChat(7001, "andoni", "hola");
        check(waitFor(g_chatsAtHead, 1, 2000), "chat is sent to the head over TCP");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        check(g_zoneB.count.load() == b5, "and NOT to the zone over UDP (they are different planes)");

        // ── (8) The receive loop sorts what arrives, and polling DRAINS ──────────────────────
        // If `poll*` did not drain, the engine would replay the same entities every frame forever.
        const int fd = g_headFd.load();
        if (fd >= 0) {
            DGS::EntityTransfer e{}; e.uuid = 4242; e.chunkX = 5;
            DGS::Packet pe; pe.pack(e);
            g_wire.send(fd, pe.getRawData(), pe.getSize());

            DGS::ChatMessage cm{}; cm.uuid = 9; std::snprintf(cm.text, sizeof(cm.text), "eco");
            DGS::Packet pc; pc.pack(cm);
            g_wire.send(fd, pc.getRawData(), pc.getSize());

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
                  "and a chat lands in pollChats() (they are sorted, not lumped together)");
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
    tApi.join(); tHead.join(); tZa.join(); tZb.join();

    std::printf("\n== client_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
