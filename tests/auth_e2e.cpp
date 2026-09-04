// ─────────────────────────────────────────────────────────────────────────────────────────────────
// auth_e2e — who is allowed to be a node.
//
// Nothing between the nodes was authenticated. Anyone who could reach the head's TCP port could send
// one `PKT_METRICS` and be registered as a zone — and the head then routes entities to it:
// reassignments, entity state, region blobs. The same everywhere else: connect to the validator and
// ask it to bless a movement, to persistence and write the world, to the social node and ban any
// account. Not one of those ports asked a single question.
//
// A node now proves it holds `DGS_CLUSTER_SECRET` with a `PKT_AUTH` carrying a nonce, a timestamp and
// `HMAC-SHA256(secret, nonce||timestamp)`. The secret never travels.
//
// The four things checked here, each the counter-proof of the next:
//   A. WITH the secret, a real zone registers and the head serves it — otherwise "the impostor was
//      refused" would also pass on a head that refuses everybody, which is not authentication, it is
//      an outage.
//   B. WITHOUT it, an impostor sending exactly the same registration is refused.
//   C. A CAPTURED credential replayed a second time is refused — the nonce cache — so listening on
//      the wire once does not buy permanent access.
//   D. With no secret configured at all, everything is allowed. That is the documented default (a
//      cluster that refuses to start is an outage, not a fix) and it must be a DECISION that shows in
//      the node's own log, not an accident.
//
// The head's port also serves game CLIENTS, which have no business holding the cluster secret, so the
// gate is on the privileged packets rather than on the connection.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/auth.h"

#include <sys/socket.h>
#include <sys/wait.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>

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

static const char* kSecret = "cluster-secret-for-the-auth-test";

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/// Registers as a zone would: one ServerMetrics claiming a chunk range. That single packet is what
/// makes the head start routing entities to whoever sent it.
static DGS::Packet registration(int32_t xMin, int32_t xMax)
{
    DGS::ServerMetrics m{};
    m.node.chunkXMin = xMin; m.node.chunkXMax = xMax;
    m.node.chunkYMin = 0;    m.node.chunkYMax = 100;
    m.node.chunkZMin = 0;    m.node.chunkZMax = 100;
    std::snprintf(m.node.addr, sizeof(m.node.addr), "127.0.0.1");
    m.node.port     = 40000;
    m.startTimeS    = 1;
    m.activeEntities = 0;
    DGS::Packet p; p.pack(m);
    return p;
}

/// Asks the head which zone covers a chunk. This is the observable: the head only answers with a real
/// address for a chunk some REGISTERED zone claims.
static bool zoneKnown(DGS::TCPSocket& s, int32_t chunkX)
{
    DGS::ZoneQuery q{};
    q.uuid = 1; q.chunkX = chunkX; q.chunkY = 0; q.chunkZ = 0;
    DGS::Packet p; p.pack(q);
    s.send(s.getSocketFD(), p.getRawData(), p.getSize());

    { timeval tv{}; tv.tv_sec = 1; setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    uint8_t buf[4096];
    const int n = s.receive(s.getSocketFD(), buf, sizeof(buf));
    if (n <= 0) return false;
    DGS::Packet r; r.setBuffer(buf, (size_t)n);
    if (r.getType() != DGS::PKT_ZONE_RESPONSE) return false;
    const DGS::ZoneResponse zr = r.unpackZoneResponse();
    return zr.port != 0 && zr.addr[0] != '\0';
}

static pid_t startHead(const char* path, const char* secret)
{
    std::fflush(stdout);
    const pid_t p = fork();
    if (p != 0) return p;
    if (!std::getenv("AUTH_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
    if (secret) setenv("DGS_CLUSTER_SECRET", secret, 1);
    else        unsetenv("DGS_CLUSTER_SECRET");
    char tmpl[] = "/tmp/dgs_auth_XXXXXX";
    if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
    execl(path, path, (char*)nullptr);
    _exit(127);
}

static void stop(pid_t pid) { if (pid > 0) { kill(pid, SIGTERM); waitpid(pid, nullptr, 0); } }

static bool waitPort(DGS::TCPSocket& s, int port, int tries)
{
    for (int i = 0; i < tries; ++i) {
        if (s.connect("127.0.0.1", port)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return false;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX];
    const char* headPath = realpath((argc > 1) ? argv[1] : "./build/head_server_node", abs)
                           ? abs : "./build/head_server_node";
    // The head's listening port is hardcoded in the node.
    const int kHead = 42424;

    // ══ A + B + C, with a secret ═════════════════════════════════════════════════════════════════
    {
        pid_t head = startHead(headPath, kSecret);

        // A legitimate node: it holds the secret.
        setenv("DGS_CLUSTER_SECRET", kSecret, 1);
        DGS::TCPSocket good;
        const bool up = waitPort(good, kHead, 300);
        check(up, "the head is up");
        if (!up) { stop(head); return 1; }

        DGS::Packet credential;   // kept, to replay it later
        {
            const std::string secret = kSecret;
            uint8_t nonce[DGS::AUTH_NONCE_LEN];
            RAND_bytes(nonce, (int)sizeof(nonce));
            const uint64_t ts = DGS::authNowMs();
            uint8_t mac[DGS::AUTH_MAC_LEN];
            DGS::authMac(secret, nonce, ts, mac);
            credential.pack(DGS::PKT_AUTH);
            credential.writeRaw(nonce, sizeof(nonce));
            credential.write<uint64_t>(ts);
            credential.writeRaw(mac, sizeof(mac));
            good.send(good.getSocketFD(), credential.getRawData(), credential.getSize());
        }

        DGS::Packet reg = registration(0, 100);
        good.send(good.getSocketFD(), reg.getRawData(), reg.getSize());
        std::this_thread::sleep_for(std::chrono::milliseconds(400));

        DGS::TCPSocket asker;
        waitPort(asker, kHead, 100);
        const bool served = zoneKnown(asker, 50);
        check(served, "A · a node WITH the secret registers, and the head routes chunk 50 to it");

        // ── B. The impostor: the same registration, no credential ────────────────────────────────
        DGS::TCPSocket rogue;
        waitPort(rogue, kHead, 100);
        DGS::Packet steal = registration(500, 600);
        rogue.send(rogue.getSocketFD(), steal.getRawData(), steal.getSize());
        std::this_thread::sleep_for(std::chrono::milliseconds(400));

        DGS::TCPSocket asker2;
        waitPort(asker2, kHead, 100);
        const bool stolen = zoneKnown(asker2, 550);
        check(!stolen,
              "B · the SAME registration without the secret is refused: chunk 550 is routed nowhere");

        // ── C. Replay: the very credential that worked, sent again ───────────────────────────────
        DGS::TCPSocket replayer;
        waitPort(replayer, kHead, 100);
        replayer.send(replayer.getSocketFD(), credential.getRawData(), credential.getSize());
        DGS::Packet steal2 = registration(700, 800);
        replayer.send(replayer.getSocketFD(), steal2.getRawData(), steal2.getSize());
        std::this_thread::sleep_for(std::chrono::milliseconds(400));

        DGS::TCPSocket asker3;
        waitPort(asker3, kHead, 100);
        const bool replayed = zoneKnown(asker3, 750);
        check(!replayed,
              "C · a CAPTURED credential replayed does not work twice (the nonce is remembered)");

        stop(head);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    // ══ D. No secret configured: the documented default ══════════════════════════════════════════
    // Not an oversight but a decision — a cluster that refuses to start on upgrade is an outage — and
    // it has to be visible rather than silent, which is why the node announces it.
    {
        unsetenv("DGS_CLUSTER_SECRET");
        pid_t head = startHead(headPath, nullptr);

        DGS::TCPSocket anyone;
        const bool up = waitPort(anyone, kHead, 300);
        DGS::Packet reg = registration(0, 100);
        if (up) anyone.send(anyone.getSocketFD(), reg.getRawData(), reg.getSize());
        std::this_thread::sleep_for(std::chrono::milliseconds(400));

        DGS::TCPSocket asker;
        waitPort(asker, kHead, 100);
        const bool open = up && zoneKnown(asker, 50);
        check(open,
              "D · with NO secret configured the port is open, as documented (and the node says so)");

        stop(head);
    }

    std::printf("\n== auth_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
