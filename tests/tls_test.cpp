// ─────────────────────────────────────────────────────────────────────────────────────────────────
// tls_test — what anyone on the path can read, and who is allowed to be on it.
//
// Node authentication decided WHO may connect. It did nothing about the fact that **every packet on
// every link travelled in clear**: player positions, verdicts, bans, the entity state one zone hands
// to its neighbour. Anyone with a tap on the wire read all of it, and anyone able to inject into the
// stream could change it.
//
// `TCPSocket` speaks TLS now when `DGS_TLS_CERT` / `DGS_TLS_KEY` / `DGS_TLS_CA` are set, and it is
// MUTUAL: the listener demands a client certificate signed by that CA and the connector verifies the
// server against the same one. Encryption without identity would only mean a private conversation
// with a stranger.
//
// Five checks, each the counter-proof of another:
//   A. with certificates on both ends, a packet still makes the round trip intact — otherwise
//      everything else here would be measuring a link that simply does not work.
//   B. the bytes on the wire are NOT the plaintext. Read by a third socket that man-in-the-middles
//      the connection, so the claim is about what actually travels rather than about a flag.
//   C. a client holding a certificate from a DIFFERENT CA is REFUSED — otherwise "TLS is on" would be
//      satisfied by a server that encrypts for anybody.
//   D. with the environment unset, the link is plain and still works, which is the documented default.
//   E. and with TLS OFF the same tap DOES read the plaintext — so (B) measured the encryption rather
//      than a packet that was never sent or a tap watching the wrong socket.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
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
static const int kPort  = 21711;
static const int kProxy = 21712;
static const char* kSecret = "a-very-recognisable-payload-string";

static std::string g_dir;

static void setCerts(const char* cert, const char* key, const char* ca)
{
    if (!cert) { unsetenv("DGS_TLS_CERT"); unsetenv("DGS_TLS_KEY"); unsetenv("DGS_TLS_CA"); return; }
    setenv("DGS_TLS_CERT", (g_dir + "/" + cert).c_str(), 1);
    setenv("DGS_TLS_KEY",  (g_dir + "/" + key).c_str(),  1);
    setenv("DGS_TLS_CA",   (g_dir + "/" + ca).c_str(),   1);
}

/// A chat packet whose text is `kSecret`: something a wire tap could recognise if it were in clear.
static DGS::Packet secretPacket()
{
    DGS::ChatMessage m{};
    m.uuid = 42;
    std::snprintf(m.text, sizeof(m.text), "%s", kSecret);
    DGS::Packet p; p.pack(m);
    return p;
}

/// Server thread: accepts one connection and echoes back whatever it decodes.
static void server(std::atomic<bool>& ready, std::atomic<bool>& accepted,
                   std::atomic<bool>& gotSecret, std::atomic<bool>& stop, int port)
{
    DGS::TCPSocket s;
    if (!s.listen(port)) { ready = true; return; }
    { timeval tv{}; tv.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    ready = true;
    while (!stop)
    {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        accepted = true;
        { timeval tv{}; tv.tv_sec = 2; setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
        uint8_t buf[8192];
        const int n = s.receive(fd, buf, sizeof(buf));
        if (n > 0)
        {
            DGS::Packet p; p.setBuffer(buf, (size_t)n);
            if (p.getType() == DGS::PKT_CHAT &&
                std::string(p.unpackChatMessage().text) == kSecret) gotSecret = true;
            s.send(fd, buf, (size_t)n);   // echo
        }
        s.closeClient(fd);
        break;
    }
}

/// A tap between client and server: forwards both ways and keeps everything it sees.
/// This is what an attacker on the path has, which is why the encryption claim is checked HERE and
/// not by asking the socket whether it thinks it is encrypted.
static void tap(std::atomic<bool>& ready, std::atomic<bool>& stop, std::string& seen, int listenPort,
                int forwardPort)
{
    const int srv = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(listenPort);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(srv, (sockaddr*)&a, sizeof(a)) < 0 || ::listen(srv, 4) < 0) { ready = true; close(srv); return; }
    { timeval tv{}; tv.tv_usec = 200000; setsockopt(srv, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    ready = true;

    int cli = -1;
    while (!stop && cli < 0) cli = ::accept(srv, nullptr, nullptr);
    if (cli < 0) { close(srv); return; }

    const int up = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in b{}; b.sin_family = AF_INET; b.sin_port = htons(forwardPort);
    b.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(up, (sockaddr*)&b, sizeof(b)) < 0) { close(cli); close(srv); return; }

    { timeval tv{}; tv.tv_usec = 200000;
      setsockopt(cli, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(up,  SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    char buf[8192];
    while (!stop && std::chrono::steady_clock::now() < until)
    {
        ssize_t n = ::recv(cli, buf, sizeof(buf), MSG_DONTWAIT);
        if (n > 0) { seen.append(buf, (size_t)n); ::send(up, buf, (size_t)n, 0); }
        n = ::recv(up, buf, sizeof(buf), MSG_DONTWAIT);
        if (n > 0) { seen.append(buf, (size_t)n); ::send(cli, buf, (size_t)n, 0); }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    close(up); close(cli); close(srv);
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);
    g_dir = (argc > 1) ? argv[1] : "/tmp/dgs-tls";

    // The certificates are made by `tools/tls/make_certs.sh`; without them there is nothing to test.
    {
        const std::string probe = g_dir + "/node.crt";
        if (FILE* f = std::fopen(probe.c_str(), "r")) std::fclose(f);
        else {
            std::printf("  ──────────────────────────────────────────────────────────────────────\n");
            std::printf("  SKIPPED: no certificates in %s.\n", g_dir.c_str());
            std::printf("           ./tools/tls/make_certs.sh %s\n", g_dir.c_str());
            std::printf("  ──────────────────────────────────────────────────────────────────────\n");
            std::printf("\n== tls_test: skipped ==\n");
            return 0;
        }
    }

    // ══ A + B. A round trip through a tap, with TLS on ═══════════════════════════════════════════
    {
        setCerts("node.crt", "node.key", "ca.crt");
        std::atomic<bool> sReady{false}, accepted{false}, gotSecret{false}, stop{false};
        std::atomic<bool> tReady{false};
        std::string onTheWire;

        std::thread srv(server, std::ref(sReady), std::ref(accepted), std::ref(gotSecret),
                        std::ref(stop), kPort);
        while (!sReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::thread mitm(tap, std::ref(tReady), std::ref(stop), std::ref(onTheWire), kProxy, kPort);
        while (!tReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));

        DGS::TCPSocket c;
        bool up = false;
        for (int i = 0; i < 100 && !up; ++i) {
            if (c.connect("127.0.0.1", kProxy, 2000)) up = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        check(up, "A · a TLS connection is established through the tap");

        bool echoed = false;
        if (up) {
            DGS::Packet p = secretPacket();
            c.send(c.getSocketFD(), p.getRawData(), p.getSize());
            { timeval tv{}; tv.tv_sec = 2;
              setsockopt(c.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
            uint8_t buf[8192];
            const int n = c.receive(c.getSocketFD(), buf, sizeof(buf));
            if (n > 0) {
                DGS::Packet r; r.setBuffer(buf, (size_t)n);
                echoed = r.getType() == DGS::PKT_CHAT &&
                         std::string(r.unpackChatMessage().text) == kSecret;
            }
        }
        check(echoed, "A · and the packet makes the round trip intact through it");
        check(gotSecret.load(), "A · the server decoded exactly what was sent");

        stop = true;
        mitm.join(); srv.join();

        const bool inClear = onTheWire.find(kSecret) != std::string::npos;
        std::printf("    the tap captured %zu bytes; the plaintext is %s in them\n",
                    onTheWire.size(), inClear ? "PRESENT" : "absent");
        check(!onTheWire.empty() && !inClear,
              "B · what actually travels does NOT contain the plaintext");
    }

    // ══ C. An impostor: a certificate from another CA ════════════════════════════════════════════
    {
        std::atomic<bool> sReady{false}, accepted{false}, gotSecret{false}, stop{false};
        setCerts("node.crt", "node.key", "ca.crt");
        std::thread srv(server, std::ref(sReady), std::ref(accepted), std::ref(gotSecret),
                        std::ref(stop), kPort + 2);
        while (!sReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));

        // The client presents a certificate signed by a CA the server does not trust.
        setCerts("rogue.crt", "rogue.key", "rogue-ca.crt");
        DGS::TCPSocket rogue;
        const bool up = rogue.connect("127.0.0.1", kPort + 2, 2000);
        std::printf("    a certificate from another CA -> connect %s\n", up ? "SUCCEEDED" : "refused");
        check(!up, "C · a peer whose certificate this CA did not sign is REFUSED");

        stop = true;
        // Unblock the accept loop so the thread can finish.
        setCerts("node.crt", "node.key", "ca.crt");
        srv.join();
    }

    // ══ D. No certificates configured: plain, and still working ══════════════════════════════════
    {
        setCerts(nullptr, nullptr, nullptr);
        std::atomic<bool> sReady{false}, accepted{false}, gotSecret{false}, stop{false};
        std::thread srv(server, std::ref(sReady), std::ref(accepted), std::ref(gotSecret),
                        std::ref(stop), kPort + 4);
        while (!sReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));

        DGS::TCPSocket c;
        bool up = false;
        for (int i = 0; i < 100 && !up; ++i) {
            if (c.connect("127.0.0.1", kPort + 4, 2000)) up = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        if (up) { DGS::Packet p = secretPacket(); c.send(c.getSocketFD(), p.getRawData(), p.getSize()); }
        for (int i = 0; i < 100 && !gotSecret; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        check(up && gotSecret.load(),
              "D · with no certificates configured the link is plain and still works (the default)");
        stop = true;
        srv.join();
    }

    // ══ E. The counter-proof for (B): the same tap, with TLS OFF ═════════════════════════════════
    // Without this, "the plaintext is not on the wire" would also pass if the packet had simply never
    // been sent, or if the tap were watching the wrong socket.
    {
        setCerts(nullptr, nullptr, nullptr);
        std::atomic<bool> sReady{false}, accepted{false}, gotSecret{false}, stop{false};
        std::atomic<bool> tReady{false};
        std::string onTheWire;

        std::thread srv(server, std::ref(sReady), std::ref(accepted), std::ref(gotSecret),
                        std::ref(stop), kPort + 6);
        while (!sReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::thread mitm(tap, std::ref(tReady), std::ref(stop), std::ref(onTheWire),
                         kProxy + 6, kPort + 6);
        while (!tReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));

        DGS::TCPSocket c;
        bool up = false;
        for (int i = 0; i < 100 && !up; ++i) {
            if (c.connect("127.0.0.1", kProxy + 6, 2000)) up = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        if (up) { DGS::Packet p = secretPacket(); c.send(c.getSocketFD(), p.getRawData(), p.getSize()); }
        for (int i = 0; i < 100 && !gotSecret; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        stop = true;
        mitm.join(); srv.join();

        const bool inClear = onTheWire.find(kSecret) != std::string::npos;
        std::printf("    with TLS OFF the same tap captured %zu bytes; the plaintext is %s\n",
                    onTheWire.size(), inClear ? "PRESENT" : "absent");
        check(inClear,
              "E · without TLS the tap DOES read the plaintext (so (B) measured the encryption)");
    }

    std::printf("\n== tls_test: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
