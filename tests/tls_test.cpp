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
// Six groups of checks, each the counter-proof of another:
//   A. with certificates on both ends, a packet still makes the round trip intact — otherwise
//      everything else here would be measuring a link that simply does not work.
//   B. the bytes on the wire are NOT the plaintext. Read by a third socket that man-in-the-middles
//      the connection, so the claim is about what actually travels rather than about a flag.
//   C. a client holding a certificate from a DIFFERENT CA is REFUSED — otherwise "TLS is on" would be
//      satisfied by a server that encrypts for anybody.
//   D. with the environment unset, the link is plain and still works, which is the documented default.
//   E. and with TLS OFF the same tap DOES read the plaintext — so (B) measured the encryption rather
//      than a packet that was never sent or a tap watching the wrong socket.
//   F. ONE CONNECTION, TWO THREADS: a sender and a receiver on the same descriptor at the same time,
//      which is exactly what `Client` does and what TLS does not tolerate by itself.
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

// ── F. One connection, two threads ───────────────────────────────────────────────────────────────
// The frames are self-describing so that a SPLICED stream is detectable and not merely suspected:
// bytes 0..3 are the sequence number, the length is a function of it, and every remaining byte is
// `seq & 0xff`. A frame assembled out of two writers' bytes fails at least one of the three.
static const int    kFrames   = 200;
static size_t frameLen(int seq) { return 64 + (size_t)(seq % 40) * 100; }   // 64 B … 3.9 kB

static void fillFrame(int seq, std::vector<uint8_t>& out)
{
    out.assign(frameLen(seq), (uint8_t)(seq & 0xff));
    const uint32_t s = (uint32_t)seq;
    std::memcpy(out.data(), &s, sizeof(s));
}

/// Reads whole frames and writes each one straight back, on one thread. Its job is only to give the
/// client something to talk to; the interesting concurrency is on the client side.
static void echoServer(std::atomic<bool>& ready, std::atomic<bool>& stop, int port)
{
    DGS::TCPSocket s;
    if (!s.listen(port)) { ready = true; return; }
    { timeval tv{}; tv.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    ready = true;

    int fd = -1;
    while (!stop && fd < 0)
    {
        fd = s.accept();
        if (fd < 0) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (fd < 0) return;
    { timeval tv{}; tv.tv_usec = 200000; setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

    std::vector<uint8_t> buf(8192);
    int echoed = 0;
    while (!stop && echoed < kFrames)
    {
        const int n = s.receive(fd, buf.data(), buf.size());
        if (n == 0) break;          // orderly close
        if (n < 0) continue;        // timeout: go round and re-check `stop`
        if (!s.send(fd, buf.data(), (size_t)n)) break;
        ++echoed;
    }
    s.closeClient(fd);
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

    // ══ F. One TLS connection used from two threads at once ══════════════════════════════════════
    // ⚠️ THIS WAS A DOCUMENTED HOLE BEFORE IT WAS A TEST. `Client` sends from the caller's thread and
    // receives on `recvLoop`, over one `TCPSocket`. Without TLS the kernel serialises that; with TLS
    // both threads drive the SAME `SSL`, which has one record layer and one error state, and the
    // result is undefined behaviour rather than a lost message.
    //
    // What is checked is a PROPERTY OF THE STREAM, not an absence of crashes: every frame is echoed
    // back whole and in one piece. A missing lock does not have to crash to be caught — it produces a
    // frame whose length, sequence number and filler no longer agree.
    //
    // MEASURED, with the two locks in `network.cpp` commented out and nothing else changed:
    //   6 runs out of 6 detected it — 3 exited with SIGSEGV before finishing the exchange, and the
    //   other 3 came back red having received 37, 39 and 41 of the 200 frames.
    // With the locks in place: 200 sent, 200 received, 0 damaged, over 20 consecutive runs. So the
    // hole this closes was undefined behaviour in the literal sense, not a theoretical one.
    {
        setCerts("node.crt", "node.key", "ca.crt");
        std::atomic<bool> sReady{false}, stop{false};
        std::thread srv(echoServer, std::ref(sReady), std::ref(stop), kPort + 8);
        while (!sReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));

        DGS::TCPSocket c;
        bool up = false;
        for (int i = 0; i < 100 && !up; ++i) {
            if (c.connect("127.0.0.1", kPort + 8, 2000)) up = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        { timeval tv{}; tv.tv_sec = 2;
          setsockopt(c.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
          setsockopt(c.getSocketFD(), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)); }
        check(up, "F · a TLS connection for the two-thread exchange");

        std::atomic<int> sent{0}, got{0}, damaged{0};
        std::vector<int> seen(kFrames, 0);
        const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(20);

        if (up)
        {
            const int fd = c.getSocketFD();

            std::thread writer([&]() {
                std::vector<uint8_t> frame;
                for (int i = 0; i < kFrames && std::chrono::steady_clock::now() < until; ++i)
                {
                    fillFrame(i, frame);
                    if (!c.send(fd, frame.data(), frame.size())) break;
                    ++sent;
                }
            });

            std::thread reader([&]() {
                std::vector<uint8_t> buf(8192);
                while (got < kFrames && std::chrono::steady_clock::now() < until)
                {
                    const int n = c.receive(fd, buf.data(), buf.size());
                    if (n == 0) break;
                    if (n < 0) continue;   // SO_RCVTIMEO expired: re-check the deadline
                    ++got;

                    uint32_t seq = 0;
                    std::memcpy(&seq, buf.data(), sizeof(seq));
                    bool ok = seq < (uint32_t)kFrames && (size_t)n == frameLen((int)seq);
                    for (int b = 4; ok && b < n; ++b)
                        if (buf[b] != (uint8_t)(seq & 0xff)) ok = false;
                    if (!ok) ++damaged;
                    else     ++seen[seq];
                }
            });

            writer.join();
            reader.join();
        }

        stop = true;
        srv.join();

        int missing = 0, duplicated = 0;
        for (int i = 0; i < kFrames; ++i) { if (seen[i] == 0) ++missing; else if (seen[i] > 1) ++duplicated; }
        std::printf("    two threads on one TLS link: sent %d, received %d, damaged %d, "
                    "missing %d, duplicated %d\n",
                    sent.load(), got.load(), damaged.load(), missing, duplicated);

        check(sent.load() == kFrames && got.load() == kFrames,
              "F · every frame written while another thread was reading came back");
        check(damaged.load() == 0 && missing == 0 && duplicated == 0,
              "F · and each one arrived WHOLE (no frame spliced out of two writers' bytes)");

        // Counter-proof for the line above: the verifier must reject a damaged frame, or "0 damaged"
        // would be satisfied by a check that accepts anything.
        {
            std::vector<uint8_t> a, b;
            fillFrame(3, a);
            fillFrame(7, b);
            a.resize(frameLen(3) / 2);
            a.insert(a.end(), b.begin(), b.begin() + (long)(frameLen(3) - a.size()));  // a splice
            uint32_t seq = 0; std::memcpy(&seq, a.data(), sizeof(seq));
            bool ok = seq < (uint32_t)kFrames && a.size() == frameLen((int)seq);
            for (size_t i = 4; ok && i < a.size(); ++i)
                if (a[i] != (uint8_t)(seq & 0xff)) ok = false;
            check(!ok, "F · the intactness check DOES reject a spliced frame (it is not a rubber stamp)");
        }
    }

    // ══ G. A peer that connects and says nothing ═════════════════════════════════════════════════
    // ⚠️ ONE SOCKET, NO CREDENTIALS, NO TRAFFIC, AND THE NODE STOPS ACCEPTING. `accept()` ran
    // `SSL_accept` on the caller's own loop with a blocking descriptor, so anybody who could reach the
    // port could open a connection, send nothing, and park that node's accept loop for ever. Node
    // authentication does not help: the handshake happens BEFORE anyone gets to prove who they are.
    //
    // The check is a fact about the LISTENER, not about the silent peer: after the silent connection,
    // a legitimate client must still get through. `DGS_TLS_HANDSHAKE_MS` is turned down so the test
    // costs its own timeout rather than the 5 s default.
    {
        setenv("DGS_TLS_HANDSHAKE_MS", "600", 1);
        setCerts("node.crt", "node.key", "ca.crt");
        std::atomic<bool> sReady{false}, stop{false};
        std::thread srv(echoServer, std::ref(sReady), std::ref(stop), kPort + 10);
        while (!sReady) std::this_thread::sleep_for(std::chrono::milliseconds(10));

        // A raw socket: it completes the TCP handshake and then never speaks TLS.
        const int mute = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(kPort + 10);
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        const bool connected = ::connect(mute, (sockaddr*)&a, sizeof(a)) == 0;
        check(connected, "G · a silent peer opens a TCP connection to the node");

        const auto t0 = std::chrono::steady_clock::now();
        DGS::TCPSocket c;
        bool up = false;
        for (int i = 0; i < 60 && !up; ++i) {
            if (c.connect("127.0.0.1", kPort + 10, 2000)) up = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        bool echoed = false;
        if (up) {
            { timeval tv{}; tv.tv_sec = 2;
              setsockopt(c.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
            std::vector<uint8_t> frame; fillFrame(11, frame);
            std::vector<uint8_t> buf(8192);
            if (c.send(c.getSocketFD(), frame.data(), frame.size())) {
                const int n = c.receive(c.getSocketFD(), buf.data(), buf.size());
                echoed = n == (int)frame.size() && std::memcmp(buf.data(), frame.data(), (size_t)n) == 0;
            }
        }
        const long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0).count();
        std::printf("    a real client got served %ld ms after a silent peer took the accept slot\n", ms);
        check(echoed, "G · the listener SURVIVES it and still serves a legitimate client");

        ::close(mute);
        stop = true;
        srv.join();

        // Counter-proof: the deadline is what does the work, so raising it must bring the freeze back.
        // With 2.5 s of patience on the listener, a client that only waits 800 ms is NOT served — which
        // is what the old code did with no upper bound at all, for ever.
        setenv("DGS_TLS_HANDSHAKE_MS", "2500", 1);
        std::atomic<bool> sReady2{false}, stop2{false};
        std::thread srv2(echoServer, std::ref(sReady2), std::ref(stop2), kPort + 12);
        while (!sReady2) std::this_thread::sleep_for(std::chrono::milliseconds(10));

        const int mute2 = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in a2{}; a2.sin_family = AF_INET; a2.sin_port = htons(kPort + 12);
        a2.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ::connect(mute2, (sockaddr*)&a2, sizeof(a2));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));   // let the listener take it

        DGS::TCPSocket blocked;
        const bool served = blocked.connect("127.0.0.1", kPort + 12, 800);
        std::printf("    with the listener's patience raised to 2500 ms, a 800 ms client was %s\n",
                    served ? "STILL SERVED" : "not served");
        check(!served, "G · and the deadline is what saves it (raise it and the freeze comes back)");

        ::close(mute2);
        stop2 = true;
        srv2.join();
        unsetenv("DGS_TLS_HANDSHAKE_MS");
    }

    std::printf("\n== tls_test: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
