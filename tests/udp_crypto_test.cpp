// ─────────────────────────────────────────────────────────────────────────────────────────────────
// udp_crypto_test — the busiest traffic in the system used to travel in clear.
//
// TLS covers the TCP control plane. It left the UDP game plane untouched, and that is where the
// personal data is: **every player's position, twenty times a second, and the zone's broadcast of
// everyone else's**. Anyone on the path read where every player in a zone was — the same feed the
// observer token exists to protect — and, since nothing was authenticated either, could forge a
// position for somebody else's uuid.
//
// Each datagram is sealed with AES-256-GCM now: `nonce(12) || ciphertext || tag(16)`. One key for the
// game plane (`DGS_UDP_KEY`), so a zone still encrypts each broadcast frame ONCE and sends the same
// bytes to everybody — a per-peer DTLS session would have made the zone encrypt the same snapshot N
// times and thrown away the serialise-once-per-tick property the capacity numbers rest on.
//
// Five checks, and each is another's counter-proof:
//   A. with the key, a datagram round-trips intact — otherwise everything below would be measuring a
//      link that simply does not work.
//   B. a receiver WITHOUT the key sees bytes that do not contain the plaintext. Read off a plain
//      socket, so the claim is about what actually travels.
//   C. and with no key configured at all, that same plain socket DOES read the plaintext — which is
//      what makes (B) a measurement of the encryption rather than of a packet nobody sent.
//   D. a datagram with ONE BYTE CHANGED is refused. Encryption that hides the contents from a reader
//      while accepting anything from a writer is half a job; the GCM tag is the other half.
//   E. a receiver with the WRONG key is refused too, rather than handed rubbish.
//
// What this deliberately does NOT do is prevent replay: a captured datagram can be re-sent inside its
// lifetime. The layer above already deals with that for the traffic that matters — the validator's
// minimum-dt discard exists to reject duplicated and reordered samples, and S1 rejects the implausible.
// It is said out loud because a reader would otherwise assume GCM's nonce covers it, and it does not.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
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

// Below the ephemeral range on purpose — see the note in `validator_e2e.cpp`.
static const int kPort = 21721;
static const char* kMarker = "a-very-recognisable-player-name";

static void setKey(const char* k)
{
    if (k) setenv("DGS_UDP_KEY", k, 1);
    else   unsetenv("DGS_UDP_KEY");
}

/// A chat packet carrying a recognisable string: something a wire tap could spot in clear.
static DGS::Packet marked()
{
    DGS::ChatMessage m{};
    m.uuid = 7;
    std::snprintf(m.text, sizeof(m.text), "%s", kMarker);
    DGS::Packet p; p.pack(m);
    return p;
}

int main()
{
    signal(SIGPIPE, SIG_IGN);

    // ══ A. Round trip with the key ═══════════════════════════════════════════════════════════════
    {
        setKey("the-game-plane-key");
        DGS::UDPSocket rx, tx;
        rx.bind(kPort);
        { timeval tv{}; tv.tv_usec = 300000;
          setsockopt(rx.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
        tx.bind(0);

        DGS::Packet p = marked();
        tx.send("127.0.0.1", kPort, p.getRawData(), p.getSize());

        uint8_t buf[8192];
        std::string from; int port = 0;
        const int n = rx.receive(buf, sizeof(buf), from, port);
        bool same = n == (int)p.getSize() && std::memcmp(buf, p.getRawData(), (size_t)n) == 0;
        std::printf("    sealed round trip: %d bytes back, %zu sent\n", n, p.getSize());
        check(same, "A · with the key, the datagram comes back byte for byte");
    }

    // ══ B. What a listener without the key actually sees ═════════════════════════════════════════
    {
        setKey("the-game-plane-key");
        DGS::UDPSocket tx; tx.bind(0);

        // The tap: a plain socket with NO key, so `receive` hands back the raw wire bytes.
        setKey(nullptr);
        DGS::UDPSocket tap; tap.bind(kPort + 1);
        { timeval tv{}; tv.tv_usec = 300000;
          setsockopt(tap.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

        setKey("the-game-plane-key");
        DGS::Packet p = marked();
        tx.send("127.0.0.1", kPort + 1, p.getRawData(), p.getSize());

        setKey(nullptr);
        uint8_t buf[8192];
        std::string from; int port = 0;
        const int n = tap.receive(buf, sizeof(buf), from, port);
        const std::string wire((const char*)buf, n > 0 ? (size_t)n : 0);
        const bool inClear = wire.find(kMarker) != std::string::npos;
        std::printf("    a tap without the key captured %d bytes; the marker is %s\n",
                    n, inClear ? "PRESENT" : "absent");
        check(n > 0 && !inClear, "B · what travels does NOT contain the plaintext");
        check(n == (int)p.getSize() + 28,
              "B · and it is 28 bytes longer: nonce(12) + tag(16), the price of the seal");
    }

    // ══ C. The counter-proof for (B): no key at all ══════════════════════════════════════════════
    {
        setKey(nullptr);
        DGS::UDPSocket tx, tap;
        tx.bind(0);
        tap.bind(kPort + 2);
        { timeval tv{}; tv.tv_usec = 300000;
          setsockopt(tap.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

        DGS::Packet p = marked();
        tx.send("127.0.0.1", kPort + 2, p.getRawData(), p.getSize());

        uint8_t buf[8192];
        std::string from; int port = 0;
        const int n = tap.receive(buf, sizeof(buf), from, port);
        const std::string wire((const char*)buf, n > 0 ? (size_t)n : 0);
        std::printf("    with NO key the same tap captured %d bytes; the marker is %s\n",
                    n, wire.find(kMarker) != std::string::npos ? "PRESENT" : "absent");
        check(n > 0 && wire.find(kMarker) != std::string::npos,
              "C · without a key the marker IS on the wire (so (B) measured the encryption)");
    }

    // ══ D. One byte changed ══════════════════════════════════════════════════════════════════════
    // Confidentiality without integrity would be a cipher that hides the contents from a reader and
    // accepts anything from a writer.
    {
        setKey("the-game-plane-key");
        DGS::UDPSocket tx; tx.bind(0);
        setKey(nullptr);
        DGS::UDPSocket relay; relay.bind(kPort + 3);
        { timeval tv{}; tv.tv_usec = 300000;
          setsockopt(relay.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
        DGS::UDPSocket rx;
        rx.bind(kPort + 4);

        // Capture the sealed bytes, flip one, and deliver them to a receiver that HAS the key.
        setKey("the-game-plane-key");
        DGS::Packet p = marked();
        tx.send("127.0.0.1", kPort + 3, p.getRawData(), p.getSize());

        setKey(nullptr);
        uint8_t wire[8192];
        std::string from; int port = 0;
        const int n = relay.receive(wire, sizeof(wire), from, port);
        bool tamperedRefused = false, intactAccepted = false;
        if (n > 20)
        {
            DGS::UDPSocket raw; raw.bind(0);   // no key: sends the bytes exactly as given
            { timeval tv{}; tv.tv_usec = 300000;
              setsockopt(rx.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

            // The intact one first: without it, "the tampered one was refused" would also pass on a
            // receiver that refuses everything.
            raw.send("127.0.0.1", kPort + 4, wire, (size_t)n);
            setKey("the-game-plane-key");
            uint8_t buf[8192];
            intactAccepted = rx.receive(buf, sizeof(buf), from, port) == (int)p.getSize();

            setKey(nullptr);
            wire[n / 2] ^= 0x01;               // one bit, in the middle of the ciphertext
            raw.send("127.0.0.1", kPort + 4, wire, (size_t)n);
            setKey("the-game-plane-key");
            tamperedRefused = rx.receive(buf, sizeof(buf), from, port) <= 0;
        }
        check(intactAccepted, "D · the untouched sealed datagram is accepted");
        check(tamperedRefused, "D · and ONE BIT changed makes it refused (the tag is checked)");
    }

    // ══ E. The wrong key ═════════════════════════════════════════════════════════════════════════
    {
        setKey("the-game-plane-key");
        DGS::UDPSocket tx; tx.bind(0);
        DGS::UDPSocket rx; rx.bind(kPort + 5);
        { timeval tv{}; tv.tv_usec = 300000;
          setsockopt(rx.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

        DGS::Packet p = marked();
        tx.send("127.0.0.1", kPort + 5, p.getRawData(), p.getSize());

        setKey("a-completely-different-key");
        uint8_t buf[8192];
        std::string from; int port = 0;
        const int n = rx.receive(buf, sizeof(buf), from, port);
        std::printf("    receiving with the wrong key -> %d\n", n);
        check(n <= 0, "E · a receiver holding the WRONG key gets nothing, not rubbish");
    }

    setKey(nullptr);
    std::printf("\n== udp_crypto: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
