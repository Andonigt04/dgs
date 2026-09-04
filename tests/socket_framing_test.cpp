// ─────────────────────────────────────────────────────────────────────────────────────────────────
// socket_framing_test — the transport's own contract: ONE `send` is ONE `receive`.
//
// Every link in this system relies on that. `TCPSocket::send` writes a 4-byte `htonl` length prefix
// and `TCPSocket::receive` reads exactly one message with `recvAll`, which is why nodes can decode a
// packet straight out of a read without accumulating a stream. Nothing tested it, and the one place
// where the contract broke took everything after it down with it.
//
// ⚠️ A MESSAGE TOO BIG FOR THE READER'S BUFFER USED TO DESYNCHRONISE THE CONNECTION FOR EVER.
// `receive` returned -1 WITHOUT consuming the payload, so the next call read four bytes from the
// middle of that payload and took them for a length. Measured with a 277-byte message sent into a
// 256-byte reader followed by three ordinary ones:
//
//     read -1   (the oversized one, correctly refused — but its bytes are still in the stream)
//     read -1   (four bytes of payload read as a length)
//     read 255  <- 255 BYTES OF GARBAGE HANDED BACK AS A VALID PACKET
//     read -1
//
// So the three innocent messages behind it were lost AND a node would have decoded arbitrary bytes as
// a typed packet. It is now consumed and discarded: one message is lost, the stream stays in step.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <signal.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
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
static const int kPort = 21671;

static DGS::Packet chat(uint32_t uuid, const char* text)
{
    DGS::ChatMessage m{};
    m.uuid = uuid;
    std::snprintf(m.text, sizeof(m.text), "%s", text);
    DGS::Packet p; p.pack(m);
    return p;
}

int main()
{
    signal(SIGPIPE, SIG_IGN);

    std::atomic<bool> ready{false};
    std::atomic<bool> serverDone{false};
    std::vector<int>  reads;        // what each receive returned
    std::vector<int>  types;        // and the type it decoded (-1 when there was nothing)
    std::vector<uint32_t> uuids;

    std::thread rx([&]{
        DGS::TCPSocket s;
        if (!s.listen(kPort)) { ready = true; serverDone = true; return; }
        ready = true;
        int fd = -1;
        for (int i = 0; i < 200 && fd < 0; ++i) {
            fd = s.accept();
            if (fd < 0) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (fd < 0) { serverDone = true; return; }
        { timeval tv{}; tv.tv_sec = 2; setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

        // DELIBERATELY SMALL: the oversized message below does not fit in it. That is the subject.
        uint8_t small[256];
        for (int i = 0; i < 5; ++i)
        {
            const int n = s.receive(fd, small, sizeof(small));
            reads.push_back(n);
            if (n > 0) {
                DGS::Packet p; p.setBuffer(small, (size_t)n);
                types.push_back((int)p.getType());
                uuids.push_back(p.getType() == DGS::PKT_CHAT ? p.unpackChatMessage().uuid : 0u);
            } else { types.push_back(-1); uuids.push_back(0u); }
        }
        s.closeClient(fd);
        serverDone = true;
    });

    while (!ready) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    DGS::TCPSocket c;
    bool up = false;
    for (int i = 0; i < 200 && !up; ++i) {
        if (c.connect("127.0.0.1", kPort)) up = true;
        else std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    check(up, "the two ends connect");
    if (!up) { serverDone = true; rx.join(); std::printf("\n== socket_framing: %d OK · %d FAILED ==\n", g_pass, g_fail); return 1; }

    // ── One message that does NOT fit the reader's 256-byte buffer ───────────────────────────────
    DGS::ChatMessage big{};
    big.uuid = 1;
    for (size_t i = 0; i < sizeof(big.text) - 1; ++i) big.text[i] = 'x';
    DGS::Packet pb; pb.pack(big);
    std::printf("    oversized message: %zu B into a 256 B reader\n", pb.getSize());
    c.send(c.getSocketFD(), pb.getRawData(), pb.getSize());

    // ── ...followed by three perfectly ordinary ones ─────────────────────────────────────────────
    for (uint32_t i = 0; i < 3; ++i) {
        DGS::Packet p = chat(100 + i, "hi");
        c.send(c.getSocketFD(), p.getRawData(), p.getSize());
    }

    for (int i = 0; i < 300 && !serverDone; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    rx.join();

    std::printf("    reads: ");
    for (size_t i = 0; i < reads.size(); ++i) std::printf("%d ", reads[i]);
    std::printf("\n");

    check(reads.size() == 5 && reads[0] == -1,
          "the oversized message is REFUSED (it does not fit, and is not truncated into the buffer)");

    // THE POINT. Before the fix these three were lost and one read came back as garbage.
    bool survived = reads.size() == 5;
    for (int i = 1; i <= 3 && survived; ++i)
        survived = reads[i] > 0 && types[i] == DGS::PKT_CHAT && uuids[i] == (uint32_t)(99 + i);
    check(survived,
          "and the three ordinary messages BEHIND it arrive intact, in order (the stream stays in sync)");

    bool noGarbage = true;
    for (size_t i = 0; i < reads.size(); ++i)
        if (reads[i] > 0 && types[i] != DGS::PKT_CHAT) noGarbage = false;
    check(noGarbage, "and nothing that is not a real packet is ever handed back as one");

    // ── A length that cannot be trusted at all ───────────────────────────────────────────────────
    // Skipping this many bytes is not possible (they may not exist, and the peer may be hostile), so
    // the connection is reported as finished and the caller's existing teardown path runs. The
    // alternative is decoding whatever arrives next as a packet, which is how this class of bug hurts.
    {
        std::atomic<bool> r2{false};
        int  result = 12345;
        std::thread rx2([&]{
            DGS::TCPSocket s;
            if (!s.listen(kPort + 1)) { r2 = true; return; }
            r2 = true;
            int fd = -1;
            for (int i = 0; i < 200 && fd < 0; ++i) {
                fd = s.accept();
                if (fd < 0) std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            if (fd < 0) return;
            { timeval tv{}; tv.tv_sec = 2; setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
            uint8_t buf[4096];
            result = s.receive(fd, buf, sizeof(buf));
            s.closeClient(fd);
        });
        while (!r2) std::this_thread::sleep_for(std::chrono::milliseconds(10));

        DGS::TCPSocket c2;
        bool up2 = false;
        for (int i = 0; i < 200 && !up2; ++i) {
            if (c2.connect("127.0.0.1", kPort + 1)) up2 = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (up2) {
            const uint32_t lie = htonl(900000000u);   // far beyond MAX_PACKET_SIZE
            ::send(c2.getSocketFD(), &lie, 4, 0);
            const uint8_t junk[8] = {1,2,3,4,5,6,7,8};
            ::send(c2.getSocketFD(), junk, sizeof(junk), 0);
        }
        rx2.join();
        std::printf("    corrupt length 900000000 -> receive returned %d\n", result);
        check(up2 && result == 0,
              "a length beyond MAX_PACKET_SIZE is reported as a dead connection, not decoded");
    }

    std::printf("\n== socket_framing: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
