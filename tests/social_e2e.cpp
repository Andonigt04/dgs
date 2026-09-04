// ─────────────────────────────────────────────────────────────────────────────────────────────────
// `social_node` — 221 lines without a test. It is the social plane: chat, guilds, accounts and bans.
//
// It is not a plain repeater, which is why it has to be tested with PAIRS that refute each other. Four
// of its rules, each with its opposite case:
//
//   1. it fans out to subscribers, BUT NOT to the sender  (otherwise everyone would hear themselves)
//   2. it limits to 2 messages/s PER UUID                 (counterpart: another uuid in the same window
//                                                          DOES pass, or it would be a global drop, not
//                                                          an anti-spam)
//   3. it does not route the LOCAL channel, the zone does (counterpart: the guild channel DOES go out)
//   4. it persists social deltas, BUT NOT chat            (counterpart: chat must not appear in persistence)
//
// Each rule alone is indistinguishable from a malfunction: "nothing arrives" passes (1), (3) and (4) at
// once. The counterparts are what separate "it works" from "it is dead".
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
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
#include <vector>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);
}

static const int kSocialPort = 21491;
static const int kPersPort   = 21492;

static std::atomic<bool> g_done{false};
static std::atomic<int>  g_persChats{0};    // chats that reached persistence (must be 0)
static std::atomic<int>  g_persDeltas{0};   // social deltas persisted

static void fakePersistence(std::atomic<bool>& ready)
{
    DGS::TCPSocket s;
    if (!s.listen(kPersPort)) { ready = true; return; }
    { timeval ta{}; ta.tv_usec = 200000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &ta, sizeof(ta)); }
    ready = true;
    while (!g_done) {
        const int fd = s.accept();
        if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
        uint8_t buf[8192];
        timeval tv{}; tv.tv_usec = 200000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (!g_done) {
            const int n = s.receive(fd, buf, sizeof(buf));
            if (n <= 0) continue;
            DGS::Packet r; r.setBuffer(buf, n);
            if (r.getType() == DGS::PKT_CHAT)              ++g_persChats;
            else if (r.getType() == DGS::PKT_SOCIAL_DELTA) ++g_persDeltas;
            else if (r.getType() == DGS::PKT_SOCIAL_QUERY) {
                // ⚠️ THE FAKE HAS TO ANSWER. The social node asks for its stored state on start-up and
                // does not accept subscribers until it has it — serving during that window would mean
                // telling a zone "nobody is banned". A stub that stays silent is a database that never
                // answers, so the node quite correctly waited out its deadline and this test's 1.5 s
                // expectations expired first. An empty answer is still an answer: a bare PKT_NONE.
                DGS::Packet end; end.pack(DGS::PKT_NONE);
                s.send(fd, end.getRawData(), end.getSize());
            }
        }
        s.closeClient(fd);
    }
}

static void sendChat(DGS::TCPSocket& s, uint32_t uuid, uint8_t channel, const char* text)
{
    DGS::ChatMessage c{};
    c.uuid = uuid;
    c.channel = channel;
    std::snprintf(c.username, sizeof(c.username), "u%u", uuid);
    std::snprintf(c.text, sizeof(c.text), "%s", text);
    DGS::Packet p; p.pack(c);
    s.send(s.getSocketFD(), p.getRawData(), p.getSize());
}

/// Reads ONE packet with a deadline. @return the type, or -1 if nothing arrives.
static int receiveType(DGS::TCPSocket& s, int msLimit, DGS::Packet& out)
{
    timeval tv{}; tv.tv_sec = msLimit / 1000; tv.tv_usec = (msLimit % 1000) * 1000;
    setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    uint8_t buf[8192];
    const int n = s.receive(s.getSocketFD(), buf, sizeof(buf));
    if (n <= 0) return -1;
    out.setBuffer(buf, n);
    return (int)out.getType();
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char abs[PATH_MAX];
    const char* argPath  = (argc > 1) ? argv[1] : "./build/social_node";
    const char* nodePath = realpath(argPath, abs) ? abs : argPath;

    std::atomic<bool> pl{false};
    std::thread tp(fakePersistence, std::ref(pl));
    while (!pl) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const pid_t pid = fork();
    if (pid < 0) { std::printf("[FAIL] fork\n"); g_done = true; tp.join(); return 1; }
    if (pid == 0) {
        if (!std::getenv("SOCIAL_E2E_VERBOSE")) std::freopen("/dev/null", "w", stdout);
        setenv("SOCIAL_TCP_PORT",  std::to_string(kSocialPort).c_str(), 1);
        setenv("PERSISTENCE_HOST", "127.0.0.1", 1);
        setenv("PERSISTENCE_PORT", std::to_string(kPersPort).c_str(), 1);
        char tmpl[] = "/tmp/dgs_social_XXXXXX";
        if (const char* d = mkdtemp(tmpl)) { if (chdir(d) != 0) {} }
        execl(nodePath, nodePath, (char*)nullptr);
        _exit(127);
    }

    DGS::TCPSocket A, B;
    bool up = false;
    for (int i = 0; i < 200 && !up; ++i) {
        if (A.connect("127.0.0.1", kSocialPort) && B.connect("127.0.0.1", kSocialPort)) up = true;
        else std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    check(up, "the social node accepts two subscribers");

    if (up) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        DGS::Packet rec;

        // (1) A speaks on the guild channel -> B receives it.
        sendChat(A, 1, DGS::CHAT_GUILD, "hello");
        const bool gotFirst = receiveType(B, 1500, rec) == DGS::PKT_CHAT;
        check(gotFirst, "a chat from A reaches B");
        // ⚠️ ONLY UNPACK WHAT ARRIVED. This used to unpack unconditionally, so the FIRST failure threw
        // `Packet read overflow` out of `main` and the process ABORTED — every later check silently
        // never ran, and the output ended in a core dump instead of a verdict.
        const uint64_t seq1 = gotFirst ? rec.unpackChatMessage().seq : 0;

        // (1b) ...and it does NOT come back to A.
        // ⚠️ A SHORT DEADLINE, ON PURPOSE. Waiting here burns clock, and case (2) measures a 500 ms
        // window from the previous message: with a 600 ms wait the window had already elapsed and the
        // "anti-spam does not work" I was seeing was MINE, not the node's.
        check(receiveType(A, 250, rec) == -1, "the chat does NOT come back to the sender");

        // (2) A second message from the SAME uuid INSIDE the window -> dropped by the rate limit.
        sendChat(A, 1, DGS::CHAT_GUILD, "spam");
        check(receiveType(B, 400, rec) == -1,
              "a second chat from the SAME uuid within 500 ms is dropped (anti-spam)");

        // (2b) COUNTERPART: another uuid in the same window DOES pass. Without this, the previous case
        //      could simply mean "the node stopped fanning out".
        sendChat(A, 2, DGS::CHAT_GUILD, "another");
        const int t2 = receiveType(B, 1500, rec);
        check(t2 == DGS::PKT_CHAT, "another uuid in the SAME window does pass (the limit is per uuid)");
        if (t2 == DGS::PKT_CHAT) {
            const uint64_t seq2 = rec.unpackChatMessage().seq;
            std::printf("    chat channel seq: %llu -> %llu\n",
                        (unsigned long long)seq1, (unsigned long long)seq2);
            check(seq2 == seq1 + 1, "the channel's sequence number ADVANCES one at a time (anti-replay)");
        }

        // (3) LOCAL channel: routed by the zone through spatial interest, it does not go out here.
        sendChat(A, 3, DGS::CHAT_LOCAL, "local");
        check(receiveType(B, 600, rec) == -1,
              "the social node does not fan out the LOCAL channel (the owning zone does)");

        // (4) A social delta IS persisted... and reaches B.
        DGS::SocialDelta d{};
        d.targetUuid = 7; d.scopeUuid = 99; d.kind = 0; d.rank = 1;
        DGS::Packet pd; pd.pack(d);
        A.send(A.getSocketFD(), pd.getRawData(), pd.getSize());
        check(receiveType(B, 1500, rec) == DGS::PKT_SOCIAL_DELTA, "a social delta reaches B");

        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        check(g_persDeltas.load() >= 1, "the social delta DOES reach persistence");
        // (4b) ...and chat does NOT. This is the pair that says persistence is selective, not a mirror.
        check(g_persChats.load() == 0, "chats are NOT persisted (selective persistence)");
        std::printf("    persistence  ·  deltas %d  ·  chats %d\n",
                    g_persDeltas.load(), g_persChats.load());
    }

    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    g_done = true;
    tp.join();

    std::printf("\n== social_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
