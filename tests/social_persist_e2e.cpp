// ─────────────────────────────────────────────────────────────────────────────────────────────────
// social_persist_e2e — a ban has to survive a restart.
//
// `SocialState` was a plain local variable in `social_node`: every guild, ban, friendship and
// permission lived only in that process. It did send its deltas to the persistence node — its own
// comment calls that node "source of truth for bans/guilds" — but that node understood nothing except
// entities and dropped them without a word. So the situation was concrete and bad: **restarting the
// social node unbanned every account.** A moderator's decision, undone by an operator restarting a
// service.
//
// Two halves had to exist for either to be worth anything, and a third that only showed up once the
// first two were in place:
//
//   1. persistence has to STORE the social plane (guild membership with rank, friendships, account
//      state) — and deliberately NOT parties, which are session-scoped;
//   2. the social node has to ASK for it back on start-up;
//   3. and it has to REPLAY state to a subscriber that connects. It never did: bans were broadcast at
//      the instant they happened and never again, so a zone that started later served a banned account
//      happily. Without this, restoring state into memory changes nothing in the world.
//
// The phases below separate those, so a pass says which one works:
//   A. control — a subscriber connecting to the SAME live node hears about a ban issued before it
//      arrived. That is (3) alone, with no database involved.
//   B. the subject — the node is killed and a NEW process started. A subscriber connecting to it must
//      still hear the ban. That is (1) + (2), and it is the check that used to be impossible.
//   C. counter-proof — an account that was never banned is not announced to anybody, so "it heard a
//      ban" cannot be satisfied by a node that simply announces everything.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
#include <sys/wait.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
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
static const int kSocialPort = 21681;
static const int kPersPort   = 21682;

static const char*    kTestDb    = "dgs_social_test";
static const uint32_t kBanned    = 7771;
static const uint32_t kInnocent  = 7772;
static const uint32_t kGuild     = 500;

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static pid_t spawn(const char* path, void (*envFn)(), const char* tag)
{
    std::fflush(stdout);   // fork duplicates an unflushed buffer; see persistence_e2e
    const pid_t p = fork();
    if (p != 0) return p;
    if (!std::getenv("SOCIAL_PERSIST_VERBOSE")) std::freopen("/dev/null", "w", stdout);
    envFn();
    std::string tmpl = std::string("/tmp/dgs_") + tag + "_XXXXXX";
    std::vector<char> t(tmpl.begin(), tmpl.end()); t.push_back('\0');
    if (const char* d = mkdtemp(t.data())) { if (chdir(d) != 0) {} }
    execl(path, path, (char*)nullptr);
    _exit(127);
}

static std::string g_uri;
static void persEnv()
{
    setenv("PERSISTENCE_PORT", std::to_string(kPersPort).c_str(), 1);
    setenv("MONGO_URI", g_uri.c_str(), 1);
    setenv("MONGO_DB",  kTestDb, 1);
}
static void socialEnv()
{
    setenv("SOCIAL_TCP_PORT",  std::to_string(kSocialPort).c_str(), 1);
    setenv("PERSISTENCE_HOST", "127.0.0.1", 1);
    setenv("PERSISTENCE_PORT", std::to_string(kPersPort).c_str(), 1);
}

static void stop(pid_t pid)
{
    if (pid <= 0) return;
    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
}

static bool waitPort(DGS::TCPSocket& s, int port, int tries)
{
    for (int i = 0; i < tries; ++i) {
        if (s.connect("127.0.0.1", port)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return false;
}

/// What a subscriber hears in `ms`: banned uuids, and guild ranks by member.
struct Heard
{
    std::map<uint32_t, uint32_t> bans;        // uuid → durationS (0 = permanent)
    std::map<uint32_t, int>      guildRank;   // uuid → rank
    int packets = 0;
};

static Heard listen(DGS::TCPSocket& s, int ms)
{
    Heard h;
    { timeval tv{}; tv.tv_usec = 100000;
      setsockopt(s.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
    uint8_t buf[8192];
    const uint64_t until = nowMs() + (uint64_t)ms;
    while (nowMs() < until)
    {
        const int n = s.receive(s.getSocketFD(), buf, sizeof(buf));
        if (n <= 0) continue;
        ++h.packets;
        DGS::Packet p; p.setBuffer(buf, (size_t)n);
        try {
            if (p.getType() == DGS::PKT_ACCOUNT) {
                const DGS::AccountAction a = p.unpackAccountAction();
                if (a.action == DGS::ACC_BAN) h.bans[a.targetUuid] = a.durationS;
            } else if (p.getType() == DGS::PKT_SOCIAL_DELTA) {
                const DGS::SocialDelta d = p.unpackSocialDelta();
                if (d.kind == DGS::SOCIAL_GUILD_RANK) h.guildRank[d.targetUuid] = (int)d.rank;
            }
        } catch (const std::exception&) { /* malformed: ignore */ }
    }
    return h;
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);

    char absSocial[PATH_MAX], absPers[PATH_MAX];
    const char* socialPath = realpath((argc > 1) ? argv[1] : "./build/social_node", absSocial)
                             ? absSocial : "./build/social_node";
    const char* persPath   = realpath((argc > 2) ? argv[2] : "./build/persistance_node", absPers)
                             ? absPers : "./build/persistance_node";

    const char* mHost = std::getenv("MONGO_HOST") ? std::getenv("MONGO_HOST") : "127.0.0.1";
    const int   mPort = std::atoi(std::getenv("MONGO_PORT") ? std::getenv("MONGO_PORT") : "27017");
    g_uri = "mongodb://" + std::string(mHost) + ":" + std::to_string(mPort);

    {
        DGS::TCPSocket probe;
        if (!probe.connect(mHost, mPort, 1000))
        {
            std::printf("  ──────────────────────────────────────────────────────────────────────\n");
            std::printf("  SKIPPED: a ban surviving a restart needs a live Mongo at %s:%d.\n", mHost, mPort);
            std::printf("           podman run -d --rm -p 27017:27017 docker.io/library/mongo:7\n");
            std::printf("  ──────────────────────────────────────────────────────────────────────\n");
            if (std::getenv("DGS_REQUIRE_MONGO")) {
                check(false, "DGS_REQUIRE_MONGO is set but no database answered: a failure, not a skip");
                std::printf("\n== social_persist_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
                return 1;
            }
            std::printf("\n== social_persist_e2e: skipped ==\n");
            return 0;
        }
    }

    const pid_t pers = spawn(persPath, persEnv, "social_pers");
    { DGS::TCPSocket t; bool ok = waitPort(t, kPersPort, 300);
      check(ok, "the persistence node is up against a live database");
      if (!ok) { stop(pers); return 1; } }

    // ══ A. A ban is issued, and a subscriber that arrives AFTERWARDS hears it ════════════════════
    pid_t socialA = spawn(socialPath, socialEnv, "social_a");
    DGS::TCPSocket writer;
    const bool aUp = waitPort(writer, kSocialPort, 300);
    check(aUp, "the social node accepts a writer");
    if (!aUp) { stop(socialA); stop(pers); return 1; }

    {
        DGS::AccountAction ban{};
        ban.actorUuid = 1; ban.targetUuid = kBanned; ban.action = DGS::ACC_BAN;
        ban.durationS = 0;   // permanent
        std::snprintf(ban.reason, sizeof(ban.reason), "cheating");
        DGS::Packet p; p.pack(ban);
        writer.send(writer.getSocketFD(), p.getRawData(), p.getSize());

        DGS::SocialDelta join{};
        join.scopeUuid = kGuild; join.targetUuid = kBanned; join.kind = DGS::SOCIAL_GUILD_JOIN;
        DGS::Packet pj; pj.pack(join);
        writer.send(writer.getSocketFD(), pj.getRawData(), pj.getSize());

        DGS::SocialDelta rank{};
        rank.scopeUuid = kGuild; rank.targetUuid = kBanned; rank.kind = DGS::SOCIAL_GUILD_RANK;
        rank.rank = 3;
        DGS::Packet pr; pr.pack(rank);
        writer.send(writer.getSocketFD(), pr.getRawData(), pr.getSize());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    {
        DGS::TCPSocket late;
        const bool ok = waitPort(late, kSocialPort, 200);
        Heard h = listen(late, 800);
        std::printf("    same live node, a LATE subscriber heard: %d packets, %zu bans\n",
                    h.packets, h.bans.size());
        check(ok && h.bans.count(kBanned) == 1,
              "a subscriber connecting AFTER the ban still hears it (state is replayed, not just broadcast)");
        check(h.guildRank.count(kBanned) && h.guildRank[kBanned] == 3,
              "and the guild rank comes with it, not just the membership");
    }

    // ══ B. The node is killed and a NEW process started ══════════════════════════════════════════
    stop(socialA);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    pid_t socialB = spawn(socialPath, socialEnv, "social_b");

    DGS::TCPSocket after;
    const bool bUp = waitPort(after, kSocialPort, 400);
    check(bUp, "a NEW social node process starts and accepts subscribers");

    Heard h2;
    if (bUp) h2 = listen(after, 1500);
    std::printf("    after the restart, a subscriber heard: %d packets, %zu bans, %zu ranks\n",
                h2.packets, h2.bans.size(), h2.guildRank.size());
    check(bUp && h2.bans.count(kBanned) == 1,
          "the BAN SURVIVED the restart (it used to be undone by restarting the process)");
    check(h2.bans.count(kBanned) && h2.bans[kBanned] == 0,
          "and it is still PERMANENT, not silently turned into a finite one");
    check(h2.guildRank.count(kBanned) && h2.guildRank[kBanned] == 3,
          "the guild membership and its rank survived too");

    // ══ C. Counter-proof ═════════════════════════════════════════════════════════════════════════
    check(h2.bans.count(kInnocent) == 0,
          "an account that was never banned is NOT announced (it is not simply broadcasting everyone)");

    stop(socialB);
    stop(pers);

    std::printf("\n== social_persist_e2e: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
