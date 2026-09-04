// ================================================================================================
// social_node — SOCIAL/ACCOUNT PLANE (§3.7, P7).
//
// A single social/account node owns the NON-spatial plane (guilds, parties, friends, bans, guild
// economy): rule 1 of §3.7 ("one owner per data type"). Zones and the head only read ids. It receives
// escalated validation events from the head (validator → head → social → every zone sees "uuid
// banned") and social deltas from clients; it keeps the small state in memory and broadcasts events by
// SEQUENCE (seq per channel — GhostDelta-style, but per channel rather than per chunk) to the
// subscribed ONLINE members.
//
// This node is also the CHAT SERVICE: routing per channel and subscription per uuid (not by
// proximity), a per-channel rate limit (CHAT_RATE_MS per uuid) and an ordering seq per channel, all
// BEFORE the fan-out (anti-spam/anti-abuse — NOT physics → it never reaches the validator, §3.7).
//
// Account states (bans/permissions) are applied here and forwarded to the connected zones so they can
// block entry. Write-through persistence → persistance_node (MongoDB) over TCP (PERSISTENCE_*).
// ================================================================================================
#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/auth.h"
#include "include/dgs/types.h"
#include <csignal>
#include <poll.h>
#include <atomic>
#include <thread>
#include <mutex>
#include <memory>

#include <sys/epoll.h>
#include <map>
#include <set>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <ctime>
#include <iostream>

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// In-memory state of the social plane (source of truth for this session; write-through to persistence).
struct SocialState
{
    // guildId → {member → rank}
    std::map<uint32_t, std::map<uint32_t, uint8_t>> guilds;
    // partyId → members
    std::map<uint32_t, std::set<uint32_t>> parties;
    // uuid → {friend}
    std::map<uint32_t, std::set<uint32_t>> friends;
    // banned uuid → {until (0 = permanent), reason}
    std::map<uint32_t, std::pair<uint64_t, std::string>> banned;
    // permissions per account
    std::map<uint32_t, uint32_t> perms;
    // seq per channel (ordering/fan-out)
    std::map<uint8_t, uint64_t> seqByChannel;
};

static void broadcast(int fd, DGS::TCPSocket& s, const std::set<int>& subscribers, const uint8_t* raw, size_t n)
{
    for (int sub : subscribers)
        if (sub != fd) s.send(sub, raw, n);
}

// ⚠️ A SUBSCRIBER THAT CONNECTS LATE USED TO LEARN NOTHING. Bans and guild changes were broadcast at
// the moment they happened and never again, so a zone that started afterwards — or reconnected, or was
// scaled up — served a banned account happily, and restoring this node's state from the database would
// have changed nothing in the world for the same reason. State has to be replayed to whoever arrives.
//
// Only the DURABLE plane is replayed (bans, permissions, guild membership, friendships). Chat is not
// state and parties are session-scoped. It goes to that one subscriber, not to everybody.
static void sendStateTo(DGS::TCPSocket& s, int fd, const SocialState& st)
{
    int sent = 0;
    for (const auto& [uuid, ban] : st.banned)
    {
        // A ban whose deadline has already passed is not replayed: the clock released it.
        if (ban.first != 0 && nowMs() >= ban.first) continue;
        DGS::AccountAction a{};
        a.targetUuid = uuid;
        a.action     = DGS::ACC_BAN;
        a.durationS  = ban.first == 0 ? 0u : (uint32_t)((ban.first - nowMs()) / 1000 + 1);
        std::snprintf(a.reason, sizeof(a.reason), "%s", ban.second.c_str());
        DGS::Packet p; p.pack(a);
        s.send(fd, p.getRawData(), p.getSize());
        ++sent;
    }
    for (const auto& [uuid, flags] : st.perms)
    {
        DGS::AccountAction a{};
        a.targetUuid = uuid; a.action = DGS::ACC_SET_PERM; a.permFlags = flags;
        DGS::Packet p; p.pack(a);
        s.send(fd, p.getRawData(), p.getSize());
        ++sent;
    }
    for (const auto& [guildId, members] : st.guilds)
        for (const auto& [uuid, rank] : members)
        {
            DGS::SocialDelta d{};
            d.scopeUuid = guildId; d.targetUuid = uuid;
            d.kind = DGS::SOCIAL_GUILD_JOIN;
            { DGS::Packet p; p.pack(d); s.send(fd, p.getRawData(), p.getSize()); ++sent; }
            d.kind = DGS::SOCIAL_GUILD_RANK; d.rank = rank;
            { DGS::Packet p; p.pack(d); s.send(fd, p.getRawData(), p.getSize()); ++sent; }
        }
    for (const auto& [uuid, fr] : st.friends)
        for (uint32_t other : fr)
        {
            DGS::SocialDelta d{};
            d.targetUuid = uuid; d.scopeUuid = other; d.kind = DGS::SOCIAL_FRIEND_ADD;
            DGS::Packet p; p.pack(d); s.send(fd, p.getRawData(), p.getSize()); ++sent;
        }
    if (sent > 0)
        std::cout << "[Social] replayed " << sent << " state records to FD=" << fd << std::endl;
}

// Applies a social delta to the state and broadcasts it with a seq (§3.7).
static void applySocialDelta(DGS::TCPSocket& s, const std::set<int>& subscribers,
                             int fd, DGS::Packet& p, SocialState& st, DGS::TCPSocket& persistence)
{
    auto d = p.unpackSocialDelta();

    uint64_t seq = ++st.seqByChannel[DGS::PKT_SOCIAL_DELTA];
    d.seq = seq;

    switch (d.kind)
    {
        case DGS::SOCIAL_GUILD_JOIN:  st.guilds[d.scopeUuid][d.targetUuid] = 0; break;
        case DGS::SOCIAL_GUILD_LEAVE: st.guilds[d.scopeUuid].erase(d.targetUuid); break;
        case DGS::SOCIAL_GUILD_RANK:  if (st.guilds[d.scopeUuid].count(d.targetUuid)) st.guilds[d.scopeUuid][d.targetUuid] = d.rank; break;
        case DGS::SOCIAL_GUILD_DISBAND: st.guilds.erase(d.scopeUuid); break;
        case DGS::SOCIAL_PARTY_JOIN:  st.parties[d.scopeUuid].insert(d.targetUuid); break;
        case DGS::SOCIAL_PARTY_LEAVE: st.parties[d.scopeUuid].erase(d.targetUuid); break;
        case DGS::SOCIAL_FRIEND_ADD:  st.friends[d.targetUuid].insert(d.scopeUuid); break;
        case DGS::SOCIAL_FRIEND_REMOVE: st.friends[d.targetUuid].erase(d.scopeUuid); break;
        case DGS::SOCIAL_ZONE_UPDATE: /* routing info; opaque to the social plane */ break;
    }

    DGS::Packet out; out.pack(d);
    broadcast(fd, s, subscribers, out.getRawData(), out.getSize());

    // Write-through of guild/economy state → persistance_node (best-effort; if it is down we go on).
    persistence.send(persistence.getSocketFD(), out.getRawData(), out.getSize());

    std::cout << "[Social] delta kind=" << (int)d.kind
              << " target=" << d.targetUuid << " scope=" << d.scopeUuid
              << " seq=" << seq << std::endl;
}

// Chat service: per-channel routing + per-uuid rate limit + ordering seq (§3.7).
static void handleChat(DGS::TCPSocket& s, const std::set<int>& subscribers,
                       int fd, DGS::Packet& p, SocialState& st)
{
    auto c = p.unpackChatMessage();

    static const uint32_t RATE_MS = 500;   // max 2 msgs/s per uuid (anti-spam)
    static std::map<uint32_t, uint64_t> lastChatAt;

    uint64_t now = nowMs();
    auto last = lastChatAt.find(c.uuid);
    if (last != lastChatAt.end() && now - last->second < RATE_MS)
    {
        std::cout << "[Social] chat rate-limit uuid=" << c.uuid
                  << " (" << (now - last->second) << "ms ago)" << std::endl;
        return;   // dropped (anti-spam) — never reaches the fan-out
    }
    lastChatAt[c.uuid] = now;

    // Local channel → routed by the zone through spatial interest; the rest by subscription here (§3.7).
    if (c.channel == DGS::CHAT_LOCAL) return;   // the owning zone emits it, not the social node

    c.seq = ++st.seqByChannel[DGS::PKT_CHAT];
    c.timestampMs = (uint32_t)std::time(nullptr);

    DGS::Packet out; out.pack(c);
    broadcast(fd, s, subscribers, out.getRawData(), out.getSize());

    std::cout << "[Social] chat channel=" << (int)c.channel << " uuid=" << c.uuid
              << " seq=" << c.seq << std::endl;
}

// Account action (ban/permissions): applied here and forwarded to every connected zone.
static void handleAccount(DGS::TCPSocket& s, const std::set<int>& subscribers,
                          int fd, DGS::Packet& p, SocialState& st, DGS::TCPSocket& persistence)
{
    auto a = p.unpackAccountAction();

    switch (a.action)
    {
        case DGS::ACC_BAN:
            st.banned[a.targetUuid] = { a.durationS ? nowMs() + (uint64_t)a.durationS * 1000 : 0,
                                        std::string(a.reason) };
            break;
        case DGS::ACC_UNBAN: st.banned.erase(a.targetUuid); break;
        case DGS::ACC_SET_PERM: st.perms[a.targetUuid] = a.permFlags; break;
        default: return;
    }

    DGS::Packet out; out.pack(a);
    broadcast(fd, s, subscribers, out.getRawData(), out.getSize());
    persistence.send(persistence.getSocketFD(), out.getRawData(), out.getSize());

    std::cout << "[Social] account action=" << (int)a.action
              << " target=" << a.targetUuid << (a.action == DGS::ACC_BAN ? " BAN" : "") << std::endl;
}

int main()
{
    // ⚠️ A NODE MUST NOT DIE BECAUSE A PEER HUNG UP. Writing to a socket whose other end has closed
    // raises SIGPIPE, and its default action is to KILL the process. No node installed this, and the
    // whole suite stayed green anyway: every test calls `signal(SIGPIPE, SIG_IGN)` before `fork()`, and
    // a child INHERITS an ignored disposition — so under CTest the nodes survived, and started from a
    // shell, systemd, Docker or `dgs run` they died the first time a peer disconnected.
    // Measured with the same binary and the same environment: parent ignoring SIGPIPE -> ran the full
    // 6 s; ordinary parent -> exit 141 (128 + SIGPIPE) within seconds of the head closing.
    // A closed peer is an ordinary event: `send` returns EPIPE and the reconnect paths handle it.
    std::signal(SIGPIPE, SIG_IGN);
    DGS::TCPSocket socialSocket;
    const int socialPort = std::atoi(std::getenv("SOCIAL_TCP_PORT") ? std::getenv("SOCIAL_TCP_PORT") : "42430");
    if (!socialSocket.listen(socialPort))
    {
        std::cerr << "[Social] Failed to listen on 42430" << std::endl;
        return 1;
    }
    std::cout << "[Social] Listening on TCP:42430 (social/account plane + chat)" << std::endl;

    // Write-through to persistence, and — the half that did not exist — the RESTORE.
    //
    // ⚠️ `SocialState` IS A PLAIN LOCAL VARIABLE. Every guild, ban, friendship and permission lived
    // only in this process, and the write-through it performed was being discarded by a persistence
    // node that understood nothing but entities. So restarting this node **unbanned every account**:
    // a moderator's decision undone by an operator restarting a service. Both halves are fixed now —
    // that node stores the social plane, and this one asks for it back on start-up.
    //
    // The connect happens on a WORKER, and with a deadline. It used to be a blocking `connect` with no
    // timeout on the path to the epoll loop: `connect` bounds the TCP handshake and not the name
    // resolution before it, and an unresolvable `PERSISTENCE_HOST` cost 11.2 seconds on this machine —
    // a social node that listens but accepts nobody. The zone node has this same note twice, for the
    // same reason.
    DGS::TCPSocket persistence;
    const char* persHost = std::getenv("PERSISTENCE_HOST") ? std::getenv("PERSISTENCE_HOST") : "persistence";
    int         persPort = std::atoi(std::getenv("PERSISTENCE_PORT") ? std::getenv("PERSISTENCE_PORT") : "42429");

    std::mutex                     linkMtx;
    std::unique_ptr<DGS::TCPSocket> linkReady;
    std::vector<DGS::Packet>       restorePackets;
    std::atomic<bool>              linkPending{true};
    bool                           restoreMerged = false;

    const uint64_t startedMs = nowMs();
    const int restoreDeadlineMs = std::atoi(std::getenv("SOCIAL_RESTORE_MS")
                                            ? std::getenv("SOCIAL_RESTORE_MS") : "2000") + 1500;

    std::thread linkThread([&]{
        auto sock = std::unique_ptr<DGS::TCPSocket>(new DGS::TCPSocket());
        if (sock->connect(persHost, persPort, 500)) DGS::sendAuth(*sock);
        else
        {
            std::cout << "[Social] Persistence unavailable at " << persHost << ":" << persPort
                      << " -> in-memory state only" << std::endl;
            linkPending = false;
            return;
        }

        std::vector<DGS::Packet> got;
        if (!(std::getenv("SOCIAL_RESTORE") && std::string(std::getenv("SOCIAL_RESTORE")) == "0"))
        {
            DGS::Packet q; q.pack(DGS::PKT_SOCIAL_QUERY);
            sock->send(sock->getSocketFD(), q.getRawData(), q.getSize());

            const int restoreMs = std::atoi(std::getenv("SOCIAL_RESTORE_MS")
                                            ? std::getenv("SOCIAL_RESTORE_MS") : "2000");
            const uint64_t deadline = nowMs() + (uint64_t)restoreMs;
            uint8_t buf[8192];
            bool done = false;
            while (!done && nowMs() < deadline)
            {
                pollfd pfd{ sock->getSocketFD(), POLLIN, 0 };
                if (::poll(&pfd, 1, 50) <= 0 || !(pfd.revents & POLLIN)) continue;
                const int n = sock->receive(sock->getSocketFD(), buf, sizeof(buf));
                if (n <= 0) break;
                DGS::Packet p; p.setBuffer(buf, (size_t)n);
                if (p.getType() == DGS::PKT_NONE) { done = true; break; }
                got.push_back(p);
            }
            std::cout << "[Social] restored " << got.size() << " social records"
                      << (done ? "" : " (answer incomplete)") << std::endl;
        }

        { std::lock_guard<std::mutex> lk(linkMtx);
          restorePackets = std::move(got);
          linkReady = std::move(sock); }
        linkPending = false;
    });

    // NODE-ONLY PORT: zones subscribe here to learn bans and guild changes, and a subscriber can also
    // ISSUE a ban. Unauthenticated, anyone who could reach it could ban any account in the world.
    DGS::AuthGate gate("Social");
    gate.announce();

    int epollFD = epoll_create1(0);
    epoll_event ev;
    ev.events  = EPOLLIN;
    ev.data.fd = socialSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, socialSocket.getSocketFD(), &ev);

    epoll_event events[64];
    std::set<int> subscribers;
    SocialState st;

    while (true)
    {
        // ⚠️ A BOUNDED WAIT, not the infinite one this had. The restored state arrives on a worker and
        // has to be merged here, on the thread that owns `st` — with `-1` the node would sit blocked
        // until somebody happened to connect, so a node nobody talks to would never restore at all.
        int n = epoll_wait(epollFD, events, 64, 200);

        // The restored social plane, applied exactly once, through the SAME handlers that apply a live
        // delta. Replaying it any other way would be a second implementation of the rules, free to
        // disagree with the first.
        // ⚠️ DO NOT SERVE STATE YOU HAVE NOT LOADED. Accepting a subscriber before the restore has
        // landed means replaying an EMPTY state to it — telling a zone "nobody is banned" — and it
        // also means applying live deltas with a persistence socket that is not connected yet, so
        // those writes are lost. Both were measured: the ban made it neither into the database nor
        // into the subscriber. Connections wait in the listen backlog, which is what a backlog is for.
        //
        // Bounded, though: a database that never answers must not keep the social plane offline for
        // ever. Past the deadline the node starts serving and says plainly that its state may be
        // incomplete, rather than pretending.
        if (!restoreMerged && linkPending.load() && nowMs() - startedMs > (uint64_t)restoreDeadlineMs)
        {
            std::cerr << "[Social] persistence did not answer in " << restoreDeadlineMs
                      << " ms -> serving WITHOUT restored state (bans may be missing)" << std::endl;
            restoreMerged = true;
        }

        if (!restoreMerged && !linkPending.load())
        {
            restoreMerged = true;
            std::vector<DGS::Packet> pending;
            { std::lock_guard<std::mutex> lk(linkMtx);
              pending = std::move(restorePackets);
              if (linkReady) { persistence = std::move(*linkReady); linkReady.reset(); } }

            int applied = 0;
            for (auto& p : pending)
            {
                // Applied to the local state only: this is state coming BACK, not new decisions, so it
                // is neither re-broadcast nor written through again.
                try {
                    if (p.getType() == DGS::PKT_SOCIAL_DELTA)
                    {
                        const DGS::SocialDelta d = p.unpackSocialDelta();
                        switch (d.kind)
                        {
                            case DGS::SOCIAL_GUILD_JOIN:  st.guilds[d.scopeUuid][d.targetUuid] = 0; break;
                            case DGS::SOCIAL_GUILD_RANK:
                                if (st.guilds[d.scopeUuid].count(d.targetUuid))
                                    st.guilds[d.scopeUuid][d.targetUuid] = d.rank;
                                break;
                            case DGS::SOCIAL_FRIEND_ADD:  st.friends[d.targetUuid].insert(d.scopeUuid); break;
                            default: break;
                        }
                        ++applied;
                    }
                    else if (p.getType() == DGS::PKT_ACCOUNT)
                    {
                        const DGS::AccountAction a = p.unpackAccountAction();
                        if (a.action == DGS::ACC_BAN)
                            st.banned[a.targetUuid] = { a.durationS ? nowMs() + (uint64_t)a.durationS * 1000 : 0,
                                                        std::string(a.reason) };
                        else if (a.action == DGS::ACC_SET_PERM)
                            st.perms[a.targetUuid] = a.permFlags;
                        ++applied;
                    }
                } catch (const std::exception&) { /* malformed record: skip it, stay up */ }
            }
            if (applied > 0)
                std::cout << "[Social] applied " << applied << " restored records ("
                          << st.banned.size() << " bans, " << st.guilds.size() << " guilds)" << std::endl;
        }

        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;

            if (fd == socialSocket.getSocketFD())
            {
                // Not ready yet: leave it in the backlog rather than answer with an empty world.
                if (!restoreMerged) continue;
                int newFD = socialSocket.accept();
                if (newFD < 0) continue;
                subscribers.insert(newFD);
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                std::cout << "[Social] Subscriber connected FD=" << newFD << std::endl;
                sendStateTo(socialSocket, newFD, st);   // what it missed before it arrived
            }
            else if (subscribers.count(fd))
            {
                // An epoll wake-up is not proof of application data once TLS is on: the handshake's
                // trailing records (a TLS 1.3 `NewSessionTicket`, say) wake it too, and the blocking
                // read below would then wait for a message nobody sent. See `TCPSocket::pending`.
                if (socialSocket.tlsEnabled() && !socialSocket.pending(fd)) continue;

                uint8_t buffer[8192];
                int bytes = socialSocket.receive(fd, buffer, sizeof(buffer));
                if (bytes <= 0)
                {
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    socialSocket.closeClient(fd);
                    subscribers.erase(fd);
                    gate.forget(fd);
                    continue;
                }

                DGS::Packet p;
                p.setBuffer(buffer, bytes);

                if (gate.consume(fd, p)) continue;
                if (!gate.allows(fd)) { gate.refuse(fd, (int)p.getType()); continue; }
                switch (p.getType())
                {
                    case DGS::PKT_SOCIAL_DELTA: applySocialDelta(socialSocket, subscribers, fd, p, st, persistence); break;
                    case DGS::PKT_CHAT:         handleChat(socialSocket, subscribers, fd, p, st); break;
                    case DGS::PKT_ACCOUNT:      handleAccount(socialSocket, subscribers, fd, p, st, persistence); break;
                    default: break;
                }
            }
        }
    }

    return 0;
}
