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
#include "include/dgs/types.h"
#include <csignal>

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

    // Optional write-through to persistence (MongoDB) — source of truth for bans/guilds.
    DGS::TCPSocket persistence;
    const char* persHost = std::getenv("PERSISTENCE_HOST") ? std::getenv("PERSISTENCE_HOST") : "persistence";
    int         persPort = std::atoi(std::getenv("PERSISTENCE_PORT") ? std::getenv("PERSISTENCE_PORT") : "42429");
    if (!persistence.connect(persHost, persPort))
        std::cout << "[Social] Persistence unavailable at " << persHost << ":" << persPort
                  << " -> in-memory state only" << std::endl;

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
        int n = epoll_wait(epollFD, events, 64, -1);
        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;

            if (fd == socialSocket.getSocketFD())
            {
                int newFD = socialSocket.accept();
                if (newFD < 0) continue;
                subscribers.insert(newFD);
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                std::cout << "[Social] Subscriber connected FD=" << newFD << std::endl;
            }
            else if (subscribers.count(fd))
            {
                uint8_t buffer[8192];
                int bytes = socialSocket.receive(fd, buffer, sizeof(buffer));
                if (bytes <= 0)
                {
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    socialSocket.closeClient(fd);
                    subscribers.erase(fd);
                    continue;
                }

                DGS::Packet p;
                p.setBuffer(buffer, bytes);
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
