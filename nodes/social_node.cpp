// ================================================================================================
// social_node — PLANO SOCIAL/CUENTA (§3.7, P7).
//
// Un solo nodo social/account es el dueño del plano NO espacial (guilds, parties, amigos, bans,
// economía de gremio): regla 1 de §3.7 ("un solo dueño por tipo de dato"). Las zonas/head solo leen
// ids. Recibe del head los eventos de validación escalados (validador → head → social → todas las
// zonas ven "uuid baneado") y de los clientes los deltas sociales; mantiene el estado pequeño en
// memoria y difunde los eventos por SECUENCIA (seq por canal, tipo GhostDelta pero de canal, no de
// chunk) a los miembros ONLINE suscritos.
//
// Este nodo también hace de SERVICIO DE CHAT: routing por canal y suscripción por uuid (no por
// proximidad), rate-limit por canal (CHAT_RATE_MS por uuid) y seq de orden por canal, ANTES del
// fan-out (anti-spam/anti-abuse — NO física → no pasa por el validador, §3.7).
//
// Estados de cuenta (bans/permisos) se aplican aquí y se reenvían a las zonas conectadas para que
// bloqueen entrada. Persistencia write-through → persistance_node (MongoDB) vía TCP (PERSISTENCE_*).
// ================================================================================================
#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/types.h"

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

// Estado en memoria del plano social (fuente de verdad en esta sesión; write-through a persistence).
struct SocialState
{
    // guildId → {miembro → rango}
    std::map<uint32_t, std::map<uint32_t, uint8_t>> guilds;
    // partyId → miembros
    std::map<uint32_t, std::set<uint32_t>> parties;
    // uuid → {amigo}
    std::map<uint32_t, std::set<uint32_t>> friends;
    // uuid baneado → {hasta (0=permanente), motivo}
    std::map<uint32_t, std::pair<uint64_t, std::string>> banned;
    // permisos por cuenta
    std::map<uint32_t, uint32_t> perms;
    // seq por canal (orden/fan-out)
    std::map<uint8_t, uint64_t> seqByChannel;
};

static void broadcast(int fd, DGS::TCPSocket& s, const std::set<int>& subscribers, const uint8_t* raw, size_t n)
{
    for (int sub : subscribers)
        if (sub != fd) s.send(sub, raw, n);
}

// Aplica un delta social al estado y lo difunde con seq (§3.7).
static void applySocialDelta(DGS::TCPSocket& s, const std::set<int>& subscribers,
                             int fd, DGS::Packet& p, SocialState& st, DGS::TCPSocket& persistence)
{
    auto d = p.unpackSocialDelta();

    uint64_t seq = ++st.seqByChannel[PKT_SOCIAL_DELTA];
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
        case DGS::SOCIAL_ZONE_UPDATE: /* routing info; opaco para el plano social */ break;
    }

    DGS::Packet out; out.pack(d);
    broadcast(fd, s, subscribers, out.getRawData(), out.getSize());

    // Write-through de la economía/guild → persistance_node (best-effort; si no está, seguimos).
    persistence.send(persistence.getSocketFD(), out.getRawData(), out.getSize());

    std::cout << "[Social] delta kind=" << (int)d.kind
              << " target=" << d.targetUuid << " scope=" << d.scopeUuid
              << " seq=" << seq << std::endl;
}

// Servicio de chat: routing por canal + rate-limit por uuid + seq de orden (§3.7).
static void handleChat(DGS::TCPSocket& s, const std::set<int>& subscribers,
                       int fd, DGS::Packet& p, SocialState& st)
{
    auto c = p.unpackChatMessage();

    static const uint32_t RATE_MS = 500;   // máx 2 msgs/s por uuid (anti-spam)
    static std::map<uint32_t, uint64_t> lastChatAt;

    uint64_t now = nowMs();
    auto last = lastChatAt.find(c.uuid);
    if (last != lastChatAt.end() && now - last->second < RATE_MS)
    {
        std::cout << "[Social] chat rate-limit uuid=" << c.uuid
                  << " (hace " << (now - last->second) << "ms)" << std::endl;
        return;   // descartado (anti-spam) — no llega al fan-out
    }
    lastChatAt[c.uuid] = now;

    // Canal local → lo enruta la zona por interés espacial; el resto, por suscripción aquí (§3.7).
    if (c.channel == DGS::CHAT_LOCAL) return;   // la zona dueña lo emite, no el social

    c.seq = ++st.seqByChannel[PKT_CHAT];
    c.timestampMs = (uint32_t)std::time(nullptr);

    DGS::Packet out; out.pack(c);
    broadcast(fd, s, subscribers, out.getRawData(), out.getSize());

    std::cout << "[Social] chat canal=" << (int)c.channel << " uuid=" << c.uuid
              << " seq=" << c.seq << std::endl;
}

// Acción de cuenta (ban/permisos): se aplica aquí y se reenvía a todas las zonas conectadas.
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

    std::cout << "[Social] cuenta action=" << (int)a.action
              << " target=" << a.targetUuid << (a.action == DGS::ACC_BAN ? " BAN" : "") << std::endl;
}

int main()
{
    DGS::TCPSocket socialSocket;
    if (!socialSocket.listen(42430))
    {
        std::cerr << "[Social] Error al escuchar en 42430" << std::endl;
        return 1;
    }
    std::cout << "[Social] Escuchando TCP:42430 (plano social/cuenta + chat)" << std::endl;

    // Write-through opcional a persistence (MongoDB) — fuente de verdad para bans/guilds.
    DGS::TCPSocket persistence;
    const char* persHost = std::getenv("PERSISTENCE_HOST") ? std::getenv("PERSISTENCE_HOST") : "persistence";
    int         persPort = std::atoi(std::getenv("PERSISTENCE_PORT") ? std::getenv("PERSISTENCE_PORT") : "42429");
    if (!persistence.connect(persHost, persPort))
        std::cout << "[Social] Persistence no disponible en " << persHost << ":" << persPort
                  << " -> solo estado en memoria" << std::endl;

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
                std::cout << "[Social] Suscriptor conectado FD=" << newFD << std::endl;
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
