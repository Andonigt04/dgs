#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/auth.h"
#include "include/dgs/logger.h"
#include "include/dgs/types.h"
#include <csignal>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/options/index.hpp>
#include <mongocxx/options/update.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/database.hpp>
#include <exception>
#include <cstdlib>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/types.hpp>
#include <vector>
#include <cstring>
#include <ctime>

#include <sys/epoll.h>
#include <map>
#include <set>
#include <iostream>

// The most entities one region query can ever return, whatever the caller asks for. A request off the
// network must not be able to make this node stream an entire world into a single socket.
static const uint32_t RANGE_HARD_CAP = 4096;

// The same idea for the social plane: one query must not stream a whole world's guilds into a socket.
static const uint32_t SOCIAL_HARD_CAP = 8192;

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// ⚠️ THIS USED TO `insert_one` EVERY UPDATE, so one entity that kept moving produced one document per
// update for ever. Measured against a live Mongo: six updates of the same uuid left **six documents**,
// the collection had **only the `_id_` index** (any lookup by uuid is a full scan) and **no document
// carried a timestamp**, so there was no way even to tell which of them was the latest — ObjectId's
// embedded clock has one-second resolution and the write rate is 10-20 Hz per player.
//
// What this node is for is RESTORING STATE, so it stores state: one document per entity, upserted,
// with the time it was written. The audit trail already exists separately, in the CSV `Logger`. Storage
// stops growing with time and starts being proportional to the number of entities, which is the thing
// it is actually describing.
//
// ⚠️ AND `stats` WERE NEVER STORED AT ALL. Health, speed and damage went into the packet, crossed the
// validator, reached this node and were dropped on the floor: an entity restored from Mongo came back
// at zero health. Found by reading a document back for the first time — the write path alone could
// never have shown it, because nothing ever read.
void storeEntity(mongocxx::collection& col, const DGS::EntityTransfer& e)
{
    bsoncxx::builder::stream::document set{};
    set << "uuid"      << (int32_t)e.uuid
        << "type"      << (int32_t)e.type
        << "chunkX"    << e.chunkX
        << "chunkY"    << e.chunkY
        << "chunkZ"    << e.chunkZ
        << "localX"    << (double)e.pos[0]
        << "localY"    << (double)e.pos[1]
        << "localZ"    << (double)e.pos[2]
        << "angle"     << (int32_t)e.angle
        << "dataSize"  << (int32_t)e.dataSize
        << "state"     << (int32_t)e.state
        << "speedX"    << (double)e.stats.speed[0]
        << "speedY"    << (double)e.stats.speed[1]
        << "speedZ"    << (double)e.stats.speed[2]
        << "health"    << (double)e.stats.health
        << "baseDMG"   << (double)e.stats.baseDMG
        << "healing"   << (double)e.stats.healing
        << "updatedAtMs" << (int64_t)nowMs();

    // C6 (§3.6): the module's OPAQUE blob. The DGS does not interpret it — it only carries and stores it
    // as binary so the module can rebuild its state (same code on client and server).
    // Without this, reloading a zone loses the module's state even though pos/stats survive.
    if (e.dataSize > 0)
    {
        set << "moduleBlob" << bsoncxx::types::b_binary{
            bsoncxx::binary_sub_type::k_binary, e.dataSize, e.data};
    }

    bsoncxx::builder::stream::document filter{};
    filter << "uuid" << (int32_t)e.uuid;
    bsoncxx::builder::stream::document update{};
    update << "$set" << set.view();

    mongocxx::options::update opts{};
    opts.upsert(true);
    col.update_one(filter.view(), update.view(), opts);
}

/// One stored document -> one EntityTransfer. Shared by the by-uuid read and the by-range one, so the
/// two can never come to disagree about what a stored entity means.
static DGS::EntityTransfer decodeEntity(const bsoncxx::document::view& v)
{
    DGS::EntityTransfer out{};
    auto num = [&](const char* k) -> double {
        auto it = v.find(k);
        if (it == v.end()) return 0.0;
        switch (it->type())
        {
            case bsoncxx::type::k_double:  return it->get_double().value;
            case bsoncxx::type::k_int32:   return (double)it->get_int32().value;
            case bsoncxx::type::k_int64:   return (double)it->get_int64().value;
            default:                       return 0.0;
        }
    };

    out.uuid   = (uint32_t)num("uuid");
    out.type   = (DGS::EntityType)(int)num("type");
    out.chunkX = (int32_t)num("chunkX");
    out.chunkY = (int32_t)num("chunkY");
    out.chunkZ = (int32_t)num("chunkZ");
    out.pos[0] = (float)num("localX");
    out.pos[1] = (float)num("localY");
    out.pos[2] = (float)num("localZ");
    out.angle  = (uint16_t)num("angle");
    out.state  = (DGS::EntityState)(int)num("state");
    out.stats.speed[0] = (float)num("speedX");
    out.stats.speed[1] = (float)num("speedY");
    out.stats.speed[2] = (float)num("speedZ");
    out.stats.health   = (float)num("health");
    out.stats.baseDMG  = (float)num("baseDMG");
    out.stats.healing  = (float)num("healing");

    auto blob = v.find("moduleBlob");
    if (blob != v.end() && blob->type() == bsoncxx::type::k_binary)
    {
        const auto b = blob->get_binary();
        // The stored length is authority over the declared one: a document written by an older or
        // different writer must not be able to make this copy past the end of `data`.
        const uint32_t n = b.size < DGS::MAX_ENTITY_DATA ? b.size : DGS::MAX_ENTITY_DATA;
        std::memcpy(out.data, b.bytes, n);
        out.dataSize = (uint16_t)n;
    }
    return out;
}

/// Reads one entity's last stored state. @return whether there was anything stored for that uuid.
///
/// This is the half that did not exist. Everything about "persistence" is a claim about being able to
/// get the state BACK, and nothing in the system could: the node only ever wrote.
bool loadEntity(mongocxx::collection& col, uint32_t uuid, DGS::EntityTransfer& out)
{
    bsoncxx::builder::stream::document filter{};
    filter << "uuid" << (int32_t)uuid;

    auto doc = col.find_one(filter.view());
    if (!doc) return false;
    out = decodeEntity(doc->view());
    return true;
}

/// Everything stored inside a chunk range, capped. This is what a ZONE needs at start-up: it knows the
/// region it serves and not one single uuid inside it, so a by-uuid read could never have restored it.
///
/// `limit` is clamped rather than trusted: the request comes over the network, and one query must not
/// be able to turn into an unbounded stream out of this node.
std::vector<DGS::EntityTransfer> loadRange(mongocxx::collection& col,
                                           const DGS::PersistRange& r,
                                           uint32_t hardCap)
{
    using bsoncxx::builder::stream::document;
    using bsoncxx::builder::stream::open_document;
    using bsoncxx::builder::stream::close_document;
    using bsoncxx::builder::stream::finalize;

    document filter{};
    filter << "chunkX" << open_document << "$gte" << r.chunkXMin << "$lte" << r.chunkXMax << close_document
           << "chunkY" << open_document << "$gte" << r.chunkYMin << "$lte" << r.chunkYMax << close_document
           << "chunkZ" << open_document << "$gte" << r.chunkZMin << "$lte" << r.chunkZMax << close_document;

    const uint32_t cap = (r.limit == 0 || r.limit > hardCap) ? hardCap : r.limit;

    mongocxx::options::find opts{};
    opts.limit((int64_t)cap);

    std::vector<DGS::EntityTransfer> out;
    out.reserve(cap < 256 ? cap : 256);
    for (auto&& doc : col.find(filter.view(), opts))
        out.push_back(decodeEntity(doc));
    return out;
}

// ── THE SOCIAL PLANE ────────────────────────────────────────────────────────────────────────────
// ⚠️ THE SOCIAL NODE'S WRITE-THROUGH WAS BEING DROPPED ON THE FLOOR. It sends every guild/ban delta
// here — its own comment calls this node "source of truth for bans/guilds" — and this node understood
// nothing but entities, so `tryUnpackEntityTransfer` refused them and the loop moved on. Combined with
// `SocialState` being a plain local variable in that node, the consequence was concrete and bad: **a
// restart of the social node unbanned every account**, dissolved every guild and forgot every
// friendship. A ban that a moderator applied was undone by an operator restarting a process.
//
// What is stored and what is NOT, because the choice matters more than the code:
//   · guild membership (with rank), friendships, account state (ban deadline, reason, permissions);
//   · NOT parties — they are session-scoped by design, and pretending they survive is worse than
//     losing them;
//   · NOT SOCIAL_ZONE_UPDATE — that is routing, not state.
//
// One document per fact, upserted, so a delta never needs a read-modify-write and two of them cannot
// race into a lost update.
static void storeSocial(mongocxx::database& db, const DGS::SocialDelta& d)
{
    using bsoncxx::builder::stream::document;
    mongocxx::options::update up{};
    up.upsert(true);

    switch (d.kind)
    {
        case DGS::SOCIAL_GUILD_JOIN:
        case DGS::SOCIAL_GUILD_RANK:
        {
            auto col = db["guild_members"];
            document filter{}; filter << "guildId" << (int32_t)d.scopeUuid << "uuid" << (int32_t)d.targetUuid;
            document set{};    set    << "$set" << bsoncxx::builder::stream::open_document
                                      << "guildId" << (int32_t)d.scopeUuid
                                      << "uuid"    << (int32_t)d.targetUuid
                                      << "rank"    << (int32_t)d.rank
                                      << "updatedAtMs" << (int64_t)nowMs()
                                      << bsoncxx::builder::stream::close_document;
            col.update_one(filter.view(), set.view(), up);
            break;
        }
        case DGS::SOCIAL_GUILD_LEAVE:
        {
            document filter{}; filter << "guildId" << (int32_t)d.scopeUuid << "uuid" << (int32_t)d.targetUuid;
            db["guild_members"].delete_one(filter.view());
            break;
        }
        case DGS::SOCIAL_GUILD_DISBAND:
        {
            document filter{}; filter << "guildId" << (int32_t)d.scopeUuid;
            db["guild_members"].delete_many(filter.view());
            break;
        }
        case DGS::SOCIAL_FRIEND_ADD:
        {
            auto col = db["friends"];
            document filter{}; filter << "uuid" << (int32_t)d.targetUuid << "friendUuid" << (int32_t)d.scopeUuid;
            document set{};    set    << "$set" << bsoncxx::builder::stream::open_document
                                      << "uuid"       << (int32_t)d.targetUuid
                                      << "friendUuid" << (int32_t)d.scopeUuid
                                      << "updatedAtMs" << (int64_t)nowMs()
                                      << bsoncxx::builder::stream::close_document;
            col.update_one(filter.view(), set.view(), up);
            break;
        }
        case DGS::SOCIAL_FRIEND_REMOVE:
        {
            document filter{}; filter << "uuid" << (int32_t)d.targetUuid << "friendUuid" << (int32_t)d.scopeUuid;
            db["friends"].delete_one(filter.view());
            break;
        }
        default: break;   // parties and zone updates are not durable state
    }
}

static void storeAccount(mongocxx::database& db, const DGS::AccountAction& a)
{
    using bsoncxx::builder::stream::document;
    auto col = db["accounts"];
    document filter{}; filter << "uuid" << (int32_t)a.targetUuid;

    if (a.action == DGS::ACC_UNBAN)
    {
        // The ban is lifted, not the account forgotten: permissions live in the same document.
        document set{}; set << "$set" << bsoncxx::builder::stream::open_document
                            << "bannedUntilMs" << (int64_t)-1
                            << "reason" << ""
                            << "updatedAtMs" << (int64_t)nowMs()
                            << bsoncxx::builder::stream::close_document;
        mongocxx::options::update up{}; up.upsert(true);
        col.update_one(filter.view(), set.view(), up);
        return;
    }

    document set{};
    auto inner = set << "$set" << bsoncxx::builder::stream::open_document
                     << "uuid" << (int32_t)a.targetUuid
                     << "updatedAtMs" << (int64_t)nowMs();
    if (a.action == DGS::ACC_BAN)
    {
        // 0 seconds means PERMANENT on the wire; stored as 0 so "no deadline" cannot be confused with
        // "expired a long time ago".
        inner << "bannedUntilMs" << (int64_t)(a.durationS ? (int64_t)(nowMs() + (uint64_t)a.durationS * 1000) : 0)
              << "reason" << std::string(a.reason);
    }
    else if (a.action == DGS::ACC_SET_PERM)
    {
        inner << "permFlags" << (int32_t)a.permFlags;
    }
    inner << bsoncxx::builder::stream::close_document;

    mongocxx::options::update up{}; up.upsert(true);
    col.update_one(filter.view(), set.view(), up);
}

/// The whole durable social plane, as the packets that rebuild it.
static void answerSocialQuery(mongocxx::database& db, DGS::TCPSocket& sock, int fd, uint32_t hardCap)
{
    uint32_t sent = 0;
    auto push = [&](const DGS::Packet& p) { sock.send(fd, p.getRawData(), p.getSize()); ++sent; };

    auto num = [](const bsoncxx::document::view& v, const char* k) -> int64_t {
        auto it = v.find(k);
        if (it == v.end()) return 0;
        switch (it->type())
        {
            case bsoncxx::type::k_int32: return it->get_int32().value;
            case bsoncxx::type::k_int64: return it->get_int64().value;
            case bsoncxx::type::k_double: return (int64_t)it->get_double().value;
            default: return 0;
        }
    };

    // Guild membership: JOIN puts the member in, RANK restores the rank the JOIN cannot carry.
    for (auto&& doc : db["guild_members"].find({}))
    {
        if (sent + 2 > hardCap) break;
        DGS::SocialDelta d{};
        d.scopeUuid  = (uint32_t)num(doc, "guildId");
        d.targetUuid = (uint32_t)num(doc, "uuid");
        d.kind = DGS::SOCIAL_GUILD_JOIN;
        { DGS::Packet p; p.pack(d); push(p); }
        d.kind = DGS::SOCIAL_GUILD_RANK;
        d.rank = (uint8_t)num(doc, "rank");
        { DGS::Packet p; p.pack(d); push(p); }
    }

    for (auto&& doc : db["friends"].find({}))
    {
        if (sent + 1 > hardCap) break;
        DGS::SocialDelta d{};
        d.targetUuid = (uint32_t)num(doc, "uuid");
        d.scopeUuid  = (uint32_t)num(doc, "friendUuid");
        d.kind = DGS::SOCIAL_FRIEND_ADD;
        DGS::Packet p; p.pack(d); push(p);
    }

    const int64_t now = (int64_t)nowMs();
    for (auto&& doc : db["accounts"].find({}))
    {
        if (sent + 1 > hardCap) break;
        DGS::AccountAction a{};
        a.targetUuid = (uint32_t)num(doc, "uuid");

        const int64_t until = num(doc, "bannedUntilMs");
        const bool hasBan = doc.find("bannedUntilMs") != doc.end() && until >= 0
                            && (until == 0 || until > now);
        if (hasBan)
        {
            a.action    = DGS::ACC_BAN;
            // Back to a DURATION, because that is what the wire carries. A ban with a deadline that has
            // already passed is not replayed at all: restoring it would re-ban somebody the clock had
            // already released.
            a.durationS = (until == 0) ? 0u : (uint32_t)((until - now) / 1000 + 1);
            auto r = doc.find("reason");
            if (r != doc.end() && r->type() == bsoncxx::type::k_utf8)
                std::snprintf(a.reason, sizeof(a.reason), "%s", std::string(r->get_string().value).c_str());
            DGS::Packet p; p.pack(a); push(p);
        }
        else if (doc.find("permFlags") != doc.end())
        {
            a.action    = DGS::ACC_SET_PERM;
            a.permFlags = (uint32_t)num(doc, "permFlags");
            DGS::Packet p; p.pack(a); push(p);
        }
    }

    DGS::Packet end; end.pack(DGS::PKT_NONE);
    sock.send(fd, end.getRawData(), end.getSize());
    std::cout << "[Persistence] social query -> " << sent << " records" << std::endl;
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
    mongocxx::instance instance{};
    const char* mongoUri = std::getenv("MONGO_URI") ? std::getenv("MONGO_URI") : "mongodb://mongodb:27017";
    mongocxx::client   client{mongocxx::uri{mongoUri}};

    // The database name is configurable so a test (or a second deployment) can work against its own
    // data instead of the operator's. It used to be hardcoded, which made an end-to-end test either
    // impossible or destructive.
    const char* mongoDb = std::getenv("MONGO_DB") ? std::getenv("MONGO_DB") : "dgs_persistance";
    auto db       = client[mongoDb];
    auto entities = db["entities"];

    DGS::Logger logger("persistence_log.csv");
    DGS::TCPSocket tcpSocket;

    const int persPort = std::atoi(std::getenv("PERSISTENCE_PORT") ? std::getenv("PERSISTENCE_PORT") : "42429");
    if (!tcpSocket.listen(persPort))
    {
        std::cerr << "[Persistence] Failed to listen on 42429" << std::endl;
        return 1;
    }

        // It does NOT say "connected": `mongocxx::client` is LAZY and has not talked to the database
    // yet. The previous message claimed a connection that does not exist, which misleads anyone
    // diagnosing an outage.
    std::cout << "[Persistence] Listening on TCP:" << persPort
              << " (Mongo: " << mongoUri << " db=" << mongoDb << ", not verified yet)" << std::endl;

    // The lookup index. The collection had only `_id_`, so finding an entity by uuid was a full scan —
    // fine with six documents, not with a world's worth. UNIQUE, because after the change to an upsert
    // one entity means one document, and letting a duplicate exist would silently reintroduce the
    // ambiguity of "which of these is the current state". Attempted, not required: it is the first
    // thing that touches the database, and if it is down that is not a reason to refuse to start.
    try {
        mongocxx::options::index uniq{};
        uniq.unique(true);
        entities.create_index(bsoncxx::builder::stream::document{} << "uuid" << 1
                              << bsoncxx::builder::stream::finalize, uniq);
        std::cout << "[Persistence] unique index on uuid ready" << std::endl;

        mongocxx::options::index uniq2{};
        uniq2.unique(true);
        db["guild_members"].create_index(bsoncxx::builder::stream::document{}
                                         << "guildId" << 1 << "uuid" << 1
                                         << bsoncxx::builder::stream::finalize, uniq2);
        db["friends"].create_index(bsoncxx::builder::stream::document{}
                                   << "uuid" << 1 << "friendUuid" << 1
                                   << bsoncxx::builder::stream::finalize, uniq2);
        db["accounts"].create_index(bsoncxx::builder::stream::document{}
                                    << "uuid" << 1
                                    << bsoncxx::builder::stream::finalize, uniq2);
        std::cout << "[Persistence] social indexes ready" << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "[Persistence] could not create the uuid index (" << ex.what()
                  << "); lookups will be a collection scan" << std::endl;
    }

    DGS::AuthGate gate("Persistence");
    gate.announce();

    int epollFD = epoll_create1(0);
    epoll_event ev;
    ev.events  = EPOLLIN;
    ev.data.fd = tcpSocket.getSocketFD();
    epoll_ctl(epollFD, EPOLL_CTL_ADD, tcpSocket.getSocketFD(), &ev);

    epoll_event events[64];
    std::set<int> clientFDs;

    // ONE `recv` IS ONE PACKET on this link, and that is a property of the transport, not luck:
    // `TCPSocket::send` writes a 4-byte `htonl` length prefix and `TCPSocket::receive` reads exactly
    // one message with `recvAll`. So a multi-packet answer (the region query below) is simply N sends
    // read by N receives, and nothing needs to reassemble anything.
    //
    // I briefly added a SECOND layer of framing here on the strength of a measurement that turned out
    // to be an artefact of the probe: it concatenated two packets into a single `send`, which the
    // socket layer then framed as ONE message containing two packets — so the reader correctly saw one
    // message and decoded the first. No node ever writes two packets in one send. Five packets sent
    // one at a time all arrived, which was the number that actually mattered.

    while (true)
    {
        int n = epoll_wait(epollFD, events, 64, -1);
        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;

            if (fd == tcpSocket.getSocketFD())
            {
                int newFD = tcpSocket.accept();
                if (newFD < 0) continue;
                clientFDs.insert(newFD);
                ev.data.fd = newFD;
                epoll_ctl(epollFD, EPOLL_CTL_ADD, newFD, &ev);
                std::cout << "[Persistence] Validator connected FD=" << newFD << std::endl;
            }
            else if (clientFDs.count(fd))
            {
                // An epoll wake-up is not proof of application data once TLS is on: the handshake's
                // trailing records (a TLS 1.3 `NewSessionTicket`, say) wake it too, and the blocking
                // read below would then wait for a message nobody sent. See `TCPSocket::pending`.
                if (tcpSocket.tlsEnabled() && !tcpSocket.pending(fd)) continue;

                uint8_t buffer[8192];
                int bytes = tcpSocket.receive(fd, buffer, sizeof(buffer));
                if (bytes <= 0)
                {
                    epoll_ctl(epollFD, EPOLL_CTL_DEL, fd, nullptr);
                    tcpSocket.closeClient(fd);
                    clientFDs.erase(fd);
                    gate.forget(fd);
                    continue;
                }

                DGS::Packet p;
                p.setBuffer(buffer, bytes);

                // NODE-ONLY PORT. Nothing here belongs to a player: it stores and returns the world's
                // state, so an unauthenticated stranger writing to it (or reading it back) is not a
                // scenario worth supporting.
                if (gate.consume(fd, p)) continue;
                if (!gate.allows(fd)) { gate.refuse(fd, (int)p.getType()); continue; }

                // A READ. Answer with the stored state, or a bare PKT_NONE when there is nothing for
                // that uuid — the caller has to be able to tell "never seen" from "seen, and empty".
                if (p.getType() == DGS::PKT_PERSIST_QUERY)
                {
                    uint32_t want = 0;
                    try { want = p.unpackPersistQuery(); }
                    catch (const std::exception&) { continue; }

                    DGS::EntityTransfer found{};
                    bool have = false;
                    try {
                        have = loadEntity(entities, want, found);
                    } catch (const std::exception& ex) {
                        // Same rule as the write path: a database that is not answering degrades the
                        // service, it does not take the node down.
                        std::cerr << "[Persistence] FAILED to load uuid=" << want
                                  << ": " << ex.what() << std::endl;
                    }

                    DGS::Packet resp;
                    if (have) resp.pack(found);
                    else      resp.pack(DGS::PKT_NONE);
                    tcpSocket.send(fd, resp.getRawData(), resp.getSize());
                    std::cout << "[Persistence] query uuid=" << want
                              << (have ? " -> found" : " -> not stored") << std::endl;
                    continue;
                }

                // A REGION. What a zone asks at start-up: everything you have inside these chunks. The
                // answer is a STREAM — one packet per entity, then a bare PKT_NONE to say it has ended.
                // The terminator is not decoration: without it "no entities stored" and "the answer has
                // not arrived yet" are the same silence, and a zone would have to guess with a timeout.
                if (p.getType() == DGS::PKT_PERSIST_RANGE)
                {
                    DGS::PersistRange r{};
                    try { r = p.unpackPersistRange(); }
                    catch (const std::exception&) { continue; }

                    std::vector<DGS::EntityTransfer> found;
                    try {
                        found = loadRange(entities, r, RANGE_HARD_CAP);
                    } catch (const std::exception& ex) {
                        std::cerr << "[Persistence] FAILED range query: " << ex.what() << std::endl;
                    }

                    for (const auto& fe : found)
                    {
                        DGS::Packet ep; ep.pack(fe);
                        tcpSocket.send(fd, ep.getRawData(), ep.getSize());
                    }
                    DGS::Packet end; end.pack(DGS::PKT_NONE);
                    tcpSocket.send(fd, end.getRawData(), end.getSize());

                    std::cout << "[Persistence] range x[" << r.chunkXMin << "," << r.chunkXMax
                              << "] y[" << r.chunkYMin << "," << r.chunkYMax
                              << "] z[" << r.chunkZMin << "," << r.chunkZMax
                              << "] -> " << found.size() << " entities" << std::endl;
                    continue;
                }

                // The SOCIAL plane. These used to arrive here and be silently discarded, because this
                // node only understood entities — while the social node believed it was writing
                // through and its own state was a plain local variable. A restart unbanned everyone.
                if (p.getType() == DGS::PKT_SOCIAL_DELTA)
                {
                    try {
                        const DGS::SocialDelta d = p.unpackSocialDelta();
                        storeSocial(db, d);
                        std::cout << "[Persistence] social delta kind=" << (int)d.kind
                                  << " target=" << d.targetUuid << " scope=" << d.scopeUuid << std::endl;
                    } catch (const std::exception& ex) {
                        std::cerr << "[Persistence] FAILED to store social delta: " << ex.what() << std::endl;
                    }
                    continue;
                }
                if (p.getType() == DGS::PKT_ACCOUNT)
                {
                    try {
                        const DGS::AccountAction a = p.unpackAccountAction();
                        storeAccount(db, a);
                        std::cout << "[Persistence] account action=" << (int)a.action
                                  << " target=" << a.targetUuid << std::endl;
                    } catch (const std::exception& ex) {
                        std::cerr << "[Persistence] FAILED to store account action: " << ex.what() << std::endl;
                    }
                    continue;
                }
                if (p.getType() == DGS::PKT_SOCIAL_QUERY)
                {
                    try { answerSocialQuery(db, tcpSocket, fd, SOCIAL_HARD_CAP); }
                    catch (const std::exception& ex) {
                        std::cerr << "[Persistence] FAILED social query: " << ex.what() << std::endl;
                        DGS::Packet end; end.pack(DGS::PKT_NONE);
                        tcpSocket.send(fd, end.getRawData(), end.getSize());
                    }
                    continue;
                }

                DGS::EntityTransfer e{};
                if (!p.tryUnpackEntityTransfer(e)) continue;   // malformed: drop it, stay up

                // ⚠️ WITH `try`. The Mongo call THROWS when the database does not answer, and nothing
                // caught it here: an uncaught exception is `std::terminate`, which means **a MongoDB
                // outage killed the entire persistence node with the first entity that arrived**. A
                // database failure has to degrade the service, not take it down: the node keeps
                // accepting and serving, and the loss is recorded.
                bool stored = true;
                try {
                    storeEntity(entities, e);
                } catch (const std::exception& ex) {
                    stored = false;
                    std::cerr << "[Persistence] FAILED to store uuid=" << e.uuid
                              << ": " << ex.what() << std::endl;
                }

                DGS::LogEntry entry{};
                entry.time_stamp  = (uint64_t)std::time(nullptr);
                entry.type        = DGS::LOG_TRANSFER;
                entry.entityType  = e.type;
                entry.uuid        = e.uuid;
                entry.fd          = fd;
                entry.bytes       = (uint32_t)bytes;

                logger.log(entry);

                std::cout << "[Persistence] Entity " << (stored ? "stored" : "LOST")
                          << " uuid=" << e.uuid << std::endl;
            }
        }
    }

    return 0;
}
