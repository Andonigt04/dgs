#include "include/dgs/client.h"

#include <chrono>
#include <cmath>
#include <httplib.h>
#include <iostream>
#include <cstring>
#include <sys/socket.h>

namespace DGS
{
    /// Pulls one JSON string field out of a small, known body. NOT a JSON parser, deliberately: the
    /// body is what `tools/fake_login_api` documents, and a parser would be a dependency and an attack
    /// surface for a value the client is free to ignore.
    static std::string jsonString(const std::string& body, const char* field)
    {
        const std::string key = std::string("\"") + field + "\":\"";
        const size_t a = body.find(key);
        if (a == std::string::npos) return "";
        const size_t b = body.find('"', a + key.size());
        if (b == std::string::npos) return "";
        return body.substr(a + key.size(), b - a - key.size());
    }

    static uint32_t jsonNumber(const std::string& body, const char* field)
    {
        const std::string key = std::string("\"") + field + "\":";
        const size_t a = body.find(key);
        if (a == std::string::npos) return 0;
        return (uint32_t)std::strtoul(body.c_str() + a + key.size(), nullptr, 10);
    }

    /// The same, for numbers that do not fit in 32 bits or are not integers. A millisecond epoch is
    /// both: `jsonNumber` would have truncated it to nonsense and the truncation would have looked
    /// like a working clock that was simply wrong.
    static double jsonDouble(const std::string& body, const char* field, bool* found = nullptr)
    {
        if (found) *found = false;
        const std::string key = std::string("\"") + field + "\":";
        const size_t a = body.find(key);
        if (a == std::string::npos) return 0.0;
        if (found) *found = true;
        return std::strtod(body.c_str() + a + key.size(), nullptr);
    }

    /// Adopts whatever keys the login handed out. A client needs TWO to speak an encrypted game plane:
    /// the GROUP key, because the zone seals its broadcast once for everybody and the client has to be
    /// able to open it, and its OWN session key, so what it says about itself is not readable by the
    /// player next to it. Either may be absent; both are optional.
    void Client::applyLoginKeys(const std::string& body)
    {
        const std::string group = jsonString(body, "groupKey");
        if (!group.empty() && !setUdpSessionKey(0, group))
            std::cerr << "[Client] login groupKey is not 64 hex characters -> ignored" << std::endl;

        const std::string sh = jsonString(body, "socialHost");
        const uint32_t    sp = jsonNumber(body, "socialPort");
        if (!sh.empty() && sp != 0) { m_socialHost = sh; m_socialPort = (int)sp; }

        // ⚠️ THE WORLD CLOCK, and until this existed every client had its own. `m_simulationTime` in
        // the engine starts at 0 and accumulates the local frame `dt`, and EVERYTHING the world does
        // with time is an analytic function of it — orbits (`orbitPositionAt(orbit, t)`), therefore
        // the planet's rotation, therefore day and night, and also the weather fronts and the tide.
        // So two players who started the game five minutes apart were in two different worlds: one at
        // noon and one at midnight, watching different storms.
        //
        // It does not need replicating tick by tick, precisely BECAUSE it is analytic: one anchor is
        // enough. The login hands out the epoch, the rate, and its own clock reading; from there each
        // client advances the same number at the same rate from the same origin.
        //
        // The elapsed part is measured with a STEADY clock, not the wall clock, so the world does not
        // jump when the machine syncs its time or the user changes the timezone. The client's wall
        // clock is not trusted for anything: only the server's reading is, taken once here.
        //
        // What this does NOT correct is the request's flight time: the anchor is off by roughly half
        // the round trip. At a scale of 1 that is milliseconds against a day, so it is left alone —
        // but it is the reason this is an anchor and not a synchronisation protocol.
        bool haveEpoch = false;
        const double epochMs = jsonDouble(body, "worldEpochMs", &haveEpoch);
        const double nowMs   = jsonDouble(body, "serverNowMs");
        if (haveEpoch && nowMs > 0.0)
        {
            bool haveScale = false;
            const double scale = jsonDouble(body, "worldTimeScale", &haveScale);
            m_worldScale   = (haveScale && scale > 0.0) ? scale : 1.0;
            m_worldAtLogin = jsonDouble(body, "worldStartSeconds")
                           + ((nowMs - epochMs) / 1000.0) * m_worldScale;
            m_worldAnchor  = std::chrono::steady_clock::now();
            m_haveWorldTime = true;
            std::cout << "[Client] world clock: t=" << m_worldAtLogin << " s (rate x"
                      << m_worldScale << ") from the login" << std::endl;
        }

        const uint32_t    session = jsonNumber(body, "session");
        const std::string key     = jsonString(body, "udpKey");

        // ⚠️ QUIEN ERES, y hasta ahora nadie lo preguntaba. El juego usaba una constante
        // (`PLAYER_UUID = 1`) para identificarse, asi que DOS clientes eran la misma entidad: se
        // pisaban la posicion en la zona, compartian el limite de ritmo del chat, y —lo peor— cada uno
        // descartaba al otro como si fuera su propio eco, porque el filtro de "ese soy yo" compara
        // contra ese mismo numero. Dos jugadores conectados no podian verse por construccion.
        //
        // El login ya reparte un numero distinto por sesion; esto solo lo guarda para que alguien
        // pueda usarlo. NO es una identidad de cuenta: cambia en cada login, asi que lo que persista
        // con este uuid no se reconoce en la siguiente sesion. Eso lo arregla un login de verdad
        // devolviendo un id de cuenta estable, no este campo.
        if (session != 0) m_session = session;

        if (session != 0 && !key.empty())
        {
            if (setUdpSessionKey(session, key))
                std::cout << "[Client] UDP session " << session << " issued by the login" << std::endl;
            else
                std::cerr << "[Client] login udpKey is not 64 hex characters -> group key only"
                          << std::endl;
        }
    }

    bool Client::connect(const std::string& headHost, int headPort, const std::string& username, const std::string& password, const std::string& apiHost,  int apiPort)
    {
        httplib::Client api(apiHost + ":" + std::to_string(apiPort));
        std::string body = R"({"email":")" + username + R"(","password":")" + password + R"("})";
        auto res = api.Post("/api/auth/login", body, "application/json");

        if (!res || res->status != 200)
        {
            std::cerr << "[Client] Login failed" << std::endl;
            return false;
        }
        std::cout << "[Client] Login OK" << std::endl;

        // ⚠️ THE ANSWER USED TO BE THROWN AWAY. This checked the status code and nothing else, so the
        // one place a player can be handed a key — the only authenticated exchange in their whole
        // session — carried nothing. That is why per-session UDP keys were "the mechanism works, the
        // plumbing does not exist": the plumbing is these ten lines.
        //
        // A missing or malformed field is not an error: the client falls back to the group key from the
        // environment, which is what it did before and still works. It is a login handing out keys, not
        // a login demanding them.
        applyLoginKeys(res->body);

        if (!m_tcp.connect(headHost, headPort))
        {
            std::cerr << "[Client] Failed to connect to HeadServer" << std::endl;
            return false;
        }

        struct timeval tv { 3, 0 };
        setsockopt(m_tcp.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        // ⚠️ THE PROBE IS NOT A REQUIREMENT, and treating it as one broke every cluster whose world
        // does not happen to cover the origin. This asked the head about chunk (0,0,0) — where the
        // player is NOT, because they have not sent a transform yet — and refused to connect when
        // nobody owned it. Measured against a demo cluster aimed at the game's real spawn chunk
        // (146089): login OK, world clock OK, "DGS connect -> FAILED", and a player standing in a
        // world that was perfectly willing to serve them.
        //
        // It became fatal when `applyZone` started refusing empty responses, which was the right fix
        // for a different problem (the client used to store addr="" port=0 and send its position
        // nowhere). The two are separable: a transport failure — no answer at all — is fatal here,
        // and "the head answered: nobody covers chunk 0" is simply not this function's business.
        // `hasZone()` is what a caller asks, and `sendTransform` re-queries the moment the player's
        // real chunk is known.
        if (!queryZone(0, 0, 0, 0) && !m_headAnswered)
        {
            std::cerr << "[Client] the head did not answer the first zone query" << std::endl;
            return false;
        }

        // Mark chunk (0,0,0) as known so sendTransform doesn't re-query immediately.
        m_lastChunkX = 0;
        m_lastChunkY = 0;
        m_lastChunkZ = 0;

        // ── The chat link ───────────────────────────────────────────────────────────────────────
        // Best effort, deliberately: a cluster with no social node still plays, it just has no chat.
        // Making the whole connection fail over it would trade a missing feature for a missing game.
        if (m_socialHost.empty())
        {
            const char* h = std::getenv("SOCIAL_HOST");
            m_socialHost = (h && *h) ? h : headHost;
        }
        if (m_socialPort == 0)
        {
            const char* pt = std::getenv("SOCIAL_TCP_PORT");
            m_socialPort = (pt && *pt) ? std::atoi(pt) : 42430;
        }
        m_socialUp = m_social.connect(m_socialHost, m_socialPort, 2000);
        if (m_socialUp)
        {
            struct timeval stv { 0, 200000 };
            setsockopt(m_social.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &stv, sizeof(stv));
            std::cout << "[Client] chat via social node " << m_socialHost << ":" << m_socialPort
                      << std::endl;
        }
        else
            std::cerr << "[Client] no social node at " << m_socialHost << ":" << m_socialPort
                      << " -> no chat (everything else works)" << std::endl;

        // The zone answers on the socket we send from, so it needs a deadline for the same reason the
        // TCP one does: the loop has to come up for air and notice `m_running` went false.
        { struct timeval utv { 0, 200000 };
          setsockopt(m_udp.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &utv, sizeof(utv)); }

        m_running = true;
        m_recvThread = std::thread(&Client::recvLoop, this);
        m_udpThread  = std::thread(&Client::udpLoop,  this);
        if (m_socialUp) m_socialThread = std::thread(&Client::socialLoop, this);

        return true;
    }

    void Client::disconnect()
    {
        m_running = false;
        if (m_recvThread.joinable())
            m_recvThread.join();
        if (m_udpThread.joinable())
            m_udpThread.join();
        if (m_socialThread.joinable())
            m_socialThread.join();
    }

    /// @return whether the head actually named a zone.
    ///
    /// ⚠️ AN EMPTY ANSWER USED TO BE ACCEPTED AS AN ANSWER. When no zone covers a chunk the head
    /// replies with an all-zero `ZoneResponse`, and this stored it: address "", port 0. From that
    /// moment the client sent its position to nowhere, in complete silence, and the only trace was one
    /// line reading `[Client] ZoneNode: :0` in the middle of a start-up log.
    ///
    /// It is not a hypothetical: it is what a real game client does the first time it spawns somewhere
    /// the cluster does not cover — a player on the surface of a planet 146 000 chunks from the origin,
    /// against a demo cluster covering chunks 0 to 7. Keeping the previous zone is the better failure:
    /// the player goes on talking to whoever owned them, which is wrong but visible, instead of
    /// vanishing.
    bool Client::applyZone(const ZoneResponse& zone)
    {
        // The head spoke, whatever it said. `connect` needs to tell "no answer" (a dead head) from
        // "no zone there" (a perfectly healthy head answering about empty space).
        m_headAnswered = true;

        if (zone.port <= 0 || zone.addr[0] == '\0')
        {
            std::cerr << "[Client] the head has NO ZONE for that chunk -> keeping "
                      << (m_zonePort ? m_zoneAddr + ":" + std::to_string(m_zonePort) : std::string("none"))
                      << " (is a zone_node covering where you are?)" << std::endl;
            return false;
        }

        // ⚠️ The `strncpy(&m_zoneAddr[0], zone.addr, 16)` that used to be here wrote 16 bytes into a
        // `std::string` whose buffer held 9 ("127.0.0.1") — straight past its capacity, undefined
        // behaviour, and pointless: the very next line assigned the value properly anyway.
        m_zoneAddr = zone.addr;
        m_zonePort = zone.port;
        std::cout << "[Client] ZoneNode: " << m_zoneAddr << ":" << m_zonePort << std::endl;
        return true;
    }

    bool Client::queryZone(uint32_t uuid, int32_t chunkX, int32_t chunkY, int32_t chunkZ)
    {
        ZoneQuery query{};
        query.uuid   = uuid;
        query.chunkX = chunkX;
        query.chunkY = chunkY;
        query.chunkZ = chunkZ;

        Packet qp;
        qp.pack(query);

        // With the receive thread running, IT owns the descriptor: ask, then wait for it to hand the
        // answer over. Reading here as well is what used to lose the response (see client.h).
        if (m_running.load())
        {
            std::unique_lock<std::mutex> lk(m_mtx);
            m_zoneAnswered = false;
            m_tcp.send(m_tcp.getSocketFD(), qp.getRawData(), qp.getSize());

            if (!m_zoneCv.wait_for(lk, std::chrono::seconds(3), [this] { return m_zoneAnswered; }))
            {
                std::cerr << "[Client] No ZoneResponse received" << std::endl;
                return false;
            }
            const ZoneResponse zone = m_zoneResp;
            lk.unlock();
            return applyZone(zone);
        }

        // Before the receive thread exists (during `connect`) this thread is the only reader.
        m_tcp.send(m_tcp.getSocketFD(), qp.getRawData(), qp.getSize());

        uint8_t buf[256];
        int bytes = m_tcp.receive(m_tcp.getSocketFD(), buf, sizeof(buf));
        if (bytes <= 0)
        {
            std::cerr << "[Client] No ZoneResponse received" << std::endl;
            return false;
        }

        Packet rp;
        rp.setBuffer(buf, bytes);
        return applyZone(rp.unpackZoneResponse());
    }

    void Client::sendEntityUDP(const EntityTransfer& e)
    {
        // ⚠️ THIS USED TO memcpy THE WHOLE STRUCT: 4160 bytes per update, at 20 Hz, 83 KB/s of UPLOAD
        // per player — of which 4096 bytes were the `data[]` blob, empty for a player who is only
        // moving. `Packet::pack` honours `dataSize` and tags the datagram with its type, which is also
        // what lets the zone recognise it without comparing sizes.
        Packet p;
        p.pack(e);
        m_udp.send(m_zoneAddr, m_zonePort, p.getRawData(), p.getSize());
    }

    /// Milliseconds a crossing player keeps reporting to the zone it is LEAVING. Six datagrams at
    /// 20 Hz, so losing one to UDP does not lose the handoff.
    static uint64_t announceMs()
    {
        const char* v = std::getenv("ZONE_ANNOUNCE_MS");
        const int ms = v ? std::atoi(v) : 0;
        return (uint64_t)(ms > 0 ? ms : 300);
    }

    static uint64_t nowMs()
    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    }

    void Client::sendState(uint32_t uuid, int32_t chunkX, int32_t chunkY, int32_t chunkZ,
                          const float pos[3], const float rot[4],
                          const uint8_t* data, uint16_t size)
    {
        // ⚠️ THIS USED TO MAKE THE AUTHORITY HANDOFF UNREACHABLE, and the handoff is the whole point of
        // running more than one zone. It re-queried the head the instant the chunk changed and then
        // sent to the NEW zone — so the zone that OWNED the player never heard that it had left.
        //
        // And the owner is the only one who can start a handoff: `zone_node`'s `checkAndTransfer`
        // walks the entities it owns and cedes the ones whose chunk has left its bounds. With nobody
        // telling it, the player simply stopped reporting and the lease GC dropped them a few seconds
        // later — no REASSIGN, no ghost promotion, no lease released, and above all NO SERVER-SIDE
        // STATE TRANSFERRED: the new zone rebuilt the entity from the client's own datagram, which
        // makes the client the source of truth for its own stats at every border it crosses.
        //
        // Measured with the real client, a real head and two real zone_nodes, walking uuid 4242 from
        // chunk 0 (zone A) into chunk 1 (zone B): "out of bounds. Transferring" x0, "handoff ACKED" x0,
        // "promoted to real" x0. The path was live, tested by `handoff_e2e`, and reached by nothing —
        // because that test announces the crossing to the old zone, which is what the client did not do.
        //
        // So: announce first. Keep reporting the NEW chunk to the CURRENT owner for a moment, let it
        // notice and hand us over, and only then ask the head where to send next. Authority moves when
        // the servers agree it has, not when the client decides.
        const bool crossing = chunkX != m_lastChunkX || chunkY != m_lastChunkY || chunkZ != m_lastChunkZ;

        if (crossing && m_announceUntilMs == 0 && m_zonePort != 0)
            m_announceUntilMs = nowMs() + announceMs();

        if (crossing && (m_announceUntilMs == 0 || nowMs() >= m_announceUntilMs))
        {
            m_announceUntilMs = 0;
            // ⚠️ ONLY cache the chunk if the query actually SUCCEEDED. It used to be cached either way,
            // so a failed query was remembered as done: the client kept the previous zone's address and
            // never asked again for that chunk. A player crossing a border went on talking to the zone
            // they had just left, permanently — invisible to the one that now owned them.
            if (!queryZone(uuid, chunkX, chunkY, chunkZ)) return;
            m_lastChunkX = chunkX;
            m_lastChunkY = chunkY;
            m_lastChunkZ = chunkZ;
        }

        EntityTransfer e{};
        e.uuid   = uuid;
        // Ni `type` ni `stats`: no los decide el cliente. La zona pone que es un jugador y con que
        // techo de velocidad se le juzga, porque preguntarselo al sospechoso no es un anti-trampas
        // (ver `client_claims_e2e`). Lo que mande aqui daria igual; no mandarlo hace obvio por que.
        e.chunkX = chunkX;
        e.chunkY = chunkY;
        e.chunkZ = chunkZ;
        e.pos[0] = pos[0];
        e.pos[1] = pos[1];
        e.pos[2] = pos[2];
        
        // Quaternion yaw → uint16 angle
        float yaw  = std::atan2(2.0f * (rot[3] * rot[1]), 1.0f - 2.0f * (rot[1] * rot[1]));
        e.angle    = static_cast<uint16_t>((yaw + 3.14159265f) * (32767.5f / 3.14159265f));

        // El payload del juego, si hay algo nuevo que decir. `size = 0` es "sin cambios", no "vacio":
        // es lo que permite que un inventario sobreviva a veinte transformadas por segundo sin viajar
        // en cada una (la zona lo conserva; ver la nota larga de `dataSize` en `zone_node`).
        e.dataSize = std::min<uint16_t>(size, (uint16_t)sizeof(e.data));
        if (e.dataSize && data) std::memcpy(e.data, data, e.dataSize);

        sendEntityUDP(e);
    }



    uint32_t Client::sendAction(uint32_t actor, uint16_t action, uint32_t target,
                                int32_t chunkX, int32_t chunkY, int32_t chunkZ,
                                const float pos[3], uint16_t angle,
                                const uint8_t* data, uint16_t size)
    {
        // ⚠️ SIN ZONA NO HAY A QUIEN PEDIRSELO. Una transformada se repite veinte veces por segundo y
        // perder las primeras no se nota; una peticion se manda UNA vez. Enviada antes de que el head
        // diga a que zona, saldria al puerto 0 y la accion no habria ocurrido para nadie, en silencio.
        if (m_zonePort == 0)
        {
            std::cerr << "[Client] accion descartada: todavia no hay zona (mueve al jugador primero)"
                      << std::endl;
            return 0;
        }

        ActionRequest a{};
        a.requestId = m_nextActionId++;
        a.actor  = actor;
        a.action = action;
        a.target = target;
        a.chunkX = chunkX; a.chunkY = chunkY; a.chunkZ = chunkZ;
        a.pos[0] = pos[0]; a.pos[1] = pos[1]; a.pos[2] = pos[2];
        a.angle  = angle;
        a.dataSize = std::min<uint16_t>(size, (uint16_t)MAX_ENTITY_DATA);
        if (a.dataSize) std::memcpy(a.data, data, a.dataSize);

        Packet p; p.pack(a);
        // Una accion es un evento RARO —tirar, colocar, romper— asi que registrarlas todas no cuesta
        // nada y quita la peor ambiguedad que tiene este camino: "el objeto no aparece" no distinguia
        // entre no haberse enviado, no haber llegado y haber sido rechazado. Si el envio falla hay
        // que decirlo: es la unica peticion que se manda UNA vez.
        const bool ok = m_udp.send(m_zoneAddr, m_zonePort, p.getRawData(), p.getSize());
        std::cerr << "[Client] accion " << (int)action << " req=" << a.requestId
                  << " actor=" << actor << " -> " << m_zoneAddr << ":" << m_zonePort
                  << " (" << p.getSize() << " bytes)" << (ok ? "" : "  ENVIO FALLIDO") << std::endl;
        if (!ok) return 0;
        return a.requestId;
    }

    std::vector<ActionAck> Client::pollActionResults()
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        return std::move(m_actionAcks);
    }


    std::vector<EntityTransfer> Client::pollEntities()
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        return std::move(m_incomingEntities);
    }

    std::vector<GhostDelta> Client::pollGhosts()
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        return std::move(m_incomingGhosts);
    }

    std::vector<ChatMessage> Client::pollChats()
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        return std::move(m_incomingChats);
    }

    void Client::sendChat(uint32_t uuid, const std::string& username, const std::string& text,
                          uint8_t channel)
    {
        ChatMessage msg{};
        msg.uuid = uuid;
        msg.channel = channel;
        std::strncpy(msg.username, username.c_str(), sizeof(msg.username) - 1);
        std::strncpy(msg.text,     text.c_str(),     sizeof(msg.text)     - 1);

        Packet p;
        p.pack(msg);

        // Proximity chat is a fact about the world, not about the social graph: it goes down the same
        // link as the player's position, to the zone that owns them, and comes back filtered by the
        // same interest radius as everything else on that plane.
        if (channel == CHAT_LOCAL)
        {
            if (m_zonePort != 0)
                m_udp.send(m_zoneAddr, m_zonePort, p.getRawData(), p.getSize());
            return;
        }

        // ⚠️ NOT TO THE HEAD. It used to, and the head answered by broadcasting the message to every
        // connection it had — see the note on `m_social`. The social node is where the channels, the
        // per-uuid rate limit and the ban list live, and it was the one thing in this system nothing
        // was talking to.
        if (!m_socialUp) return;
        m_social.send(m_social.getSocketFD(), p.getRawData(), p.getSize());
    }

    /// Reads the chat the social node fans out. Its own thread for the same reason the world has one:
    /// three sockets that block independently, and a quiet one must not hold up a busy one.
    void Client::socialLoop()
    {
        uint8_t buf[8192];
        while (m_running)
        {
            const int bytes = m_social.receive(m_social.getSocketFD(), buf, sizeof(buf));
            if (bytes <= 0) continue;   // the 200 ms deadline, or the node hung up

            Packet p;
            p.setBuffer(buf, (size_t)bytes);
            if (p.getType() != PKT_CHAT) continue;   // guild/party deltas are not this client's business

            std::lock_guard<std::mutex> lk(m_mtx);
            m_incomingChats.push_back(p.unpackChatMessage());
        }
    }

    /// How many un-polled updates are kept before new ones are dropped.
    ///
    /// ⚠️ A BOUND, BECAUSE THIS QUEUE IS FED BY THE WORLD AND DRAINED BY THE GAME. The zone broadcasts
    /// every entity near you ten times a second; a caller that stops polling — paused, loading, stuck
    /// on a frame — would otherwise grow this without limit at the rate of the whole neighbourhood.
    /// Dropping the NEWEST is the right end to drop from here: what is already queued is older and the
    /// game is about to overwrite it with whatever comes next anyway.
    static const size_t kMaxQueued = 4096;

    /// Reads the ZONE's broadcast. The other half of the client, and it did not exist.
    ///
    /// ⚠️ TWO SOCKETS, TWO PLANES, and only one of them was being read. The TCP link to the head
    /// carries control — which zone covers your chunk, chat — and the UDP link to the ZONE carries the
    /// world: every entity near you, ten times a second, sealed if the game plane is encrypted
    /// (`UDPSocket::receive` unseals and drops anything that fails to authenticate, so a datagram that
    /// arrives here has already proved it came from someone holding the key).
    ///
    /// It is a separate thread from `recvLoop` because the two sockets block independently: sharing one
    /// thread would mean a quiet head delays the world, or a quiet world delays a zone response.
    void Client::udpLoop()
    {
        // The repository's convention: receive with at least MAX_PACKET_SIZE and treat a full buffer as
        // a truncated datagram (`receivedWasTruncated`), rather than parsing a cut-off packet.
        std::vector<uint8_t> buf(MAX_PACKET_SIZE);
        std::string from;
        int port = 0;

        while (m_running)
        {
            const int n = m_udp.receive(buf.data(), buf.size(), from, port);
            // <= 0 is the ordinary case here: the 200 ms deadline expiring, or a datagram that failed
            // its tag. Neither is worth a log line ten times a second.
            if (n <= 0) continue;
            if (receivedWasTruncated(n, buf.size())) continue;

            Packet p;
            p.setBuffer(buf.data(), (size_t)n);

            std::lock_guard<std::mutex> lk(m_mtx);
            switch (p.getType())
            {
                case PKT_ENTITY_TRANSFER:
                {
                    // A malformed datagram costs the datagram, not the game — the same guard `recvLoop`
                    // needs, and more so here: this one is fed by the network at ten times the rate.
                    EntityTransfer inc{};
                    if (p.tryUnpackEntityTransfer(inc) && m_incomingEntities.size() < kMaxQueued)
                        m_incomingEntities.push_back(inc);
                    break;
                }
                case PKT_GHOST_DELTA:
                {
                    if (m_incomingGhosts.size() >= kMaxQueued) break;
                    try { m_incomingGhosts.push_back(p.unpackGhostDelta()); }
                    catch (const std::exception&) { }   // unpackGhostDelta throws; the world must not
                    break;
                }
                case PKT_CHAT:
                {
                    // Proximity chat, from the zone. It lands in the same inbox as guild and global
                    // chat: a player has one conversation, however many wires carry it.
                    if (m_incomingChats.size() >= kMaxQueued) break;
                    try { m_incomingChats.push_back(p.unpackChatMessage()); }
                    catch (const std::exception&) { }
                    break;
                }
                case PKT_ACTION_ACK:
                {
                    // El veredicto de algo que ESTE cliente pidio. Va a su propia cola: quien coloca
                    // algo necesita poder deshacerlo, y hasta que existio esto no habia por donde
                    // enterarse (ver `pollActionResults`).
                    ActionAck ackIn{};
                    if (p.tryUnpackActionAck(ackIn) && m_actionAcks.size() < kMaxQueued)
                        m_actionAcks.push_back(ackIn);
                    break;
                }
                default: break;   // the zone sends nothing else on this plane today
            }
        }
    }

    void Client::recvLoop()
    {
        uint8_t buf[8192];

        while (m_running)
        {
            int bytes = m_tcp.receive(m_tcp.getSocketFD(), buf, sizeof(buf));
            if (bytes <= 0) continue;

            Packet p;
            p.setBuffer(buf, bytes);

            std::lock_guard<std::mutex> lk(m_mtx);

            switch (p.getType())
            {
                case PKT_ENTITY_TRANSFER:
                {
                    // A malformed datagram must not take the game down with it.
                    EntityTransfer inc{};
                    if (p.tryUnpackEntityTransfer(inc)) m_incomingEntities.push_back(inc);
                    break;
                }
                case PKT_GHOST_DELTA:
                    m_incomingGhosts.push_back(p.unpackGhostDelta());
                    break;
                // PKT_CHAT is NOT handled here any more: chat comes from the social node, on its own
                // link. The head has no chat handler left either — see `socialLoop`.
                case PKT_ZONE_RESPONSE:
                    // The answer belongs to whoever is waiting in `queryZone`, not to this loop.
                    m_zoneResp     = p.unpackZoneResponse();
                    m_zoneAnswered = true;
                    m_zoneCv.notify_all();
                    break;
                default: break;
            }
        }
    }
}
