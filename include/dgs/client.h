#ifndef DGS_CLIENT_H
#define DGS_CLIENT_H

#include "include/dgs/types.h"
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>

namespace DGS
{
    class Client
    {
    public:
        ~Client() { disconnect(); }

        bool connect(const std::string& headHost, int headPort, const std::string& username, const std::string& password, const std::string& apiHost,  int apiPort);

        void disconnect();

        /// LO QUE ESTE JUGADOR DICE DE SI MISMO, en una sola llamada.
        ///
        /// ⚠️ ERAN TRES —`sendTransform`, `sendStats`, `sendInventory`— y las tres mandaban el MISMO
        /// paquete con un campo distinto relleno. No eran tres canales: eran tres formas de decir
        /// "esta es mi entidad", y tenerlas separadas costo dos bugs reales, los dos por la misma
        /// razon —que cada una tenia que acordarse de lo que las otras habian dicho—:
        ///
        ///   · `sendStats` mandaba una posicion a cero, o sea que declarar la vida te teletransportaba
        ///     al origen de tu chunk y S1 lo rechazaba como un salto;
        ///   · y un `sendTransform` posterior mandaba unos `Stats` en blanco que borraban la velocidad
        ///     declarada, dejando al jugador con el metro de holgura por todo margen.
        ///
        /// Ahora es una. Y lo que NO lleva es tan importante como lo que lleva: ni `type` ni `stats`,
        /// porque un cliente no decide que es ni a que velocidad se le juzga — eso lo pone la zona
        /// (ver `client_claims_e2e`). Lo que el juego tenga de suyo —vida, dano, inventario, lo que
        /// sea, que cambia de un juego a otro y no tiene por que parecerse a `Stats`— va en `data`,
        /// que es opaco y que el DGS no interpreta.
        ///
        /// `size = 0` significa "no tengo nada nuevo que decir de mi payload", no "mi payload esta
        /// vacio": es lo que permite que un inventario sobreviva a veinte transformadas por segundo
        /// sin viajar en cada una.
        ///
        /// Re-pregunta al head sola cuando cambias de chunk, y anuncia la frontera a la zona que dejas
        /// — sin eso el traspaso de autoridad no ocurre (ver la nota larga en el .cpp).
        void sendState(uint32_t uuid,
                       int32_t chunkX, int32_t chunkY, int32_t chunkZ,
                       const float pos[3], const float rot[4],
                       const uint8_t* data = nullptr, uint16_t size = 0);

        /// PIDE algo. No lo hace: lo pide.
        ///
        /// Un `sendTransform` es un hecho consumado — "estoy aqui" — y la zona lo comprueba y lo
        /// acepta. Una accion es una peticion: la zona la manda al validador con `kind = 1`, la ruta
        /// que falla CERRADO, y solo si vuelve aceptada ocurre algo. Por eso colocar una mesa es esto
        /// y no un `EntityTransfer` con `STATE_WORLD_OWNED`: lo segundo dejaba que un cliente creara
        /// objetos eternos por su cuenta, y la zona ya no se lo permite.
        ///
        /// El uuid del objeto NO se manda: lo asigna el servidor. Si lo eligiera quien pide, acertar
        /// un numero bastaria para sobrescribir la mesa de otro.
        ///
        /// Sin modulo de reglas cargado en el validador esto se RECHAZA siempre, y esa es la conducta
        /// correcta: una accion colada no se deshace.
        /// @return el numero con el que reconoceras la respuesta en `pollActionResults`, o 0 si ni
        ///         siquiera se pudo mandar (sin zona todavia).
        uint32_t sendAction(uint32_t actor, uint16_t action, uint32_t target,
                            int32_t chunkX, int32_t chunkY, int32_t chunkZ,
                            const float pos[3], uint16_t angle,
                            const uint8_t* data, uint16_t size);

        /// LOS VEREDICTOS QUE HAN LLEGADO desde la ultima vez que preguntaste.
        ///
        /// ⚠️ SIN ESTO LA PREDICCION OPTIMISTA ERA UNA MENTIRA. Un juego coloca lo que pides al
        /// instante —y hace bien, o construir se sentiria a 100 ms— pero luego tiene que poder
        /// DESHACERLO si el servidor dice que no. Hasta aqui no habia por donde enterarse: pedias, lo
        /// ponias en tu pantalla, y si te lo rechazaban seguia ahi para ti y para nadie mas.
        std::vector<ActionAck> pollActionResults();



        /// ⚠️ THE CHANNEL DECIDES WHICH WIRE IT TAKES, because it decides who is allowed to hear it.
        /// `CHAT_LOCAL` goes over the UDP link to the ZONE — the only process that knows who is near
        /// you, and which already computes exactly that every tick for the entity broadcast. Everything
        /// else goes to the social node, which is the only one that knows who is in your guild. The
        /// zone never learns what a guild is and the social node never learns where anybody stands.
        void sendChat(uint32_t uuid, const std::string& username, const std::string& text,
                      uint8_t channel = CHAT_GLOBAL);

        std::vector<EntityTransfer> pollEntities();
        std::vector<GhostDelta>     pollGhosts();
        std::vector<ChatMessage>    pollChats();

        bool isConnected() const { return m_running.load(); }

        /// Is there a zone serving where this player is?
        ///
        /// ⚠️ CONNECTED IS NOT THE SAME AS PLAYABLE, and nothing said so. The head answers a chunk it
        /// cannot route with an all-zero ZoneResponse; `applyZone` now refuses it and keeps whatever
        /// zone it had, but `connect()` still returns true and the game happily draws a character in a
        /// world that receives none of its movement. Chat and social genuinely do work in that state,
        /// so refusing the connection would cost more than it buys — but a game has to be able to ASK,
        /// and until this existed it could not. False means: the login worked, the head is answering,
        /// and no zone covers where you are standing.
        bool hasZone() const { return m_zonePort != 0; }

        /// Is there a world clock, and what time is it in the world?
        ///
        /// Every client used to run its own: the engine's simulation time starts at zero and
        /// accumulates the local frame delta, and orbits, day/night, weather and tide are all analytic
        /// functions of that one number — so two players who started five minutes apart were literally
        /// in different worlds. The login hands out an anchor (epoch, rate, and the server's own clock
        /// reading) and this advances it with a steady clock. See `applyLoginKeys`.
        ///
        /// `false` means nobody handed out an anchor, and the caller should keep its own clock rather
        /// than pretend: a wrong world time is worse than an unsynchronised one.
        /// El identificador que el login dio a ESTA sesion, o 0 si no dio ninguno.
        ///
        /// Sirve para que dos clientes no sean la misma entidad (ver `applyLoginKeys`). No es una
        /// identidad de cuenta: cambia en cada login.
        uint32_t sessionId() const { return m_session; }

        bool   hasWorldTime() const { return m_haveWorldTime; }
        double worldTimeSeconds() const
        {
            if (!m_haveWorldTime) return 0.0;
            const double elapsed = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - m_worldAnchor).count();
            return m_worldAtLogin + elapsed * m_worldScale;
        }

        /// Where that zone is, for a log line or an overlay. Empty when `hasZone()` is false.
        std::string zoneEndpoint() const
        {
            return m_zonePort ? (m_zoneAddr + ":" + std::to_string(m_zonePort)) : std::string();
        }

    private:
        TCPSocket m_tcp;     // → HeadServer  (control: which zone covers my chunk)
        UDPSocket m_udp;     // → ZoneServer  (the game plane: transforms out, the world in)
        // ⚠️ CHAT HAS ITS OWN LINK, and it is not a third connection for the sake of it. It used to go
        // through the head, which fanned every message out to every connection it held — so a player's
        // zone answer arrived BEHIND everyone else's conversation on the same stream. Measured: 96
        // chatters took a zone query from 0.07 ms to 43.6 ms with the head at 11% of one core. It was
        // never CPU; it was one channel carrying two things with different deadlines.
        TCPSocket m_social;  // → SocialNode  (chat, and the rules that go with it)

        std::string m_zoneAddr;
        int         m_zonePort = 0;

        // The world clock anchor (see `hasWorldTime`). Steady, not wall: the world must not jump when
        // the machine corrects its own time.
        // Has the head ever answered a zone query? Not "is there a zone" — that is `hasZone()`. This
        // separates a dead head from a live one saying nobody covers that chunk; see `connect`.
        std::vector<ActionAck>                m_actionAcks;           // ver pollActionResults()
        uint32_t                              m_nextActionId  = 1;    // correlacion cliente<->respuesta
        uint32_t                              m_session       = 0;   // ver sessionId()
        bool                                  m_headAnswered  = false;

        bool                                  m_haveWorldTime = false;
        double                                m_worldAtLogin  = 0.0;   // world seconds at the anchor
        double                                m_worldScale    = 1.0;   // world seconds per real second
        std::chrono::steady_clock::time_point m_worldAnchor{};

        // Where the chat service is. A login response may say (`socialHost`/`socialPort`); otherwise
        // the environment does, which is what a node would use.
        std::string m_socialHost;
        int         m_socialPort = 0;
        bool        m_socialUp   = false;

        int32_t m_lastChunkX = INT32_MIN;
        int32_t m_lastChunkY = INT32_MIN;
        int32_t m_lastChunkZ = INT32_MIN;

        // ⚠️ THE BORDER IS ANNOUNCED TO THE ZONE BEING LEFT, and until this existed the authority
        // handoff was unreachable code. See the long note in `sendTransform`. Zero = not crossing.
        uint64_t m_announceUntilMs = 0;

        // ⚠️ AQUI VIVIA EL PARCHE DE TENER TRES LLAMADAS. Habia que recordar los ultimos `Stats` y la
        // ultima posicion porque `sendStats` y `sendInventory` construian un `EntityTransfer` propio y
        // se pisaban entre ellos: uno mandaba la posicion a cero (teletransporte al origen del chunk,
        // rechazado por S1) y el otro los `Stats` en blanco (velocidad 0, todo movimiento rechazado).
        // Con una sola llamada no hay dos versiones de la misma entidad que puedan discrepar, asi que
        // no hay nada que recordar.

        std::thread       m_recvThread;
        // ⚠️ A SECOND THREAD, BECAUSE THE WORLD ARRIVES ON A DIFFERENT SOCKET. `m_udp` was used to SEND
        // and nothing else — `recvLoop` reads the TCP link to the head, and in a real cluster the head
        // routes entity transfers to ZONES, not to clients. So a player connected, was routed, sent
        // their position and saw an empty world: the zone's broadcast, which is the entire game plane,
        // had no reader. Measured against one live zone at one moment: the client sent 60 transforms
        // and heard back 0 entities, while a raw UDP socket speaking the same protocol sent 320 and
        // received 316 datagrams. The datagrams were arriving; nobody read them.
        std::thread       m_udpThread;
        std::thread       m_socialThread;
        std::mutex        m_mtx;
        std::atomic<bool> m_running{ false };

        std::vector<EntityTransfer> m_incomingEntities;
        std::vector<GhostDelta>     m_incomingGhosts;
        std::vector<ChatMessage>    m_incomingChats;

        // ⚠️ ONE READER PER SOCKET. `queryZone` used to read `m_tcp` directly while `recvLoop` was
        // reading the very same descriptor from another thread. Two consequences, and the second is
        // worse than the first:
        //   · the answer was routinely swallowed by `recvLoop` (which discards PKT_ZONE_RESPONSE in its
        //     `default:`), so the query timed out — measured: 6 of 9 runs;
        //   · and because `receive()` reads a 4-byte length prefix and then the payload, the two threads
        //     could split a single message between them and desynchronise the stream for good.
        // Now `recvLoop` owns the descriptor and hands the response over through this pair.
        std::condition_variable m_zoneCv;
        bool                    m_zoneAnswered = false;
        ZoneResponse            m_zoneResp{};

        void applyLoginKeys(const std::string& body);
        bool queryZone(uint32_t uuid, int32_t chunkX, int32_t chunkY, int32_t chunkZ);
        bool applyZone(const ZoneResponse& z);
        void recvLoop();
        void udpLoop();
        void socialLoop();
        void sendEntityUDP(const EntityTransfer& e);
    };
}

#endif // DGS_CLIENT_H
