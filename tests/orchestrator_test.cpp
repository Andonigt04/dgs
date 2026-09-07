// ─────────────────────────────────────────────────────────────────────────────────────────────────
// THE ORCHESTRATOR — 1049 lines that take every cluster-wide decision, and had almost no coverage.
//
// What existed: `spawn_parity_test` exercised two pure config helpers (`zoneSpawnEnv`,
// `resolveSpawnBackend`), and `head_routing_e2e` drove `updateNodeTopology`/`findTargetNode` end to end
// through the real head. Everything else — the lifecycle queue and its priorities, merge, split,
// reassignment on failure, lease eviction, the drain fail-safe, the drain ack — had nothing. Those are
// the decisions the node-level tests keep *observing*; this is where they are *made*.
//
// HOW IT IS OBSERVED WITHOUT A CLUSTER. The orchestrator's only outward channel is
// `socket.send(fd, ...)`, and `fd` is just a number to it. So each fake zone is a `socketpair`: the
// orchestrator writes into one end and the test reads the other. That makes the drain requests and the
// delete confirmations directly readable — no k8s, no mocks, no reaching into private state. The rest
// of the assertions come from the public surface it already exposes: `activeZones`, `findTargetNode`,
// `zoneState`, `replicas`.
//
// The spawn backend is forced to LOCAL with `DGS_ZONE_BIN=/bin/true`, so a SPLIT forks something inert
// instead of a real node — the decision is what is under test, not the process it materialises.
//
// ⚠️ EVERY THRESHOLD IS READ ONCE PER PROCESS. They live in `static const … = evalCfg(...)` inside the
// functions, so the first call freezes them for the life of the binary. That is why all the environment
// is set at the very top of `main`, before anything is constructed, and why the whole file runs under
// ONE configuration. It is also worth knowing in production: changing `EVAL_*` needs a restart, not a
// reload.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/orchestrator.h"
#include "include/dgs/packet.h"

#include <sys/socket.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>

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

// A fake zone: a socketpair. The orchestrator writes into `orchFd`; the test reads `testFd`.
struct FakeZone
{
    int orchFd = -1;
    int testFd = -1;
    int port   = 0;
};

static int g_nextPort = 30500;

static DGS::TCPSocket g_wire;   // only used for its send/receive over an arbitrary fd

/// Registers a zone covering [xMin,xMax] on X (Y and Z always 0..99) and returns its socketpair.
static FakeZone makeZone(DGS::Orchestrator& o, int32_t xMin, int32_t xMax)
{
    FakeZone z{};
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) return z;
    z.orchFd = sv[0];
    z.testFd = sv[1];
    z.port   = g_nextPort++;

    // A read deadline on the test side: nothing in this file may block forever.
    timeval tv{}; tv.tv_usec = 300000;
    setsockopt(z.testFd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    DGS::ServerMetrics m{};
    m.node.chunkXMin = xMin; m.node.chunkXMax = xMax;
    m.node.chunkYMin = 0;    m.node.chunkYMax = 99;
    m.node.chunkZMin = 0;    m.node.chunkZMax = 99;
    std::snprintf(m.node.addr, sizeof(m.node.addr), "127.0.0.1");
    m.node.port = z.port;
    o.updateNodeTopology(z.orchFd, m);
    return z;
}

/// Refreshes a zone's lease, exactly as the head does on every PKT_METRICS.
static void refresh(DGS::Orchestrator& o, const FakeZone& z, int32_t xMin, int32_t xMax)
{
    DGS::ServerMetrics m{};
    m.node.chunkXMin = xMin; m.node.chunkXMax = xMax;
    m.node.chunkYMin = 0;    m.node.chunkYMax = 99;
    m.node.chunkZMin = 0;    m.node.chunkZMax = 99;
    std::snprintf(m.node.addr, sizeof(m.node.addr), "127.0.0.1");
    m.node.port = z.port;
    o.updateNodeTopology(z.orchFd, m);
}

/// Reads one packet the orchestrator sent to this zone. @return its type, or -1 if nothing arrived.
static int readType(const FakeZone& z)
{
    uint8_t buf[8192];
    const int n = g_wire.receive(z.testFd, buf, sizeof(buf));
    if (n <= 0) return -1;
    return (int)buf[0];
}

/// Reads one packet and, if it is a resize Command, returns the new maximum it carries. -1 otherwise.
/// This is what makes the population-cut phase a measurement: the type alone says a split happened,
/// the VALUE says where it cut.
static int readResizeTo(const FakeZone& z)
{
    uint8_t buf[8192];
    const int n = g_wire.receive(z.testFd, buf, sizeof(buf));
    if (n <= 0 || buf[0] != DGS::PKT_COMMAND) return -1;
    DGS::Packet p; p.setBuffer(buf, (size_t)n);
    auto cmd = p.unpackCommand();
    return (cmd.purpose == DGS::CMD_TRANSFER_SERVER) ? cmd.chunkX : -1;
}

/// Registers a zone with a POPULATION PROFILE: `fill(bucket)` says how many entities are in each of
/// the 32 buckets of the X axis. Y and Z are left empty, so the cut can only come from X.
template <typename F>
static void refreshWithPopulation(DGS::Orchestrator& o, const FakeZone& z,
                                  int32_t xMin, int32_t xMax, F fill)
{
    DGS::ServerMetrics m{};
    m.node.chunkXMin = xMin; m.node.chunkXMax = xMax;
    m.node.chunkYMin = 0;    m.node.chunkYMax = 0;
    m.node.chunkZMin = 0;    m.node.chunkZMax = 0;
    std::snprintf(m.node.addr, sizeof(m.node.addr), "127.0.0.1");
    m.node.port = z.port;
    for (uint32_t b = 0; b < DGS::MAX_SPLIT_BUCKETS; ++b) m.popX[b] = fill(b);
    o.updateNodeTopology(z.orchFd, m);
}

/// Como `refresh`, pero diciendo tambien el rango en Y: hace falta para construir topologias que NO
/// esten alineadas, que es donde la envolvente y la union dejan de ser lo mismo.
static void refreshBox(DGS::Orchestrator& o, const FakeZone& z,
                       int32_t xMin, int32_t xMax, int32_t yMin, int32_t yMax)
{
    DGS::ServerMetrics m{};
    m.node.chunkXMin = xMin; m.node.chunkXMax = xMax;
    m.node.chunkYMin = yMin; m.node.chunkYMax = yMax;
    m.node.chunkZMin = 0;    m.node.chunkZMax = 99;
    std::snprintf(m.node.addr, sizeof(m.node.addr), "127.0.0.1");
    m.node.port = z.port;
    o.updateNodeTopology(z.orchFd, m);
}

/// Lee un comando de resize y devuelve el rango que lleva. `outMin/outMax` solo valen si devuelve true.
/// Es lo que convierte la fase de fusion en una medida: el tipo del paquete dice que ALGO se mando,
/// el rango dice QUE se mando — y "el superviviente crece hasta cubrir al muerto" es el rango.
static bool readResizeRange(const FakeZone& z, int32_t& outMin, int32_t& outMax)
{
    uint8_t buf[8192];
    const int n = g_wire.receive(z.testFd, buf, sizeof(buf));
    if (n <= 0 || buf[0] != DGS::PKT_COMMAND) return false;
    DGS::Packet p; p.setBuffer(buf, (size_t)n);
    auto cmd = p.unpackCommand();
    if (cmd.purpose != DGS::CMD_TRANSFER_SERVER) return false;
    outMax = cmd.chunkX; outMin = cmd.chunkY;
    return true;
}

/// Drains and counts whatever is queued for this zone, so one phase cannot pollute the next.
static int drainPackets(const FakeZone& z)
{
    int n = 0;
    while (readType(z) >= 0) ++n;
    return n;
}

static bool inActiveZones(const DGS::Orchestrator& o, int fd)
{
    for (const auto& z : o.activeZones) if (z.fd == fd) return true;
    return false;
}

int main()
{
    signal(SIGPIPE, SIG_IGN);

    // ⚠️ BEFORE anything else: these are frozen on first use (see the header note).
    setenv("DGS_SPAWN_BACKEND",       "local",    1);
    setenv("DGS_ZONE_BIN",            "/bin/true", 1);   // a SPLIT must fork something inert
    setenv("EVAL_LIFECYCLE_SETTLE_S", "0",        1);    // no anti-flap delay: ops run when queued
    setenv("EVAL_MERGE_WINDOW_S",     "0",        1);
    setenv("EVAL_MIN_REPLICAS",       "0",        1);    // otherwise nothing can ever be given up
    setenv("EVAL_ZONE_LEASE_S",       "1",        1);    // short enough to wait out in a test
    setenv("EVAL_DRAIN_TIMEOUT_S",    "1",        1);
    setenv("EVAL_SWEEP_MS",           "0",        1);    // sweep on every evaluation
    setenv("EVAL_COOLDOWN_S",         "0",        1);

    DGS::TCPSocket headSocket;
    DGS::Orchestrator o(headSocket);

    // ══ (1) ROUTABILITY: a zone being retired receives nothing ════════════════════════════════
    // `head_routing_e2e` already proves routing by chunk end to end. What is untested is the rule
    // that makes a handoff safe: a DRAINING or DEAD zone must drop out of routing IMMEDIATELY, or
    // entities keep being sent to a node that is on its way out.
    {
        FakeZone A = makeZone(o, 0, 49);
        FakeZone B = makeZone(o, 50, 99);

        check(o.findTargetNode(10, 5, 5) == A.orchFd && o.findTargetNode(60, 5, 5) == B.orchFd,
              "each chunk routes to the zone that covers it (baseline)");

        o.markZoneState(A.orchFd, DGS::ZoneState::DRAINING);
        check(o.findTargetNode(10, 5, 5) == -1,
              "a DRAINING zone stops receiving IMMEDIATELY (nothing is routed to it)");
        check(o.findTargetNode(60, 5, 5) == B.orchFd,
              "and its neighbour keeps receiving (it is not a global blackout)");

        o.markZoneState(A.orchFd, DGS::ZoneState::READY);
        check(o.findTargetNode(10, 5, 5) == A.orchFd,
              "back to READY it routes again (the rule is the state, not a one-way removal)");

        o.markZoneState(A.orchFd, DGS::ZoneState::DEAD);
        check(o.findTargetNode(10, 5, 5) == -1, "a DEAD zone receives nothing either");

        close(A.testFd); close(B.testFd);
    }

    // ══ (2) THE LIFECYCLE QUEUE: priority, and ONE operation per tick ══════════════════════════
    // The whole point of the queue is that a dead pod is evicted before anybody scales, and that two
    // operations never land in the same tick. Both halves need the other zone to mean anything.
    {
        DGS::TCPSocket s2;
        DGS::Orchestrator q(s2);
        FakeZone A = makeZone(q, 200, 249);   // will be asked to SPLIT (lowest priority)
        FakeZone B = makeZone(q, 250, 299);   // will be asked to EVICT (highest priority)

        const size_t zonesBefore = q.activeZones.size();

        q.enqueueLifecycle(A.orchFd, DGS::LifecycleOp::LIFECYCLE_SPLIT);
        q.enqueueLifecycle(B.orchFd, DGS::LifecycleOp::LIFECYCLE_EVICT);

        check(q.processLifecycleQueue(), "the queue reports that it ran an operation");
        check(!inActiveZones(q, B.orchFd),
              "the EVICT goes FIRST: the dead pod leaves the topology before anyone scales");
        check(inActiveZones(q, A.orchFd),
              "and the SPLIT did NOT run in the same tick (one operation per tick)");

        check(q.processLifecycleQueue(), "the next tick runs the pending operation");
        check(readType(A) == DGS::PKT_COMMAND,
              "the SPLIT reaches the zone as a resize Command over its own socket");
        check(q.activeZones.size() == zonesBefore - 1,
              "the split does not remove anyone: only the evicted zone is gone");

        check(!q.processLifecycleQueue(), "with the queue empty it reports that it did nothing");

        drainPackets(A);
        close(A.testFd); close(B.testFd);
    }

    // ══ (3) SAME ZONE, TWO OPERATIONS: the higher priority survives ═══════════════════════════
    // The queue holds one operation per zone. Which one it keeps is a decision, and it has to hold in
    // BOTH insertion orders — otherwise it is not a priority, it is just "the last one wins".
    {
        DGS::TCPSocket s3;
        DGS::Orchestrator pr(s3);
        FakeZone A = makeZone(pr, 300, 349);
        FakeZone B = makeZone(pr, 350, 399);   // neighbour, so a REASSIGN has somewhere to go

        pr.enqueueLifecycle(A.orchFd, DGS::LifecycleOp::LIFECYCLE_SPLIT);
        pr.enqueueLifecycle(A.orchFd, DGS::LifecycleOp::LIFECYCLE_EVICT);
        pr.processLifecycleQueue();
        check(!inActiveZones(pr, A.orchFd),
              "low then HIGH: the high-priority operation replaces the queued one (EVICT wins)");
        check(!pr.processLifecycleQueue(),
              "and the replaced one does NOT stay behind for a later tick");

        FakeZone C = makeZone(pr, 400, 449);
        pr.enqueueLifecycle(C.orchFd, DGS::LifecycleOp::LIFECYCLE_EVICT);
        pr.enqueueLifecycle(C.orchFd, DGS::LifecycleOp::LIFECYCLE_SPLIT);
        pr.processLifecycleQueue();
        check(!inActiveZones(pr, C.orchFd),
              "HIGH then low: the low-priority one does NOT overwrite it (still EVICT)");

        close(A.testFd); close(B.testFd); close(C.testFd);
    }

    // ══ (3b) MERGE AND SPLIT ARE NOT A PRIORITY QUESTION ══════════════════════════════════════
    // ⚠️ A ZONE COULD NEVER SCALE UP ONCE IT HAD BEEN IDLE. MERGE outranks SPLIT, the queue holds one
    // operation per zone, and `enqueueLifecycle` kept the higher-priority one — so a MERGE queued
    // while the zone was empty sat in that slot and every later SPLIT was discarded in silence.
    //
    // Measured before the fix: 80 players in one zone with the shipped defaults, 25 seconds. The head
    // printed "Umbral alcanzado ... -> encolado SPLIT" 217 times and spawned ZERO children.
    //
    // They are not a priority question. They are contradictory statements about the same zone made by
    // the same evaluation of the same metrics sample — "it is idle, drain it" versus "it is drowning,
    // divide it" — and the newest one is the true one. Crash and reassign still outrank both, which is
    // what phase (3) above pins.
    {
        DGS::TCPSocket s3b;
        DGS::Orchestrator mx(s3b);
        FakeZone A = makeZone(mx, 600, 649);
        FakeZone B = makeZone(mx, 650, 699);   // a neighbour, so a MERGE would have somewhere to go

        // The order that used to fail: idle first, then loaded.
        mx.enqueueLifecycle(A.orchFd, DGS::LifecycleOp::LIFECYCLE_MERGE);
        mx.enqueueLifecycle(A.orchFd, DGS::LifecycleOp::LIFECYCLE_SPLIT);
        mx.processLifecycleQueue();
        check(readType(A) == DGS::PKT_COMMAND,
              "MERGE then SPLIT: the zone is SPLIT — the newer decision replaces the stale one");
        drainPackets(A);

        // And the other way round, which must also be the newest one and not "the higher priority".
        // The assertion is that the SPLIT does NOT win, not that a DRAIN goes out: whether a merge
        // actually proceeds depends on `tryMergeDown`'s own preconditions (a victim, the hysteresis
        // window, the replica floor), and pinning those here would make this a test of the merge
        // rather than of the queue rule it is about.
        mx.enqueueLifecycle(B.orchFd, DGS::LifecycleOp::LIFECYCLE_SPLIT);
        mx.enqueueLifecycle(B.orchFd, DGS::LifecycleOp::LIFECYCLE_MERGE);
        check(mx.processLifecycleQueue(), "the queue runs the operation it kept");
        check(readType(B) != DGS::PKT_COMMAND,
              "SPLIT then MERGE: the SPLIT does NOT win — same rule, both directions");

        drainPackets(B);
        close(A.testFd); close(B.testFd);
    }

    // ══ (3c) THE CUT FOLLOWS THE PEOPLE, NOT THE BOX ══════════════════════════════════════════
    // A zone splits because of the players in it, and players are not spread evenly across its box.
    // The orchestrator used to know only the box and one entity COUNT, so it cut down the geometric
    // middle: with a town in chunks 0..10 of a 0..100 zone the cut landed at 50, the child got 51..100
    // — empty — and the parent kept every single player. It cost a process and relieved nothing, and
    // it took about five generations (~2.5 min at one split per 30 s) to walk the cut down to them.
    //
    // Zones now report a coarse population histogram per axis and the cut goes where the population
    // divides in half. The two halves of this phase are each other's counter-proof: the SAME code on a
    // skewed population must answer near the crowd and on a flat one must answer near the middle. One
    // of them alone would pass on a constant.
    {
        DGS::TCPSocket s3c;
        DGS::Orchestrator pc(s3c);

        // Skewed: everybody in the first quarter of the box.
        FakeZone A = makeZone(pc, 0, 100);
        refreshWithPopulation(pc, A, 0, 100, [](uint32_t b){ return (uint16_t)(b < 8 ? 10 : 0); });
        drainPackets(A);
        pc.enqueueLifecycle(A.orchFd, DGS::LifecycleOp::LIFECYCLE_SPLIT);
        pc.processLifecycleQueue();
        const int cutSkewed = readResizeTo(A);
        std::printf("    population in chunks 0..24 of 0..100 -> cut at %d\n", cutSkewed);
        check(cutSkewed >= 0 && cutSkewed < 25,
              "a crowd at one end is cut INSIDE the crowd, not at the middle of the box");

        // Flat: the same box, the same code, an even population.
        FakeZone B = makeZone(pc, 0, 100);
        refreshWithPopulation(pc, B, 0, 100, [](uint32_t){ return (uint16_t)10; });
        drainPackets(B);
        pc.enqueueLifecycle(B.orchFd, DGS::LifecycleOp::LIFECYCLE_SPLIT);
        pc.processLifecycleQueue();
        const int cutFlat = readResizeTo(B);
        std::printf("    an even population over 0..100        -> cut at %d\n", cutFlat);
        check(cutFlat > 40 && cutFlat < 60,
              "an even population is cut near the middle — so the one above is a measurement");

        close(A.testFd); close(B.testFd);
    }

    // ══ (3d) LA FUSION TIENE QUE DEJAR EL MUNDO CUBIERTO ══════════════════════════════════════
    // Una zona se apaga y otra se queda su region. Si ese traspaso no llega hasta el final, los chunks
    // de la victima no son de nadie: el head contesta con una `ZoneResponse` vacia y un jugador que
    // este ahi deja de existir para el mundo. Es el unico hueco de la cadena de escalado que puede
    // ROMPER EL MUNDO en vez de solo desaprovecharlo, asi que se mide en dos momentos distintos —
    // y el segundo es el que importa:
    //
    //   · JUSTO DESPUES de fusionar, cuando el head acaba de anotar la union en su topologia;
    //   · y DESPUES DE LA SIGUIENTE MUESTRA DE METRICAS del superviviente, que es lo que pasa 100 ms
    //     mas tarde en un cluster de verdad. `updateNodeTopology` sobrescribe la caja del head con la
    //     que reporta la zona, y una zona reporta la suya de siempre: no hay forma de que CREZCA.
    //
    // Mirar solo lo primero daria verde a un sistema que pierde la region un decimo de segundo despues.
    {
        DGS::TCPSocket s3d;
        DGS::Orchestrator mg(s3d);
        FakeZone S = makeZone(mg, 700, 749);   // superviviente
        FakeZone V = makeZone(mg, 750, 799);   // victima, vecina por una cara

        const int32_t victimChunk = 775;       // en el medio de la victima
        check(mg.findTargetNode(victimChunk, 5, 5) == V.orchFd,
              "antes de fusionar, el chunk de la victima es suyo");

        // ⚠️ DOS VECES, PORQUE LA PRIMERA NO FUSIONA: la histeresis exige una ventana de carga baja
        // sostenida, asi que la primera llamada solo ABRE la ventana y devuelve false — "nunca por una
        // sola muestra". Un cluster real lo pide diez veces por segundo; pedirlo una vez y concluir
        // que la fusion no funciona seria medir la histeresis creyendo medir la fusion.
        for (int i = 0; i < 2; ++i) {
            mg.enqueueLifecycle(S.orchFd, DGS::LifecycleOp::LIFECYCLE_MERGE);
            mg.processLifecycleQueue();
        }

        const int justAfter = mg.findTargetNode(victimChunk, 5, 5);
        std::printf("    justo tras fusionar, el chunk %d va a fd=%d (superviviente=%d)\n",
                    victimChunk, justAfter, S.orchFd);
        check(justAfter == S.orchFd,
              "3d · tras la fusion, el superviviente cubre la region de la victima");

        // ⚠️ Y ESTO ES LO QUE HACE QUE DURE. Anotarlo en la topologia del head no basta: a los 100 ms
        // llega la siguiente muestra de metricas y `updateNodeTopology` reescribe la caja del head con
        // la que reporta la zona. Si a la zona no se le dice que ha crecido, reporta la suya de
        // siempre y la region de la victima se queda SIN DUENO — medido antes de arreglarlo:
        // `findTargetNode` devolvia -1.
        int32_t rMin = 0, rMax = 0;
        const bool told = readResizeRange(S, rMin, rMax);
        std::printf("    al superviviente se le manda el rango [%d..%d] (told=%d)\n", rMin, rMax, (int)told);
        check(told && rMin == 700 && rMax == 799,
              "3d · y se le DICE a la zona que ha crecido, con el rango de la union");

        // Con esa orden aplicada, la zona reporta la caja nueva y la cobertura se sostiene.
        refresh(mg, S, rMin, rMax);
        const int afterMetrics = mg.findTargetNode(victimChunk, 5, 5);
        std::printf("    tras la siguiente muestra de metricas, va a fd=%d\n", afterMetrics);
        check(afterMetrics == S.orchFd,
              "3d · y SIGUE cubierta cuando el superviviente vuelve a reportar (no hay mundo huerfano)");

        drainPackets(S); drainPackets(V);
        close(S.testFd); close(V.testFd);
    }

    // ══ (3e) LA ENVOLVENTE NO ES LA UNION ══════════════════════════════════════════════════════
    // `absorbRegion` calculaba min/max en los tres ejes y se quedaba con la caja que contiene a las
    // dos. Eso es MAS GRANDE que su union en cuanto no estan alineadas, y lo que sobra no es espacio
    // vacio: es territorio de un TERCERO. Como `findTargetNode` devuelve la primera coincidencia por
    // orden de insercion, el superviviente le robaba los chunks a un vecino que seguia vivo, sin un
    // solo mensaje.
    //
    // Aqui A y B se tocan en X pero NO coinciden en Y, asi que su union no es una caja. C ocupa
    // justo el trozo que la envolvente se inventaria. La comprobacion es sobre C: siga siendo suyo.
    {
        DGS::TCPSocket s3e;
        DGS::Orchestrator bb(s3e);
        FakeZone A = makeZone(bb, 800, 809);   // makeZone da Y[0..99]
        FakeZone B = makeZone(bb, 810, 819);
        FakeZone C = makeZone(bb, 810, 819);

        // B se queda con la mitad baja de Y; C con la alta. A sigue con Y entero.
        refreshBox(bb, B, 810, 819, 0, 49);
        refreshBox(bb, C, 810, 819, 50, 99);

        const int32_t stolenChunk = 815;       // en X de B/C, en la Y ALTA -> es de C
        check(bb.findTargetNode(stolenChunk, 75, 5) == C.orchFd,
              "antes de nada, el trozo alto es de C");

        for (int i = 0; i < 2; ++i) {
            bb.enqueueLifecycle(A.orchFd, DGS::LifecycleOp::LIFECYCLE_MERGE);
            bb.processLifecycleQueue();
        }

        const int after = bb.findTargetNode(stolenChunk, 75, 5);
        std::printf("    tras intentar fusionar A con su vecino, el trozo de C va a fd=%d (C=%d)\n",
                    after, C.orchFd);
        check(after == C.orchFd,
              "3e · una union que no es una caja NO se fusiona: nadie le roba su region a un tercero");

        drainPackets(A); drainPackets(B); drainPackets(C);
        close(A.testFd); close(B.testFd); close(C.testFd);
    }

    // ══ (4) HANDOFF ON A FAILED METRIC (P6) ═══════════════════════════════════════════════════
    // This is the head-side counterpart of the zone's circuit breaker: when a zone reports its
    // validator down, the master must move its region to a healthy neighbour rather than leave it
    // serving unvalidated. Three things have to happen together, and the third is the one that makes
    // it safe: the region must not be left unserved.
    DGS::TCPSocket sHandoff;
    DGS::Orchestrator h(sHandoff);
    FakeZone P{}, Q{};
    {
        P = makeZone(h, 500, 549);
        Q = makeZone(h, 550, 599);

        DGS::ValidatorStatus st{};
        st.state = 2; st.reqTimeout = 40; st.failedTransfers = 12;
        h.notifyValidatorDown(P.orchFd, st);

        check(h.zoneState(P.orchFd) == DGS::ZoneState::DRAINING,
              "a zone reporting its validator DOWN is put into DRAINING");
        check(readType(P) == DGS::PKT_DRAIN,
              "and it is told to drain over its own connection (PKT_DRAIN)");
        check(h.findTargetNode(510, 5, 5) == Q.orchFd,
              "its region is ALREADY covered by the healthy neighbour (nothing is left unserved)");
        check(h.findTargetNode(560, 5, 5) == Q.orchFd,
              "and the neighbour keeps its own region too (it absorbed, it did not swap)");
    }

    // ══ (4b) COUNTER-PROOF: with no healthy neighbour it does NOT hand over ═══════════════════
    // Draining the only zone covering a region would leave it unserved — worse than serving it
    // without a verdict. Without this case, (4) would also pass on an orchestrator that drains
    // anything that complains.
    {
        DGS::TCPSocket sLonely;
        DGS::Orchestrator lonely(sLonely);
        FakeZone only = makeZone(lonely, 0, 99);

        DGS::ValidatorStatus st{};
        st.state = 2; st.reqTimeout = 99;
        lonely.notifyValidatorDown(only.orchFd, st);

        check(lonely.zoneState(only.orchFd) == DGS::ZoneState::READY,
              "with NO healthy neighbour the zone is KEPT (an unserved region is worse)");
        check(lonely.findTargetNode(50, 5, 5) == only.orchFd,
              "and it goes on receiving traffic");
        check(readType(only) == -1, "no drain request is sent to it");
        close(only.testFd);
    }

    // ══ (5) THE DRAIN ACK: only while DRAINING, and only the right requestId ═══════════════════
    // `requestId` exists so a late ack from a previous operation cannot destroy a zone that has since
    // gone back into service. Accepting a stale ack is a lost node.
    {
        DGS::ZoneLifecycle stale{ 999999, 1 };          // an id that was never issued
        h.handleZoneLifecycle(P.orchFd, stale);
        check(h.zoneState(P.orchFd) == DGS::ZoneState::DRAINING,
              "an ack with an unknown requestId is IGNORED (the zone is not destroyed)");
        check(readType(P) == -1, "and nothing is confirmed back to it");

        // The real id is the one the orchestrator issued in (4). It is not exposed, but it is the
        // first one handed out by this instance, so it is recoverable by replaying the sequence: the
        // only ack that must work is the one carrying it.
        bool accepted = false;
        for (uint32_t id = 1; id <= 8 && !accepted; ++id)
        {
            h.handleZoneLifecycle(P.orchFd, DGS::ZoneLifecycle{ id, 1 });
            accepted = !inActiveZones(h, P.orchFd);
        }
        check(accepted, "the ack carrying the issued requestId IS accepted and destroys the zone");
        check(h.zoneState(P.orchFd) == DGS::ZoneState::DESTROYED, "the zone ends up DESTROYED");
        check(readType(P) == DGS::PKT_DELETE_ZONE,
              "and its exit is confirmed to it (PKT_DELETE_ZONE)");

        // And an ack for a zone that is not draining changes nothing.
        h.handleZoneLifecycle(Q.orchFd, DGS::ZoneLifecycle{ 1, 1 });
        check(inActiveZones(h, Q.orchFd) && h.zoneState(Q.orchFd) != DGS::ZoneState::DESTROYED,
              "an ack from a zone that was NOT draining is ignored");

        close(P.testFd); close(Q.testFd);
    }

    // ══ (6) DRAIN FAIL-SAFE: a node that never acks must not strand its region ════════════════
    // If the drain is left half-done the region has no owner: the zone is out of routing and nobody
    // took it. Past the deadline it has to go back into service.
    {
        DGS::TCPSocket sFs;
        DGS::Orchestrator fs(sFs);
        FakeZone A = makeZone(fs, 0, 49);
        FakeZone B = makeZone(fs, 50, 99);

        DGS::ValidatorStatus st{}; st.state = 2;
        fs.notifyValidatorDown(A.orchFd, st);
        check(fs.zoneState(A.orchFd) == DGS::ZoneState::DRAINING, "the zone enters DRAINING");

        // Before the deadline: it stays draining. Without this the next assertion would pass on an
        // orchestrator that simply never drains anything.
        DGS::ServerMetrics m{};
        m.node.chunkXMin = 0; m.node.chunkXMax = 49;
        m.node.chunkYMin = 0; m.node.chunkYMax = 99;
        m.node.chunkZMin = 0; m.node.chunkZMax = 99;
        m.node.port = A.port;
        refresh(fs, A, 0, 49);
        fs.evaluateServer(m, A.orchFd);
        check(fs.zoneState(A.orchFd) == DGS::ZoneState::DRAINING,
              "before the deadline it is still DRAINING (the fail-safe does not fire early)");

        std::this_thread::sleep_for(std::chrono::milliseconds(1300));   // EVAL_DRAIN_TIMEOUT_S = 1
        refresh(fs, A, 0, 49);
        refresh(fs, B, 50, 99);
        fs.evaluateServer(m, A.orchFd);
        check(fs.zoneState(A.orchFd) == DGS::ZoneState::READY,
              "past the deadline with no ack it goes back to READY (the region is never stranded)");
        check(fs.findTargetNode(10, 5, 5) == A.orchFd, "and it receives traffic again");

        drainPackets(A); drainPackets(B);
        close(A.testFd); close(B.testFd);
    }

    // ══ (7) LEASE: a zone that stops reporting is evicted; one that keeps reporting is not ════
    // A crashed pod leaves no ack and no close — it simply goes quiet. Without the sweep its replica
    // leaks and its range stays in the topology, so traffic keeps being routed into a void.
    {
        DGS::TCPSocket sLease;
        DGS::Orchestrator lease(sLease);
        FakeZone live = makeZone(lease, 0, 49);
        FakeZone dead = makeZone(lease, 50, 99);

        DGS::ServerMetrics m{};
        m.node.chunkXMin = 0; m.node.chunkXMax = 49;
        m.node.chunkYMin = 0; m.node.chunkYMax = 99;
        m.node.chunkZMin = 0; m.node.chunkZMax = 99;
        m.node.port = live.port;

        // `dead` stops reporting here; `live` keeps its lease renewed, exactly as the head does.
        for (int i = 0; i < 8; ++i)
        {
            refresh(lease, live, 0, 49);
            lease.evaluateServer(m, live.orchFd);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

        check(!inActiveZones(lease, dead.orchFd),
              "the zone that stopped reporting is EVICTED when its lease expires");
        check(inActiveZones(lease, live.orchFd),
              "and the one still reporting is NOT (it is a lease, not a purge)");
        check(lease.findTargetNode(60, 5, 5) == -1,
              "its range stops routing: no traffic is sent into a void");

        close(live.testFd); close(dead.testFd);
    }

    // ══ (8) THE FIRST SAMPLE DECIDES NOTHING ═════════════════════════════════════════════════
    // Rates are derived from accumulated counters, so the first metric has no Δ to compare against.
    // Acting on it would mean scaling on a number that does not exist yet.
    {
        DGS::TCPSocket sBase;
        DGS::Orchestrator base(sBase);
        FakeZone A = makeZone(base, 0, 49);
        FakeZone B = makeZone(base, 50, 99);

        DGS::ServerMetrics m{};
        m.node.chunkXMin = 0; m.node.chunkXMax = 49;
        m.node.chunkYMin = 0; m.node.chunkYMax = 99;
        m.node.chunkZMin = 0; m.node.chunkZMax = 99;
        m.node.port = A.port;
        // ⚠️ `performance` is the TICK TIME IN MILLISECONDS, not the 0..1 the struct comment claims.
        // This line used to read 0.01f "way over every threshold", which under the old inverted signal
        // (`performance < EVAL_LOAD_PERF`) meant an idle 0.01 ms tick counted as overloaded. 90 ms of a
        // 100 ms budget is what an actually struggling zone looks like.
        m.ramUsage = 0.99f; m.performance = 90.0f;    // out of RAM AND missing its tick budget
        m.failedTransfers = 10000;                    // and failing hard
        m.bytesTx = 1000000; m.bytesRx = 1;

        refresh(base, A, 0, 49);
        base.evaluateServer(m, A.orchFd);
        check(base.zoneState(A.orchFd) == DGS::ZoneState::READY && readType(A) == -1,
              "the FIRST sample only sets the baseline: nothing is decided on it");

        // The second one, with the same numbers, does act — which is what makes the line above a
        // statement about the baseline rather than about the thresholds.
        refresh(base, A, 0, 49);
        base.evaluateServer(m, A.orchFd);
        check(base.zoneState(A.orchFd) == DGS::ZoneState::DRAINING,
              "the SECOND one does act on it (failedTransfers over threshold → handoff)");

        drainPackets(A); drainPackets(B);
        close(A.testFd); close(B.testFd);
    }

    // ══ (9) REPLICA ACCOUNTING: it must not rot, because the rot is silent and permanent ═════
    // Found by this file, and it is the reason phase (4) failed the first time it ran.
    // `currentReplicas` is only incremented by zones the orchestrator SPAWNED, but decremented for
    // every zone that leaves — and a zone can join on its own (the base zone-node, or a replica
    // deployed by hand: they connect and register through `updateNodeTopology`). Measured with a probe
    // before the fix: three self-registered zones evicted took the counter from 1 to **-2**, and it
    // stayed at -2 while healthy zones rejoined. Both `tryMergeDown` and `tryReassign` guard on it
    // against `EVAL_MIN_REPLICAS` (default 1), so once it rotted the cluster could never merge NOR hand
    // over a failing zone again — for the life of the process, and precisely when it matters most.
    //
    // The guards now ask the topology instead (`routableZoneCount()`), which is derived and cannot
    // drift. The counter is still reported outward, so it is also kept out of negative territory.
    {
        DGS::TCPSocket sAcc;
        DGS::Orchestrator acc(sAcc);

        FakeZone z1 = makeZone(acc, 0, 32);      // three zones that JOIN on their own,
        FakeZone z2 = makeZone(acc, 33, 65);     // exactly as the base node and any hand-deployed
        FakeZone z3 = makeZone(acc, 66, 99);     // replica do

        check(acc.routableZoneCount() == 3,
              "the derived count sees the three zones that registered themselves");

        for (const FakeZone& z : { z1, z2, z3 })
        {
            acc.enqueueLifecycle(z.orchFd, DGS::LifecycleOp::LIFECYCLE_EVICT);
            acc.processLifecycleQueue();
        }
        check(acc.routableZoneCount() == 0, "and sees them gone once evicted");
        check(acc.replicas() >= 0,
              "the reported replica count never goes NEGATIVE (it used to reach -2)");

        // The part that actually bites: after that churn, a failing zone must STILL be handed over.
        FakeZone A = makeZone(acc, 0, 49);
        FakeZone B = makeZone(acc, 50, 99);
        DGS::ValidatorStatus st{}; st.state = 2; st.reqTimeout = 40;
        acc.notifyValidatorDown(A.orchFd, st);

        check(acc.zoneState(A.orchFd) == DGS::ZoneState::DRAINING,
              "after the churn, a failing zone can STILL be handed over (the guard did not rot)");
        check(acc.findTargetNode(10, 5, 5) == B.orchFd,
              "and its region is covered by the neighbour, as in phase (4)");

        drainPackets(A); drainPackets(B);
        close(z1.testFd); close(z2.testFd); close(z3.testFd);
        close(A.testFd);  close(B.testFd);
    }

    // Reap whatever `/bin/true` children the SPLIT forked.
    while (waitpid(-1, nullptr, WNOHANG) > 0) {}

    std::printf("\n== orchestrator: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
