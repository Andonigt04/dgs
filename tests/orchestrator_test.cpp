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
        m.ramUsage = 0.99f; m.performance = 0.01f;    // way over every threshold
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
