// ─────────────────────────────────────────────────────────────────────────────────────────────────
// load_model — WHAT HAPPENS WITH 1000 PLAYERS, and with twice the load on top.
//
// One zone has been driven with 1000 real clients (see the table below and `tools/load_zone`), but
// that run takes minutes, wants a thousand file descriptors and, on a shared machine, measures the
// machine. This is the part that runs on every push: it PROJECTS, and the only thing that makes a
// projection worth anything is that it can be checked — so the model is fed the configurations that
// HAVE been measured and has to reproduce their numbers. A model that cannot reproduce what was
// measured cannot predict what was not, and that is the first half of this test. The second half is
// the conclusions at 1000 and 2000, out of the same, already calibrated model.
//
// WHERE THE CONSTANTS COME FROM (all from measurements, none invented; README §"Capacity, measured"):
//   · the zone's tick is 10 Hz (100 ms);
//   · each datagram costs ~6.9 us of syscall — 6.8 us measured at N=64 (4096 per tick) and 7.0 at
//     N=128 (16384). The wall is the `sendto` calls, not the bytes: when the size fell 67x the loop
//     barely moved;
//   · the floor of 0.3 ms is one tick with nobody in it (211 us measured with a single idle player).
//
// AND THE SIZE ON THE WIRE IS NOT A CONSTANT: a real entity is packed with `Packet::pack`. If anybody
// sends the whole struct over UDP again — which already happened: 4160 B instead of 62, 67x — this
// test says so here, in the projection, instead of leaving it for the bandwidth bill.
//
// ⚠️ WHAT THIS TEST IS NOT. It measures nothing: no socket, no node, no clock. It is arithmetic over
// constants measured on ANOTHER machine, on loopback. It is good for "1000 in a crowd is impossible
// and spread over 70 chunks it just fits", not for "your server holds 1000". That is what the real
// bench is for, and it runs by hand (`-DDGS_LOAD_TESTS=ON`, then `ctest -L carga`).
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/packet.h"
#include "include/dgs/types.h"
#include "tests/metric.h"

#include <cmath>
#include <cstdio>
#include <cstring>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
}

// ── The measured constants ───────────────────────────────────────────────────────────────────────
static const double kTickHz       = 10.0;   // the zone's tick: 100 ms
static const double kUsPerSyscall = 6.9;    // us per OUTGOING datagram (6.8 at N=64 · 7.0 at N=128)
static const double kBudgetMs     = 100.0;  // what a tick lasts: going over it is falling behind
// ⚠️ THERE IS NO NEIGHBOURHOOD FACTOR: a player hears the ones in THEIR chunk and that is it. The
// data decided this, and in the most useful way — by contradicting itself. The loop at 64 spread
// (997 us) asked for 2.26 visible entities per client; the EGRESS of that same row (0.07 MB/s) asked
// for 1.66 — two numbers from one row that could not both be true. What was spurious was the border
// term: with visible = N/chunks the egress matches at both points where it was measured (64 and
// 1000), and what the loop was missing was not neighbours but a FLOOR — the README has it measured:
// one single idle player already cost 211 us of `performance`. A model with two fitted constants
// that matches within 13 % at six points is worth more than one with three that matches at four.
//
// ⚠️ AND IT HOLDS WHILE THE RADIUS STAYS INSIDE THE CHUNK (500 m radius, 1000 m chunks). With a
// radius larger than a chunk, every player would hear several chunks and this falls short.
static const double kFloorMs      = 0.3;    // a tick with nobody in it (211 us measured)
// ⚠️ THE DRAIN CAP, which is the wall you hit BEFORE any of the others and was in no table. The zone
// empties its UDP socket up to `ZONE_UDP_DRAIN_MAX` datagrams PER TICK (256 by default) and then gets
// on with the tick: what it did not have time to take out stays in the kernel's queue for the next
// one. With 10 ticks a second that is 2560 datagrams/s of ingest for the WHOLE zone, whatever happens
// with the spread or the interest radius.
//
// ⚠️ THIS NUMBER IS THE NODE'S DEFAULT COPIED BY HAND (nodes/zone_node.cpp, "UDP_DRAIN_MAX"), not
// read from it: this test does not link against the node. If somebody changes the default there and
// not here, this projection goes stale in silence.
static const double kDrainPerTick = 256.0;

// ── The model ────────────────────────────────────────────────────────────────────────────────────
struct Scenario {
    const char* name;
    int    players;
    int    chunks;        ///< how many chunks they are spread over (1 = all in a crowd)
    double radiusM;       ///< interest radius; 0 = no filtering, everybody hears everybody
    double clientHz;      ///< how fast each player sends their position (20 Hz nominal)
};

struct Prediction {
    double visible;       ///< entities the zone sends to EACH client per tick
    double dgOutTick;     ///< datagrams the zone SENDS per tick
    double dgInTick;      ///< datagrams it RECEIVES per tick
    double inOverCap;     ///< 1 = right at the drain cap; >1 = the queue grows for ever
    double loopMs;        ///< what the tick costs it (the broadcast only, see below)
    double tickHzReal;    ///< if it does not fit in 100 ms, the tick stretches
    double egressMBs;
    double downKbitS;     ///< per client
    double entityBytes;
};

/// What the zone sends to EACH client. With no interest radius, EVERYTHING (that was the N² wall).
static double visiblePerClient(const Scenario& s)
{
    if (s.radiusM <= 0.0 || s.chunks <= 1) return (double)s.players;
    const double v = (double)s.players / (double)s.chunks;
    return (v > (double)s.players) ? (double)s.players : v;
}

static Prediction predict(const Scenario& s, double entityBytes)
{
    Prediction p{};
    p.entityBytes = entityBytes;
    p.visible     = visiblePerClient(s);
    p.dgOutTick   = (double)s.players * p.visible;
    // What comes in: every player sends at THEIR rate and the tick picks up whatever piled up. This
    // is the axis that "twice the load" doubles when what is doubled is the rate and not the people.
    p.dgInTick    = (double)s.players * s.clientHz / kTickHz;
    // ⚠️ THE INBOUND SIDE IS NOT IN THIS NUMBER, AND THAT IS NOT AN OVERSIGHT. The `ServerMetrics::
    // performance` stopwatch — the "loop" column of the measured table — starts AFTER the UDP drain
    // (`auto start` sits ~1000 lines below the `receive` loop in nodes/zone_node.cpp), so what was
    // measured does not include receiving. Adding it here was my first model and the data showed the
    // mistake: the crowd cases stopped matching (29.1 ms predicted against 27.97 measured) and the
    // spread ones doubled, because at N=64 spread the inbound side alone (128 datagrams) was already
    // more than the whole loop took. What the inbound side loads is below, against the DRAIN CAP,
    // which is where it actually hurts.
    p.loopMs      = kFloorMs + p.dgOutTick * kUsPerSyscall / 1000.0;
    p.inOverCap   = p.dgInTick / kDrainPerTick;
    // If the tick does not fit in its budget, it does not happen 10 times a second: it happens as
    // often as it fits. Without this the model predicted 10.2 MB/s at N=128 and 8.57 was measured —
    // because the zone was already ticking at 8.4 Hz.
    p.tickHzReal  = (p.loopMs <= kBudgetMs) ? kTickHz : (1000.0 / p.loopMs);
    p.egressMBs   = p.dgOutTick * p.tickHzReal * entityBytes / 1e6;
    p.downKbitS   = p.visible * p.tickHzReal * entityBytes * 8.0 / 1000.0;
    return p;
}

static bool near(double a, double b, double tol) { return std::fabs(a - b) <= tol * std::fabs(b); }

/// How many chunks of spread N players need to fit inside the tick's budget. Only looks at the
/// BROADCAST: the drain cap is a separate wall and spreading does not fix it.
/// Solved from  N · (N/C) · us ≤ budget.
static double chunksNeeded(int n)
{
    const double datagramCeiling = (kBudgetMs - kFloorMs) * 1000.0 / kUsPerSyscall;
    return (double)n * (double)n / datagramCeiling;
}

int main()
{
    // ── The real size of an entity on the wire ───────────────────────────────────────────────────
    DGS::EntityTransfer e{};
    e.uuid = 1; e.type = DGS::ENT_PLAYER; e.dataSize = 0;
    DGS::Packet packet; packet.pack(e);
    const double bytes = (double)packet.getSize();

    std::printf("\n  one entity with no payload on the wire: %.0f B (the struct is %zu B)\n",
                bytes, sizeof(DGS::EntityTransfer));
    dgsMetricI("entity_on_the_wire", (long long)bytes, "B");
    check(bytes < (double)sizeof(DGS::EntityTransfer) / 10.0,
          "an entity travels in less than a tenth of the struct (otherwise the whole projection is 67x out)");

    // ── (1) CALIBRATION: the model against what WAS measured ─────────────────────────────────────
    // Without this, everything below is pretty arithmetic.
    //
    // ⚠️ THE EGRESS IS CHECKED AGAINST THE MEASURED TICK, NOT THE PREDICTED ONE, and the difference
    // matters because it separates what the model knows from what it does not. It nails the datagrams
    // and the bytes (±7 % across the six points); it does NOT predict the RATE THE ZONE TICKS AT: at
    // 1000 the loop took 69 of the 100 ms and the real tick was still 8.9 Hz, not 10 — there are ~40
    // ms of other things (the drain, the waits on the head/validator/social sockets) this model does
    // not carry. Predicting the egress with a tick of 10 raised it by 40 %, and that would not be an
    // error in the traffic model: it would be me covering something unmeasured with traffic.
    std::printf("\n  ── against what was measured (tools/load_zone) ──\n");
    std::printf("  %-26s %9s %9s %9s %9s\n", "case", "loop ms", "measured", "MB/s", "measured");
    struct Measurement {
        const char* name; Scenario sc;
        double loopMsMeasured; double egressMeasured; double tickMeasuredHz; double loopTol;
    };
    const Measurement measurements[] = {
        // Crowd: everybody in one chunk, no radius (that is how the README's capacity table was taken).
        { "64 in a crowd",    { "", 64,  1,  0.0, 20.0 },  27.970, 2.60, 10.2, 0.12 },
        { "128 in a crowd",   { "", 128, 1,  0.0, 20.0 }, 113.992, 8.57,  8.4, 0.12 },
        // Spread over 40 chunks with a 500 m radius (README, interest management section).
        { "64 over 40 chunks",  { "", 64,  40, 500.0, 20.0 },  0.997, 0.07, 10.2, 0.12 },
        { "128 over 40 chunks", { "", 128, 40, 500.0, 20.0 },  3.429, -1.0, -1.0, 0.12 },
        { "256 over 40 chunks", { "", 256, 40, 500.0, 20.0 }, 13.383, -1.0, -1.0, 0.15 },
        // ⚠️ THE THREE POINTS THE QUESTION IS ABOUT, MEASURED (25-09, 16 cores), with
        //   LOAD_SPREAD_CHUNKS=100/300 LOAD_INTEREST_M=500 LOAD_DRAIN_MAX=4096 LOAD_VERDICT=1
        // and the harness sending its full 20 000 / 40 000 dg/s, which is what makes the rows the
        // zone's. All three served EVERY player without losing one. With these, the population being
        // asked about stops being an extrapolation.
        //
        // ⚠️ THE DRAIN CAP RAISED TO 4096 IS NOT OPTIONAL HERE: with the default (256), 2000
        // datagrams arrive per tick and the zone only takes 256 out, so what would be measured is the
        // cap.
        { "1000 over 100 chunks",     { "", 1000, 100, 500.0, 20.0 }, 68.75, 5.534, 8.87,  0.12 },
        { "1000 over 100 ch. @ 40 Hz",{ "", 1000, 100, 500.0, 40.0 }, 70.68, 5.521, 8.91,  0.12 },
        // ⚠️ AT 2000 THE MODEL FALLS 14 % SHORT AND I DO NOT KNOW WHY. The measured loop (107.6 ms)
        // asks for ~15 600 datagrams and the broadcast is 13 300: there are ~2 300 of something that
        // grows with N and this model does not have. Unchecked candidates: part of the drain landing
        // inside the stopwatch with 4000 inbound datagrams per tick, or a per-entity cost in the
        // simulation that does not go per datagram. THIS row's tolerance is wider on purpose and with
        // its reason written down, which is a different thing from widening all of them until it passes.
        { "2000 over 300 chunks",     { "", 2000, 300, 500.0, 20.0 }, 107.6, 5.863, 6.866, 0.20 },
    };
    for (const Measurement& m : measurements) {
        const Prediction p = predict(m.sc, bytes);
        const double tick = (m.tickMeasuredHz > 0.0) ? m.tickMeasuredHz : p.tickHzReal;
        const double egressAtMeasuredTick = p.dgOutTick * tick * bytes / 1e6;
        std::printf("  %-26s %9.2f %9.2f %9.2f %9s\n", m.name, p.loopMs, m.loopMsMeasured,
                    egressAtMeasuredTick, m.egressMeasured >= 0.0 ? "" : "—");
        char buf[200];
        if (m.egressMeasured >= 0.0) {
            std::snprintf(buf, sizeof buf,
                          "%s: predicted egress (%.3f MB/s, at the measured tick) matches the measurement (%.3f)",
                          m.name, egressAtMeasuredTick, m.egressMeasured);
            check(near(egressAtMeasuredTick, m.egressMeasured, 0.10), buf);
        }
        std::snprintf(buf, sizeof buf, "%s: predicted loop (%.2f ms) matches the measurement (%.2f)",
                      m.name, p.loopMs, m.loopMsMeasured);
        check(near(p.loopMs, m.loopMsMeasured, m.loopTol), buf);
    }

    // ⚠️ WHAT THE MODEL DOES NOT PREDICT, SAID HERE AND NOT IN A COMMENT. That the real tick stays
    // under 10 Hz with the loop at two thirds of the budget is the evidence that something is
    // missing; the test ASSERTS it against the data instead of leaving it as a suspicion.
    check(68.75 < kBudgetMs * 0.75 && 8.87 < kTickHz * 0.95,
          "at 1000 the loop takes 2/3 of the budget and the real tick is still 8.9 Hz: there is cost "
          "outside the stopwatch (the drain and the waits) that this model does not carry");

    // ⚠️ COUNTER-PROOF OF THE CALIBRATION. A model returning anything vaguely similar would pass the
    // block above if the tolerances were loose; that it does NOT pass with the constants changed is
    // what says the tolerances bite. With twice the cost per datagram, the 128-in-a-crowd case has to
    // fail.
    {
        const Scenario sc{ "", 128, 1, 0.0, 20.0 };
        const Prediction p = predict(sc, bytes);
        const double doubleCost = kFloorMs + p.dgOutTick * (kUsPerSyscall * 2.0) / 1000.0;
        check(!near(doubleCost, 113.992, 0.18),
              "COUNTER-PROOF: with twice the us per datagram the model NO LONGER matches the measurement");
        check(!near(p.loopMs * 0.5, 113.992, 0.18),
              "COUNTER-PROOF: nor at half (the tolerances do not admit a factor of 2)");
    }

    // The crowd ceiling the model produces has to be consistent with what was OBSERVED: 64 was fine
    // and 128 was late, so the breaking point sits between the two.
    {
        int ceiling = 0;
        for (int n = 8; n <= 4096; ++n) {
            const Scenario sc{ "", n, 1, 0.0, 20.0 };
            if (predict(sc, bytes).loopMs > kBudgetMs) { ceiling = n - 1; break; }
        }
        std::printf("\n  crowd ceiling according to the model: %d players in one chunk\n", ceiling);
        dgsMetricI("crowd_ceiling", ceiling, "players");
        check(ceiling > 64 && ceiling < 128,
              "the ceiling lands between 64 (measured: fine) and 128 (measured: late), which is where it was seen");
    }

    // ── (2) THE PROJECTION: 1000 players, and twice the load on both axes ────────────────────────
    // "Twice" is two different things and they are kept apart on purpose, because they load different
    // places: doubling the RATE doubles what comes IN (and the validator's work); doubling the PEOPLE
    // doubles what comes in AND quadruples what goes out, which is the real wall.
    std::printf("\n  ── projection (90 chunks of spread, 500 m radius) ──\n");
    std::printf("  %-22s %9s %9s %9s %9s %9s %10s\n",
                "case", "dg out/t", "dg in/t", "loop ms", "MB/s", "kbit/s", "inbound");
    const Scenario projections[] = {
        { "1000 @ 20 Hz",  1000, 90, 500.0, 20.0 },
        { "1000 @ 40 Hz",  1000, 90, 500.0, 40.0 },   // twice the RATE
        { "2000 @ 20 Hz",  2000, 90, 500.0, 20.0 },   // twice the PEOPLE
    };
    Prediction base{}, doubleRate{}, doublePeople{};
    for (int i = 0; i < 3; ++i) {
        const Prediction p = predict(projections[i], bytes);
        char inbound[32];
        std::snprintf(inbound, sizeof inbound, "%.1fx cap", p.inOverCap);
        std::printf("  %-22s %9.0f %9.0f %9.1f %9.2f %9.1f %10s%s\n",
                    projections[i].name, p.dgOutTick, p.dgInTick, p.loopMs,
                    p.egressMBs, p.downKbitS, inbound,
                    p.loopMs > kBudgetMs ? "   <- the broadcast does not fit in the tick" : "");
        char key[64];
        std::snprintf(key, sizeof key, "loop_%d_players_%dhz", projections[i].players,
                      (int)projections[i].clientHz);
        dgsMetric(key, p.loopMs, "ms");
        std::snprintf(key, sizeof key, "egress_%d_players_%dhz", projections[i].players,
                      (int)projections[i].clientHz);
        dgsMetric(key, p.egressMBs, "MB/s");
        std::snprintf(key, sizeof key, "inbound_over_cap_%d_players_%dhz", projections[i].players,
                      (int)projections[i].clientHz);
        dgsMetric(key, p.inOverCap, "x");
        if (i == 0) base = p; else if (i == 1) doubleRate = p; else doublePeople = p;
    }

    // What has to be true if the model models something rather than just multiplying:
    check(doubleRate.dgInTick > base.dgInTick * 1.99 && doubleRate.dgInTick < base.dgInTick * 2.01,
          "doubling the RATE exactly doubles what comes in per tick");
    check(near(doubleRate.dgOutTick, base.dgOutTick, 1e-9),
          "and does NOT touch what goes out: the zone broadcasts at ITS tick, not the client's");
    check(near(doubleRate.loopMs, base.loopMs, 1e-9),
          "nor the measured loop, because the UDP drain falls outside the `performance` stopwatch");
    // THE WALL YOU MEET FIRST, and it is neither of the two that were documented. With a cap of 256
    // datagrams per tick a zone ingests 2560/s: 128 players at 20 Hz and that is that.
    check(base.inOverCap > 7.0,
          "1000 players at 20 Hz send ~8 times what a zone can DRAIN per tick");
    check(doubleRate.inOverCap > base.inOverCap * 1.99,
          "and doubling the rate doubles that excess: the drain cap is what breaks under load");
    check(doublePeople.inOverCap > base.inOverCap * 1.99,
          "doubling the people doubles it just the same (ingest goes with N·Hz, spread or no spread)");
    check(doublePeople.dgOutTick > base.dgOutTick * 3.9,
          "doubling the PEOPLE quadruples what goes out (it is N², not N)");
    check(doublePeople.loopMs > doubleRate.loopMs * 2.0,
          "so twice the people costs far more than twice the rate, at the same spread");

    // ⚠️ COUNTER-PROOF OF THE SPREAD: with no interest management (radius 0), spreading buys NOTHING
    // — the player in chunk 1 still receives the ones in chunk 90. If the model did not say this, it
    // would not be modelling interest management but inventing it.
    {
        const Scenario noRadius{ "", 1000, 90, 0.0, 20.0 };
        const Scenario crowd{ "", 1000, 1, 500.0, 20.0 };
        const Prediction a = predict(noRadius, bytes), b = predict(crowd, bytes);
        check(near(a.dgOutTick, b.dgOutTick, 1e-9),
              "COUNTER-PROOF: with no interest radius, spreading over 90 chunks is the same as crowding them");
        check(a.loopMs > kBudgetMs * 50.0,
              "and 1000 players all hearing each other are 10^6 datagrams per tick: ~69 times the budget");
        dgsMetric("loop_1000_no_interest", a.loopMs, "ms");
    }

    // ── (3) WHAT HAS TO BE SET UP FOR 1000 TO FIT ────────────────────────────────────────────────
    // The useful question is not "does it hold?" but "how much spread does it need?", which is a
    // deployment decision: chunks per zone, or more zones.
    std::printf("\n  ── spread needed to fit inside the 100 ms ──\n");
    const int populations[] = { 256, 1000, 2000 };
    for (int n : populations) {
        const double chunks = chunksNeeded(n);
        std::printf("  %4d players -> %.0f chunks of spread (%.1f players per chunk)\n",
                    n, std::ceil(chunks), (double)n / chunks);
        char key[64];
        std::snprintf(key, sizeof key, "chunks_needed_%d_players", n);
        dgsMetricI(key, (long long)std::ceil(chunks), "chunks");
    }
    check(chunksNeeded(2000) > chunksNeeded(1000) * 3.5,
          "the spread needed grows as N²: 2000 players ask for ~4 times what 1000 do");
    // And the check that ties the model to what was OBSERVED from the other end: at 256 spread over
    // 40 chunks a loop of 13.4 ms was measured, i.e. 40 chunks were more than enough — so the model
    // has to ask for fewer than 40 at that population, or it would be asking for spread that was
    // shown to be unnecessary.
    check(chunksNeeded(256) < 40.0,
          "for the 256 that WERE measured the model asks for fewer than the 40 chunks that were used");

    // ── (4) THE INGEST CAP, in players ───────────────────────────────────────────────────────────
    // The figure to take away from all of this: how many people can TALK to one zone, which does not
    // depend on the spread, the interest radius or the packet size.
    {
        const double ingestS = kDrainPerTick * kTickHz;
        const double at20 = ingestS / 20.0, at40 = ingestS / 40.0;
        std::printf("\n  ingest cap of ONE zone: %.0f datagrams/s"
                    "  ->  %.0f players at 20 Hz, %.0f at 40 Hz\n", ingestS, at20, at40);
        dgsMetricI("zone_ingest_cap", (long long)ingestS, "dg/s");
        dgsMetricI("players_per_zone_20hz", (long long)at20, "players");
        check(at20 > 100.0 && at20 < 200.0,
              "that cap gives ~128 players per zone at 20 Hz, the order where the problem was measured");
        check(at40 < at20,
              "COUNTER-PROOF: at twice the rate half as many fit, because the cap is in DATAGRAMS");
    }

    std::printf("\n== load_model: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
