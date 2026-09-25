#pragma once
// ─────────────────────────────────────────────────────────────────────────────────────────────────
// THE TESTS' METRIC CHANNEL: one line per measured number, in a shape a machine can read.
//
//      METRIC <key> <value> [<unit>]
//
// The tests in this repo already measured things and already printed them — "loopback RTT: 0.214 ms",
// "zona: 3 pings, rtt ultimo 1.02 ms", "the proxy failed to forward 12 of 400 datagrams" — each one
// in its own sentence. That reads fine on the console of whoever launched it and nowhere else: in
// GitHub Actions it is buried in the job's log, which has to be opened, expanded and searched. A
// figure that gets worse between two commits is seen by nobody. `tools/ci_test_summary.py` collects
// these lines out of CTest's XML and puts them on the run's summary page, which is what you land on.
//
// ⚠️ IT DOES NOT REPLACE THE TEST'S SENTENCE, it goes NEXT TO it. The sentence carries the context
// ("three pings 100 ms apart, the zone echoes the sealed pong") and is what makes a failure legible;
// the METRIC line carries no context at all, only the number.
//
// ⚠️ AND IT DOES NOT REPLACE A `check`: a metric decides nothing. The verdict still belongs to the
// check, which is the thing that holds the threshold ("RTT < 50 ms on loopback"). Publishing the
// number is so it can be WATCHED; making it fail is a different decision and it is taken in the
// check, not here. A threshold hidden in a dashboard is a test you cannot read.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include <cstdio>

/// A measured number. The key with no spaces (`rtt_loopback`); the unit is free text and may be absent.
inline void dgsMetric(const char* key, double value, const char* unit = "") {
    std::printf("METRIC %s %.4g%s%s\n", key, value, (unit && *unit) ? " " : "", unit ? unit : "");
}

/// The same, for exact counts (bytes, datagrams, violations): an integer must not come out as 1e+06.
inline void dgsMetricI(const char* key, long long value, const char* unit = "") {
    std::printf("METRIC %s %lld%s%s\n", key, value, (unit && *unit) ? " " : "", unit ? unit : "");
}
