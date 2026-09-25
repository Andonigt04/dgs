#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────────────────────────
# ci_test_summary.py — the tests' figures, on the job's page in GitHub Actions.
#
# The tests in this repo measure things: how many checks pass, how long each one takes, the loopback
# RTT, the bytes on the wire, the false positives the validator produces on a degraded network. All
# of it was printed and all of it stayed in the job's LOG: open it, expand the step, find the line,
# compare it by eye against another day's. Nobody does that, so a metric that gets worse between two
# commits is seen by no one.
#
# This reads the JUnit XML that CTest writes (`ctest --output-junit`) and produces MARKDOWN for
# `$GITHUB_STEP_SUMMARY`, which is the page you land on when you open the run.
#
# ⚠️ THE XML CARRIES EACH TEST'S OUTPUT, BUT CLIPPED TO 1024 BYTES by default — and it clips from the
# END, which is exactly where a test prints its summary line and its metrics. Measured: `udp_crypto`
# arrived here with "[This part of the test output was removed since it exceeds the threshold of 1024
# bytes.]" and without its OK/FAILED line. That is why the workflow passes
# `--test-output-size-passed/--test-output-size-failed`, and why this script WARNS in the summary
# when it sees the clipping: a missing number has to be noticeable, not invisible.
#
# It decides nothing: the verdict is `ctest`'s exit code. This only publishes.
#
#   ctest --test-dir build --output-junit build/ctest.xml ...
#   python3 tools/ci_test_summary.py build/ctest.xml --title "Tests" >> $GITHUB_STEP_SUMMARY
#   python3 tools/ci_test_summary.py --selftest      # it tests itself (CTest runs this)
# ─────────────────────────────────────────────────────────────────────────────────────────────────
import argparse
import re
import sys
import xml.etree.ElementTree as ET

# The three shapes in which this repo's tests count their checks. Three and not one because they were
# written at different times; unifying them is a separate change, and meanwhile the summary cannot go
# blank for the ones that do not follow the majority shape.
RE_SUMMARY = re.compile(r"^==\s*[^:\n]+:\s*(\d+)\s+OK\s*\S?\s*(\d+)\s+FAILED\s*==", re.M)
RE_CHECKS  = re.compile(r"(\d+)\s+checks?,\s*(\d+)\s+failures?", re.M)
RE_OK_MARK = re.compile(r"^\s*\[ok\]", re.M)
RE_FAIL_MK = re.compile(r"^\s*\[FAIL\]\s*(.*)$", re.M)
# ⚠️ SPACES AND TABS, NOT `\s`. With `\s*` before the unit, the newline counted as space and the unit
# swallowed the WHOLE next line: a metric with no unit came out in the summary as
# "0 METRIC dropped_datagrams_clean 0". Seen on the first real run (`net_degraded`), not in the
# fixtures — which all carried a unit.
RE_METRIC  = re.compile(r"^METRIC[ \t]+(\S+)[ \t]+(-?[0-9][0-9a-zA-Z.+-]*)[ \t]*([^\n\r]*)$", re.M)
RE_TRUNC   = re.compile(r"exceeds the threshold of \d+ bytes")
# ⚠️ A SKIP CTEST CANNOT SEE. `tls_test` (and others) print "== tls_test: skipped ==" and exit 0 when
# the certificates are missing: to CTest that is a PASSED test, and it showed up green here with zero
# checks. It is the same hole ci.yml itself warns about — "a skip nobody notices is a test that does
# not exist" — one level further down. It is not turned into a failure here (that decision belongs to
# the test, not to the dashboard): it is MARKED, which is what was missing.
RE_SKIPPED = re.compile(r"^==\s*[^:\n]+:\s*skipped\s*==", re.M | re.I)


def checks_of(out):
    """(ok, failed, where_from). `None` when the test counts no checks in any of the shapes."""
    m = list(RE_SUMMARY.finditer(out))
    if m:
        return int(m[-1].group(1)), int(m[-1].group(2)), "summary"
    m = list(RE_CHECKS.finditer(out))
    if m:
        return int(m[-1].group(1)), int(m[-1].group(2)), "checks"
    ok, fail = len(RE_OK_MARK.findall(out)), len(RE_FAIL_MK.findall(out))
    if ok or fail:
        return ok, fail, "marks"
    return None


def parse(xml_text):
    """CTest's XML -> a list of tests with what has to be shown for each one."""
    root = ET.fromstring(xml_text)
    tests = []
    for tc in root.iter("testcase"):
        out = (tc.findtext("system-out") or "") + "\n" + (tc.findtext("system-err") or "")
        status = (tc.get("status") or "").lower()
        # CTest marks a failure with a <failure> child; `status` says "run"/"fail"/"notrun".
        failed = tc.find("failure") is not None or status in ("fail", "failed")
        skipped = (tc.find("skipped") is not None or status in ("notrun", "disabled")
                   or bool(RE_SKIPPED.search(out)))
        try:
            secs = float(tc.get("time") or 0.0)
        except ValueError:
            secs = 0.0
        tests.append({
            "name": tc.get("name") or "?",
            "failed": failed,
            "skipped": skipped,
            "secs": secs,
            "checks": checks_of(out),
            "fails": [f.strip() for f in RE_FAIL_MK.findall(out) if f.strip()],
            "metrics": [(k, v, u.strip()) for k, v, u in RE_METRIC.findall(out)],
            "truncated": bool(RE_TRUNC.search(out)),
        })
    return tests


def render(tests, title):
    total = len(tests)
    failed = [t for t in tests if t["failed"]]
    skipped = [t for t in tests if t["skipped"] and not t["failed"]]
    ok_checks = sum(t["checks"][0] for t in tests if t["checks"])
    bad_checks = sum(t["checks"][1] for t in tests if t["checks"])
    secs = sum(t["secs"] for t in tests)

    L = []
    L.append("## %s" % title)
    L.append("")
    if total == 0:
        # An empty XML is a result, and a bad one: it means CTest ran no tests at all.
        L.append("⚠️ **the XML carries not a single test** — `ctest` never ran, or was pointed elsewhere.")
        return "\n".join(L) + "\n"
    verdict = ("❌ %d of %d failing" % (len(failed), total)) if failed else ("✅ %d of %d passing" % (total, total))
    line = "%s · %d checks (%d failed) · %.1f s" % (verdict, ok_checks, bad_checks, secs)
    if skipped:
        line += " · %d skipped" % len(skipped)
    L.append(line)
    L.append("")

    L.append("| test | | time | checks | failed |")
    L.append("|---|:--:|--:|--:|--:|")
    for t in sorted(tests, key=lambda x: (not x["failed"], -x["secs"])):
        icon = "❌" if t["failed"] else ("⏭️" if t["skipped"] else "✅")
        c = t["checks"]
        L.append("| `%s` | %s | %.2f s | %s | %s |" % (
            t["name"], icon, t["secs"],
            str(c[0]) if c else "—",
            (("**%d**" % c[1]) if c[1] else "0") if c else "—"))
    L.append("")

    # The checks that failed, RIGHT HERE: if you have to open the log to find out which one, the
    # summary has done nothing for you.
    bad = [(t["name"], f) for t in tests for f in t["fails"]]
    if bad:
        L.append("### Failed checks")
        L.append("")
        for name, f in bad:
            L.append("- `%s` — %s" % (name, f))
        L.append("")

    metrics = [(t["name"], k, v, u) for t in tests for (k, v, u) in t["metrics"]]
    if metrics:
        L.append("### Metrics")
        L.append("")
        L.append("| test | metric | value |")
        L.append("|---|---|--:|")
        for name, k, v, u in metrics:
            L.append("| `%s` | %s | %s%s |" % (name, k, v, (" " + u) if u else ""))
        L.append("")
        L.append("<sub>The tests measure these and they decide nothing: the verdict is the checks'.</sub>")
        L.append("")

    clipped = [t["name"] for t in tests if t["truncated"]]
    if clipped:
        L.append("> ⚠️ CTest clipped the output of %s, so their figures may be missing here. "
                 "Raise the limit with `--test-output-size-passed`/`--test-output-size-failed`."
                 % ", ".join("`%s`" % c for c in clipped))
        L.append("")
    return "\n".join(L) + "\n"


# ── The collector's own test ─────────────────────────────────────────────────────────────────────
# A counter-proof in every block: it is not enough that what is there comes out, what is NOT there
# must stay out. Without that, a `render` that always printed the metrics table would pass too.
def selftest():
    # `checks` arrives already evaluated against the block's `md`: this only counts and prints.
    def case(name, checks):
        for descr, cond in checks:
            print("  [%s] %s · %s" % ("ok" if cond else "FAIL", name, descr))
            if not cond:
                case.bad += 1
    case.bad = 0

    xml_ok = """<?xml version="1.0"?><testsuite>
      <testcase name="wire" time="0.5" status="run"><system-out>
[wire_test] 66 checks, 0 failures
METRIC entity_on_the_wire 62 B
</system-out></testcase>
      <testcase name="ping_pong" time="1.25" status="run"><system-out>
  [ok]   one
METRIC rtt_loopback 0.214 ms
== ping_pong: 7 OK &#183; 0 FAILED ==
</system-out></testcase></testsuite>"""
    md = render(parse(xml_ok), "T")
    case("green", [
        ("says both pass",                   "✅ 2 of 2 passing" in md),
        ("adds the checks up (66+7)",        "73 checks" in md),
        ("adds both times up",               "1.8 s" in md),
        ("pulls the metric with its unit",   "| 0.214 ms |" in md),
        ("and the other test's",             "entity_on_the_wire" in md),
        ("COUNTER-PROOF: no failures, no failed-checks section",
                                             "Failed checks" not in md),
        ("COUNTER-PROOF: no clipping notice", "clipped" not in md),
    ])

    # A metric with NO unit, with another line behind it. The case that really broke: if the space
    # before the unit admits a newline, the unit eats the next line.
    xml_no_unit = """<?xml version="1.0"?><testsuite>
      <testcase name="net_degraded" time="1" status="run"><system-out>
METRIC false_positives_clean 0
METRIC dropped_datagrams_clean 6
    5 % loss    55    6
  [ok]   with 20 % loss it accuses nobody
</system-out></testcase></testsuite>"""
    md = render(parse(xml_no_unit), "T")
    case("no unit", [
        ("the unitless metric stands alone", "| 0 |" in md),
        ("so does the second one",           "| 6 |" in md),
        ("COUNTER-PROOF: it does not eat the line next to it",
                                             "dropped_datagrams_clean 6" not in md),
        ("COUNTER-PROOF: nor the console table",
                                             "5 % loss" not in md),
    ])

    xml_bad = """<?xml version="1.0"?><testsuite>
      <testcase name="zone_e2e" time="2" status="fail"><failure message="x"/><system-out>
  [ok]   one that does
  [FAIL] the zone never returns the lease
== zone_e2e: 1 OK &#183; 1 FAILED ==
</system-out></testcase>
      <testcase name="wire" time="0.1" status="run"><system-out>[wire_test] 66 checks, 0 failures</system-out></testcase>
      </testsuite>"""
    md = render(parse(xml_bad), "T")
    case("red", [
        ("says how many fail",               "❌ 1 of 2 failing" in md),
        ("counts the failed check",          "(1 failed)" in md),
        ("names the one that failed, no log needed", "the zone never returns the lease" in md),
        ("the broken test comes first",      md.index("zone_e2e") < md.index("wire")),
        ("COUNTER-PROOF: no metrics, no metrics table", "### Metrics" not in md),
    ])

    xml_skip = """<?xml version="1.0"?><testsuite>
      <testcase name="tls" time="0.01" status="run"><system-out>
  SKIPPED: no certificates in /tmp/dgs-tls.
== tls_test: skipped ==
</system-out></testcase>
      <testcase name="wire" time="0.1" status="run"><system-out>[wire_test] 66 checks, 0 failures</system-out></testcase>
      </testsuite>"""
    md = render(parse(xml_skip), "T")
    case("skipped", [
        ("the test that skips itself gets marked", "⏭️" in md),
        ("and is counted apart",                   "1 skipped" in md),
        ("without stopping both counting as passed", "✅ 2 of 2 passing" in md),
        ("COUNTER-PROOF: the one that does not skip carries no mark",
                                                   md.count("⏭️") == 1),
    ])

    xml_trunc = """<?xml version="1.0"?><testsuite><testcase name="udp_crypto" time="0.1" status="run">
      <system-out>  [ok]   something
[This part of the test output was removed since it exceeds the threshold of 1024 bytes.]</system-out>
      </testcase></testsuite>"""
    md = render(parse(xml_trunc), "T")
    case("clipped", [
        ("warns about CTest's clipping",     "clipped" in md),
        ("and says which test",              "`udp_crypto`" in md),
        ("falls back to counting [ok] marks", "| 1 |" in md),
    ])

    md = render(parse('<?xml version="1.0"?><testsuite></testsuite>'), "T")
    case("empty", [
        ("an XML with no tests is called out as a problem", "not a single test" in md),
        ("COUNTER-PROOF: and not as a success",             "passing" not in md),
    ])

    print("== ci_test_summary: %s ==" % ("0 FAILED" if case.bad == 0 else "%d FAILED" % case.bad))
    return 1 if case.bad else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("junit", nargs="?", help="XML from `ctest --output-junit`")
    ap.add_argument("--title", default="Tests")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        return selftest()
    if not a.junit:
        ap.error("the XML is required (or --selftest)")
    try:
        with open(a.junit, encoding="utf-8", errors="replace") as f:
            xml = f.read()
    except OSError as e:
        # With no XML there is no summary, but the step must NOT take the job down: ctest already
        # gave the verdict.
        sys.stdout.write("## %s\n\n⚠️ could not read `%s`: %s\n" % (a.title, a.junit, e))
        return 0
    try:
        sys.stdout.write(render(parse(xml), a.title))
    except ET.ParseError as e:
        sys.stdout.write("## %s\n\n⚠️ CTest's XML cannot be read: %s\n" % (a.title, e))
    return 0


if __name__ == "__main__":
    sys.exit(main())
