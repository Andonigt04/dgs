// ─────────────────────────────────────────────────────────────────────────────────────────────────
// ⚠️ This used to not be a test: it wrote 10 entries, slept 100 ms and did `return 0` without ever
// reopening the file. If the logger had written NOTHING it would still have come out green — hence
// CI's `timeout 5 ./logger_test || true`.
//
// And there was a second reason it could not check anything: `Logger` opens in APPEND mode and the
// test always wrote to `server_logs.csv` in the current directory, so rows piled up between runs and
// counting was impossible. It now uses its own file, removed before starting.
//
// What is checked is what the logger actually promises: that the header is there, that EVERY entry
// arrives, IN ORDER, and with the fields it was given (not defaulted zeros).
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/logger.h"

#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
}

int main()
{
    const std::string path = "logger_test_out.csv";
    std::remove(path.c_str());          // APPEND: without this it piles up on the previous run

    constexpr int kEntries = 10;
    {
        DGS::Logger logger(path);
        DGS::LogEntry e{};
        e.time_stamp  = 1715000000;
        e.type        = DGS::LOG_TRANSFER;
        e.entityType  = DGS::ENT_PLAYER;
        e.fd          = 5;
        e.bytes       = 226;
        e.ramUsage    = 0.45f;
        e.performance = 0.12f;

        for (int i = 0; i < kEntries; ++i) { e.uuid = (uint32_t)i; logger.log(e); }
    }   // <- destroying the Logger drains its pool and closes the ofstream: the file is complete here

    std::ifstream in(path);
    check(in.good(), "the log file exists and can be read");
    if (!in.good()) { std::printf("\n== logger: %d OK · %d FAILED ==\n", g_pass, g_fail); return 1; }

    std::vector<std::string> lines;
    for (std::string l; std::getline(in, l); ) if (!l.empty()) lines.push_back(l);

    check(!lines.empty() && lines[0] == "timestamp,type,entityType,uuid,fd,bytes,ram,performance",
          "the first line is the CSV header");
    check((int)lines.size() == kEntries + 1,
          "there are exactly 10 data rows (none lost on close, none extra)");
    if ((int)lines.size() != kEntries + 1)
        std::printf("         (there were %zu lines including the header)\n", lines.size());

    // Content: the uuids run 0..9 IN ORDER (the pool has 1 thread, so ordering is a promise) and the
    // fields are the ones passed in. Counting rows is not enough: ten rows of zeros are also ten.
    bool ordered = true, fieldsOk = true;
    for (int i = 1; i < (int)lines.size(); ++i) {
        std::stringstream ss(lines[i]);
        std::string field; std::vector<std::string> c;
        while (std::getline(ss, field, ',')) c.push_back(field);
        if (c.size() != 8) { fieldsOk = false; break; }
        if (c[3] != std::to_string(i - 1)) ordered = false;   // uuid
        if (c[5] != "226")                 fieldsOk = false;  // bytes
    }
    check(ordered,  "the uuids come out 0..9 IN ORDER (the 1-thread pool guarantees it)");
    check(fieldsOk, "every row has 8 columns and keeps its data (bytes=226, not zeros)");

    std::remove(path.c_str());
    std::printf("\n== logger: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
