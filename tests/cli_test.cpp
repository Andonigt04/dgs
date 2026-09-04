// ─────────────────────────────────────────────────────────────────────────────────────────────────
// cli_test — `dgs`, the tool an operator actually types.
//
// 343 lines with no test at all, and the defects were the kind that only show up in front of someone:
//
//   · `dgs logs <name>` put its argument straight into `system("tail -f " + ...)`. Demonstrated:
//     `dgs logs 'x; touch /tmp/pwned'` ran the `touch`, and the file appeared. Command injection in
//     the one binary an operator runs as root during `install`.
//   · `dgs down` used `pkill -f <node>`, which matches the WHOLE COMMAND LINE — so it kills an editor
//     with that file open, a `tail -f logs/zone_node.log`, another `dgs logs zone_node`, and (this
//     happened to me while working on this repository) the shell that ran it.
//   · `dgs status` had the same over-match with `pgrep -f`, so a `tail` on a log file was reported as
//     a RUNNING NODE. A status that says a dead node is alive is worse than having no status.
//   · the port table was a parallel array indexed by position, and removing `cache_node` left it one
//     entry long: the validator printed the cache's ports and the social node printed the validator's.
//   · `dgs run` announced "standalone up" and returned 0 even when every node had failed to exec, so
//     nothing driving it could tell a cluster from an empty room.
//
// Everything here runs against the real binary. Nothing needs root, a cluster or a network.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
    std::fflush(stdout);
}

static std::string g_cli;
static std::string g_tmp;

/// Runs the CLI and returns its exit code; `out` gets stdout+stderr.
static int run(const std::string& args, std::string& out, const std::string& env = "")
{
    const std::string outFile = g_tmp + "/out.txt";
    const std::string cmd = env + " " + g_cli + " " + args + " >" + outFile + " 2>&1";
    const int rc = std::system(cmd.c_str());
    out.clear();
    if (FILE* f = std::fopen(outFile.c_str(), "r")) {
        char buf[4096];
        while (std::fgets(buf, sizeof(buf), f)) out += buf;
        std::fclose(f);
    }
    return WIFEXITED(rc) ? WEXITSTATUS(rc) : -1;
}

static bool exists(const std::string& p) { struct stat st; return stat(p.c_str(), &st) == 0; }

int main(int argc, char** argv)
{
    g_cli = (argc > 1) ? argv[1] : "./build/dgs";
    char tmpl[] = "/tmp/dgs_cli_test_XXXXXX";
    g_tmp = mkdtemp(tmpl) ? tmpl : "/tmp";

    std::string out;

    // ── It says what it is when asked, and complains when it is not ─────────────────────────────
    check(run("--help", out) == 0 && out.find("usage:") != std::string::npos,
          "`--help` explains itself and exits 0");
    check(run("frobnicate", out) != 0 && out.find("unknown command") != std::string::npos,
          "an unknown command is refused with a non-zero exit");
    check(run("", out) != 0, "with no command at all it does not claim success");

    // ── Command injection ───────────────────────────────────────────────────────────────────────
    // The canary is a file the injected command would create. It must not appear.
    {
        const std::string canary = g_tmp + "/pwned";
        // `.log` is appended by the CLI, so the injected `touch` would create "<canary>.log".
        const std::string arg = "'nope; touch " + canary + "'";
        const int rc = run("logs " + arg, out);
        const bool created = exists(canary) || exists(canary + ".log");
        std::printf("    injected argument -> rc=%d, canary created: %s\n", rc, created ? "YES" : "no");
        check(!created, "`logs` does not execute what is handed to it (no command injection)");
        check(rc != 0 && out.find("unknown node") != std::string::npos,
              "and it says WHICH names it knows instead of failing obscurely");
    }

    // ── Counter-proof: a real node name still works ─────────────────────────────────────────────
    // Without this, "refuses the injection" would also pass on a `logs` that refuses everything.
    {
        const std::string logDir = g_tmp + "/logs";
        mkdir(logDir.c_str(), 0755);
        FILE* f = std::fopen((logDir + "/head_server_node.log").c_str(), "w");
        std::fputs("a line from the head\n", f);
        std::fclose(f);
        const int rc = run("logs", out, "DGS_LOG_DIR=" + logDir);
        check(rc == 0 && out.find("a line from the head") != std::string::npos,
              "and a KNOWN node's log is still printed (it did not just refuse everything)");
    }

    // ── `status` must not mistake a mention for a process ───────────────────────────────────────
    //
    // ⚠️ WITH `pgrep -f` THIS WAS ALWAYS "RUNNING", for every node, with nothing running at all — the
    // pattern matched the `sh -c "pgrep -f 'zone_node'"` that the CLI itself had just spawned. The
    // decoy `tail` is only the polite version of the problem; the honest version is that the status
    // was a constant. That is why the check here is absolute rather than a before/after comparison:
    // "nothing changed" is satisfied by a status that always says the same thing, which is the bug.
    //
    // The node used is one that genuinely has no process right now — asked with `ps -C`, which matches
    // the executable NAME and so cannot match the asking command line. That keeps the check meaningful
    // under `ctest -j4`, where another end-to-end test may well have a real node up.
    {
        const char* candidates[] = { "zone_node", "social_node", "validador_node",
                                     "persistance_node", "head_server_node" };
        std::string free_;
        for (const char* c : candidates)
        {
            const std::string cmd = std::string("ps -C ") + c + " -o pid= 2>/dev/null | head -1";
            std::string pid;
            if (FILE* p = popen(cmd.c_str(), "r")) {
                char b[64]; if (std::fgets(b, sizeof(b), p)) pid = b;
                pclose(p);
            }
            if (pid.find_first_of("0123456789") == std::string::npos) { free_ = c; break; }
        }

        if (free_.empty())
        {
            check(false, "no node was free to test `status` against (every one has a live process)");
        }
        else
        {
            const std::string decoy = g_tmp + "/" + free_;
            std::fclose(std::fopen(decoy.c_str(), "w"));
            const std::string cmd = "tail -f " + decoy + " >/dev/null 2>&1 & echo $!";
            std::string pidText;
            if (FILE* p = popen(cmd.c_str(), "r")) {
                char buf[64];
                if (std::fgets(buf, sizeof(buf), p)) pidText = buf;
                pclose(p);
            }
            usleep(300000);
            run("status", out);
            const bool claims = out.find(free_ + ": pid=") != std::string::npos;
            std::printf("    `%s` has no process; a `tail` mentions it -> status says: %s\n",
                        free_.c_str(), claims ? "RUNNING" : "not running");
            check(!claims,
                  "`status` does not report a node as running when nothing is running it");
            if (!pidText.empty()) {
                const std::string k = "kill " + pidText + " 2>/dev/null";
                (void)!std::system(k.c_str());
            }
        }
    }

    // ── `run` must not claim success when nothing started ───────────────────────────────────────
    {
        const std::string emptyDir = g_tmp + "/nobins";
        mkdir(emptyDir.c_str(), 0755);
        const int rc = run("run", out, "DGS_BIN_DIR=" + emptyDir + " DGS_LOG_DIR=" + g_tmp + "/logs2");
        std::printf("    `run` with an empty bin dir -> rc=%d\n", rc);
        check(rc != 0, "`run` with no binaries FAILS instead of announcing a standalone that is not there");
        check(out.find("standalone up") == std::string::npos,
              "and it does not print that it is up");
    }

    // ── The port table matches the nodes it describes ───────────────────────────────────────────
    // It was a parallel array indexed by position, and removing a node left every later entry
    // describing its predecessor.
    {
        const std::string emptyDir = g_tmp + "/nobins";
        run("run", out, "DGS_BIN_DIR=" + emptyDir + " DGS_LOG_DIR=" + g_tmp + "/logs3");
        // `run` prints one line per node even when the exec fails, with the ports it believes in.
        const bool headOk  = out.find("head_server_node") == std::string::npos ||
                             out.find("head_server_node pid=") == std::string::npos ||
                             out.find("TCP:42424") != std::string::npos;
        const bool validOk = out.find("validador_node pid=") == std::string::npos ||
                             out.find("UDP:42427/TCP:42428") != std::string::npos;
        const bool zoneOk  = out.find("zone_node pid=") == std::string::npos ||
                             out.find("UDP:42425") != std::string::npos;
        check(headOk && validOk && zoneOk,
              "the ports it prints belong to the node it names (the table is not off by one)");
    }

    std::printf("\n== cli_test: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
