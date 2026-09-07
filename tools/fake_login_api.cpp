// ─────────────────────────────────────────────────────────────────────────────────────────────────
// fake_login_api — the front door, faked, so a client can get through it in a test.
//
// ⚠️ THIS ACCEPTS EVERY PASSWORD. It is a stub for development and recording, it is not authentication,
// and it must never be deployed. It is a separate binary with an unmistakable name for that reason,
// it binds to 127.0.0.1 unless told otherwise, it prints what it is on every start, and it is in no
// k8s manifest or Dockerfile. If you find this running anywhere that matters, that is the incident.
//
// WHY IT EXISTS. `DGS::Client::connect` does a `POST /api/auth/login` and refuses to go any further
// on anything but a 200 — before it has even asked the head which zone it belongs to. Nothing in this
// repository implements that endpoint: grep for `/api/auth/login` and you find the client and the two
// tests that fake it in-process. So a real game client cannot connect to a real cluster at all, which
// is a strange thing for a system with 30 passing end-to-end tests, and it is exactly the kind of gap
// those tests cannot see: every one of them stands up its own fake.
//
// WHAT IT HANDS BACK, and why it is shaped like this. A session id and its UDP key belong in the login
// response — that is the plumbing the README lists as missing for per-session UDP keys. The key here is
// derived EXACTLY as the servers derive it (`HMAC-SHA256(SHA256(DGS_UDP_MASTER), session)`), so what a
// client is given is the key a zone will actually accept. That makes this stub a working description of
// the endpoint somebody has to write for real.
//
// The client ADOPTS these now (`Client::applyLoginKeys` -> `DGS::setUdpSessionKey`), so an encrypted
// game plane no longer needs a key in the player's environment. It still ignores the token: nothing
// downstream asks for one yet.
//
//   fake_login_api [port]        (default 8080, which is what Survival's menu defaults to)
//   FAKE_API_HOST   127.0.0.1    bind address — change it and you are on your own
//   DGS_UDP_MASTER  (unset)      when set, each login gets a real per-session UDP key
//   DGS_UDP_KEY     (unset)      when set, the login also hands out the group key (to READ the world)
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include <httplib.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <atomic>
#include <cstdio>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>

/// The GROUP key, as hex — the one the zone seals its broadcast with. A client cannot open the world
/// without it, so a login that hands out a session key and not this one leaves the player deaf.
/// `DGS_UDP_KEY` is a passphrase everywhere else in the system (SHA-256 of it is the key), so what goes
/// on the wire here is that hash, not the passphrase: the client decodes hex and does not re-hash.
static double envDouble(const char* name, double def)
{
    const char* v = std::getenv(name);
    return v ? std::atof(v) : def;
}

/// Milliseconds since the Unix epoch, as a double so it survives JSON without a 64-bit integer type.
static double nowMs()
{
    return (double)std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

/// The world's origin. Fixed for the life of the process so every login in a session agrees, and
/// overridable so a cluster that restarts does not restart its world with it.
static double worldEpochMs()
{
    static const double e = [] {
        const char* v = std::getenv("WORLD_EPOCH_MS");
        return v ? std::atof(v) : nowMs();
    }();
    return e;
}

static std::string groupKeyHex()
{
    const char* k = std::getenv("DGS_UDP_KEY");
    if (!k || !*k) return "";
    unsigned char out[32];
    SHA256((const unsigned char*)k, std::strlen(k), out);
    static const char* hex = "0123456789abcdef";
    std::string s;
    s.reserve(64);
    for (int i = 0; i < 32; ++i) { s += hex[out[i] >> 4]; s += hex[out[i] & 0xF]; }
    return s;
}

/// The key a zone would derive for this session, as hex. Empty when there is no master configured,
/// which is the ordinary case: then the client falls back to the group key and everything still works.
static std::string sessionKeyHex(uint32_t session)
{
    const char* master = std::getenv("DGS_UDP_MASTER");
    if (!master || !*master) return "";

    unsigned char mk[32];
    SHA256((const unsigned char*)master, std::strlen(master), mk);

    unsigned char out[32];
    unsigned int len = 0;
    HMAC(EVP_sha256(), mk, 32, (const unsigned char*)&session, sizeof(session), out, &len);
    if (len != 32) return "";

    static const char* hex = "0123456789abcdef";
    std::string s;
    s.reserve(64);
    for (int i = 0; i < 32; ++i) { s += hex[out[i] >> 4]; s += hex[out[i] & 0xF]; }
    return s;
}

/// The email, for the log line only. Deliberately not a JSON parser: the body is
/// `{"email":"...","password":"..."}` and this is a stub, not a service.
static std::string emailFrom(const std::string& body)
{
    const std::string key = "\"email\":\"";
    const size_t a = body.find(key);
    if (a == std::string::npos) return "<none>";
    const size_t b = body.find('"', a + key.size());
    if (b == std::string::npos) return "<none>";
    return body.substr(a + key.size(), b - a - key.size());
}

int main(int argc, char** argv)
{
    const int port = (argc > 1) ? std::atoi(argv[1]) : 8080;
    const char* host = std::getenv("FAKE_API_HOST") ? std::getenv("FAKE_API_HOST") : "127.0.0.1";

    std::atomic<uint32_t> nextSession{1};

    httplib::Server api;

    api.Post("/api/auth/login", [&](const httplib::Request& req, httplib::Response& res) {
        const uint32_t session = nextSession.fetch_add(1);
        const std::string key  = sessionKeyHex(session);

        const std::string group = groupKeyHex();

        std::string body = "{\"token\":\"fake-" + std::to_string(session) + "\"";
        body += ",\"session\":" + std::to_string(session);
        // Where the chat service is. A client should not have to be told this by its environment: the
        // login is the one place that knows which cluster it just let you into.
        body += ",\"socialHost\":\"" + std::string(std::getenv("SOCIAL_HOST") ? std::getenv("SOCIAL_HOST") : "127.0.0.1") + "\"";
        body += ",\"socialPort\":" + std::string(std::getenv("SOCIAL_TCP_PORT") ? std::getenv("SOCIAL_TCP_PORT") : "42430");
        // ⚠️ THE WORLD CLOCK, handed out with the keys and for the same reason: it is a cluster-wide
        // constant a client cannot invent for itself. Without it the engine starts its simulation
        // clock at zero and accumulates frame deltas, so two players who launched five minutes apart
        // are in different worlds — one at noon, one at midnight, watching different weather. See
        // `Client::applyLoginKeys`.
        //
        // WORLD_EPOCH_MS anchors the world's origin (default: the moment this process started, which
        // makes a dev cluster's world begin when the cluster does). WORLD_TIME_SCALE is world seconds
        // per real second, and WORLD_START_S is where the clock reads at the epoch — set it to move
        // the world's starting hour. `serverNowMs` is this process's own reading, so the client never
        // has to trust its own wall clock.
        body += ",\"worldEpochMs\":"     + std::to_string(worldEpochMs());
        body += ",\"serverNowMs\":"      + std::to_string(nowMs());
        body += ",\"worldTimeScale\":"   + std::to_string(envDouble("WORLD_TIME_SCALE", 1.0));
        body += ",\"worldStartSeconds\":" + std::to_string(envDouble("WORLD_START_S", 0.0));
        if (!key.empty())   body += ",\"udpKey\":\"" + key + "\"";
        if (!group.empty()) body += ",\"groupKey\":\"" + group + "\"";
        body += "}";

        std::printf("[fake-api] login %s -> session %u%s%s\n",
                    emailFrom(req.body).c_str(), session,
                    key.empty()   ? " (no DGS_UDP_MASTER: no session key)" : " + session key",
                    group.empty() ? " (no DGS_UDP_KEY: plain UDP)"         : " + group key");
        std::fflush(stdout);
        res.set_content(body, "application/json");
    });

    // So a health check can tell "up" from "not there", and so the failure path stays testable: this
    // one answers 401, which is what a client must handle and what `client_e2e` pins.
    api.Get("/api/health", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("{\"ok\":true}", "application/json");
    });
    api.Post("/api/auth/reject", [](const httplib::Request&, httplib::Response& res) {
        res.status = 401;
        res.set_content("{\"error\":\"nope\"}", "application/json");
    });

    std::printf("╭──────────────────────────────────────────────────────────────────────╮\n");
    std::printf("│  fake_login_api — ACCEPTS EVERY PASSWORD. Development only.          │\n");
    std::printf("│  Not authentication. Never deploy this.                              │\n");
    std::printf("╰──────────────────────────────────────────────────────────────────────╯\n");
    std::printf("[fake-api] POST http://%s:%d/api/auth/login\n", host, port);
    std::fflush(stdout);

    if (!api.listen(host, port))
    {
        std::fprintf(stderr, "[fake-api] could not bind %s:%d\n", host, port);
        return 1;
    }
    return 0;
}
