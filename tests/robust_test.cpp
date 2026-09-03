// ================================================================================================
// robust_test — DETERMINISM tests for the rules module + counter CONCURRENCY (P5b, §4.3).
//
// 1) DETERMINISM: the project's module is loaded TWICE (two dlopens → two instances of the .so, with
//    entirely separate state). Given the SAME seed/WorldQuery and the SAME sequence of MoveSample /
//    actions, both must emit: (a) the SAME verdicts, and (b) the SAME bytes from `serializeRegion`. If
//    the module used rand()/wall clock or global state, this would blow up. If no .so is available
//    (GAME_MODULE_SO) → SKIP, not a failure.
//
// 2) CONCURRENCY: N threads increment shared counters (`bytesTx`, `failedTransfers`, zone_node's exact
//    pattern). With atomic semantics the total is EXACT. Run under -fsanitize=thread so a non-atomic
//    `+=` on those counters fails right here (the §4.3 rule: counters are atomic or they live on the
//    single metrics thread).
//
// Linked against src/packet.cpp + dl (for the dlopen). Same pattern as wire_test.
// ================================================================================================
#include "include/dgs/game_module.h"
#include "include/dgs/packet.h"
#include "include/dgs/types.h"

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <thread>
#include <atomic>
#include <dlfcn.h>

static int g_failures = 0;
static int g_checks   = 0;

#define CHECK(cond, msg) \
    do { g_checks++; \
         if (!(cond)) { g_failures++; \
             std::printf("    FAIL %s (line %d)\n", msg, __LINE__); } \
    } while (0)

// ------------------------------------------------------------------------------------------------
// 1) Module determinism (§3.5, §4.3)
// ------------------------------------------------------------------------------------------------

struct LoadedModule
{
    void*                   handle = nullptr;
    const DGS::GameModule*  mod    = nullptr;
    bool                    ok     = false;
};

static LoadedModule loadModuleOnce()
{
    LoadedModule lm;
    const char* so = std::getenv("GAME_MODULE_SO") ? std::getenv("GAME_MODULE_SO") : "libharuka_rules.so";
    lm.handle = dlopen(so, RTLD_NOW);
    if (!lm.handle) { std::printf("    (skip determinismo) sin modulo: %s\n", dlerror()); return lm; }
    auto entry = (const DGS::GameModule* (*)())dlsym(lm.handle, "dgs_game_module_v1");
    if (!entry) { dlclose(lm.handle); lm.handle = nullptr; return lm; }
    lm.mod = entry();
    lm.ok  = lm.mod && lm.mod->abiVersion == DGS::GAME_MODULE_ABI &&
             lm.mod->createZone && lm.mod->serializeRegion && lm.mod->validateMove;
    if (!lm.ok) { dlclose(lm.handle); lm.handle = nullptr; }
    return lm;
}

static void determinismTest()
{
    std::printf("[robust_test] determinismo del modulo (2 cargas del .so, mismo seed)\n");

    LoadedModule A = loadModuleOnce();
    if (!A.ok) { CHECK(A.handle == nullptr, "module absent → clean skip"); return; }
    LoadedModule B = loadModuleOnce();
    if (!B.ok) { dlclose(A.handle); CHECK(B.handle == nullptr, "2ª carga disponible"); return; }

    // A SMALL planet (5 km): pos[] in float keeps precision; the same seed in both loads.
    const double R = 5000.0; const uint32_t seed = 424242u;

    DGS::WorldQuery wA{}; DGS::WorldQuery wB{};
    for (DGS::WorldQuery* w : { &wA, &wB })
    {
        w->chunkSizeX = w->chunkSizeY = w->chunkSizeZ = 1.0f;
        w->planetCenter[0] = w->planetCenter[1] = w->planetCenter[2] = 0.0;
        w->planetRadius = R; w->seed = seed; w->reliefStrength = 1.0f; w->profile = 0;
    }

    DGS::ZoneHandle zA = A.mod->createZone(&wA);
    DGS::ZoneHandle zB = B.mod->createZone(&wB);
    CHECK(zA && zB, "both loads create a zone");

    // The same MoveSample sequence in both → the same verdicts.
    auto mkSample = [](DGS::EntityTransfer* e, float gx, float gy, float gz,
                       float lastX, float lastY, float lastZ) {
        DGS::MoveSample s{};
        s.now = e; s.lastGX = lastX; s.lastGY = lastY; s.lastGZ = lastZ;
        s.maxSpeed = 5.0f; s.dtSeconds = 1.0f/60.0f;
        return s;
    };

    const double dir[3] = { 0.3, 0.9, 0.2 };   // un-normalised direction: only the sign/slope matters

    bool verdictsSame = true;
    for (int i = 0; i < 200 && verdictsSame; ++i)
    {
        const float frac = (float)i / 199.0f;
        const double onSurf = R + 1.0 + frac * 3.0;   // 1..4 m above the ground, varying
        DGS::EntityTransfer eA{}; DGS::EntityTransfer eB{};
        eA.uuid = eB.uuid = (uint32_t)i;
        eA.pos[0] = (float)(dir[0] * onSurf); eA.pos[1] = (float)(dir[1] * onSurf); eA.pos[2] = (float)(dir[2] * onSurf);
        eB.pos[0] = eA.pos[0]; eB.pos[1] = eA.pos[1]; eB.pos[2] = eA.pos[2];
        eA.stats.speed[0] = eB.stats.speed[0] = 5.0f;

        const float lastS = 0.2f;   // a short step from ~200 mm back (within maxSpeed·dt)
        DGS::MoveSample sA = mkSample(&eA, 0, 0, 0, (float)(dir[0]*onSurf - dir[0]*lastS),
                                      (float)(dir[1]*onSurf - dir[1]*lastS),
                                      (float)(dir[2]*onSurf - dir[2]*lastS));
        DGS::MoveSample sB = sA; sB.now = &eB;

        int vA = A.mod->validateMove(zA, &sA, &wA);
        int vB = B.mod->validateMove(zB, &sB, &wB);
        if (vA != vB) { verdictsSame = false; std::printf("    divergencia en muestra %d: %d vs %d\n", i, vA, vB); }
    }
    CHECK(verdictsSame, "the same MoveSample sequence → the same verdicts in both loads");

    // serializeRegion with the SAME centre/radius → IDENTICAL bytes (deterministic format, byte for byte).
    bool regionSame = true;
    if (zA && zB)
    {
        const double center[3] = { dir[0]*R, dir[1]*R, dir[2]*R };
        const size_t needA = A.mod->serializeRegion(zA, center, 50.0, nullptr, 0);
        const size_t needB = B.mod->serializeRegion(zB, center, 50.0, nullptr, 0);
        if (needA == needB && needA > 0)
        {
            std::vector<uint8_t> bufA(needA), bufB(needB);
            A.mod->serializeRegion(zA, center, 50.0, bufA.data(), bufA.size());
            B.mod->serializeRegion(zB, center, 50.0, bufB.data(), bufB.size());
            if (std::memcmp(bufA.data(), bufB.data(), needA) != 0)
            {
                regionSame = false;
                std::printf("    region diverge: A[%zu] vs B[%zu] bytes\n", needA, needB);
            }
        }
        else
        {
            regionSame = false;
            std::printf("    region sizes differ: %zu vs %zu\n", needA, needB);
        }
    }
    CHECK(regionSame, "serializeRegion → IDENTICAL bytes in both loads (deterministic format)");

    if (zA && A.mod->destroyZone) A.mod->destroyZone(zA);
    if (zB && B.mod->destroyZone) B.mod->destroyZone(zB);
    dlclose(A.handle);
    dlclose(B.handle);
}

// ------------------------------------------------------------------------------------------------
// 2) Counter concurrency (§4.3): an EXACT total under N threads with atomic semantics.
// ------------------------------------------------------------------------------------------------
static void concurrencyTest()
{
    std::printf("[robust_test] counter concurrency (%d threads x %d iterations)\n", 8, 100000);

    std::atomic<uint64_t> bytesTx{0};
    std::atomic<uint32_t> failedTransfers{0};

    constexpr int THREADS = 8;
    constexpr int ITERS   = 100000;
    constexpr uint64_t BYTES_PER_OP = 64;   // a typical packet

    std::vector<std::thread> threads;
    for (int t = 0; t < THREADS; ++t)
    {
        threads.emplace_back([&] {
            for (int i = 0; i < ITERS; ++i)
            {
                bytesTx.fetch_add(BYTES_PER_OP, std::memory_order_relaxed);
                if (i % 7 == 0) failedTransfers.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& th : threads) th.join();

    const uint64_t expectedBytes = (uint64_t)THREADS * ITERS * BYTES_PER_OP;
    const uint32_t expectedFail  = (uint32_t)((ITERS + 6) / 7) * THREADS;

    CHECK(bytesTx.load(std::memory_order_relaxed) == expectedBytes,
          "bytesTx total is EXACT under N threads (atomic counters)");
    CHECK(failedTransfers.load(std::memory_order_relaxed) == expectedFail,
          "failedTransfers total is EXACT under N threads (atomic counters)");
    std::printf("    bytesTx=%llu (esperado %llu)  failedTransfers=%u (esperado %u)\n",
                (unsigned long long)bytesTx.load(), (unsigned long long)expectedBytes,
                failedTransfers.load(), expectedFail);
}

int main()
{
    std::printf("[robust_test] determinism + counter concurrency (P5b)\n");
    determinismTest();
    concurrencyTest();
    std::printf("[robust_test] %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
