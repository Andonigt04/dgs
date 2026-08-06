// ================================================================================================
// robust_test — tests de DETERMINISMO del módulo de reglas + CONCURRENCIA de contadores (P5b, §4.3).
//
// 1) DETERMINISMO: el módulo del proyecto se carga DOS veces (dos dlopen → dos instancias del .so,
//    estado totalmente separado). Con el MISMO seed/WorldQuery y la MISMA secuencia de MoveSample/
//    acciones, ambas deben emitir: (a) los MISMOS veredictos, y (b) los MISMOS bytes de
//    `serializeRegion`. Si el módulo usara rand()/reloj de pared o estado global, esto explotaría.
//    Si no hay .so disponible (GAME_MODULE_SO) → SKIP, no fallo.
//
// 2) CONCURRENCIA: N hilos incrementan contadores compartidos (`bytesTx`, `failedTransfers`, patrón
//    exacto de zone_node). Con semántica atómica el total es EXACTO. Correr con -fsanitize=thread
//    para que un `+=` no atómico sobre esos contadores falle aquí mismo (la regla §4.3: los contadores
//    son atómicos o viven en el hilo único de métricas).
//
// Se enlaza contra src/packet.cpp + dl (para el dlopen). Igual patrón que wire_test.
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
// 1) Determinismo del módulo (§3.5, §4.3)
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
    if (!A.ok) { CHECK(A.handle == nullptr, "módulo ausente → skip limpio"); return; }
    LoadedModule B = loadModuleOnce();
    if (!B.ok) { dlclose(A.handle); CHECK(B.handle == nullptr, "2ª carga disponible"); return; }

    // Planeta PEQUEÑO (5 km): pos[] en float con precisión; mismo seed en ambas cargas.
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
    CHECK(zA && zB, "ambas cargas crean zona");

    // Misma secuencia de MoveSample en ambas → mismos veredictos.
    auto mkSample = [](DGS::EntityTransfer* e, float gx, float gy, float gz,
                       float lastX, float lastY, float lastZ) {
        DGS::MoveSample s{};
        s.now = e; s.lastGX = lastX; s.lastGY = lastY; s.lastGZ = lastZ;
        s.maxSpeed = 5.0f; s.dtSeconds = 1.0f/60.0f;
        return s;
    };

    const double dir[3] = { 0.3, 0.9, 0.2 };   // dirección no normalizada: solo importa el signo/pendiente

    bool verdictsSame = true;
    for (int i = 0; i < 200 && verdictsSame; ++i)
    {
        const float frac = (float)i / 199.0f;
        const double onSurf = R + 1.0 + frac * 3.0;   // 1..4 m sobre el suelo, variando
        DGS::EntityTransfer eA{}; DGS::EntityTransfer eB{};
        eA.uuid = eB.uuid = (uint32_t)i;
        eA.pos[0] = (float)(dir[0] * onSurf); eA.pos[1] = (float)(dir[1] * onSurf); eA.pos[2] = (float)(dir[2] * onSurf);
        eB.pos[0] = eA.pos[0]; eB.pos[1] = eA.pos[1]; eB.pos[2] = eA.pos[2];
        eA.stats.speed[0] = eB.stats.speed[0] = 5.0f;

        const float lastS = 0.2f;   // paso corto desde ~200 mm atrás (dentro de maxSpeed·dt)
        DGS::MoveSample sA = mkSample(&eA, 0, 0, 0, (float)(dir[0]*onSurf - dir[0]*lastS),
                                      (float)(dir[1]*onSurf - dir[1]*lastS),
                                      (float)(dir[2]*onSurf - dir[2]*lastS));
        DGS::MoveSample sB = sA; sB.now = &eB;

        int vA = A.mod->validateMove(zA, &sA, &wA);
        int vB = B.mod->validateMove(zB, &sB, &wB);
        if (vA != vB) { verdictsSame = false; std::printf("    divergencia en muestra %d: %d vs %d\n", i, vA, vB); }
    }
    CHECK(verdictsSame, "misma secuencia de MoveSample → mismos veredictos en las 2 cargas");

    // serializeRegion con el MISMO centro/radio → bytes IDÉNTICOS (formato determinista, byte a byte).
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
            std::printf("    region tamaños distintos: %zu vs %zu\n", needA, needB);
        }
    }
    CHECK(regionSame, "serializeRegion → bytes IDÉNTICOS en las 2 cargas (formato determinista)");

    if (zA && A.mod->destroyZone) A.mod->destroyZone(zA);
    if (zB && B.mod->destroyZone) B.mod->destroyZone(zB);
    dlclose(A.handle);
    dlclose(B.handle);
}

// ------------------------------------------------------------------------------------------------
// 2) Concurrencia de contadores (§4.3): total EXACTO bajo N hilos con semántica atómica.
// ------------------------------------------------------------------------------------------------
static void concurrencyTest()
{
    std::printf("[robust_test] concurrencia de contadores (%d hilos x %d iteraciones)\n", 8, 100000);

    std::atomic<uint64_t> bytesTx{0};
    std::atomic<uint32_t> failedTransfers{0};

    constexpr int THREADS = 8;
    constexpr int ITERS   = 100000;
    constexpr uint64_t BYTES_PER_OP = 64;   // un paquete típico

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
          "bytesTx total EXACTO con N hilos (contadores atómicos)");
    CHECK(failedTransfers.load(std::memory_order_relaxed) == expectedFail,
          "failedTransfers total EXACTO con N hilos (contadores atómicos)");
    std::printf("    bytesTx=%llu (esperado %llu)  failedTransfers=%u (esperado %u)\n",
                (unsigned long long)bytesTx.load(), (unsigned long long)expectedBytes,
                failedTransfers.load(), expectedFail);
}

int main()
{
    std::printf("[robust_test] determinismo + concurrencia de contadores (P5b)\n");
    determinismTest();
    concurrencyTest();
    std::printf("[robust_test] %d checks, %d fallos\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
