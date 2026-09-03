// ─────────────────────────────────────────────────────────────────────────────────────────────────
// TOY rules module for the DGS's own tests.
//
// The validator `dlopen`s the project's module (`GAME_MODULE_SO`, by default `libharuka_rules.so`,
// which lives in the engine). If the DGS's tests needed THAT module, the network project would stop
// being testable on its own, which is the opposite of why it is kept separate. So here is a minimal
// module with ONE obvious rule:
//
//     legal  <=>  distance travelled <= maxSpeed * dt * MARGIN
//
// No terrain, no physics: that is the game module's business, and the game's own tests cover it
// (`haruka-cpp/tests/test_dgs.cpp`, which dlopens the real module). What is tested here is the NODE:
// that it loads a module, that it genuinely asks it about every request, and that it returns the
// verdict correlated with the `requestId` it received.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/game_module.h"

#include <cmath>
#include <cstdlib>

namespace
{
    // Margin over the theoretical limit: absorbs the host's own network jitter. Generous on purpose —
    // this module is not here to tune tolerances but to give an unambiguous YES and NO.
    constexpr double kMargin = 1.5;

    DGS::ZoneHandle createZone(const DGS::WorldQuery*) { return (DGS::ZoneHandle)1; }
    void            destroyZone(DGS::ZoneHandle)       {}

    int validateMove(DGS::ZoneHandle, const DGS::MoveSample* s, const DGS::WorldQuery* w)
    {
        if (!s || !s->now || !w) return 0;

        // ⚠️ NO ×1000. The header used to document `chunkSize*` in "km", but the REAL game module
        // (`default_rules.cpp`) uses it as METRES: `chunkX * chunkSizeX + pos`. A toy module that did
        // not follow the SAME convention as the real one would make the test measure something else.
        const double gx = (double)s->now->chunkX * w->chunkSizeX + s->now->pos[0];
        const double gy = (double)s->now->chunkY * w->chunkSizeY + s->now->pos[1];
        const double gz = (double)s->now->chunkZ * w->chunkSizeZ + s->now->pos[2];

        const double dx = gx - s->lastGX, dy = gy - s->lastGY, dz = gz - s->lastGZ;
        const double dist = std::sqrt(dx * dx + dy * dy + dz * dz);

        const double dt = (s->dtSeconds > 0.0f) ? s->dtSeconds : 0.0;
        const double cap = (double)s->maxSpeed * dt * kMargin;

        return (dist <= cap) ? 1 : 0;
    }

#ifdef STUB_WITH_ACTIONS
    // A CONTROLLABLE oracle for the action path. It exists so two rejections that look identical from
    // outside can be told apart: "there is no rule, hence fail-closed" and "there is a rule and it says
    // no". Without both cases, a validator that rejected EVERY action would pass for safe when it is
    // in fact broken.
    int validateAction(DGS::ZoneHandle, uint32_t, const uint8_t* blob, uint16_t n,
                       const DGS::WorldQuery*)
    {
        if (!blob || n == 0) return 0;                    // empty blob: not an action
        const char* e = std::getenv("STUB_ACTION_VERDICT");
        return (e && *e == '0') ? 0 : 1;
    }
#endif

    const DGS::GameModule g_module = {
        DGS::GAME_MODULE_ABI,
        "stub-rules-for-tests",
        createZone,
        destroyZone,
        validateMove,
#ifdef STUB_WITH_ACTIONS
        validateAction,
#else
        nullptr,   // no action rule -> the host must apply FAIL-CLOSED
#endif
        nullptr,   // setPieceCatalog
        nullptr,   // serializeRegion
        nullptr,   // mergeRegion
        nullptr,   // dropRegion
        nullptr,   // step
    };
}

extern "C" const DGS::GameModule* dgs_game_module_v1()
{
    return &g_module;
}
