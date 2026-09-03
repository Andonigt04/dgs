#ifndef DGS_GAME_MODULE_H
#define DGS_GAME_MODULE_H

// ================================================================================================
// STABLE ABI between the DGS HOST (which dlopens) and the per-project RULES MODULE (.so).
//
// Goal (see the game's PLAN_DGS_ANTICHEAT docs): the DGS is GENERIC and is never edited per game.
// Each project ships its lib<project>_rules.so exporting `dgs_game_module_v1()`; the DGS delegates
// ALL game semantics to it (physics, casting, what moves, world editing). The same code is compiled
// statically into the CLIENT (prediction) and as a .so for the DGS (validation) → identical rules.
//
// Versioning: if the ABI breaks, add `dgs_game_module_v2()` (a new symbol); the core is NOT edited.
// F0: validateMove only (replicating the historical validate()). F1+: validateAction, step, serialize…
// ================================================================================================
#include "include/dgs/types.h"
#include <cstdint>
#include <cstddef>   // size_t (serializeRegion)

namespace DGS
{
    static constexpr uint32_t GAME_MODULE_ABI = 4;   // v4: module PER ZONE (lifecycle + handoff)

    // READ-ONLY world state the host lends to the module (lives for the whole session).
    struct WorldQuery
    {
        // ⚠️ METRES, despite the historical "km" in this comment. The unit is the one the host passes
        // in `Command::chunkSize*`, and everything downstream de-quantises the global position with it.
        float chunkSizeX, chunkSizeY, chunkSizeZ;

        // ACTIVE PLANET — for validating movement against the TERRAIN (no walking through the ground,
        // no flying). The host fills this in from its world. The module rebuilds the WorldGenParams
        // from `seed` and samples the ANALYTIC terrain (same CPU sampler on client and server → no GL,
        // deterministic). All in METRES, the same units as the global position.
        double   planetCenter[3];   // planet centre (m)
        double   planetRadius;      // radius = sea level (m)
        uint32_t seed;              // world seed (deriveWorldParams)
        float    reliefStrength;    // scene parameter
        int32_t  profile;           // 0 terran · 1 moon · 2 gas
        // F1+: getEntity(uuid), world clock (tides/wind)…
    };

    // One movement sample to validate: the NEW reported state vs the last known point.
    struct MoveSample
    {
        const EntityTransfer* now;      // what the client claims RIGHT NOW
        float lastGX, lastGY, lastGZ;   // last known GLOBAL point (m)
        float maxSpeed;                 // m/s allowed (class/state)
        float dtSeconds;                // s since the last point (measured by the host)
    };

    // GENERIC action verbs. The engine's default module understands this header (common to many
    // games); a project may ignore it and read its own format from the SAME blob. The DGS NEVER looks
    // inside: to it the action is opaque — it only carries it and delegates the verdict to the module.
    enum ActionVerb : uint16_t
    {
        ACT_NONE     = 0,
        ACT_DAMAGE   = 1,   // take health off a target
        ACT_DESTROY  = 2,   // destroy an object / structure / brick
        ACT_TRANSFER = 3,   // move an item between inventories (what/structure = opaque, after the header)
        ACT_INTERACT = 4,   // generic use/activation
        ACT_PLACE    = 5,   // PLACE a building piece (payload: PlaceAction, see below)
        ACT__COUNT
    };

    // The header that opens an action blob. What follows (game-specific payload: which item, which
    // spell, inventory layout) is OPAQUE to the default module — the project's module reads it.
    struct ActionHeader
    {
        uint16_t verb;        // ActionVerb
        uint16_t flags;       // reserved (0 for now)
        uint64_t target;      // target uuid (0 = none)
        float    at[3];       // point of the action (m, GLOBAL) — for range checks in F+
        float    amount;      // quantity (damage / item count) — must be finite and >= 0
    };

    // ACT_PLACE payload, RIGHT BEHIND the ActionHeader. Unlike every other payload — opaque to the
    // default module — the engine DOES understand this one: placing is an ENGINE verb
    // (HarukaConstruction), not a per-game one, and validating it is pure GEOMETRY. That way the server
    // decides with THE SAME code the client uses for its prediction: no reimplementing rules.
    struct PlaceAction
    {
        uint16_t typeId;      // piece type in the catalogue (the same stable id the client uses)
        uint16_t pad;
        double   pos[3];      // centre of the piece (m, GLOBAL)
        double   quat[4];     // orientation (x, y, z, w)
    };

    // Description of a buildable piece TYPE. The server cannot validate a placement without knowing how
    // BIG the piece is (with no size there is no overlap to check), so the catalogue has to travel: the
    // host sends it ONCE when loading the world and the module validates against the REAL measurements,
    // the same ones the client uses. It is static world data, not per-action.
    struct PieceDesc
    {
        uint16_t typeId;      // stable type id (the client assigns it in alphabetical order)
        uint8_t  supports;    // bitmask: 1 = rests on terrain, 2 = rests on another piece
        uint8_t  needsFlat;   // 1 = requires flat ground (foundation)
        float    half[3];     // half-extents of the piece (m)
    };

    // A ZONE = the slice of world ONE DGS node serves. It is an opaque pointer created by the module:
    // the host never looks inside. All authoritative state (placed pieces, catalogue) hangs off the
    // zone, NOT off globals in the module.
    //
    // WHY: with global state a node serving two zones would mix them, there would be nothing to
    // "reassign" when moving a chunk of scene to another node, and the state would die at `dlclose`
    // with no ordering and no chance to release it earlier. With zones, creating, handing over and
    // destroying are ordinary operations.
    typedef void* ZoneHandle;

    // The module's vtable. A NULL function pointer = "no rule" → the host applies its generic fallback.
    struct GameModule
    {
        uint32_t    abiVersion;   // MUST == GAME_MODULE_ABI or the host rejects it
        const char* name;         // e.g. "survival"

        // --- ZONE LIFECYCLE --------------------------------------------------------------------
        // The host creates a zone when it starts serving a region and DESTROYS it when it stops
        // (handoff to another node, orderly shutdown). Destruction is explicit on purpose: leaving it
        // to process exit is what produces out-of-order teardown.
        ZoneHandle (*createZone)(const WorldQuery* w);
        void       (*destroyZone)(ZoneHandle z);

        // 1 = plausible/legal movement; 0 = cheat (the host discards it and escalates suspicion in F4).
        int (*validateMove)(ZoneHandle z, const MoveSample* s, const WorldQuery* w);

        // 1 = action admissible; 0 = rejected. `blob`/`n` = OPAQUE bytes (ActionHeader + the game's
        // payload); `actor` = the uuid performing it. The default module validates STATELESS invariants
        // (known verb, finite/non-negative amount, minimum size) PLUS placement (ACT_PLACE), which is
        // engine geometry. Game semantics come from the project's module.
        int (*validateAction)(ZoneHandle z, uint32_t actor, const uint8_t* blob, uint16_t n,
                              const WorldQuery* w);

        // Catalogue of this zone's buildable pieces. The host calls it on creation, before validating
        // any placement: without each piece's size there is no overlap to check.
        void (*setPieceCatalog)(ZoneHandle z, const PieceDesc* types, uint16_t n);

        // --- HANDING A REGION between nodes ----------------------------------------------------
        // `serializeRegion` extracts the authoritative state inside a sphere (centre+radius) into a
        // buffer; `mergeRegion` folds it into another zone. Those two operations cover the two moves a
        // distributed world needs:
        //   · REASSIGN a chunk of scene: serialise on node A → merge on B → A drops it.
        //   · GROW a zone: merge the region the neighbour ceded, without reloading anything.
        // Returns bytes written, or bytes NEEDED if `cap` is too small (call with out=nullptr to ask
        // for the size). Versioned format; the module owns it.
        size_t (*serializeRegion)(ZoneHandle z, const double center[3], double radius,
                                  uint8_t* out, size_t cap);
        int    (*mergeRegion)(ZoneHandle z, const uint8_t* in, size_t n);

        // `dropRegion` releases what another node already serves (the last step of a reassignment).
        void   (*dropRegion)(ZoneHandle z, const double center[3], double radius);

        // SIMULATION (P4, §3.6): the OWNING ZONE runs `step` at a fixed tick over ONE entity it owns
        // (C4 of plan v2). Only the authoritative node advances the entity; the rest project it as a
        // ghost. `dt` = tick in seconds. Null = the zone does not simulate (validation only) and the
        // world advances from client updates.
        void   (*step)(ZoneHandle z, EntityTransfer* e, float dt, const WorldQuery* w);
    };
}

// EVERY module exports THIS symbol (C linkage → dlsym stable across compilers/versions).
extern "C" const DGS::GameModule* dgs_game_module_v1(void);

#endif // DGS_GAME_MODULE_H
