#ifndef DGS_GAME_MODULE_H
#define DGS_GAME_MODULE_H

// ================================================================================================
// ABI ESTABLE entre el HOST del DGS (que hace dlopen) y el MÓDULO DE REGLAS por proyecto (.so).
//
// Objetivo (ver docs PLAN_DGS_ANTICHEAT del juego): el DGS es GENÉRICO y NO se toca por juego. Cada
// proyecto entrega su lib<proyecto>_rules.so que exporta `dgs_game_module_v1()`; el DGS delega en él
// TODA la semántica de juego (física, casting, qué se mueve, edición de mundo). El mismo código se
// compila estático en el CLIENTE (predicción) y como .so para el DGS (validación) → mismas reglas.
//
// Versionado: si se rompe el ABI, se añade `dgs_game_module_v2()` (símbolo nuevo); el core NO se edita.
// F0: solo validateMove (replica el validate() histórico). F1+: validateAction, step, serialize...
// ================================================================================================
#include "include/dgs/types.h"
#include <cstdint>

namespace DGS
{
    static constexpr uint32_t GAME_MODULE_ABI = 2;   // v2: + validateAction (blob de acción opaco)

    // Estado de mundo de SOLO-LECTURA que el host presta al módulo (vive toda la sesión).
    struct WorldQuery
    {
        float chunkSizeX, chunkSizeY, chunkSizeZ;   // km — para des-cuantizar la posición global

        // PLANETA ACTIVO — para validar el movimiento contra el TERRENO (no atravesar el suelo,
        // no volar). El host lo rellena con su mundo. El módulo reconstruye los WorldGenParams del
        // `seed` y muestrea el terreno ANALÍTICO (mismo sampler CPU en cliente y servidor → sin GL,
        // determinista). Todo en METROS, mismas unidades que la posición global.
        double   planetCenter[3];   // centro del planeta (m)
        double   planetRadius;      // radio = nivel del mar (m)
        uint32_t seed;              // semilla del mundo (deriveWorldParams)
        float    reliefStrength;    // parámetro de escena
        int32_t  profile;           // 0 terran · 1 moon · 2 gas
        // F1+: getEntity(uuid), reloj de mundo (mareas/viento)...
    };

    // Una muestra de movimiento a validar: estado NUEVO reportado vs último punto conocido.
    struct MoveSample
    {
        const EntityTransfer* now;      // lo que el cliente afirma AHORA
        float lastGX, lastGY, lastGZ;   // último punto GLOBAL conocido (m)
        float maxSpeed;                 // m/s permitidos (clase/estado)
        float dtSeconds;                // s desde el último punto (lo mide el host)
    };

    // Verbos GENÉRICOS de acción. El módulo por defecto del motor entiende este encabezado (común a
    // muchos juegos); un proyecto puede ignorarlo y leer su propio formato del MISMO blob. El DGS NUNCA
    // mira dentro: para él la acción es opaca — solo la transporta y delega el veredicto en el módulo.
    enum ActionVerb : uint16_t
    {
        ACT_NONE     = 0,
        ACT_DAMAGE   = 1,   // quitar vida a un target
        ACT_DESTROY  = 2,   // destruir un objeto / estructura / ladrillo
        ACT_TRANSFER = 3,   // mover ítem entre inventarios (qué/estructura = opaco, tras el header)
        ACT_INTERACT = 4,   // uso/activación genérica
        ACT__COUNT
    };

    // Encabezado que abre el blob de una acción. Lo que sigue (payload específico del juego: qué ítem,
    // qué hechizo, layout del inventario) es OPACO para el módulo por defecto — lo lee el del proyecto.
    struct ActionHeader
    {
        uint16_t verb;        // ActionVerb
        uint16_t flags;       // reservado (0 por ahora)
        uint64_t target;      // uuid objetivo (0 = ninguno)
        float    at[3];       // punto de la acción (m, GLOBAL) — para validar alcance en F+
        float    amount;      // cantidad (daño / nº de ítems) — debe ser finita y >= 0
    };

    // vtable del módulo. Un puntero de función NULO = "sin regla" → el host aplica su fallback genérico.
    struct GameModule
    {
        uint32_t    abiVersion;   // DEBE == GAME_MODULE_ABI o el host lo rechaza
        const char* name;         // p.ej. "survival"

        // 1 = movimiento plausible/legal; 0 = cheat (el host descarta + escalará sospecha en F4).
        int (*validateMove)(const MoveSample* s, const WorldQuery* w);

        // 1 = acción admisible; 0 = rechazada. `blob`/`n` = bytes OPACOS (ActionHeader + payload del
        // juego); `actor` = uuid que la ejecuta. El módulo por defecto valida invariantes SIN estado
        // (verbo conocido, cantidad finita/no-negativa, tamaño mínimo); la semántica con estado (¿tiene
        // el ítem?, ¿es el dueño?, alcance real) la aporta el módulo del proyecto. NULO → el host acepta.
        int (*validateAction)(uint32_t actor, const uint8_t* blob, uint16_t n, const WorldQuery* w);

        // --- Reservado (nulo por ahora) ---
        // void (*step)(EntityTransfer* e, float dt, const WorldQuery* w);
    };
}

// CADA módulo exporta ESTE símbolo (C linkage → dlsym estable entre compiladores/versiones).
extern "C" const DGS::GameModule* dgs_game_module_v1(void);

#endif // DGS_GAME_MODULE_H
