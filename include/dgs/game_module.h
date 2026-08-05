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
#include <cstddef>   // size_t (serializeRegion)

namespace DGS
{
    static constexpr uint32_t GAME_MODULE_ABI = 4;   // v4: módulo POR ZONA (ciclo de vida + traspaso)

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
        ACT_PLACE    = 5,   // COLOCAR una pieza de construcción (payload: PlaceAction, ver abajo)
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

    // Payload de ACT_PLACE, JUSTO DETRAS del ActionHeader. A diferencia del resto de payloads —opacos
    // para el modulo por defecto— este SI lo entiende el motor: colocar es un verbo del ENGINE
    // (HarukaConstruction), no de un juego concreto, y validarlo es GEOMETRIA pura. Asi el servidor
    // decide con EL MISMO codigo que el cliente usa para su prediccion: nada de reimplementar reglas.
    struct PlaceAction
    {
        uint16_t typeId;      // tipo de pieza en el catalogo (el mismo id estable que usa el cliente)
        uint16_t pad;
        double   pos[3];      // centro de la pieza (m, GLOBAL)
        double   quat[4];     // orientacion (x,y,z,w)
    };

    // Descripción de un TIPO de pieza construible. El servidor no puede validar una colocación sin
    // saber qué TAMAÑO tiene la pieza (sin eso no hay solape que comprobar), así que el catálogo tiene
    // que viajar: el host lo envía UNA vez al cargar el mundo y el módulo valida con las medidas REALES,
    // las mismas que el cliente. Es dato estático del mundo, no por-acción.
    struct PieceDesc
    {
        uint16_t typeId;      // id estable del tipo (el cliente lo asigna por orden alfabético)
        uint8_t  supports;    // bitmask: 1 = apoya en terreno, 2 = apoya en otra pieza
        uint8_t  needsFlat;   // 1 = exige suelo llano (cimentación)
        float    half[3];     // semiejes de la pieza (m)
    };

    // Una ZONA = la porción del mundo que sirve UN nodo del DGS. Es un puntero opaco creado por el
    // módulo: el host no mira dentro. Todo el estado autoritativo (piezas colocadas, catálogo) vive
    // colgando de la zona, NO en variables globales del módulo.
    //
    // POR QUÉ: con estado global, un nodo que sirviera dos zonas las mezclaría, no habría nada que
    // "reasignar" al mover un trozo de escena a otro nodo, y el estado moriría en el `dlclose` sin orden
    // ni posibilidad de liberarlo antes. Con zonas: crear, traspasar y destruir son operaciones normales.
    typedef void* ZoneHandle;

    // vtable del módulo. Un puntero de función NULO = "sin regla" → el host aplica su fallback genérico.
    struct GameModule
    {
        uint32_t    abiVersion;   // DEBE == GAME_MODULE_ABI o el host lo rechaza
        const char* name;         // p.ej. "survival"

        // --- CICLO DE VIDA de una zona ---------------------------------------------------------
        // El host crea una zona al empezar a servir una región y la DESTRUYE al dejar de servirla
        // (traspaso a otro nodo, apagado ordenado). Destruir es explícito a propósito: dejarlo al
        // final del proceso es lo que produce liberaciones fuera de orden.
        ZoneHandle (*createZone)(const WorldQuery* w);
        void       (*destroyZone)(ZoneHandle z);

        // 1 = movimiento plausible/legal; 0 = cheat (el host descarta + escalará sospecha en F4).
        int (*validateMove)(ZoneHandle z, const MoveSample* s, const WorldQuery* w);

        // 1 = acción admisible; 0 = rechazada. `blob`/`n` = bytes OPACOS (ActionHeader + payload del
        // juego); `actor` = uuid que la ejecuta. El módulo por defecto valida invariantes SIN estado
        // (verbo conocido, cantidad finita/no-negativa, tamaño mínimo) MÁS colocación (ACT_PLACE), que
        // sí es geometría del motor. La semántica de juego la aporta el módulo del proyecto.
        int (*validateAction)(ZoneHandle z, uint32_t actor, const uint8_t* blob, uint16_t n,
                              const WorldQuery* w);

        // Catálogo de piezas construibles de esta zona. El host lo llama al crearla, antes de validar
        // colocaciones: sin el tamaño de cada pieza no hay solape que comprobar.
        void (*setPieceCatalog)(ZoneHandle z, const PieceDesc* types, uint16_t n);

        // --- TRASPASO de una región entre nodos ------------------------------------------------
        // `serializeRegion` extrae el estado autoritativo dentro de una esfera (centro+radio) a un
        // buffer; `mergeRegion` lo incorpora en otra zona. Con esas dos operaciones se resuelven los
        // dos movimientos que necesita un mundo repartido:
        //   · REASIGNAR un trozo de escena: serializar en el nodo A → mergear en B → A lo suelta.
        //   · AMPLIAR una zona: mergear la región cedida por el vecino, sin recargar nada.
        // Devuelve los bytes escritos, o los NECESARIOS si `cap` es insuficiente (llamar con out=nullptr
        // para preguntar el tamaño). Formato versionado; el módulo es dueño de él.
        size_t (*serializeRegion)(ZoneHandle z, const double center[3], double radius,
                                  uint8_t* out, size_t cap);
        int    (*mergeRegion)(ZoneHandle z, const uint8_t* in, size_t n);

        // `dropRegion` suelta lo que ya sirve otro nodo (el paso final de una reasignación).
        void   (*dropRegion)(ZoneHandle z, const double center[3], double radius);

        // SIMULACIÓN (P4, §3.6): la ZONA DUEÑA ejecuta `step` a tick fijo sobre UNA entidad que posee
        // (C4 del plan v2). Solo el nodo autoritativo avanza la entidad; los demás la proyectan como
        // ghost. `dt` = tick en segundos. Null = la zona no simula (solo validación) y el mundo va por
        // las actualizaciones del cliente.
        void   (*step)(ZoneHandle z, EntityTransfer* e, float dt, const WorldQuery* w);
    };
}

// CADA módulo exporta ESTE símbolo (C linkage → dlsym estable entre compiladores/versiones).
extern "C" const DGS::GameModule* dgs_game_module_v1(void);

#endif // DGS_GAME_MODULE_H
