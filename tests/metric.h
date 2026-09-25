#pragma once
// ─────────────────────────────────────────────────────────────────────────────────────────────────
// EL CANAL DE METRICAS DE LOS TESTS: una linea por numero medido, con una forma que lee una maquina.
//
//      METRIC <clave> <valor> [<unidad>]
//
// Los tests de este repo ya median cosas y ya las imprimian —"loopback RTT: 0.214 ms", "zona: 3
// pings, rtt ultimo 1.02 ms", "the proxy failed to forward 12 of 400 datagrams"—, cada una con su
// propia frase. Eso se lee en la consola de quien lo lanza a mano y NO se lee en ningun otro sitio:
// en GitHub Actions queda enterrado en el log del job, que hay que abrir, desplegar y buscar. Una
// cifra que empeora entre dos commits no la ve nadie. `tools/ci_test_summary.py` recoge estas lineas
// del XML de CTest y las sube al resumen del run, que es la pagina que sale al entrar.
//
// ⚠️ NO SUSTITUYE A LA FRASE del test, se pone AL LADO. La frase lleva el contexto ("tres pings a
// 100 ms, la zona devuelve el eco sellado") y es lo que hace entendible un fallo; la linea METRIC no
// lleva contexto ninguno, solo el numero.
//
// ⚠️ Y NO SUSTITUYE A UN `check`: una metrica no decide nada. El veredicto lo sigue dando el check,
// que es quien tiene el umbral ("RTT < 50 ms en loopback"). Publicar el numero es para poder MIRAR
// como se mueve; hacerlo fallar es otra decision y se toma en el check, no aqui. Un umbral escondido
// en un panel es un test que no se puede leer.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include <cstdio>

/// Un numero medido. La clave, sin espacios (`rtt_loopback`); la unidad es texto y puede faltar.
inline void dgsMetric(const char* key, double value, const char* unit = "") {
    std::printf("METRIC %s %.4g%s%s\n", key, value, (unit && *unit) ? " " : "", unit ? unit : "");
}

/// Igual, para cuentas exactas (bytes, datagramas, violaciones): un entero no debe salir en 1e+06.
inline void dgsMetricI(const char* key, long long value, const char* unit = "") {
    std::printf("METRIC %s %lld%s%s\n", key, value, (unit && *unit) ? " " : "", unit ? unit : "");
}
