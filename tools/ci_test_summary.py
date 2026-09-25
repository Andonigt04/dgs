#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────────────────────────
# ci_test_summary.py — las cifras de los tests, en la pagina del job de GitHub Actions.
#
# Los tests de este repo miden cosas: cuantas comprobaciones pasan, cuanto tarda cada uno, el RTT de
# loopback, los bytes que van por el cable, los falsos positivos del validador con la red degradada.
# Todo eso se imprimia y se quedaba en el LOG del job: hay que abrirlo, desplegar el paso, buscar la
# linea y compararla a ojo con la de otro dia. En la practica nadie lo hace, asi que una metrica que
# empeora entre dos commits no la ve nadie.
#
# Esto lee el XML JUnit que escribe CTest (`ctest --output-junit`) y produce MARKDOWN para
# `$GITHUB_STEP_SUMMARY`, que es la pagina que se ve nada mas entrar en el run.
#
# ⚠️ EL XML LLEVA LA SALIDA DE CADA TEST, PERO RECORTADA A 1024 BYTES por defecto — y el recorte es
# por el FINAL, que es justo donde un test imprime su resumen y sus metricas. Medido: `udp_crypto`
# llegaba aqui con "[This part of the test output was removed since it exceeds the threshold of 1024
# bytes.]" y sin su linea de OK/FAILED. Por eso el workflow pasa
# `--test-output-size-passed/--test-output-size-failed`, y por eso este script AVISA en el resumen
# cuando ve el recorte: un numero que falta tiene que notarse, no desaparecer.
#
# No decide nada: el veredicto es el codigo de salida de `ctest`. Esto solo publica.
#
#   ctest --test-dir build --output-junit build/ctest.xml ...
#   python3 tools/ci_test_summary.py build/ctest.xml --title "Tests" >> $GITHUB_STEP_SUMMARY
#   python3 tools/ci_test_summary.py --selftest      # se prueba a si mismo (lo corre CTest)
# ─────────────────────────────────────────────────────────────────────────────────────────────────
import argparse
import re
import sys
import xml.etree.ElementTree as ET

# Las tres formas en que los tests de este repo cuentan sus comprobaciones. Son tres y no una porque
# se escribieron en momentos distintos; unificarlas es otro cambio, y mientras tanto el resumen no
# puede quedarse en blanco para los que no siguen la mayoritaria.
RE_SUMMARY = re.compile(r"^==\s*[^:\n]+:\s*(\d+)\s+OK\s*\S?\s*(\d+)\s+FAILED\s*==", re.M)
RE_CHECKS  = re.compile(r"(\d+)\s+checks?,\s*(\d+)\s+failures?", re.M)
RE_OK_MARK = re.compile(r"^\s*\[ok\]", re.M)
RE_FAIL_MK = re.compile(r"^\s*\[FAIL\]\s*(.*)$", re.M)
# El canal de metricas (tests/metric.h):  METRIC <clave> <valor> [<unidad>]
# ⚠️ ESPACIOS Y TABULADORES, NO `\s`. Con `\s*` antes de la unidad el salto de linea entraba en el
# espacio y la unidad se tragaba la linea SIGUIENTE entera: una metrica sin unidad salia en el
# resumen como "0 METRIC datagramas_perdidos_clean 0". Visto en la primera corrida de verdad
# (`net_degraded`), no en los fixtures — que todos llevaban unidad.
RE_METRIC  = re.compile(r"^METRIC[ \t]+(\S+)[ \t]+(-?[0-9][0-9a-zA-Z.+-]*)[ \t]*([^\n\r]*)$", re.M)
RE_TRUNC   = re.compile(r"exceeds the threshold of \d+ bytes")
# ⚠️ UN SALTO QUE CTEST NO VE. `tls_test` (y algún otro) imprime "== tls_test: skipped ==" y sale con
# 0 cuando le faltan los certificados: para CTest eso es un test PASADO, y en el resumen salía en
# verde con cero comprobaciones. Es el mismo agujero contra el que avisa el propio ci.yml —"a skip
# nobody notices is a test that does not exist"—, solo que un escalón más abajo. Aquí no se convierte
# en fallo (esa decisión es del test, no del panel): se MARCA, que es lo que faltaba para verlo.
RE_SKIPPED = re.compile(r"^==\s*[^:\n]+:\s*skipped\s*==", re.M | re.I)


def checks_of(out):
    """(ok, fallos, de_donde). `None` cuando el test no cuenta comprobaciones de ninguna forma."""
    m = list(RE_SUMMARY.finditer(out))
    if m:
        return int(m[-1].group(1)), int(m[-1].group(2)), "resumen"
    m = list(RE_CHECKS.finditer(out))
    if m:
        return int(m[-1].group(1)), int(m[-1].group(2)), "checks"
    ok, fail = len(RE_OK_MARK.findall(out)), len(RE_FAIL_MK.findall(out))
    if ok or fail:
        return ok, fail, "marcas"
    return None


def parse(xml_text):
    """El XML de CTest -> lista de tests con lo que hay que enseñar de cada uno."""
    root = ET.fromstring(xml_text)
    tests = []
    for tc in root.iter("testcase"):
        out = (tc.findtext("system-out") or "") + "\n" + (tc.findtext("system-err") or "")
        status = (tc.get("status") or "").lower()
        # CTest marca el fallo con un hijo <failure>; `status` dice "run"/"fail"/"notrun".
        failed = tc.find("failure") is not None or status in ("fail", "failed")
        skipped = (tc.find("skipped") is not None or status in ("notrun", "disabled")
                   or bool(RE_SKIPPED.search(out)))
        try:
            secs = float(tc.get("time") or 0.0)
        except ValueError:
            secs = 0.0
        tests.append({
            "name": tc.get("name") or "?",
            "failed": failed,
            "skipped": skipped,
            "secs": secs,
            "checks": checks_of(out),
            "fails": [f.strip() for f in RE_FAIL_MK.findall(out) if f.strip()],
            "metrics": [(k, v, u.strip()) for k, v, u in RE_METRIC.findall(out)],
            "truncated": bool(RE_TRUNC.search(out)),
        })
    return tests


def render(tests, title):
    total = len(tests)
    failed = [t for t in tests if t["failed"]]
    skipped = [t for t in tests if t["skipped"] and not t["failed"]]
    ok_checks = sum(t["checks"][0] for t in tests if t["checks"])
    bad_checks = sum(t["checks"][1] for t in tests if t["checks"])
    secs = sum(t["secs"] for t in tests)

    L = []
    L.append("## %s" % title)
    L.append("")
    if total == 0:
        # Un XML vacio es un resultado, y de los malos: significa que CTest no corrio ningun test.
        L.append("⚠️ **el XML no trae ni un test** — `ctest` no llego a correr, o se le paso otra ruta.")
        return "\n".join(L) + "\n"
    veredicto = "❌ %d de %d fallan" % (len(failed), total) if failed else "✅ %d de %d pasan" % (total, total)
    linea = "%s · %d comprobaciones (%d fallidas) · %.1f s" % (veredicto, ok_checks, bad_checks, secs)
    if skipped:
        linea += " · %d saltados" % len(skipped)
    L.append(linea)
    L.append("")

    L.append("| test | | tiempo | comprobaciones | fallidas |")
    L.append("|---|:--:|--:|--:|--:|")
    for t in sorted(tests, key=lambda x: (not x["failed"], -x["secs"])):
        icon = "❌" if t["failed"] else ("⏭️" if t["skipped"] else "✅")
        c = t["checks"]
        L.append("| `%s` | %s | %.2f s | %s | %s |" % (
            t["name"], icon, t["secs"],
            str(c[0]) if c else "—",
            (("**%d**" % c[1]) if c[1] else "0") if c else "—"))
    L.append("")

    # Las comprobaciones que fallan, AQUI: si hay que abrir el log para saber cual, el resumen no ha
    # servido de nada.
    malas = [(t["name"], f) for t in tests for f in t["fails"]]
    if malas:
        L.append("### Comprobaciones fallidas")
        L.append("")
        for name, f in malas:
            L.append("- `%s` — %s" % (name, f))
        L.append("")

    metricas = [(t["name"], k, v, u) for t in tests for (k, v, u) in t["metrics"]]
    if metricas:
        L.append("### Métricas")
        L.append("")
        L.append("| test | métrica | valor |")
        L.append("|---|---|--:|")
        for name, k, v, u in metricas:
            L.append("| `%s` | %s | %s%s |" % (name, k, v, (" " + u) if u else ""))
        L.append("")
        L.append("<sub>Las mide el test y no deciden nada: el veredicto lo dan las comprobaciones.</sub>")
        L.append("")

    cortados = [t["name"] for t in tests if t["truncated"]]
    if cortados:
        L.append("> ⚠️ CTest recortó la salida de %s, así que sus cifras pueden faltar aquí. "
                 "Sube el límite con `--test-output-size-passed`/`--test-output-size-failed`."
                 % ", ".join("`%s`" % c for c in cortados))
        L.append("")
    return "\n".join(L) + "\n"


# ── El test del recolector ───────────────────────────────────────────────────────────────────────
# Contraprueba en cada bloque: no basta con que salga lo que hay, tiene que NO salir lo que no hay.
# Sin eso, un `render` que imprimiera siempre la tabla de metricas pasaria igual.
def selftest():
    # `checks` llega ya evaluado contra el `md` del bloque: aqui solo se cuenta y se imprime.
    def case(name, checks):
        for descr, cond in checks:
            print("  [%s] %s · %s" % ("ok" if cond else "FAIL", name, descr))
            if not cond:
                case.bad += 1
    case.bad = 0

    xml_ok = """<?xml version="1.0"?><testsuite>
      <testcase name="wire" time="0.5" status="run"><system-out>
[wire_test] 66 checks, 0 failures
METRIC entidad_en_el_cable 62 B
</system-out></testcase>
      <testcase name="ping_pong" time="1.25" status="run"><system-out>
  [ok]   uno
METRIC rtt_loopback 0.214 ms
== ping_pong: 7 OK &#183; 0 FAILED ==
</system-out></testcase></testsuite>"""
    md = render(parse(xml_ok), "T")
    case("verde", [
        ("dice que pasan los dos",           "✅ 2 de 2 pasan" in md),
        ("suma las comprobaciones (66+7)",   "73 comprobaciones" in md),
        ("suma el tiempo de los dos",        "1.8 s" in md),
        ("saca la metrica con su unidad",    "| 0.214 ms |" in md),
        ("y la del otro test",               "entidad_en_el_cable" in md),
        ("CONTRAPRUEBA: sin fallos, no hay seccion de fallidas",
                                             "Comprobaciones fallidas" not in md),
        ("CONTRAPRUEBA: nada de recorte",    "recortó" not in md),
    ])

    # Una metrica SIN unidad, con otra linea detras. El caso que rompio de verdad: si el espacio de
    # antes de la unidad admite el salto de linea, la unidad se come la linea siguiente.
    xml_sin_unidad = """<?xml version="1.0"?><testsuite>
      <testcase name="net_degraded" time="1" status="run"><system-out>
METRIC falsos_positivos_clean 0
METRIC datagramas_perdidos_clean 6
    5 % loss    55    6
  [ok]   con 20 % de perdida no acusa a nadie
</system-out></testcase></testsuite>"""
    md = render(parse(xml_sin_unidad), "T")
    case("sin unidad", [
        ("la metrica sin unidad sale sola",  "| 0 |" in md),
        ("la segunda tambien",               "| 6 |" in md),
        ("CONTRAPRUEBA: no se traga la linea de al lado",
                                             "datagramas_perdidos_clean 6" not in md),
        ("CONTRAPRUEBA: ni la tabla de la consola",
                                             "5 % loss" not in md),
    ])

    xml_bad = """<?xml version="1.0"?><testsuite>
      <testcase name="zone_e2e" time="2" status="fail"><failure message="x"/><system-out>
  [ok]   una que si
  [FAIL] la zona no devuelve el lease
== zone_e2e: 1 OK &#183; 1 FAILED ==
</system-out></testcase>
      <testcase name="wire" time="0.1" status="run"><system-out>[wire_test] 66 checks, 0 failures</system-out></testcase>
      </testsuite>"""
    md = render(parse(xml_bad), "T")
    case("rojo", [
        ("dice cuantos fallan",              "❌ 1 de 2 fallan" in md),
        ("cuenta la comprobacion fallida",   "(1 fallidas)" in md),
        ("nombra la que fallo, sin abrir el log", "la zona no devuelve el lease" in md),
        ("el test roto sale el primero",     md.index("zone_e2e") < md.index("wire")),
        ("CONTRAPRUEBA: sin metricas, no hay tabla de metricas", "### Métricas" not in md),
    ])

    xml_skip = """<?xml version="1.0"?><testsuite>
      <testcase name="tls" time="0.01" status="run"><system-out>
  SKIPPED: no certificates in /tmp/dgs-tls.
== tls_test: skipped ==
</system-out></testcase>
      <testcase name="wire" time="0.1" status="run"><system-out>[wire_test] 66 checks, 0 failures</system-out></testcase>
      </testsuite>"""
    md = render(parse(xml_skip), "T")
    case("saltado", [
        ("el test que se salta a si mismo se marca",  "⏭️" in md),
        ("y se cuenta aparte",                        "1 saltados" in md),
        ("sin dejar de contar los dos como pasados",  "✅ 2 de 2 pasan" in md),
        ("CONTRAPRUEBA: el que no se salta no lleva la marca",
                                                      md.count("⏭️") == 1),
    ])

    xml_trunc = """<?xml version="1.0"?><testsuite><testcase name="udp_crypto" time="0.1" status="run">
      <system-out>  [ok]   algo
[This part of the test output was removed since it exceeds the threshold of 1024 bytes.]</system-out>
      </testcase></testsuite>"""
    md = render(parse(xml_trunc), "T")
    case("recorte", [
        ("avisa del recorte de CTest",       "recortó" in md),
        ("y dice de que test",               "`udp_crypto`" in md),
        ("cae a contar marcas [ok]",         "| 1 |" in md),
    ])

    md = render(parse('<?xml version="1.0"?><testsuite></testsuite>'), "T")
    case("vacio", [
        ("un XML sin tests se canta como problema", "ni un test" in md),
        ("CONTRAPRUEBA: y no como exito",    "pasan" not in md),
    ])

    print("== ci_test_summary: %s ==" % ("0 FALLOS" if case.bad == 0 else "%d FALLOS" % case.bad))
    return 1 if case.bad else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("junit", nargs="?", help="XML de `ctest --output-junit`")
    ap.add_argument("--title", default="Tests")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        return selftest()
    if not a.junit:
        ap.error("hace falta el XML (o --selftest)")
    try:
        with open(a.junit, encoding="utf-8", errors="replace") as f:
            xml = f.read()
    except OSError as e:
        # Sin XML no hay resumen, pero el paso NO puede tumbar el job: el veredicto ya lo dio ctest.
        sys.stdout.write("## %s\n\n⚠️ no se pudo leer `%s`: %s\n" % (a.title, a.junit, e))
        return 0
    try:
        sys.stdout.write(render(parse(xml), a.title))
    except ET.ParseError as e:
        sys.stdout.write("## %s\n\n⚠️ el XML de CTest no se puede leer: %s\n" % (a.title, e))
    return 0


if __name__ == "__main__":
    sys.exit(main())
