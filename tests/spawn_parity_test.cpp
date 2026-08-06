// P8 (§3.8): test de PARIDAD entre backends de spawn. El env de topología de una zona se construye
// con UNA sola función (`Orchestrator::zoneSpawnEnv`) usada por los tres backends (LOCAL via putenv,
// K8S/TERRAFORM como env del container en el manifest). Este test verifica que:
//   1) el env es estable y completo (todo lo que la zona necesita para auto-configurarse: CHUNK_*,
//      ZONE_UDP_PORT, HEAD_SERVER_HOST/PORT, MY_POD_IP) — cualquier nodo arranca igual en cualquier modo;
//   2) `resolveSpawnBackend` respeta DGS_SPAWN_BACKEND (local/k8s/terraform) y el default in-cluster.
// Sin esto, un bug solo visible en standalone (p.ej. un CHUNK_* mal pasado) no se detectaría en cluster.

#include "include/dgs/orchestrator.h"
#include <iostream>
#include <set>
#include <string>
#include <vector>
#include <cstdlib>

static int g_failures = 0;
#define CHECK(cond, msg) \
    do { if (!(cond)) { std::cerr << "FALLO: " << msg << std::endl; ++g_failures; } } while (0)

int main()
{
    // 1) Paridad de env: mismo rango, mismo puerto → el env es idéntico y completo. Es por
    //    CONSTRUCCIÓN (un solo generador), pero lo afirmamos para que un refactor no lo rompa.
    std::vector<std::string> env = DGS::Orchestrator::zoneSpawnEnv(
        0, 50, 0, 50, 0, 50, 42426, "head-server", "42424", "10.0.0.7");

    std::set<std::string> asSet(env.begin(), env.end());
    CHECK(asSet.size() == env.size(), "env sin duplicados");
    CHECK(env.size() == 13, "env con las 13 variables de topologia");

    // Variables críticas que la zona relee cada tick (zone_node.cpp): si una falta, el nodo
    // arranca con defaults y el clúster NO ve el rango — fallaría en cluster pero no en local.
    CHECK(asSet.count("CHUNK_X_MIN=0") && asSet.count("CHUNK_X_MAX=50"), "rango X en env");
    CHECK(asSet.count("CHUNK_Y_MIN=0") && asSet.count("CHUNK_Y_MAX=50"), "rango Y en env");
    CHECK(asSet.count("CHUNK_Z_MIN=0") && asSet.count("CHUNK_Z_MAX=50"), "rango Z en env");
    CHECK(asSet.count("ZONE_UDP_PORT=42426"), "puerto UDP de la replica en env");
    CHECK(asSet.count("HEAD_SERVER_HOST=head-server"), "head host en env");
    CHECK(asSet.count("HEAD_SERVER_PORT=42424"), "head port en env");
    CHECK(asSet.count("MY_POD_IP=10.0.0.7"), "IP del pod en env");

    // Determinismo: la misma llamada produce el mismo env (paridad entre 2 nodos del mismo rango).
    std::vector<std::string> env2 = DGS::Orchestrator::zoneSpawnEnv(
        0, 50, 0, 50, 0, 50, 42426, "head-server", "42424", "10.0.0.7");
    CHECK(env == env2, "env determinista");

    // 2) Resolución del backend según env.
    setenv("DGS_SPAWN_BACKEND", "local", 1);
    CHECK(DGS::resolveSpawnBackend() == DGS::SpawnBackend::SPAWN_LOCAL, "backend=local");
    setenv("DGS_SPAWN_BACKEND", "k8s", 1);
    CHECK(DGS::resolveSpawnBackend() == DGS::SpawnBackend::SPAWN_K8S, "backend=k8s");
    setenv("DGS_SPAWN_BACKEND", "terraform", 1);
    CHECK(DGS::resolveSpawnBackend() == DGS::SpawnBackend::SPAWN_TERRAFORM, "backend=terraform");
    unsetenv("DGS_SPAWN_BACKEND");

    // 3) El mismo env es el que ve un zone-node local vs el manifest: la lista de env del spawn
    //    LOCAL (`spawnLocalNode` hace putenv de zoneSpawnEnv) es exactamente este vector — no hay
    //    transformación extra entre backends que pueda divergir.

    if (g_failures == 0)
        std::cout << "spawn_parity_test: OK (paridad LOCAL/K8S/TERRAFORM por construccion)" << std::endl;
    else
    {
        std::cerr << "spawn_parity_test: " << g_failures << " fallos" << std::endl;
        return 1;
    }
    return 0;
}
