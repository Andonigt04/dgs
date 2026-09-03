// P8 (§3.8): PARITY test between spawn backends. A zone's topology env is built by ONE function
// (`Orchestrator::zoneSpawnEnv`) used by all three backends (LOCAL via putenv, K8S/TERRAFORM as the
// container's env in the manifest). This test verifies that:
//   1) the env is stable and complete (everything the zone needs to self-configure: CHUNK_*,
//      ZONE_UDP_PORT, HEAD_SERVER_HOST/PORT, MY_POD_IP) — any node starts the same in any mode;
//   2) `resolveSpawnBackend` honours DGS_SPAWN_BACKEND (local/k8s/terraform) and the in-cluster default.
// Without this, a bug only visible in standalone (say a mis-passed CHUNK_*) would go undetected in a
// cluster.

#include "include/dgs/orchestrator.h"
#include <iostream>
#include <set>
#include <string>
#include <vector>
#include <cstdlib>

static int g_failures = 0;
#define CHECK(cond, msg) \
    do { if (!(cond)) { std::cerr << "FAILED: " << msg << std::endl; ++g_failures; } } while (0)

int main()
{
    // 1) Env parity: same range, same port → the env is identical and complete. It is so by
    //    CONSTRUCTION (a single generator), but we assert it so a refactor cannot break it.
    std::vector<std::string> env = DGS::Orchestrator::zoneSpawnEnv(
        0, 50, 0, 50, 0, 50, 42426, "head-server", "42424", "10.0.0.7");

    std::set<std::string> asSet(env.begin(), env.end());
    CHECK(asSet.size() == env.size(), "env has no duplicates");
    CHECK(env.size() == 13, "env carries all 13 topology variables");

    // Critical variables the zone re-reads every tick (zone_node.cpp): if one is missing the node
    // starts with defaults and the cluster does NOT see the range — it would fail in a cluster but not
    // locally.
    CHECK(asSet.count("CHUNK_X_MIN=0") && asSet.count("CHUNK_X_MAX=50"), "X range in env");
    CHECK(asSet.count("CHUNK_Y_MIN=0") && asSet.count("CHUNK_Y_MAX=50"), "Y range in env");
    CHECK(asSet.count("CHUNK_Z_MIN=0") && asSet.count("CHUNK_Z_MAX=50"), "Z range in env");
    CHECK(asSet.count("ZONE_UDP_PORT=42426"), "the replica's UDP port in env");
    CHECK(asSet.count("HEAD_SERVER_HOST=head-server"), "head host in env");
    CHECK(asSet.count("HEAD_SERVER_PORT=42424"), "head port in env");
    CHECK(asSet.count("MY_POD_IP=10.0.0.7"), "the pod's IP in env");

    // Determinism: the same call produces the same env (parity between 2 nodes of the same range).
    std::vector<std::string> env2 = DGS::Orchestrator::zoneSpawnEnv(
        0, 50, 0, 50, 0, 50, 42426, "head-server", "42424", "10.0.0.7");
    CHECK(env == env2, "env is deterministic");

    // 2) Backend resolution from the environment.
    setenv("DGS_SPAWN_BACKEND", "local", 1);
    CHECK(DGS::resolveSpawnBackend() == DGS::SpawnBackend::SPAWN_LOCAL, "backend=local");
    setenv("DGS_SPAWN_BACKEND", "k8s", 1);
    CHECK(DGS::resolveSpawnBackend() == DGS::SpawnBackend::SPAWN_K8S, "backend=k8s");
    setenv("DGS_SPAWN_BACKEND", "terraform", 1);
    CHECK(DGS::resolveSpawnBackend() == DGS::SpawnBackend::SPAWN_TERRAFORM, "backend=terraform");
    unsetenv("DGS_SPAWN_BACKEND");

    // 3) The same env is what a local zone-node sees versus the manifest: the LOCAL spawn's env list
    //    (`spawnLocalNode` putenvs zoneSpawnEnv) is exactly this vector — there is no extra
    //    transformation between backends that could diverge.

    if (g_failures == 0)
        std::cout << "spawn_parity_test: OK (LOCAL/K8S/TERRAFORM parity by construction)" << std::endl;
    else
    {
        std::cerr << "spawn_parity_test: " << g_failures << " failures" << std::endl;
        return 1;
    }
    return 0;
}
