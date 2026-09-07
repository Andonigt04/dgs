# DGS — Distributed Game Server

A high-performance distributed game server in **C++17**, aimed at MMOs with galactic-scale worlds.
Microservices over **epoll** (Linux), TCP/UDP, Kubernetes orchestration, and a real-time anti-cheat
**validator**. The design motto is *"one owner per data type"*: every entity has a single owner that
simulates it, and the rest of the world sees it as a *ghost*.

---

## Architecture

<img src="image.drawio.png" />

The central idea is **ownership, not broadcast**: for each entity there is exactly one authoritative
node (the one holding the *lease*), and its neighbours project it as a ghost in the meantime. The
`validador_node` is the arbiter: the owner predicts, the validator confirms.

---

## Features

- **Authority handoff with leases** — every entity has an owner *until* (`entityOwnedUntil`),
  renewable (`ENTITY_LEASE_MS`). On expiry or reassignment, authority is handed over with
  `PKT_REASSIGN` and the new owner promotes it. No "two nodes simulating the same thing".
- **Ghost → real promotion with the edge cases resolved** — the entity a neighbour saw as a ghost is
  promoted to real when it takes the lease: (a) **without duplicating it** if we already held it, and
  (b) **without letting a stale ghost clobber a real entity** during the handoff (`zone_node.cpp:673`).
- **Owner-predicts / validator-arbitrates** — the owner predicts the movement and sends a
  `ValidateRequest` correlated by `requestId` (a per-sender seq → idempotency + anti-replay). The
  validator answers `ValidateAck` with a verdict and a weight. Latency is compensated with
  `radius = v·Δt + scale`.
- **Per-project rules module with a C ABI and `dlopen`** — each game ships its
  `lib<project>_rules.so` exporting `dgs_game_module_v1()`; the core delegates `step()` at a fixed tick
  over the entities it owns. Versioned ABI: if it changes, `dgs_game_module_v2()` is added and the core
  is not edited.
- **Determinism test by double `dlopen`** — `robust_test.cpp` loads the same `.so` **twice** (two
  instances with separate state), applies the same sequence of actions and compares `serializeRegion`
  **byte for byte**. That is real determinism, not a test comparing itself against itself.
- **Drain protocol** — `PKT_DRAIN`/`ZoneLifecycle` with `requestId` + ack: when a zone is degraded or
  reassigned, its state is drained before it is cut. **Reassignment on a failed metric** (P6): a zone
  that stops reporting health triggers `LIFECYCLE_REASSIGN` and a handoff to a healthy neighbour.
- **Load-based scaling** — the orchestrator calls the Kubernetes API to scale `zone-node` (HPA) when
  the RAM/CPU threshold is exceeded.
- **Delta compression** — `EntityTransfer.dataSize` serialises only the used bytes of `data[4096]`.
- **Dynamic chunk system** — `int32_t` coordinates plus a local `float` position (millimetre
  precision); chunk dimensions arrive from the head at start-up.
- **Terraform with a module per node** — `terraform/modules/{network,k8s,mongodb,zone_node,validador,
  social}/main.tf` + `terraform/main.tf`. Plus a **CLI** (`tools/dgs_cli.cpp`):
  `run/install/up/down/status/logs`, against the cluster or locally.
- **Social node** — `social_node` owns the **non-spatial** plane (guilds, parties, friends, bans,
  guild economy): the same one-owner rule, outside of space.
- **Anti-cheat by module** — validation uses the project's rules module (`validateMove`), not a fixed
  formula in the core.

---

## Node Overview

| Node | Protocol | Port | Role |
|------|----------|------|------|
| `head_server_node` | TCP | 42424 | Orchestration, zone routing, k8s scaling |
| `zone_node` | TCP + UDP | 42420 | Entity simulation (lease), metrics, ghosts |
| `validador_node` | UDP + TCP | 42427 / 42428 | Position validation (arbiter), forwarding to persistence |
| `persistance_node` | TCP | 42429 | MongoDB entity state (upsert + read-back), asynchronous CSV log |
| `social_node` | TCP | — | Non-spatial plane: guilds, friends, bans |
| `client_node` | HTTP + TCP + UDP | — | Login API → zone discovery → game |

---

## Tech Stack

- **Language** — C++17
- **Build** — CMake 3.14+ with FetchContent
- **Networking** — raw POSIX sockets, Linux epoll
- **Database** — MongoDB via mongocxx (built and tested against 3.11.0)
- **HTTP** — [cpp-httplib](https://github.com/yhirose/cpp-httplib) (header-only)
- **Containers** — multi-stage Docker
- **Orchestration** — Kubernetes (tested with minikube) + Terraform
- **Rules module** — `dlopen`/`dlsym` (`dgs_game_module_v1`), C ABI

---

## Project Structure

```
dgs/
├── include/dgs/
│   ├── types.h            # Shared structs and enums (packets, leases, lifecycle)
│   ├── packet.h/cpp       # Serialisation (pack/unpack)
│   ├── network.h/cpp      # TCPSocket, UDPSocket
│   ├── orchestrator.h     # Zone topology + k8s scaling API + lifecycle
│   ├── game_module.h      # ABI of the per-project rules module (dlopen)
│   ├── thread_pool.h      # N-worker ThreadPool
│   └── logger.h           # Asynchronous CSV logger
├── nodes/
│   ├── head_server_node.cpp
│   ├── zone_node.cpp
│   ├── validador_node.cpp
│   ├── persistance_node.cpp
│   ├── social_node.cpp
│   └── client_node.cpp
├── src/
│   ├── packet.cpp
│   └── network.cpp
├── tests/
│   ├── wire_test.cpp          # Golden wire-format layout (P0)
│   ├── socket_framing_test.cpp # One send is one receive, and an oversized message stays contained
│   ├── auth_e2e.cpp           # Who is allowed to be a node
│   ├── tls_test.cpp           # What anyone on the path can read (TCP)
│   ├── udp_crypto_test.cpp    # ...and on the UDP game plane
│   ├── interest_e2e.cpp       # A player is told about what is near them
│   ├── cli_test.cpp           # The `dgs` tool: injection, a status that lied, an off-by-one table
│   ├── robust_test.cpp        # Determinism by double dlopen + atomic concurrency
│   ├── spawn_parity_test.cpp  # Deterministic generation parity
│   ├── validator_e2e.cpp      # The validator end to end (arbiter answers)
│   ├── zone_e2e.cpp           # The zone end to end (asks and obeys)
│   ├── zone_policy_e2e.cpp    # Ban, lease expiry and the circuit breaker
│   ├── net_degraded.cpp       # UDP path under loss, delay and reordering
│   ├── reconnect_e2e.cpp      # Reconnection and descriptor ownership
│   ├── ghost_e2e.cpp          # Ghosts and authority handoff
│   ├── action_e2e.cpp         # The fail-closed action path
│   ├── social_e2e.cpp         # Chat, rate limit, selective persistence
│   ├── head_routing_e2e.cpp   # Routing with two disjoint zones
│   ├── persistence_e2e.cpp    # A dead database (survival) and a live one (save and retrieve)
│   ├── restore_e2e.cpp        # A zone that comes back with its world still in it
│   ├── social_persist_e2e.cpp # A ban that survives a restart of the social node
│   ├── handoff_e2e.cpp        # An entity crossing a border cannot vanish (at-least-once)
│   ├── orchestrator_test.cpp  # Lifecycle queue, handoff on failure, lease eviction
│   ├── client_e2e.cpp         # Login gate, zone discovery, per-chunk cache
│   ├── ping_pong.cpp
│   ├── thread_pool_test.cpp
│   └── logger_test.cpp
├── tools/
│   ├── dgs_cli.cpp        # CLI: run/install/up/down/status/logs
│   ├── load_zone.cpp      # How many players one zone holds, and what gives way first
│   └── demo_player.cpp    # A crowd walking around, so the viewer has something to show
├── views/
│   ├── dgs_viewer.cpp     # Cluster viewer: zones + entities moving
│   └── viewer_state.h     # What it knows, with no raylib attached (so it can be tested)
├── terraform/
│   ├── main.tf
│   └── modules/{network,k8s,mongodb,zone_node,validador,social}/
├── docker/                # Dockerfile per node
├── k8s/                   # Kubernetes manifests
│   ├── namespace.yaml
│   ├── mongodb/
│   ├── head-server/       # Deployment + Service + RBAC
│   ├── zone-node/         # Deployment + Service + HPA
│   ├── validador/
│   ├── persistence/
│   └── deploy.sh
└── third_party/
```

---

## Building

**Dependencies:**
- CMake ≥ 3.14
- GCC/Clang with C++17
- OpenSSL (`libssl-dev`)
- mongocxx driver — built and tested against **3.11.0** (with mongo-c-driver 1.30.2). Optional:
  without it every target except `persistance_node` is built, and `persistence_e2e` is not registered.
  Not packaged by every distribution; a local build works:
  `cmake -S . -B build -DCMAKE_PREFIX_PATH=<driver prefix>`

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

Binaries land in `build/`.

---

## Tests

```bash
cd build && ctest --output-on-failure
```

Every test is registered with CTest and its exit status rules — none of them is wrapped in `|| true`.
They start the real nodes as child processes, drive them over the real protocol and assert on what the
nodes publish to the outside, never on their internals.

Two of them are worth calling out:

`robust_test` loads the rules module **twice** and demands that two instances with the same state emit
the same verdicts **and the same bytes** from `serializeRegion`. If the module used `rand()`, a wall
clock or global state, the test blows up.

`net_degraded` puts a user-space degrading proxy (loss, delay, reordering) in front of the validator
and measures **false positives on an honest player**. Every case carries a positive control: a
deliberate teleport is pushed through the same path and must come out flagged — otherwise a "0 false
positives" reading would be indistinguishable from "nothing is reaching the validator", which is
exactly the mistake that was made the first time this was measured.

---

## Running Locally

Each node reads its configuration from environment variables. The defaults point at the Kubernetes DNS
names.

```bash
# HeadServer
./build/head_server_node

# ZoneNode (chunk range, in chunk units)
CHUNK_X_MIN=0 CHUNK_X_MAX=100 \
CHUNK_Y_MIN=0 CHUNK_Y_MAX=100 \
CHUNK_Z_MIN=0 CHUNK_Z_MAX=100 \
HEAD_SERVER_HOST=127.0.0.1 \
./build/zone_node

# Validator
HEAD_SERVER_HOST=127.0.0.1 PERSISTENCE_HOST=127.0.0.1 \
./build/validador_node

# Persistence
MONGO_URI=mongodb://localhost:27017 ./build/persistance_node
```

Or through the CLI (local or cluster):

```bash
./build/dgs_cli run zone_node CHUNK_X_MIN=0 CHUNK_X_MAX=10   # local
./build/dgs_cli up                                            # cluster (kubectl apply)
./build/dgs_cli status
```

### The whole thing, in one command

`dgs run` starts one of each node, and one zone cannot show the thing worth showing. For a demo — or
for looking at the system while it works — `tools/demo_cluster.sh` stands up **two** zones side by side
and sets the three variables that decide whether anything is visible:

```bash
./tools/demo_cluster.sh start     # head, persistence, validator, social, zone A (chunk 0), zone B (1..7)
./tools/demo_cluster.sh status
./tools/demo_cluster.sh stop
```

It prints what to run next: `fill_world` for the crowd, `dgs_viewer` for the panel, and a `tail` that
shows a handoff as it happens. The three variables are `INTEREST_RADIUS_M` (**0 by default = everybody
hears everybody**), `ENTITY_LEASE_MS` (3 s, which purges props) and `DGS_OBSERVE_TOKEN` (unset = the
zone refuses every observer). Zone A owns exactly one chunk so the first border a crosser reaches is a
**node** border, seconds in, rather than one of its own.

---

## Capacity, measured

`tools/load_zone` ramps a real `zone_node` with real clients at 20 Hz until it stops keeping up, and
prints what gave way. `snap/s` is the rate of complete world snapshots each client actually receives;
nominal is 10 (the 100 ms tick), and it falling is the zone losing the ability to keep its players
current. Loopback, one machine, no validator in the path:

```
     N  served   sent/s   want/s  snap/s  MB/s cli  MB/s out  lat p50  lat p95     loop
     1       1       20       20    10.2     0.001      0.00       40       42      98us
     2       2       40       40    10.2     0.001      0.00       43       45     107us
     4       4       80       80    10.2     0.003      0.01       45       47     229us
     8       8      160      160    10.2     0.005      0.04       48       49     602us
    16      16      320      320    10.2     0.010      0.16       49       50    1995us
    32      32      640      640    10.2     0.020      0.65       11       50    7139us
    64      64     1280     1280    10.2     0.041      2.60       46       47   27970us
   128     128     2560     2560     8.4     0.067      8.57       47       74  113992us
```

**One zone holds 64 players at its nominal tick, and at 128 it is late but no longer collapsing.**

The first version of this table was measured before the broadcast honoured `dataSize`, and the
difference is the reason the section exists:

| N | snap/s | egress MB/s | latency p95 | loop |
|---|---|---|---|---|
| 32 | 10.2 → 10.2 | 43.66 → **0.65** | 62 → 50 ms | 8670 → 7139 µs |
| 64 | 10.2 → 10.2 | 174.65 → **2.60** | 100 → 47 ms | 31565 → 27970 µs |
| 128 | **5.6 → 8.4** | 381.44 → **8.57** | **2148 → 74 ms** | 122322 → 113992 µs |

The zone sends every entity to every client, so egress grows as N². Each datagram used to be a whole
`EntityTransfer` — **4160 bytes, of which 4096 are the `data[]` payload that is almost always empty** —
because the UDP paths memcpy'd the struct instead of using `Packet::pack`, which has honoured
`dataSize` all along on the TCP paths. The format was never missing; the hot path was bypassing it.
An entity with no payload is now **62 bytes instead of 4160, 67×**, measured end to end at the far side
of a real zone by `tests/viewer_e2e.cpp`. At N=64 that is 2.6 MB/s out of a zone instead of 175, and
0.3 Mbit/s down per client instead of 21 — the difference between "needs a datacentre link" and "works
on a domestic line".

**The remaining wall is syscalls, not bytes.** Loop time barely moved (31.6 → 28.0 ms at N=64) while
the bytes fell 67×, and it is almost exactly proportional to the number of DATAGRAMS: 6.8 µs each at
N=64 (4096 of them), 7.0 µs at N=128 (16384). At 128 players the tick spends 114 ms of its 100 ms
budget in `sendto`. Batching them (`sendmmsg`) or sending each client only what is near it — interest
management, which this does not have — is where the next factor lives. That is measured, not done.

Getting to any of these numbers first required fixing the tick itself: it ran at **4.6 Hz, not 10**, at
every population including a single idle player, because its per-tick blocking waits stacked to
~220 ms — and the node's own `performance` metric reported 211 µs of work, so nothing in its telemetry
could ever show it. One `poll` over its sockets plus a fixed-period tick took it to 10.2 Hz and player
latency from 142 ms to 44. `tests/viewer_e2e.cpp` pins the rate so it cannot rot back.

---

## Persistence, live

`persistence_e2e` has two halves that need opposite things. The first points the node at a closed port
and checks an outage **degrades** the service instead of killing it — `insert_one` throws, and an
uncaught exception is `std::terminate`, so a Mongo outage used to take the whole node down with the
first entity that arrived. The second needs a database that is actually there.

That second half did not exist for a long time, and standing it up found that **the node only ever
wrote**: `insert_one` and nothing else, no `find` anywhere in the repository. A persistence node that
cannot restore anything is not persisting. Measured against a live Mongo, before anything was changed:

| | before | now |
|---|---|---|
| documents after 6 updates of ONE entity | 6 | 1 (upsert) |
| indexes on the collection | `_id_` only — lookup by uuid is a full scan | unique on `uuid` |
| timestamp on the document | none, so "the latest" was not expressible | `updatedAtMs` |
| `stats` (health, speed, damage) | **never stored at all** | stored |
| reading one back | impossible | `PKT_PERSIST_QUERY` → the entity, or `PKT_NONE` |

The health one is the point of doing this: it went into the packet, crossed the validator, reached the
node and was dropped on the floor. An entity restored from the database came back at zero health, and
no amount of testing the write path could have shown it, because nothing ever read.

**The test restarts the node between writing and reading**, so what comes back cannot be anything the
process was holding in memory. Its counter-proof is asking for a uuid that was never stored: it must
come back as nothing, or every check above would also pass on a node that echoed the question.

```bash
podman run -d --rm -p 27017:27017 docker.io/library/mongo:7
./build/persistence_e2e_test          # runs both halves
DGS_REQUIRE_MONGO=1 ctest --test-dir build   # a skipped live phase becomes a failure
```

Without a database the live half **skips loudly** and says how to get one. CI provides a `mongo:7`
service and sets `DGS_REQUIRE_MONGO`, because where the database is guaranteed a skip is a regression.

What this does **not** do: nothing calls the read path yet. A zone still starts empty. Restoring a
zone's entities on start-up is the obvious next step and it is not done.

---

## A node that would not start

`validador_node` did a **blocking read for an initial `Command` from the head and `return 1` if it did
not arrive** — and the head does not send one to an ordinary connection. The only `Command` anywhere in
the system is `Orchestrator::sendResizeCommand`, on a resize, and it did not even carry chunk sizes:
it built `DGS::Command cmd;` **uninitialised**, so `chunkSize*` and `addr` would have gone on the wire
as whatever was on the stack — straight into the field the validator was waiting for.

So in a real cluster the validator never came up. It sat blocked on that read with an empty log, which
is exactly how it looked while I was probing something else and wondered why it answered nothing.

It takes its chunk size from the environment now, like every other node, and still accepts a `Command`
that overrides it if one arrives within `VALIDATOR_COMMAND_MS`. A node must not depend on a message
nobody sends. (And `sendResizeCommand` is zero-initialised.)

---

## A second node that would not start

`validador_node` did `return 1` if it could not reach the persistence node at start-up. Nothing
guarantees an order in a cluster — Kubernetes starts pods as it pleases, `docker compose up` starts
them all at once — and `tools/demo_cluster.sh` reproduced it **every single time**: persistence needs a
moment to reach Mongo, the validator gave up inside that window and died, and the cluster ran with no
anti-cheat at all and a validator log two lines long. Five of six processes alive, and the missing one
was the one that judges.

The rule the rest of the system already follows is written in `zone_node`: *a node must start, and keep
ticking, whether or not there is a database*. Persistence is where a validator FORWARDS accepted state;
losing it costs durability, not validation, and validation is what the process is for. It now starts,
says so on one line, and validates.

> Measured: with no persistence at all it comes up, announces `Persistence unavailable -> validating
> anyway, accepted state is NOT forwarded`, and keeps running. And **reconnection is not implemented** —
> if persistence appears later this node will not notice until it is restarted, which is why the demo
> script waits for persistence before starting it.

---

## Running the CI without a runner

The workflow had never executed. Not "was failing" — had never run at all, so every claim in it was
untested. It cannot be pushed from here, but it can be **executed**: the job's `run:` blocks extracted
from the YAML and played in a clean `ubuntu:22.04` container with a `mongo:7` beside it, which is what
the `services:` block provides.

It failed, and then it was wrong in a second place that only a cross-check found.

**The `build-and-test` job.** It stopped at the mongocxx build:

```
-- Could NOT find PythonInterp (missing: PYTHON_EXECUTABLE)
CMake Error: BUILD_VERSION not specified and could not be calculated
             (Python was not found on the system)
```

Two things came out of that, and they need separating honestly. The Python failure is **my
environment**, not necessarily CI: GitHub's runner image ships Python, so it would probably have passed
there — until the image changed, because the workflow never declared the dependency. It is declared
now, and `-DBUILD_VERSION=3.11.0` removes the need for it entirely. The second is **real**: the job
installed `libmongoc-dev` and `libbson-dev` on every run and mongocxx ignored them —

```
-- No MongoDB C Driver path provided via CMAKE_PREFIX_PATH,
   will download C driver version 1.28.0 from the internet.
```

— so those packages were dead weight, and the C driver CI builds against is **1.28.0**, not the 1.30.2
the local runs used. The packages are gone; the version difference is recorded rather than papered over.

With that fixed the job runs **28/28 in a clean container, with `DGS_REQUIRE_MONGO=1` set and nothing
skipped** — the live-database tests really ran.

**The `tsan` job was broken in eleven places**, and no amount of reading would have shown it. `ctest`
runs every test CMake *registers*, not the ones a job happened to build: it compiled
`head_server_node`, `validador_node` and `stub_rules`, then ran tests that spawn `zone_node`,
`social_node` and `dgs`, plus four test executables it never compiled. Found by parsing `add_test`
out of `CMakeLists.txt` and differencing it against the job's target list — a check that now costs
nothing to repeat.

**And then the pipeline ran for real and found a third thing.** `zone_policy_e2e` failed in the normal
job with `LAYERS 1+2: the breaker reports the fault` — while its own output, four lines above the
FAILED line, showed `[status #2] state=0` during the outage. The assertion sampled the state **once**,
after the window, and the state *oscillates* while the arbiter is mute: the breaker trips (2),
exhausts `CB_MAX_OPEN` and fails open (0), the retry reconnects (1), and it starts again. A single
sample is a coin toss. What the layer promises is that the head **is told** at some point, which is a
property of the stream, so it is measured over the stream now: every status report during the outage
is counted, and at least one must not be "ok". Forcing the zone to always report "ok" turns it red.

> This also corrects something written here earlier. When the same test failed under TSan, that was put
> down to the instrumentation slowdown. It was not — it was this, and the slowdown only changed which
> instant the coin landed on. The exclusion has been removed and the test runs under TSan again.

Once it could run, the TSan job found things, which is the entire point of a job nobody had ever
executed:

- **`thread_pool_test`, in two steps.** TSan reported a *double lock of a mutex* and a data race on
  the barrier's `std::set`. The shared state was three stack locals captured **by reference** into
  tasks running on other threads — correct only because the pool's destructor joins, which is a fact
  about a different file — and the two calls reuse the same stack addresses, so the second call's mutex
  looked like the first call's, still held. Moving the state to the heap removed the address reuse and
  **TSan still reported the double lock**, so the construct was the problem rather than where it lived.
  The barrier never needed a mutex: it is atomics now — take a slot, record the thread, wait for the
  count. Clean under TSan, and the counter-proof still comes back as 1 on a one-worker pool.
- **`Client::pollChats` — a ThreadSanitizer artefact, established rather than assumed.** Four reports:
  a *double lock of a mutex* in `recvLoop` and data races on `m_incomingChats`, with TSan printing
  `mutexes: write M100` on **both** sides — reader and writer hold the same `m_mtx`, which is not
  something TSan reports. It reproduces **5 runs out of 5 on gcc 11** (Ubuntu 22.04) and **0 out of 6
  on gcc 16**, and the mutex TSan names is the one `queryZone` takes around `m_zoneCv.wait_for`.

  Reduced to 25 lines with no bug in them — `unique_lock` + `cv.wait_for` on one thread, `lock_guard` +
  notify on the other — and gcc 11 reports the same double lock. (The first version of that reduction
  had a real race of its own, a plain `bool stop` written unlocked; with that fixed it still reports.)
  The explicit-loop form of `wait_for` reports it too, so it is `wait_for` itself: **TSan loses the
  mutex across its internal unlock/relock, and every later access under it looks unsynchronised.**
  Nothing to fix in the client; the two tests that link `DGS::Client` — `client_e2e` and
  `client_handoff_e2e` — are excluded from the TSan job by name, with that reason written next to the
  exclusion. The second one was checked rather than assumed into the same bucket: run alone under TSan
  in the CI container it comes back **8 OK · 0 FAILED**, with the handoff completing (transfers 2,
  acked 2, promotions 2), and the two warnings are that same `recvLoop` double lock. It is excluded
  because of the instrumentation, not because it is unhappy.

---

## The game plane, encrypted

TLS covers the TCP control plane. It left the busiest and most personal traffic in the system in clear:
**every player's position, twenty times a second, and the zone's broadcast of everyone else's** —
exactly the feed the observer token exists to protect, readable by anyone on the path, and forgeable
because nothing authenticated it either.

Each UDP datagram is sealed with **AES-256-GCM**: `session(4) || nonce(12) || ciphertext || tag(16)`,
32 bytes of overhead (measured: a 53-byte packet leaves as 85). The tag is what makes forgery fail
rather than only eavesdropping — flipping **one bit** makes the receiver drop it — and the session id,
which travels in the clear, is bound in as additional authenticated data so it cannot be swapped for
somebody else's.

**Not DTLS, and the reason is measured.** A DTLS session is per peer, and a zone's broadcast is one
payload to N recipients: the zone would hold N sessions and encrypt the same snapshot N times, throwing
away the serialise-once-per-tick property that `dataSize` and interest management were measured
against. So it is a group cipher — one key, one seal per frame, N sends.

That distinction is not theoretical. The first version sealed inside `send`, i.e. **once per
recipient**, and the load ramp showed it immediately:

| 256 players, spread, 500 m radius | loop |
|---|---|
| no encryption | 13 383 µs |
| sealed per recipient | 18 301 µs (**+37 %**) |
| sealed once per frame | **13 263 µs** |

Encryption is effectively free at this scale once the frame is sealed where it is built. The same run
also caught the node's own `bytesTx` still counting the **plaintext** size, so its bandwidth metric
under-reported by 28 bytes per datagram the moment encryption was switched on — a capacity number
quietly measuring something other than the wire.

**Two kinds of key, because the broadcast and the uplink are different problems.**

| | key | why |
|---|---|---|
| the zone's **broadcast** | the group key, `DGS_UDP_KEY`, session 0 | the same payload goes to N recipients; one key is what lets the zone seal each frame **once** instead of once per recipient |
| a client's **uplink** | `HMAC-SHA256(DGS_UDP_MASTER, session)` | the servers hold the master and derive; a client is issued only its own key, so it cannot read the player next to it |

The group key alone was the obvious hole: every client holds it, so any client that could capture
another's uplink could read it. That is closed. **What is still missing is the plumbing, not the
mechanism**: the login API already authenticates and returns a token, and that is where the session id
and its key belong — `tests/udp_crypto_test.cpp` computes them exactly as that endpoint would, which is
the honest way to say the mechanism works and the front door is not wired to it yet.

And **replay is not prevented**: a captured datagram can be re-sent inside its lifetime. The
layer above already handles that for the traffic that matters — the validator's minimum-dt discard
exists to reject duplicated and reordered samples — but it is said out loud because a reader would
otherwise assume GCM's nonce covers replay, and it does not.

`tests/udp_crypto_test.cpp` reads what actually travels off a plain socket with no key: with the key
set the marker is **absent** from the wire, without it the marker is **present** — the second line is
what makes the first mean anything. `tests/interest_e2e.cpp` then proves it end to end against a real
zone: with the key a player and a viewer still receive the world, and without it they get nothing
usable.

---

## What anyone on the path can read

Node authentication decided *who* may connect. It did nothing about the fact that **every packet on
every link travelled in clear**: player positions, verdicts, bans, the entity state one zone hands to
its neighbour. Anyone with a tap read all of it, and anyone able to inject into the stream changed it.

`TCPSocket` speaks TLS when `DGS_TLS_CERT`, `DGS_TLS_KEY` and `DGS_TLS_CA` are all set, and it is
**mutual**: the listener demands a client certificate signed by that CA and the connector verifies the
server against the same one. Encryption without identity is only a private conversation with a
stranger. Unset means plain TCP, for the same reason node auth defaults open — a transport change that
bricks every deployment on upgrade is an outage, not a fix.

`tests/tls_test.cpp` puts a **man-in-the-middle socket between the two ends** and checks what actually
travels, rather than asking the socket whether it believes it is encrypted:

```
TLS on : the tap captured 8861 bytes; the plaintext is absent
TLS off: the tap captured  120 bytes; the plaintext is PRESENT
```

The second line is what makes the first mean anything. A certificate from a different CA is refused
(`certificate verify failed` / `tlsv1 alert unknown ca`), which is what separates "encrypted" from
"encrypted for anybody".

**Two ways `poll` and TLS disagree, and both bite.** This is the part that cost the most:

- OpenSSL can hold **decrypted bytes the kernel no longer has**, so `poll` says "nothing" while a
  message is already in hand.
- and the kernel can have bytes that are **not application data** — a TLS 1.3 server sends
  `NewSessionTicket` right after the handshake — so `poll` says "readable" and the blocking read that
  follows waits for four bytes that never come.

The second one froze the whole cluster the first time TLS was switched on: the validator connected to
the head, polled, saw "readable", and hung there for ever **with an empty log**, while the zone waited
on it and logged nothing either. Neither node said anything was wrong. `TCPSocket::pending()` asks
OpenSSL — buffered plaintext, or a non-blocking `SSL_peek` that processes tickets and reports only real
data — and every readiness gate and epoll loop in the repository asks it before reading.

The suite runs with TLS **off**, so that was found by standing the real cluster up with certificates
and watching it: head, persistence, validator, social, zone and four players, which now completes the
chain end to end with **4 documents in Mongo and no handshake failures**.

**A third disagreement, found later: a closed connection is not "no data".** The readiness gates ask
`poll(fd) && pending(fd)`. When a peer *dies*, `poll` reports the descriptor readable — that is what
EOF looks like — and `pending()` answered "no application data", so the node **never called `receive`**
and never got its 0. `zone_node` then noticed the head was gone only later, through a failed `send`:
exactly the blindness that [One `send` is one `receive`](#one-send-is-one-receive) records for plain
TCP, restored by TLS through a different door. Measured with certificates on, `reconnect_e2e` went red
on "it detects the hang-up as a CLEAN CLOSE". `pending()` now answers *"will a read make progress"*,
and the end of a connection is progress.

Three spellings of that end, incidentally: `close_notify` is what a polite peer sends, while a node
that is **killed** sends nothing and the socket just stops — which OpenSSL 3 calls `unexpected eof
while reading` and OpenSSL 1 an `SSL_ERROR_SYSCALL` with an empty error queue. Only the first looked
like a close.

### One connection, two threads

`Client` sends from the caller's thread and receives on `recvLoop`, over the **same** `TCPSocket`.
Without TLS the kernel serialises that and it is correct. With TLS it is not: `SSL_read` and
`SSL_write` share one record layer, one sequence number and one error queue, and running them at once
is undefined behaviour — not a lost message. This was written down as a known trap before it was
fixed, which turned out to understate it. With the locks removed and nothing else changed:

| | |
|---|---|
| 6 runs of `tls_test` phase F | **6 detected it** |
| of those | **3 exited with SIGSEGV** mid-exchange |
| the other 3 | red, having received **37, 39 and 41** of 200 frames |
| with the locks in place | 200/200, 0 damaged, over **20 consecutive runs**, and clean under ThreadSanitizer |

Two locks per connection, because they answer different questions: one so that at most one OpenSSL
call touches an `SSL` at a time, and one so that a whole **frame** (length prefix + payload) is
written before another writer starts — without the second, two senders produce one message's length
followed by another message's bytes, and the peer cannot resynchronise. The plain path gets the frame
lock too; the interleaving problem was never TLS-specific, it was simply unreachable before.

Neither lock is ever held across a wait. A writer parked on a full socket buffer must not stop the
reader from draining the other direction, or two correct-looking locks build a deadlock. That is why
TLS descriptors are made **non-blocking** after the handshake and the waiting happens in `poll`, with
the caller's `SO_RCVTIMEO` read back and honoured — the client's receive loop checks `m_running`
between timeouts, and without that it would never check it again.

### A handshake with no deadline

`connect(host, port, timeoutMs)` promises to return within `timeoutMs`. It bounded the TCP handshake
and then called `SSL_connect` on a blocking descriptor: a service that accepts and then says nothing —
an overloaded node, a hung arbiter, a port held by something that is not us — froze the caller for
ever, and the caller is a node's tick. This is the same lesson `connectWithDeadline` was written for,
walked back in by TLS.

The listener had the worse half. `accept()` ran `SSL_accept` on the node's own loop, so **any** peer
could open a socket, send nothing, and stop that node accepting anybody else. No credentials, no
traffic, one connection — node authentication does not help, because the handshake happens *before*
anyone gets to prove who they are. Both are now bounded (`DGS_TLS_HANDSHAKE_MS`, 5 s by default), and
`tls_test` phase G measures it against a real silent peer: a legitimate client is served **657 ms**
later. Its counter-proof raises the listener's patience to 2.5 s and confirms an 800 ms client is
then *not* served — so the deadline is doing the work, not luck.

---

## Who is allowed to be a node

Nothing between the nodes was authenticated. **Anyone who could reach the head's TCP port could send
one `PKT_METRICS` and be registered as a zone** — and the head then routes entities to it:
reassignments, entity state, region blobs. The same everywhere else: connect to the validator and ask
it to bless a movement, to persistence and write the world (and, since it can now be read, read it
back), to the social node and ban any account. Not one of those ports asked a single question.

A node proves it holds `DGS_CLUSTER_SECRET` by sending, immediately after connecting, a `PKT_AUTH`
carrying a random nonce, a timestamp and `HMAC-SHA256(secret, nonce || timestamp)`. The secret never
travels. The listener checks the MAC, that the timestamp is inside a 30-second window, and that the
nonce has not been used before — so **a captured credential does not work twice**, which a bare
bearer token would.

**The head's port serves clients too**, and a player's client has no business holding the cluster
secret, so there the gate is on the *privileged packets* rather than on the connection: registering a
zone, moving authority, draining, region blobs. Zone queries stay open — clients authenticate against
the login API. The validator, persistence and social ports are node-only and gated wholesale.

What this is **not**, and it matters more than what it is:

- **it is not TLS.** The rest of the connection is in clear: anyone on the path still reads every
  packet and can modify them. This decides who may connect, not what is confidential.
- it authenticates the **connection**, not each packet; someone who can inject into an established TCP
  stream is not stopped by it.
- the replay window is bounded by a timestamp and a nonce cache rather than by a challenge. A full
  challenge-response would be stronger.

**`DGS_CLUSTER_SECRET` unset means disabled**, and that is a deliberate, uncomfortable choice.
Fail-closed is right for the observer feed — an optional debug stream, where refusing costs nothing —
and wrong here, where refusing means no cluster starts at all: a default that bricks every existing
deployment on upgrade is an outage, not a security improvement. So every node **says which of the two
it is** on the line right after it starts listening:

```
[HeadServer] ⚠ node authentication DISABLED: anyone who can reach this port can act as a node
[HeadServer] node authentication REQUIRED (DGS_CLUSTER_SECRET)
```

`tests/auth_e2e.cpp` checks the four cases against a real head, each the counter-proof of the next: a
node **with** the secret registers and gets served (otherwise "the impostor was refused" would also
pass on a head that refuses everybody), the same registration **without** it is refused, a **captured
credential replayed** is refused, and with no secret configured the port is open as documented.
Removing the gate turns exactly the middle two red.

---

## Bans that survive a restart

`SocialState` was a plain local variable: every guild, ban, friendship and permission lived only in
that process. The node *did* send its deltas to the persistence node — its own comment calls that node
*"source of truth for bans/guilds"* — and that node understood nothing but entities and dropped them
without a word. The consequence was concrete: **restarting the social node unbanned every account.** A
moderator's decision, undone by an operator restarting a service.

Three things had to be true, and the third only became visible once the first two existed:

1. **persistence stores the social plane** — guild membership *with its rank*, friendships, and
   account state (ban deadline, reason, permissions), one upserted document per fact so a delta never
   needs a read-modify-write. **Parties are deliberately not stored**: they are session-scoped, and
   pretending they survive would be worse than losing them. Nor is `SOCIAL_ZONE_UPDATE`, which is
   routing rather than state.
2. **the social node asks for it back** on start-up, and applies it through the same code paths a live
   delta goes through, so there is no second implementation of the rules free to disagree with the
   first.
3. **state is replayed to whoever connects.** It never was: bans were broadcast at the instant they
   happened and never again, so a zone that started afterwards — or reconnected, or was scaled up —
   served a banned account happily. Without this, restoring state into memory changes nothing in the
   world.

And the node **does not accept subscribers until its state has loaded**. Serving during that window
means telling a zone "nobody is banned", and it also meant applying live deltas through a persistence
socket that was not connected yet — measured: the ban reached neither the database nor the subscriber.
Connections wait in the listen backlog. It is bounded: past `SOCIAL_RESTORE_MS` + 1.5 s the node starts
serving anyway and says plainly that its state may be incomplete, because a database that never answers
must not keep the social plane offline for ever.

> Two side effects worth recording, because both were the readiness gate doing its job. `social_e2e`'s
> fake persistence never answered the state query — a stub that stays silent is a database that never
> answers, so the node correctly waited out its deadline and that test's 1.5-second expectations
> expired first; the fake speaks the protocol now, and an empty answer is still an answer. And when
> that check failed, `social_e2e` unpacked the packet it had *not* received, threw `Packet read
> overflow` out of `main` and **aborted**: every later check silently never ran and the output ended in
> a core dump instead of a verdict. It only unpacks what arrived now.

`tests/social_persist_e2e.cpp` separates the mechanisms so a pass says *which* one works: a subscriber
on the same live node hears a ban issued before it arrived (replay, no database involved), then the
node is killed and a **new process** must still announce it (store + restore). Its counter-proof is an
account that was never banned, which must not be announced to anyone. With `SOCIAL_RESTORE=0` the live
replay stays green and only the restart phase goes red — exactly the separation the phases promise.

---

## One `send` is one `receive`

`TCPSocket::send` writes a 4-byte `htonl` length prefix and `TCPSocket::receive` reads exactly one
message with `recvAll`. That is why every node can decode a packet straight out of a read instead of
accumulating a stream. Nothing tested it, and the one place the contract broke took the rest of the
connection with it.

**A message too big for the reader's buffer used to desynchronise the connection permanently.**
`receive` returned `-1` *without consuming the payload*, so the next call read four bytes from the
middle of that payload and took them for a length. Measured with a 277-byte message sent into a
256-byte reader followed by three ordinary ones:

```
read -1     the oversized one, correctly refused — but its bytes are still in the stream
read -1     four bytes of payload read as a length
read 255    <- 255 BYTES OF GARBAGE HANDED BACK AS A VALID PACKET
read -1
```

The three innocent messages behind it were lost, and a node would have decoded arbitrary bytes as a
typed packet. Now the payload is consumed and discarded: one message is lost and the stream stays in
step — the same probe gives `-1 24 24 24`, the three chats intact and in order. A length beyond
`MAX_PACKET_SIZE` cannot be skipped safely (those bytes may not exist, and the peer may be hostile), so
it is reported as a dead connection and the caller's existing teardown runs.

`tests/socket_framing_test.cpp` pins all of it. Reverting the fix takes it to **2 OK · 3 FAILED**.

---

## A node that was removed: `cache_node`

It existed for a good reason that stopped being true. Entity positions used to reach the validator over
**UDP**, and a TCP queue alongside it was meant to guarantee what UDP dropped: zones pushed entities in,
the validator pulled them out.

The protocol moved on. The zone talks to the validator over **TCP** with a `PKT_VALIDATE_REQ`
correlated by `requestId` and answered with a verdict — reliable, correlated, and with a circuit
breaker in front of it. No node has sent UDP to the validator for a long time; only tests do, and only
to measure behaviour under a degraded network. The remaining UDP hops are client↔zone position
streams, where a lost sample is superseded by the next one 50 ms later and reliability would actively
hurt (head-of-line blocking on a stream of positions).

Which left the queue with no job, three defects and no client:

- **no correlation** — the validator popped one entity per request, with no `requestId` and no way to
  return a verdict to the zone that owns it;
- **one FIFO shared by every zone**, which is the opposite of one-owner-per-entity;
- **`pop` before delivery, unbounded** — the entity left the queue before the send, so a validator that
  died took it with it, and nothing capped the queue's growth.

The last idea worth testing was to repurpose it as the validator's shared baseline, since `lastKnown`
is per-process: a restart or a second replica would then be judging against its own history. **Measured
before writing anything, and the idea was wrong**: a validator holding no state at all judges its very
first request correctly (a plausible move → verdict 1, a teleport → verdict 0), because the baseline
travels inside the `ValidateRequest`. `lastKnown` is only read on the UDP and raw-TCP paths, and those
have no sender.

So the node was deleted, along with its Dockerfile, its k8s manifests and its test. What it was built
to guarantee — that a transfer cannot be lost — is now guaranteed where the loss actually was: in the
handoff, one section down.

---

## The `dgs` tool

343 lines with no test, and the defects were the kind that only show up in front of someone:

- **`dgs logs <name>` was a command injection.** The argument went straight into
  `system("tail -f " + ...)`. Demonstrated: `dgs logs 'x; touch /tmp/pwned'` ran the `touch` and the
  file appeared — in the one binary an operator runs as root during `install`. The name is checked
  against the known nodes now, before it goes anywhere near a shell.
- **`dgs down` used `pkill -f <node>`**, which matches the whole command line, so it kills an editor
  with that file open, a `tail -f logs/zone_node.log`, another `dgs logs zone_node` — and, memorably,
  the shell that ran it, which happened to me while working on this repository. `-x` matches the
  process *name*, which is what "stop the node" means.
- **`dgs status` was a constant.** The same `pgrep -f` over-match, but worse than a `tail` on a log
  file being mistaken for a node: the pattern matched the `sh -c "pgrep -f 'zone_node'"` that the CLI
  had just spawned to ask the question, so **every node was reported running, always, with nothing
  running at all**. A status that says a dead node is alive is worse than no status.
- **the port table was a parallel array indexed by position**, and removing `cache_node` left it one
  entry long: the validator printed the cache's ports, the social node the validator's. It is a table
  of pairs now, so it cannot name one node and print another's ports.
- **`dgs run` announced "standalone up" and returned 0 with every node dead.** A wrong `DGS_BIN_DIR`
  meant every child failed its `execl` and exited 127, and nothing driving the CLI could tell a
  cluster from an empty room.

`tests/cli_test.cpp` runs all of it against the real binary — no root, no cluster, no network — and
each check has its opposite: the injection is refused *and* a known node's log is still printed, so
"refuses" cannot be satisfied by refusing everything. Reverting the fixes turns five of the ten red.

> The status check took two attempts. The first asserted "no zone is reported running", which is a
> claim about the **machine**, not the CLI: it passed alone and failed under `ctest -j4`, where another
> end-to-end test had a real zone up. Rewriting it as "a decoy must not *change* the answer" fixed the
> flakiness and broke the test instead — "nothing changed" is satisfied by a status that always says
> the same thing, which is exactly the bug. It now picks a node that genuinely has no process (asked
> with `ps -C`, which matches the executable name and so cannot match the asking command line) and
> checks the absolute answer.

---

## Interest management

Every client was sent every entity, so a zone's egress grew as N². That is the wall the capacity table
runs into, twice: first in **bytes** (175 MB/s out of one zone at 64 players, 21 Mbit/s down per
client, before `dataSize` was honoured) and then, once the bytes were small, in **datagram count** —
about 7 µs of `sendto` each, 16384 per tick at 128 players, which put the tick 14 % over budget. Both
are the same mistake: telling everybody about everything.

A player is now sent what is near them (`INTEREST_RADIUS_M`; `0` keeps the old behaviour, which is what
the earlier numbers were measured against). Their own entity always goes — a player who stopped being
told where *they* are is watching somebody else's world — and an **observer is never filtered**,
because a viewer exists to see the whole zone.

**It buys nothing for a crowd, and the measurement says so.** With all 64 players standing in one
chunk, a 500 m radius changes nothing at all: 2.60 MB/s and 32.4 ms of loop, identical to no filtering.
A hundred players in a market square is still a hundred squared. What it buys is scale for a world
that is *spread out*, which is why `tools/load_zone` grew a `LOAD_SPREAD_CHUNKS` knob and both cases
are reported:

| players spread over 40 chunks | without | with a 500 m radius |
|---|---|---|
| N=64 · egress | 2.60 MB/s | **0.07 MB/s** |
| N=64 · loop | 33 617 µs | **997 µs** |
| N=128 · snap/s | 8.4 (over budget) | **10.2** |
| N=128 · loop | 113 992 µs | **3 429 µs** |
| N=256 | did not reach it | **256 served, 13 383 µs, at nominal tick** |

So the same machine goes from breaking at 128 players to carrying **256 with 13 ms of a 100 ms
budget**. (The `snap/s` of 12.0 printed at N=256 is above nominal and is noise in the counting window,
not a 12 Hz tick.)

> The harness itself had to be corrected to measure this. `snap/s` was computed as datagrams divided by
> N twice — sound only while every client received every entity — so under interest management it read
> **0.5 Hz** and printed "the zone is behind" about a zone that was ticking perfectly. It counts each
> client's own entity coming back now, which is one datagram per tick whatever the radius is. A harness
> that measures a world the server no longer implements is worse than no harness.

`tests/interest_e2e.cpp` pins four cases, each the counter-proof of the others: 20 km apart with a
radius, A is **not** told about B; with **no** radius it **is** (otherwise "A heard nothing" would also
pass on a zone that broadcasts nothing); in the **same place** with a radius it **is** (otherwise it
would pass on a zone that only sends you yourself); and an observer sees both throughout.

---

## Handing an entity over

An entity crossing a zone border used to be able to **vanish**. `checkAndTransfer` wrote the reassign
and the state to the head and then erased the entity **unconditionally**, ignoring both `send`
results. Three ways for it to end up owned by nobody:

- the head is down or reconnecting — the writes fail, the entity is erased anyway;
- the head cannot route the chunk (`targetFD == -1`) — it drops the reassign right there, in silence;
- the forward to the new owner fails — same silence.

Afterwards no zone owned it, no counter had moved, and the last line in the log was `Transferring...`.
This was never a UDP problem: that hop is TCP and always was. It was an ignored error and a missing
acknowledgement.

The handoff is **at-least-once** now. `EntityReassign` carries an `ack`: `0` is the request, `1` means
*routed, you may let go*, `2` means *no zone covers that chunk, keep it*. The ceding zone holds the
entity — owned, simulated, broadcast — until it is told `1`, and re-sends on a throttle
(`HANDOFF_RETRY_MS`) until then. The failure mode changed from "the entity disappears" to "the entity
lingers in the wrong zone for a moment", which is visible, recoverable, and bounded by the ordinary
lease.

`tests/handoff_e2e.cpp` drives the three cases against a real `zone_node`, and each is the others'
counter-proof: with `ack=1` the zone must **release** (otherwise "the entity stays" would also pass on
a zone that never hands anything off), with `ack=2` it must **keep**, and with the head killed
mid-handoff the entity must still be there — and the handoff must complete once the head returns.
Reverting the fix turns exactly those two cases red.

> The test's own first version was wrong in a way worth recording: it watched the zone's broadcast
> with a 5-second TTL, so an entity erased a moment after the window opened still read as present.
> The counter-proof caught it — with the fix reverted, that phase stayed **green**. It was measuring
> "was it there at some point" rather than "is it there now".

### And then nobody ever asked for one

All of the above was correct, tested, and **unreachable from the shipped client**.

Only the OWNER can start a handoff: `checkAndTransfer` walks the entities a zone owns and cedes the
ones whose chunk has left its bounds. So the crossing has to be reported to the zone you are leaving.
`handoff_e2e` does exactly that — it sends the out-of-bounds chunk to the zone that owns the entity —
which is why it passes.

`Client::sendTransform` did the opposite. On a chunk change it asked the head **first** and sent the
next datagram to the new zone, so the previous owner never heard the player had left. It simply
stopped receiving updates and the lease GC dropped them a few seconds later. Measured with the real
client, a real head and two real `zone_node`s, walking a player from chunk 0 into chunk 1:

| | |
|---|---|
| `out of bounds. Transferring` | **0** |
| `handoff ACKED` | **0** |
| `promoted to real` | **0** |

Nothing was broken. Nothing was reached. And the consequence is worse than a missing log line: with no
handoff, **no server-side state moves**. The new zone rebuilds the entity from the client's own
datagram, which quietly makes the client the source of truth for its own stats at every border it
crosses — in a system whose whole third layer exists to stop exactly that.

The client now **announces**: it keeps reporting to the zone it is leaving, carrying the new chunk,
for `ZONE_ANNOUNCE_MS` (300 ms — six datagrams at 20 Hz, so losing one to UDP does not lose the
handoff), and only then asks the head. Authority moves when the servers agree it has, not when the
client decides. `tests/client_handoff_e2e.cpp` measures the whole chain through the real `DGS::Client`
and its counter-proof is a player who never crosses, who must cause none.

> `client_e2e` had pinned the old order — *"moving to another chunk DOES re-query the head"* — and it
> was green throughout. A test can hold a contract in place long after the contract became the bug.

### Two more packets that contradicted the last one

Found while writing that test, both silent, both in the same family:

- **`sendTransform` sent a zeroed `Stats`.** The zone's S1 filter allows `maxSpeed * dt + 1 m` and
  believes the most recent packet, so `maxSpeed` was 0 and the 1 m of slack was the only thing letting
  anyone move: about **20 m/s at 20 Hz**, for every player in the engine. Anything faster was discarded
  without a word — indistinguishable from packet loss. `sendStats` could declare a speed and the very
  next transform erased it.
- **`sendStats` and `sendInventory` sent a zeroed position**, teleporting the player to the origin of
  their chunk. S1 rejected them, correctly, so declaring stats was itself unreliable.

The client now remembers both and carries them, and phase C of `client_handoff_e2e` measures it: a
player who declares 400 m/s gets 16+ of 20 fast steps accepted, one who declares nothing gets 18 of 20
**rejected**.

> That check is a comparison rather than a zero on purpose. S1 measures `dt` from the **arrival time**
> of consecutive datagrams, so when packets bunch — which they do the moment the machine is busy — two
> updates 9 m apart can arrive 1 ms apart and the allowance collapses to the slack no matter how fast
> the player is entitled to go. Idle: 0 of 20 rejected. Under the full suite: 8 of 20. That jitter
> sensitivity is S1's own, and it is still there.

---

## A world with things in it

"There are no objects in my database" turned out to be a question about the **lease**, not about
persistence. Every entity in a zone is leased to whoever reports it, and the GC purges anything not
heard from within `ENTITY_LEASE_MS` — 3 seconds by default. For a player who disconnects that is the
point. For a crate on the ground it is fatal: nothing reports a crate.

Measured, three `ENT_ITEM`s placed once through the front door and never touched again:

| | default lease (3 s) | `ENTITY_LEASE_MS=600000` |
|---|---|---|
| served at t=1 s | 3 | 3 |
| served at t=3 s | **0** | 3 |
| served at t=8 s | 0 | **3** |

Persistence was never the problem. The same items were in Mongo in under a second, and a **fresh
zone** — nothing injected, nobody connected — restored all three from the database and served them.
Then lost them again three seconds later, for the same reason.

What was missing was a way to say that an entity belongs to the WORLD rather than to a client.
`STATE_WORLD_OWNED` is that word: an entity carrying the bit is exempt from the lease GC, and is still
broadcast, still persisted, still handed over when it crosses a border. The bit is an ordinary field
of the stored document (`state: 64`), so it survives a restart the way a position does — verified by
restoring from Mongo into a new zone and watching the items outlive the lease.

`tests/world_object_e2e.cpp` pins both halves, and the second is the first's counter-proof: an item
**without** the bit is purged on schedule. Without that, "the crate is still there" would also pass on
a zone whose GC had simply stopped working, which is the more likely bug of the two.

---

## The other half of the client

A player could connect, be routed to a zone, send their position — and stand in an empty world.

`DGS::Client` used its UDP socket to **send** and nothing else. `recvLoop` read the TCP link to the
head; `m_udp` appeared exactly once in `client.cpp`, inside `sendEntityUDP`. So `pollEntities()` and
`pollGhosts()` — which is what an engine renders other players from — were fed by nothing, because in a
real cluster the head routes entity transfers to **zones**, not to clients. The zone's broadcast, the
entire game plane that interest management and the AEAD sealing were built for, had no reader.

Measured against one live zone at one moment:

| | sent | received |
|---|---|---|
| the real `DGS::Client` | 60 transforms | **0 entities, 0 ghosts** |
| `fill_world`, a raw UDP socket on the same protocol | 320 transforms | **316 datagrams** |

The datagrams were arriving. Nobody read them. Same client, same zone, after the fix: **31 entities**.

Why it stayed invisible: every test that touches the client stands up its own fake and hands it an
entity over TCP, which pins the plumbing rather than the path. `tests/client_world_e2e.cpp` asks the
question a player would — *can I see anybody?* — with the counter-proof built in: a player 20 m away
must show up, one 4 km away must not. Without the second, the first would also pass on a zone shouting
everything at everybody, and it pins interest management from the only side that matters, the receiving
one.

> A third thing it pins, because it is a trap rather than a bug: **you hear yourself**. The zone echoes
> the sender's own entity back, so a game that spawns a remote avatar for every uuid it polls will
> duplicate its own player.

The reading runs on its own thread. The two sockets block independently — a quiet head must not delay
the world, and a quiet world must not delay a zone response — and the queue it fills is bounded, because
it is fed by the whole neighbourhood at 10 Hz and drained by a game that might be paused or loading.

---

## Two things on one wire

### 80 ms of nothing, at every border a player crosses

The question was whether chat could cost the head its job as orchestrator. Answering it needed a
baseline — how long a zone query takes with the head idle — and the baseline came back at **82 ms on
loopback**. That was not load. It was `send` writing the 4-byte length prefix and the payload as two
separate calls: the textbook write-write-read that Nagle's algorithm exists to punish, with the peer's
delayed-ACK timer sitting on it for 40 ms in each direction.

| | zone query, p50 |
|---|---|
| two writes | **82.0 ms** |
| one end fixed | 41.0 ms |
| both | **0.07 ms** |

**1170×**, on the path a player takes every time they change chunk. `TCP_NODELAY` was added too and
then measured to contribute *nothing* on top — the framing was all of it — so it stays only as
insurance and is not credited with the win. There is no two-write path left at all: the plain link uses
`writev` (one segment, no copy) and TLS assembles one buffer, because a fallback that wrote twice would
have left the 40 ms stall waiting for the first message big enough to reach it.

> The first version of that fix gave every connection a fixed 64 kB assembly buffer — 65 MB on a head
> holding a thousand players, for frames that are seventy bytes. `writev` copies nothing.

### And then the answer to the question

With a real baseline, chat load against the head:

| chatters | msg/s | head CPU | zone query p50 | p95 |
|---|---|---|---|---|
| 0 | 0 | ~0 % | 0.07 ms | 0.13 |
| 48 | 963 | 3.3 % | 0.06 ms | **5.14** |
| 96 | 1618 | **11.4 %** | **43.60 ms** | 47.10 |

A **600× degradation with the head at eleven per cent of one core**. It never runs out of CPU: it is
head-of-line blocking. The head's `PKT_CHAT` handler fanned every message to every connection it held —
other players, and also every zone and validator, which received a packet they do not handle and
dropped it — so the answer a player waits for arrives *behind* everyone else's conversation on the same
TCP stream. The probe read **74 chat packets per query** to find its own reply.

Chat now goes to `social_node`, which had the channels, the per-uuid rate limit, the ban list and the
subscriber fan-out all along, and which nothing was talking to. The client opens its own link there; the
head has no chat handler left. Same load, measured again:

| | head p50 | head CPU | a listening player | a NODE |
|---|---|---|---|---|
| chat through the head | 43.60 ms | 11.4 % | — | received all of it |
| chat through social | **0.05 ms** | **0.1 %** | 192 messages | **0** |

That last column is the part that nearly made things worse. A zone holds a link to the social node to
receive **bans**, and it drains that link *one message per tick* — so fanning chat down it would have
put a ban behind the entire backlog, which is a worse place for it than the head it just left. Chat is
sent only to connections that never announced themselves as nodes.

> Which needed a fix of its own: `sendAuth` returned early when no secret was configured, so in the
> default configuration **nothing ever announced itself** and the filter quietly did nothing. It now
> always sends the packet — that is the "I am a node" part — with the MAC, the part that proves it,
> zeroed when there is no secret. Found by measuring, not by reading.

> Also found on the way: the listen backlog was **10**. Ninety-six clients connecting at once — which is
> a restart, not an attack — and the kernel refused most of them.

---

## Each channel where its audience lives

Chat had one route — the head, to everybody — and now it has three, chosen by the only thing that can
choose: **who is allowed to hear it**.

| channel | served by | because |
|---|---|---|
| `LOCAL` | `zone_node` | it is the only process that knows who is NEAR you, and it computes exactly that every tick for the entity broadcast |
| `GUILD` | `social_node` | it is the only one that knows who is in your guild |
| `TRADE`, `GLOBAL` | `social_node` | nothing spatial about them |

The split is what keeps both sides simple: **a zone never learns what a guild is, and the social node
never learns where anybody stands.** Proximity chat rides the player's existing UDP link to their zone,
sealed like everything else on that plane, filtered by the same `INTEREST_RADIUS_M` as the world, and
checked against the ban list the zone already keeps for movement. Nothing had to grow a subscription
model.

`CHAT_LOCAL` had been implemented by **nobody**: the social node returned early for it — "the owning
zone emits it" — and `zone_node` contained no mention of chat at all. Measured now, from a real client
through a real zone: the neighbour 20 m away hears it, the player 4 km away hears **0**.

And guild chat was going to **every player online**, which is not a bandwidth question but a privacy
one. It now reaches guild members only, and `social_e2e` — which had pinned the leak, asserting that a
stranger received a guild message sent into a guild that did not exist — pins the opposite, with the
counter-proof that a global message from the same sender still reaches that same stranger.

> How the node knows who is who: from the chat a connection sends, because that already carries the
> sender's uuid. No new packet, no new protocol. The cost is written under **What is missing**: a
> player who has only listened is unknown, and is therefore not sent their guild's conversation.

---

## A key a player was never handed

Every key in the system came from the environment. For a node an operator starts that is right; for a
**player** it is not, and the per-session key is the case that makes it obvious: it exists so that what
one player says about themselves is not readable by the player standing next to them, which a value
baked into `DGS_UDP_SESSION` before the process starts — and shared with everyone who can read the unit
file — does not achieve.

The login response is where a key belongs, and it was being thrown away: `Client::connect` checked the
status code and ignored the body. So the honest description of per-session keys was "the mechanism
works, the plumbing to the front door does not exist".

It exists now. `DGS::setUdpSessionKey` takes a key at runtime and is consulted **before** the
environment, `Client::applyLoginKeys` reads the login response, and a client adopts **two**: the group
key, because the zone seals its broadcast once for everybody and a client that cannot open it is deaf,
and its own session key for what it sends. Either may be absent — a login that hands out nothing leaves
the client exactly where it was.

`client_world_e2e` now runs its entire exchange on an encrypted plane with **no key in the client's
environment**, which is only possible if the adoption works. Its counter-proof forgets the keys
mid-run:

| | |
|---|---|
| keys from the login | the neighbour 20 m away is seen, the one 4 km away is not |
| the same client, keys forgotten | **0 uuids heard** |

> The token is still ignored: nothing downstream asks for one. And `DGS_UDP_KEY` remains the fallback
> for a client that was issued nothing, which is what keeps a plain deployment working.

---

## Filling a cluster that is already running

`load_zone` cannot do this. It stands up its own head, validator and zone, measures them and tears the
lot down — a benchmark that owns the world it measures, which is no use when you want sixty people in
a cluster you are already looking at.

`tools/fill_world` is the other half: it starts nothing and drives N players against a live head, the
way a client does. It **asks the head which zone each player belongs to**, and that is the point — a
filler with sixty sockets pointed at one UDP port populates the picture and hides the interesting part
of it, because nobody ever crosses anything.

```bash
FILL_SPREAD_CHUNKS=8 FILL_CHUNK_BASE=50 FILL_CROSSERS=2 ./build/fill_world 32
```

```
fill_world: 32/32 players placed by the head · 8 chunks from 50 · 2 crossers
  t= 19s  sent 672/s (target 640) · received 21785 · zone queries 40
```

It prints the rate it **achieved**, not the one it intended: a filler that cannot keep up is showing a
world that is not the one you think you are recording, and that looks identical to a zone dropping
people.

Two things it taught, both by being wrong first:

- **A crosser dropped in the middle of a 1 km chunk needs 83 seconds to reach the edge.** Twenty
  seconds of recording, not one handoff. They start `FILL_CROSS_START_M` from the border now, and they
  turn around at the far end instead of wrapping — a 7 km jump back to the first chunk is a teleport,
  which is exactly what S1 exists to reject.
- **Re-querying the head at the border produced zero handoffs**, which is how the client bug above was
  found. It announces to the zone it is leaving first.

To see interest management do anything, the players have to be spread out and `INTEREST_RADIUS_M` has
to be set — it is **0 by default, meaning everybody hears everybody**. With 64 players over 8 chunks,
measured here: egress 2.67 → 0.17 MB/s and loop 28 808 → 1 980 µs.

---

## Restoring a zone

A zone used to start **empty, always**, and nothing could be done about it, because **both ends of
persistence were orphaned**:

- nothing could **read** — `insert_one` and no `find` anywhere in the repository;
- nothing ever **wrote**, either. The write path was reachable only by sending the persistence node a
  raw `PKT_ENTITY_TRANSFER` over TCP, and in the live chain nobody does: the zone sends the validator a
  `PKT_VALIDATE_REQ`, the validator answers an ACK and forwards nothing, and the `cache_node` that
  would have relayed one had no client at all (it has since been removed — see below). Measured with
  head + validator + persistence + zone + four players running for five seconds: **`Entity stored` 0,
  documents in Mongo 0**.

Neither half was worth adding alone: restoring from a database nothing writes to is theatre, and
writing to one nothing reads from is a slow delete. So the loop is closed at both ends.

**The zone writes.** It is the owner, so it is the only node that knows the current state. Periodic
(`ZONE_PERSIST_MS`, default 10 s), not per update — at 10 Hz a zone of 64 players would be 640 upserts
a second to describe a world that changes far more slowly than it is observed. Only entities it holds
the lease on: writing a ghost would mean persisting a neighbour's entity from a stale projection.

**Neither the connect nor the read happens on the tick thread**, and that was learned the hard way,
twice. `connect`'s deadline covers the TCP handshake and *not* the name resolution before it: a
`PERSISTENCE_HOST` that does not resolve cost **11.2 seconds**. Doing the restore inline before the
main loop delayed the zone's first tick by that much and turned four unrelated end-to-end tests red by
starving its metrics; moving it to a thread but leaving the write-through's periodic reconnect inside
the loop froze the tick for 11 s on every retry — `zone_policy_e2e` caught that as "the tick freezes",
which is exactly the failure it exists to catch. Every connect now happens on a worker and the loop
only ever touches a socket that is already connected.

**The zone reads, once, at start-up.** One `PKT_PERSIST_RANGE` for exactly the chunks it serves; the
answer is a stream of entities ended by a bare `PKT_NONE`, so "this region is empty" is an *answer*
rather than a timeout. It is best effort: a zone must come up with no persistence node at all, so a
failure there is a log line, not a refusal to start. Restored entities take an **ordinary lease** — a
player who does not come back is purged by the same GC as anyone else. It repopulates the world, it
does not pin it.

```
players -> zone A -> persistence -> Mongo -> [zone A killed] -> zone B -> broadcast
```

`tests/restore_e2e.cpp` drives exactly that: the zone that reads is a **different process** from the
one that wrote, **no client ever connects to it**, and an outside observer confirms it is broadcasting
the four entities rather than merely counting them. The counter-proof is a third zone covering a
region that does not contain them — same database, same node, same query path, and it must come up
empty. With `ZONE_RESTORE=0` the test goes 3 OK · 3 FAILED, which is what makes the green mean
something.

**A correction, kept here because it is the more useful half of the story.** I first "fixed" this link
by adding length framing to it, on the strength of a measurement that showed the persistence node
storing **6 of 7** entities. That measurement was an artefact of my own probe: it wrote two packets in
a single `send`, and `TCPSocket::send` already length-prefixes every message, so the socket layer
framed those two packets as ONE message — the reader correctly saw one message and decoded the first
packet in it. No node ever writes two packets in one send, and the five sent one at a time all
arrived, which was the number that actually mattered.

So **one `recv` IS one packet here**, and it is a property of the transport rather than luck:
`TCPSocket::send` writes a 4-byte `htonl` prefix and `TCPSocket::receive` reads exactly one message
with `recvAll`. A multi-packet answer — the region query above — is simply N sends read by N receives.
The extra framing was removed again; what remains is this paragraph, because a claim published on a
bad measurement is worth more as a correction than as a deletion.

---

## Watching it run

`dgs_viewer` connects to the head, draws the zone boxes, and shows the entities moving inside them —
real ones solid, ghosts (a neighbour's projection of something it does not own) as wireframes, and
anything sitting in a chunk no zone covers in red.

It watches through a **read-only subscription**: `PKT_OBSERVE` over UDP, which adds it to a zone's own
observer registry. It is not a client — an observer never gets a uuid, never takes a lease and never
becomes an entity — and the subscription is a lease, so a closed window stops costing the zone
bandwidth. `tests/viewer_e2e.cpp` drives all of that headless against a real `zone_node`.

**It is authenticated, and it has to be.** This feed is every entity's position ten times a second,
which is exactly what a wallhack wants, and it started life as one unauthenticated byte: anyone who
could reach a zone's UDP port could ask for it and nothing would record that they had. The zone now
requires the shared secret in `DGS_OBSERVE_TOKEN` and **fails closed** — with none configured there
are no observers at all, so forgetting to set it cannot leave the feed open. The token is compared
without returning early on the first wrong byte, and authenticated observers are capped
(`OBSERVER_MAX`, default 8) because each one multiplies a zone's egress by a whole extra client.

The honest limit: the token travels in clear inside the datagram. This closes *"anyone who can reach
the port"*, not *"anyone who can read the wire"* — on an untrusted path it is sniffable and replayable.
The answer to that is DTLS or a private network for the observer plane, and it is not done. Nothing
between the nodes themselves is authenticated or encrypted either (see **What is missing**).

```bash
export DGS_OBSERVE_TOKEN=$(head -c 24 /dev/urandom | base64)   # same value for zones and viewer

# a world with something in it
./build/head_server_node &
CHUNK_X_MIN=0 CHUNK_X_MAX=100 CHUNK_Y_MIN=0 CHUNK_Y_MAX=100 CHUNK_Z_MIN=0 CHUNK_Z_MAX=100 \
CHUNK_SIZE_X=1000 CHUNK_SIZE_Y=1000 CHUNK_SIZE_Z=1000 ZONE_UDP_PORT=42420 \
HEAD_SERVER_HOST=127.0.0.1 GAME_MODULE_SO=./build/stub_rules.so ./build/zone_node &

./build/demo_player 127.0.0.1 42420 8 &   # eight entities walking in circles
./build/dgs_viewer                        # 0 all views · 1-6 one view · G ghosts · TAB list
```

`dgs_viewer` needs raylib; without it every other target still builds. `DGS_CHUNK_SIZE` (default 1000)
has to match the zone's `CHUNK_SIZE_*`, because chunk size is not on the wire — the head only hands it
to the nodes in their initial `Command`.

---

## Deploying to Kubernetes (minikube)

> Set `DGS_CLUSTER_SECRET` on **every** node or on none. A half-configured cluster is one where the
> unconfigured nodes still accept anybody, which is the worst of both: the operator believes it is
> closed and it is not. `k8s/zone-node/deployment.yaml` carries the secret block, commented, with the
> `kubectl create secret` line to go with it.


```bash
minikube start --driver=docker
./k8s/deploy.sh
kubectl get pods -n dgs -w
```

The HeadServer uses a `ServiceAccount` with a `Role` limited to *patching* the `zone-node` Deployment,
so it can scale nodes through the Kubernetes REST API when the load threshold is exceeded
(`RAM > 80%`).

---

## Coordinate System

A two-level world space, to reach galactic scale with millimetre precision:

```
global position = chunkIndex * chunkSize + localPos

chunkIndex  → int32_t   (chunk grid)
chunkSize   → float     (sent by the head at start-up, e.g. 1500.0)
localPos    → float     (sub-chunk offset)
```

Chunk sizes travel in the head's initial `Command` and are used as **metres** everywhere downstream
(`chunkX * chunkSizeX + pos[0]`), on both the client and the server, so the two predictions agree.

Zone bounds are defined per node as `[chunkXMin, chunkXMax] × [chunkYMin, chunkYMax] ×
[chunkZMin, chunkZMax]`.

---

## Anti-Cheat

The owner predicts the movement and asks for validation; the validator arbitrates:

```
radius = (maxSpeed × Δt) + SCALE + (LATENCY_COM × maxSpeed)
```

If the reported position falls outside the `radius`, the packet is discarded and the event is logged.
The validation itself (`validateMove`) is executed by the **project's rules module** loaded through
`dlopen`, not by a fixed formula in the core.

Samples arriving below a minimum interval (`VALIDADOR_MIN_DT_MS`, default 5 ms) are neither judged nor
used to update the baseline. That is the defence against UDP reordering: an old sample arriving after a
newer one carries a large distance and a near-zero `dt` — the exact signature of a teleport. Measured
through the degrading proxy, without this it produced 13 false violations out of 14 reordering events.
Not updating the baseline is the other half: a cheater flooding closely spaced samples buys no
distance, which `net_degraded` verifies with a flood of 20 jumps of 100 m at 1 ms apart.

---

## Environment Variables

| Variable | Default | Used by |
|----------|---------|---------|
| `HEAD_SERVER_HOST` | `head-server` | zone_node, validator |
| `HEAD_SERVER_PORT` | `42424` | zone_node, validator |
| `PERSISTENCE_HOST` | `persistence` | validator |
| `PERSISTENCE_PORT` | `42429` | validator |
| `VALIDADOR_UDP_PORT` | `42427` | validator |
| `VALIDADOR_TCP_PORT` | `42428` | validator |
| `VALIDADOR_MIN_DT_MS` | `5` | validator (minimum-dt discard, anti-reordering) |
| `MONGO_URI` | `mongodb://mongodb:27017` | persistence |
| `MONGO_DB` | `dgs_persistance` | persistence (so a test does not touch live data) |
| `MONGO_HOST` / `MONGO_PORT` | `127.0.0.1` / `27017` | persistence_e2e (where to find a live database) |
| `DGS_REQUIRE_MONGO` | _(unset)_ | persistence_e2e, restore_e2e (turns the live-phase skip into a failure) |
| `ZONE_PERSIST_MS` | `10000` | zone_node (write-through period; 0 disables it) |
| `HANDOFF_RETRY_MS` | `1000` | zone_node (spacing between re-sends of an un-acked handoff) |
| `INTEREST_RADIUS_M` | `0` (= tell everyone everything) | zone_node (how far a player is told about) |
| `LOAD_SPREAD_CHUNKS` | `0` (= one chunk) | load_zone (spread the synthetic players over N chunks) |
| `ZONE_RESTORE` | _(unset = on)_ | zone_node (`0` starts with an empty world) |
| `ZONE_RESTORE_MS` | `2000` | zone_node (deadline for the restore answer) |
| `ZONE_RESTORE_MAX` | `1024` | zone_node (cap on entities asked for; the node caps at 4096 anyway) |
| `SOCIAL_RESTORE` | _(unset = on)_ | social_node (`0` starts with an empty social plane) |
| `SOCIAL_RESTORE_MS` | `2000` | social_node (deadline for the restore answer) |
| `DGS_CLUSTER_SECRET` | _(unset = ports open)_ | every node (shared secret for node authentication) |
| `DGS_TLS_CERT` / `DGS_TLS_KEY` / `DGS_TLS_CA` | _(unset = plain TCP)_ | every node (mutual TLS; all three or none) |
| `DGS_TLS_HANDSHAKE_MS` | `5000` | every node (deadline for a TLS handshake, both directions) |
| `DGS_UDP_KEY` | _(unset = plain UDP)_ | zone, clients, viewer (group key: the broadcast) |
| `DGS_UDP_MASTER` | _(unset)_ | servers only (derives per-session uplink keys) |
| `DGS_UDP_SESSION` / `DGS_UDP_SESSION_KEY` | _(unset = group key)_ | a client's own session id and its 64-hex key |
| `ZONE_ANNOUNCE_MS` | `300` | client (how long a crossing player keeps reporting to the zone it leaves) |
| `VALIDATOR_COMMAND_MS` | `500` | validador_node (how long it looks for an initial `Command`) |
| `API_HOST` | `api` | client |
| `API_PORT` | `8080` | client |
| `HTTPS_ENABLE` | _(unset)_ | client |
| `CHUNK_X/Y/Z_MIN/MAX` | `0 / 100` | zone_node |
| `ENTITY_LEASE_MS` | `3000` | zone_node (per-entity lease duration) |
| `GHOST_TTL_MS` | `3000` | zone_node (ghost projection TTL) |
| `VALIDATOR_RETRY_MS` | `2000` | zone_node (spacing between validator reconnects) |
| `VALIDATOR_RETRY_MAX_MS` | `30000` | zone_node (cap of the reconnect backoff) |
| `VALIDATOR_CONNECT_MS` | `500` | zone_node (connect deadline, keeps the tick from freezing) |
| `ZONE_TICK_US` | `100000` | zone_node (tick period; the loop is fixed-period, not fixed-pause) |
| `ZONE_UDP_DRAIN_MAX` | `256` | zone_node (datagrams drained per tick, so a flood cannot starve it) |
| `DGS_OBSERVE_TOKEN` | _(unset → observers refused)_ | zone_node, dgs_viewer |
| `OBSERVER_LEASE_MS` | `5000` | zone_node (an observer must re-subscribe to keep being fed) |
| `OBSERVER_MAX` | `8` | zone_node (cap on authenticated observers) |

---

## What is missing

Written down because a list of what is not there is worth more than a list of what is:

- **One certificate for the whole cluster, in the test tooling.** `tools/tls/make_certs.sh` issues a
  single shared certificate so mutual TLS can be exercised end to end. A real deployment issues one per
  node from a CA it controls, and rotates them; none of that is here.
- **There is no REAL login API.** `Client::connect` does a `POST /api/auth/login` and refuses to go
  further on anything but a 200 — before it asks the head for a zone. Grep the repository: that path
  appears in the client, in the tests that fake it in-process, and in `tools/fake_login_api`, which is a
  development stub that **accepts every password**, is in no manifest, and must never be deployed. What
  the endpoint has to *do* is now settled and exercised — it hands out the group key and a per-session
  UDP key, and the client adopts both — but nothing authenticates anybody, and the token it returns is
  still ignored downstream.
- **Nothing has anything to put in `sendInventory` yet.** The path exists end to end now
  (`Haruka::Network::sendInventory` → `Application` → `Client`) and the zone keeps what it is sent, but
  no game serialises a backpack into it, so the blob is always empty.
- **A player who only listens does not hear their guild.** The social node learns which uuid is behind
  a connection from the chat that connection SENDS — which is what the protocol already carries — so a
  player who has never spoken is unknown, and an unknown connection is not given a guild's
  conversation. Erring towards silence is deliberate (the alternative leaks), but the real answer is an
  identify packet the client sends on connecting, and that is not written.
- **`TRADE` and `GLOBAL` still go to every player.** Only `GUILD` is filtered by membership. There is
  no party channel at all: the social node keeps party state and `ChatChannel` has no value for it.
- **A payload cannot be CLEARED.** `dataSize == 0` now means "unchanged", which is what makes an
  inventory survive movement — and it leaves no way to say "mine is empty now". Nothing needs it today;
  it would need a signal of its own.
- **A full payload does not fit in one datagram.** `MAX_ENTITY_DATA` is 4096 bytes, so an entity
  carrying a full inventory is a ~4.2 kB UDP datagram: over a typical 1500-byte MTU, it fragments, and
  a single lost fragment loses the whole thing. Untested, because nothing sends one yet.
- **UDP replay is not prevented.** A captured datagram can be re-sent inside its lifetime; the
  validator's minimum-dt discard and S1 are what handle that today.
- **S1 measures `dt` from the arrival time of consecutive datagrams**, so its allowance
  (`maxSpeed * dt + 1 m`) collapses to the slack whenever packets bunch up — which they do the moment
  the machine or the network is busy. Measured on an idle box: 0 of 20 legitimate fast steps rejected.
  Under the full test suite: 8 of 20. Nothing in the system compensates for it, and a player on a
  jittery connection is punished for their link rather than for their speed. Flooring `dt` at the tick
  interval, or trusting a client timestamp bounded by the server's, would both work; neither is done.
- **Nine tests do not pass with TLS switched on**, and until now nobody had ever asked. Running the
  whole suite with `DGS_TLS_*` set is a different run from the one CI does, and the first time it was
  tried it came back **13 red**; the fixes below took it to 9, and the run got twice as fast (288 s →
  129 s) because most of what was left was something waiting. What remains, classified rather than
  guessed:
  - three (`persistence_e2e`, `restore_e2e`, `social_persist_e2e`) probe **MongoDB** with a
    `TCPSocket`, and TLS here is a **per-process** switch: a socket aimed at a service that is not a
    DGS node cannot handshake with our CA. That is a design fact worth knowing before someone puts a
    node behind a TLS-terminating proxy.
  - `socket_framing` injects a corrupt length with a raw `::send` on the descriptor, underneath TLS. It
    measures a plain-TCP property and cannot mean anything through a record layer.
  - the other five (`client_e2e`, `validator_e2e`, `action_e2e`, `handoff_e2e`, `zone_policy_e2e`)
    are **not diagnosed**. Their harnesses do use `TCPSocket`, so "the fake server speaks plain TCP" is
    not the explanation; `validator_e2e` fails to connect to the node it spawned and then dies of
    SIGPIPE. Not investigated further, and saying so is the point.
- **It has never run on more than one machine.** Every number here is loopback on one box. The k8s and
  terraform manifests exist and have not been exercised against a real cluster.
- **CI has still never run on GitHub.** The workflow now runs green in a clean `ubuntu:22.04`
  container (28/28, live database included) and both jobs were fixed on the strength of that, but no
  run has happened on a real runner.

---

## License

MIT
