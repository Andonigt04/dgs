#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────────────────────────
# demo_cluster.sh — the cluster a recording needs, in one command.
#
# `dgs run` already starts a standalone cluster, but it starts ONE zone, and one zone cannot show the
# thing worth showing: a player crossing a border and the authority moving with them. This stands up
# TWO zone_nodes side by side and sets the three variables that decide whether the demo shows anything.
#
# ⚠️ THE THREE VARIABLES ARE NOT DECORATION, and each one has cost somebody an afternoon:
#   · INTEREST_RADIUS_M is 0 by default, which means EVERYBODY HEARS EVERYBODY. Recorded like that,
#     64 players cost 28 ms of loop instead of 2 and the best thing in the system is invisible.
#   · ENTITY_LEASE_MS is 3000, and the lease GC purges anything nobody reports. Props on the ground
#     vanish three seconds after you place them unless they carry STATE_WORLD_OWNED.
#   · DGS_OBSERVE_TOKEN is unset, and with no token the zone refuses every observer — which is the
#     right default in a cluster and exactly wrong when you are trying to point a viewer at it.
#
# ⚠️ ZONE A OWNS EXACTLY ONE CHUNK, on purpose. `fill_world` puts its crossers in the first chunk of
# the range, so if zone A owned several the first border they reached would be one of its own and no
# handoff would happen. One chunk means the first crossing is between two nodes, seconds in.
#
#   ./tools/demo_cluster.sh start | stop | status
# ─────────────────────────────────────────────────────────────────────────────────────────────────
set -u

BIN=${DGS_BIN_DIR:-./build}
LOG=${DGS_LOG_DIR:-./demo-logs}
PIDFILE="$LOG/demo.pids"

CHUNK_M=${CHUNK_SIZE_M:-1000}
# ⚠️ WHERE THE PLAYERS ACTUALLY ARE. A zone only answers for the chunks it covers, and a game does not
# spawn at the origin: Survival puts a character on the surface of a planet whose centre is 1.46e8 m
# away, which is chunk 146089. Against a cluster covering chunks 0..7 the head has no zone, answers
# with an empty `ZoneResponse`, and the client used to accept it and send its position to nowhere.
#
# THAT IS NO LONGER SOMETHING YOU HAVE TO GET RIGHT. Zone B is now open-ended (X0+1 .. 1000000)
# instead of X0+1..X0+7, so a client spawning anywhere positive is served without anyone guessing.
# DEMO_CHUNK_X only decides where `fill_world`'s crowd and the A/B border go — it is a camera
# position, not a requirement.
#
# To point the crowd at your player, read the game's own log line and divide by the chunk size:
#     [Character] Created (local) at 1.46089e+08, 3.28956e+06, 4.19002e+06   ->  DEMO_CHUNK_X=146089
# Y and Z are covered by a deliberately wide box below, so only X needs saying.
CHUNK_X0=${DEMO_CHUNK_X:-0}
# ⚠️ 500 m NO CUBRE UN CHUNK, y con este clúster eso significa un mundo vacío. El juego aparece en
# el ORIGEN de su chunk y `fill_world` patrulla alrededor del CENTRO (500 m adentro): 707 m en
# diagonal, fuera de un radio de 500. Medido con una sonda plantada en el punto exacto donde nace el
# personaje: "0 other entities seen" — conectado, enrutado a la zona correcta, y solo en el mundo.
# La diagonal de un chunk de 1000 m son 1414 m, así que 1500 garantiza que ves a todo el que comparte
# tu chunk. Sigue siendo interest management (no es 0, que es el defecto del zone_node y el que hace
# que 64 jugadores cuesten 28 ms de bucle en vez de 2); solo es un radio que cabe la escena.
RADIUS=${INTEREST_RADIUS_M:-1500}
LEASE=${ENTITY_LEASE_MS:-60000}
TOKEN=${DGS_OBSERVE_TOKEN:-demo-token}

HEAD_PORT=42424
ZONE_A_UDP=42425
ZONE_B_UDP=42426
VALID_TCP=42428
PERS_PORT=42429
SOCIAL_PORT=42430

start_node() {   # start_node <name> <logfile> <env assignments...>
    local name=$1 log=$2; shift 2
    if [ ! -x "$BIN/$name" ]; then
        echo "  - $name: not built, skipped"
        return
    fi
    # ⚠️ RUN THEM FROM THE LOG DIRECTORY. Several nodes write a CSV of their own next to wherever they
    # were started, so launching them from the repo root drops `headserver_log.csv` and friends into
    # the working tree on every demo.
    local abs; abs=$(cd "$(dirname "$BIN/$name")" && pwd)/$name
    ( cd "$LOG" && export "$@" && exec "$abs" ) > "$LOG/$log" 2>&1 &
    echo $! >> "$PIDFILE"
    echo "  + $name (pid $!) -> $LOG/$log"
}

cmd_stop() {
    if [ ! -f "$PIDFILE" ]; then echo "nothing to stop (no $PIDFILE)"; return 0; fi
    # ⚠️ BY PID, NOT BY NAME. `pkill -f build/zone_node` matches the very shell running that command
    # and kills it; `pkill -x head_server_node` silently matches nothing, because the name is 16
    # characters and pkill's -x compares against a 15-character field. Both have happened here.
    local n=0
    while read -r pid; do
        if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null && n=$((n+1)); fi
    done < "$PIDFILE"
    sleep 1
    rm -f "$PIDFILE"
    echo "stopped $n process(es)"
}

cmd_status() {
    if [ ! -f "$PIDFILE" ]; then echo "not running"; return 1; fi
    local alive=0 total=0
    while read -r pid; do
        total=$((total+1))
        kill -0 "$pid" 2>/dev/null && alive=$((alive+1))
    done < "$PIDFILE"
    echo "$alive of $total processes alive"
    [ "$alive" -gt 0 ]
}

cmd_start() {
    mkdir -p "$LOG"
    if [ -f "$PIDFILE" ] && cmd_status >/dev/null 2>&1; then
        echo "already running — ./tools/demo_cluster.sh stop first"; exit 1
    fi
    : > "$PIDFILE"

    local COMMON=(
        "HEAD_SERVER_HOST=127.0.0.1" "HEAD_SERVER_PORT=$HEAD_PORT"
        "VALIDATOR_HOST=127.0.0.1"   "VALIDATOR_TCP_PORT=$VALID_TCP"
        "SOCIAL_HOST=127.0.0.1"      "SOCIAL_TCP_PORT=$SOCIAL_PORT"
        "PERSISTENCE_HOST=127.0.0.1" "PERSISTENCE_PORT=$PERS_PORT"
        "MY_POD_IP=127.0.0.1"
    )
    local ZONE_COMMON=(
        # Wide on Y and Z on purpose: the interesting border for a demo is one axis, and a player who
        # falls outside the box on the other two is a silent failure, not an experiment.
        "CHUNK_Y_MIN=-1000000" "CHUNK_Y_MAX=1000000"
        "CHUNK_Z_MIN=-1000000" "CHUNK_Z_MAX=1000000"
        "CHUNK_SIZE_X=$CHUNK_M.0" "CHUNK_SIZE_Y=$CHUNK_M.0" "CHUNK_SIZE_Z=$CHUNK_M.0"
        "INTEREST_RADIUS_M=$RADIUS" "ENTITY_LEASE_MS=$LEASE"
        "DGS_OBSERVE_TOKEN=$TOKEN" "ZONE_PERSIST_MS=1000"
        # The game plane, encrypted. The zone holds the GROUP key (it seals its broadcast once with it
        # for everybody) and the MASTER (it derives any client's session key to open their uplink). No
        # client needs either in its environment: the login hands them out.
        "DGS_UDP_KEY=${DGS_UDP_KEY:-demo-group}" "DGS_UDP_MASTER=${DGS_UDP_MASTER:-demo-master}"
    )

    echo "starting the demo cluster:"
    # ⚠️ WITHOUT THIS NOTHING CAN LOG IN. `Client::connect` refuses to go past a non-200 from
    # `/api/auth/login`, and no node implements it — so a real game client cannot reach a real cluster
    # at all. `fake_login_api` ACCEPTS EVERY PASSWORD; it belongs in a dev cluster and nowhere else.
    # ⚠️ THE WORLD CLOCK COMES OUT OF HERE TOO, and it is passed explicitly rather than left to
    # inheritance so it is visible in this file. Without it every client starts its own simulation
    # clock at zero and two players who launched minutes apart are in different hours of the day.
    #   WORLD_TIME_SCALE=200 ./tools/demo_cluster.sh start   -> a day goes by in about seven minutes
    start_node fake_login_api api.log "FAKE_API_HOST=127.0.0.1" \
        "DGS_UDP_KEY=${DGS_UDP_KEY:-demo-group}" "DGS_UDP_MASTER=${DGS_UDP_MASTER:-demo-master}" \
        "WORLD_TIME_SCALE=${WORLD_TIME_SCALE:-1.0}" "WORLD_START_S=${WORLD_START_S:-0.0}"
    start_node head_server_node head.log "${COMMON[@]}"
    sleep 1
    start_node persistance_node persistence.log "${COMMON[@]}" \
        "MONGO_URI=${MONGO_URI:-mongodb://127.0.0.1:27017}" "MONGO_DB=${MONGO_DB:-dgs_demo}"
    # ⚠️ WAIT FOR PERSISTENCE BEFORE THE VALIDATOR, and not because the validator needs it — it does
    # not any more. It is so the demo forwards accepted state instead of silently running without it:
    # persistence takes a moment to reach Mongo, and a validator started inside that window comes up
    # with forwarding off and no way to turn it back on. Readiness, not a guessed sleep.
    for i in $(seq 1 40); do
        grep -q "listening\|Listening\|Persistence" "$LOG/persistence.log" 2>/dev/null && break
        sleep 0.25
    done
    # ⚠️ SIN MODULO DE REGLAS NO SE PUEDE COLOCAR NADA, y eso es correcto pero hay que decirlo. El
    # validador falla CERRADO en las acciones: sin regla que las acepte, rechaza. El modulo existe
    # (`libharuka_rules.so`, que construye el motor) y lo unico que faltaba era que estuviera donde el
    # nodo corre — los nodos se lanzan desde el directorio de logs, asi que la ruta tiene que ser
    # absoluta o `dlopen` no lo encuentra y el log dice "no rules module".
    local RULES=${GAME_MODULE_SO:-$(cd "$(dirname "$0")/../../haruka-cpp/build" 2>/dev/null && pwd)/libharuka_rules.so}
    if [ -r "$RULES" ]; then
        echo "  · reglas: $RULES"
    else
        echo "  · reglas: NO ENCONTRADAS ($RULES) -> las acciones se rechazaran (fail-closed)"
    fi
    start_node validador_node   validator.log  "${COMMON[@]}" "GAME_MODULE_SO=$RULES"
    start_node social_node      social.log     "${COMMON[@]}"
    sleep 1
    start_node zone_node zoneA.log "${COMMON[@]}" "${ZONE_COMMON[@]}" \
        "ZONE_UDP_PORT=$ZONE_A_UDP" "CHUNK_X_MIN=$CHUNK_X0" "CHUNK_X_MAX=$CHUNK_X0"
    start_node zone_node zoneB.log "${COMMON[@]}" "${ZONE_COMMON[@]}" \
        "ZONE_UDP_PORT=$ZONE_B_UDP" "CHUNK_X_MIN=$((CHUNK_X0+1))" "CHUNK_X_MAX=1000000"

    # Readiness, not a guessed sleep: a zone is up when it has told the head so.
    local i
    for i in $(seq 1 60); do
        if grep -q "Connected to HeadServer" "$LOG/zoneA.log" 2>/dev/null &&
           grep -q "Connected to HeadServer" "$LOG/zoneB.log" 2>/dev/null; then break; fi
        sleep 0.5
    done
    if ! grep -q "Connected to HeadServer" "$LOG/zoneB.log" 2>/dev/null; then
        echo; echo "  the zones did not reach the head — look at $LOG/zoneA.log and $LOG/zoneB.log"
        exit 1
    fi

    cat <<EOF

cluster up: zone A owns chunk x=${CHUNK_X0}, zone B owns x=$((CHUNK_X0+1))..1000000
  (chunk = ${CHUNK_M} m · y and z: -1000000..1000000)
  a client anywhere at x>=${CHUNK_X0} is served; DEMO_CHUNK_X only moves the crowd and the A/B border
  interest radius ${RADIUS} m · entity lease ${LEASE} ms · observer token "$TOKEN"

next:
  # the crowd — 32 players over 8 chunks, 2 of them crossing the A/B border every ~17 s
  # ⚠️ DGS_UDP_KEY IS NOT OPTIONAL HERE, and this line used to be printed without it. The game plane
  #    is sealed, a real client gets the key from the login, and `fill_world` does not log in — so
  #    without it every datagram it sends is dropped by the zone and the world stays EMPTY. It fails
  #    silently and looks exactly like a filler that is running fine: measured, "sent 252/s ·
  #    received 0" with the head reporting entities=0. With the key: received 2875, entities climbing.
  DGS_UDP_KEY=${DGS_UDP_KEY:-demo-group} \\
  FILL_SPREAD_CHUNKS=8 FILL_CHUNK_BASE=${CHUNK_X0} FILL_CROSSERS=2 CHUNK_SIZE_M=${CHUNK_M} \\
    $BIN/fill_world 32

  # a game client can now log in: the menu's defaults (127.0.0.1:42424, api 127.0.0.1:8080) match
  #   the game plane is ENCRYPTED and the client needs no key: the login hands it both

  # the viewer, in a corner of the screen
  DGS_OBSERVE_TOKEN=$TOKEN DGS_CHUNK_SIZE=${CHUNK_M}.0 $BIN/dgs_viewer 127.0.0.1 $HEAD_PORT

  # watch a handoff happen
  tail -f $LOG/zoneA.log | grep --line-buffered -E "out of bounds|handoff ACKED"

  ./tools/demo_cluster.sh stop
EOF
}

case "${1:-start}" in
    start)  cmd_start  ;;
    stop)   cmd_stop   ;;
    status) cmd_status ;;
    *) echo "usage: $0 start|stop|status"; exit 2 ;;
esac
