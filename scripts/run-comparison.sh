#!/usr/bin/env bash
#
# Cross-driver comparison: deploy each backend one at a time, run identical
# workloads with 3 worker pods (a distributed load generator), gather the
# per-worker results, tear it down, then build one comparison report.
#
# Usage:  scripts/run-comparison.sh [backend ...]      (default: all six)
# Env:    KUBE_CONTEXT=kind-matteo  NS=bench  IMAGE=oxia/benchmark:local
#         WORKLOADS=conf/workload-comparison.yaml  RESULTS=comparison-results
#         OUT=comparison-report  PER_BACKEND_TIMEOUT=600
#
# Prereqs: the worker image is built and loaded into the cluster, and the report
# jar is built (./gradlew shadowJar). See README "Cross-driver comparison".

set -uo pipefail

CHART="charts/benchmark-stack"
CTX="${KUBE_CONTEXT:-kind-matteo}"
NS="${NS:-bench}"
IMAGE="${IMAGE:-oxia/benchmark:local}"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-Never}"
NODE_POOL="${NODE_POOL:-}"               # if set, pin all pods to this GKE nodepool + tolerate its taints
WORKER_REPLICAS="${WORKER_REPLICAS:-}"   # if set, override the per-backend worker replica count
WORKER_CPU="${WORKER_CPU:-}"             # if set, override the per-backend worker CPU request/limit
STORAGE_CLASS="${STORAGE_CLASS:-}"       # if set, put every backend's data PVCs on this storage class
OXIA_IMAGE="${OXIA_IMAGE:-}"             # if set (repo:tag), run the oxia backend with this server image
WORKLOADS="${WORKLOADS:-conf/workload-comparison.yaml}"
RESULTS="${RESULTS:-comparison-results}"
OUT="${OUT:-comparison-report}"
PER_BACKEND_TIMEOUT="${PER_BACKEND_TIMEOUT:-600}"
CRASH_RESTART_LIMIT="${CRASH_RESTART_LIMIT:-5}"  # bail once any container has crash-looped this many times

if [ "$#" -gt 0 ]; then BACKENDS=("$@"); else BACKENDS=(oxia etcd zookeeper redis consul tikv); fi

JAR=$(ls build/libs/oxia-benchmark-*-all.jar 2>/dev/null | head -1)
[ -z "$JAR" ] && { echo "Build the jar first: ./gradlew shadowJar"; exit 1; }

kc() { kubectl --context "$CTX" -n "$NS" "$@"; }
hl() { helm --kube-context "$CTX" -n "$NS" "$@"; }

# Oxia's coordinator rebalances replicas and leaders on a timer, and every move is a leader
# election. On an autoscaling cluster the data servers start minutes apart, so the initial
# placement is skewed and the balancer keeps moving shards (and failing in-flight writes)
# during the measured runs. For oxia* backends the workers are therefore held back until
# replicas and leaders are evenly spread and stable, and the balancer is then frozen for
# the rest of the run. OXIA_SETTLE=0 restores the old behavior.
OXIA_SETTLE="${OXIA_SETTLE:-1}"
OXIA_SETTLE_TIMEOUT="${OXIA_SETTLE_TIMEOUT:-1200}"
OXIA_PLACEMENT="$(cd "$(dirname "$0")" && pwd)/oxia_placement.py"

# "<balanced 0|1> <term-sum> <summary>" for an oxia release (see scripts/oxia_placement.py).
oxia_placement() {
  local n; n=$(kc get sts "$1" -o jsonpath='{.spec.replicas}' 2>/dev/null)
  kc get configmap "$1-status" -o jsonpath='{.data.status}' 2>/dev/null | python3 "$OXIA_PLACEMENT" "${n:-0}"
}

# Wait for an even, stable placement, then freeze the balancer: an effectively infinite
# schedule interval and quarantine. The coordinator reads both only when it takes the
# leadership, so the coordinators are restarted in place (the data servers keep running).
oxia_settle_and_freeze() {
  local rel=$1 n cur prev="" stable=0 deadline cfg terms_before terms_after i
  n=$(kc get sts "$rel" -o jsonpath='{.spec.replicas}' 2>/dev/null)
  echo "   oxia: waiting for $n data servers"
  kc rollout status "sts/$rel" --timeout="${OXIA_SETTLE_TIMEOUT}s" >/dev/null 2>&1
  echo "   oxia: waiting for an even, stable placement (timeout ${OXIA_SETTLE_TIMEOUT}s)"
  deadline=$((SECONDS + OXIA_SETTLE_TIMEOUT))
  while [ $SECONDS -lt $deadline ]; do
    cur=$(oxia_placement "$rel")
    # Balanced, and no election since the previous reading (term-sum unchanged).
    if [ "${cur%% *}" = 1 ] && [ -n "$prev" ] && [ "$(echo "$cur" | cut -d' ' -f2)" = "$(echo "$prev" | cut -d' ' -f2)" ]; then
      stable=$((stable + 1))
    else
      stable=0
    fi
    [ "$stable" -ge 2 ] && break   # balanced and unchanged across three readings, 60s apart
    prev=$cur
    sleep 30
  done
  if [ "$stable" -ge 2 ]; then
    echo "   oxia: settled after $((SECONDS + OXIA_SETTLE_TIMEOUT - deadline))s: $(echo "$cur" | cut -d' ' -f3-)"
  else
    echo "!! oxia placement not settled after ${OXIA_SETTLE_TIMEOUT}s, freezing it as is: $(echo "$cur" | cut -d' ' -f3-)"
  fi

  cfg=$(kc get configmap "$rel-coordinator" -o jsonpath='{.data.config\.yaml}')
  if printf '%s\n' "$cfg" | grep '^loadBalancer:' >/dev/null; then
    echo "!! $rel-coordinator already sets loadBalancer, leaving the balancer as configured"
    return 0
  fi
  printf '%s\nloadBalancer:\n  scheduleInterval: 8760h\n  quarantineTime: 8760h\n' "$cfg" \
    | python3 -c 'import json,sys; print(json.dumps({"data": {"config.yaml": sys.stdin.read()}}))' > "${TMPDIR:-/tmp}/oxia-lb-patch.json"
  kc patch configmap "$rel-coordinator" --type merge --patch-file "${TMPDIR:-/tmp}/oxia-lb-patch.json" >/dev/null
  terms_before=$(oxia_placement "$rel" | cut -d' ' -f2)
  # The leading coordinator exits on SIGTERM and a standby takes the lease; a standby that
  # is waiting for the lease ignores SIGTERM, which is fine: it builds its balancer from
  # the (now frozen) config only when it becomes the leader. What matters is that the
  # coordinator leading afterwards has restarted since the patch: it is the one whose
  # current log says "Started leading", with a restart count above its count before.
  local -a restarts_before=()
  for i in $(seq 0 $((n - 1))); do restarts_before[$i]=$(oxia_coord_restarts "$rel-$i"); done
  for i in $(seq 0 $((n - 1))); do kc exec "$rel-$i" -c coordinator -- kill 1 >/dev/null 2>&1; done
  local frozen=0 leader
  deadline=$((SECONDS + 300))
  while [ $SECONDS -lt $deadline ]; do
    sleep 10
    leader=""
    for i in $(seq 0 $((n - 1))); do
      if kc logs "$rel-$i" -c coordinator 2>/dev/null | grep "Started leading" >/dev/null; then leader=$i; fi
    done
    [ -z "$leader" ] && continue
    if [ "$(oxia_coord_restarts "$rel-$leader")" -gt "${restarts_before[$leader]}" ]; then frozen=1; break; fi
    kc exec "$rel-$leader" -c coordinator -- kill 1 >/dev/null 2>&1   # still the old leader
  done
  # The new leader's first reconcile can run one balancing pass: let it, then confirm
  # nothing moves for a minute before the workers start.
  sleep 60
  cur=$(oxia_placement "$rel"); terms_after=$(echo "$cur" | cut -d' ' -f2)
  if [ "$frozen" = 1 ]; then
    echo "   oxia: balancer frozen (leader $rel-$leader restarted with the frozen config); elections while freezing: $((terms_after - terms_before)); $(echo "$cur" | cut -d' ' -f3-)"
  else
    echo "!! oxia: could not confirm a coordinator leader restarted with the frozen config; $(echo "$cur" | cut -d' ' -f3-)"
  fi
}

oxia_coord_restarts() {
  local r; r=$(kc get pod "$1" -o jsonpath='{.status.containerStatuses[?(@.name=="coordinator")].restartCount}' 2>/dev/null)
  echo "${r:-0}"
}

# The oxia-cluster subchart names everything after the release name, so oxia is
# installed as "bench-oxia" to get bench-oxia-0 / bench-oxia-coordinator. Other
# backends' charts already include the backend name, so they share release "bench".
# The oxia-N size variants (comparison/oxia-3.yaml, ...) all reuse the bench-oxia
# release/worker names — they run sequentially and are swept in between.
release_for() { case "$1" in oxia*) echo "bench-oxia" ;; *) echo "bench" ;; esac }
worker_for()  { case "$1" in oxia*) echo "bench-oxia-worker" ;; *) echo "bench-$1-worker" ;; esac }

# Remove everything in the namespace, including the oxia coordinator's runtime
# <release>-status ConfigMap (persists shard state across helm uninstall) and PVCs.
sweep() {
  for rel in $(hl list -q 2>/dev/null); do hl uninstall "$rel" >/dev/null 2>&1; done
  kc delete pvc --all >/dev/null 2>&1
  for cm in $(kc get configmap -o name 2>/dev/null | grep -E -- '-(status|release)$'); do
    kc delete "$cm" >/dev/null 2>&1
  done
  for _ in $(seq 1 40); do
    [ -z "$(kc get pods -o name 2>/dev/null)" ] && break
    sleep 3
  done
}

rm -rf "$RESULTS"; mkdir -p "$RESULTS"
echo ">> building chart dependencies"; helm dependency build "$CHART" >/dev/null 2>&1
echo ">> sweeping namespace $NS clean"; sweep
ok=()

for backend in "${BACKENDS[@]}"; do
  release=$(release_for "$backend"); worker=$(worker_for "$backend")
  echo ""
  echo "==================== $backend (release $release) ===================="
  repl_set=""; [ -n "$WORKER_REPLICAS" ] && repl_set="--set workers[0].parallelism=$WORKER_REPLICAS"
  # Each chart exposes its data-PVC storage class under a different key (redis is
  # in-memory by design and has none).
  sc_set=""
  if [ -n "$STORAGE_CLASS" ]; then
    case $backend in
      oxia*)     sc_set="--set oxia-cluster.server.storageClassName=$STORAGE_CLASS" ;;
      etcd*)      sc_set="--set etcd.persistence.storageClass=$STORAGE_CLASS" ;;
      zookeeper*) sc_set="--set zookeeper.persistence.storageClass=$STORAGE_CLASS" ;;
      consul)    sc_set="--set consul.server.storageClass=$STORAGE_CLASS" ;;
      tikv)      sc_set="--set tikv.storageClassName=$STORAGE_CLASS --set tikv.pd.storageClassName=$STORAGE_CLASS" ;;
    esac
  fi
  # Custom oxia server build (repo:tag), e.g. a release candidate under test.
  img_set=""
  if [[ "$backend" == oxia* ]] && [ -n "$OXIA_IMAGE" ]; then
    img_set="--set oxia-cluster.image.repository=${OXIA_IMAGE%%:*} --set oxia-cluster.image.tag=${OXIA_IMAGE##*:} --set oxia-cluster.image.pullPolicy=Always"
  fi
  # Oxia: workers start only when "<release>-release" exists (created after settle-and-freeze).
  gate_set=""
  if [[ "$backend" == oxia* ]] && [ "$OXIA_SETTLE" = 1 ]; then
    gate_set="--set workers[0].releaseGate=$release-release"
  fi
  if ! hl install "$release" "$CHART" --create-namespace \
        -f "$CHART/comparison/$backend.yaml" \
        --set-file "workloadsYaml=$WORKLOADS" \
        --set "workers[0].image=$IMAGE" \
        --set "workers[0].imagePullPolicy=$IMAGE_PULL_POLICY" \
        --set "workers[0].memory=${WORKER_MEM:-896Mi}" \
        ${WORKER_CPU:+--set workers[0].cpu=$WORKER_CPU} \
        $repl_set $sc_set $img_set $gate_set >/dev/null 2>&1; then
    echo "!! helm install failed, skipping"; sweep; continue
  fi

  # On a reserved/tainted pool (e.g. a GKE spot pool), pin every pod to it and tolerate its
  # taints, then recycle any pods that started before the patch so they reschedule there.
  if [ -n "$NODE_POOL" ]; then
    sched='{"spec":{"template":{"spec":{"nodeSelector":{"cloud.google.com/gke-nodepool":"'"$NODE_POOL"'"},"tolerations":[{"key":"nodepool","operator":"Exists","effect":"NoSchedule"},{"key":"cloud.google.com/gke-spot","operator":"Exists","effect":"NoSchedule"}]}}}}'
    for res in $(kc get deploy,statefulset -o name 2>/dev/null); do kc patch "$res" --type merge -p "$sched" >/dev/null 2>&1; done
    kc delete pods --all --grace-period=0 >/dev/null 2>&1
  fi

  # Oxia: the workers are scheduled now but held by their start gate until the placement
  # has settled and the balancer is frozen; then all of them are released together.
  if [[ "$backend" == oxia* ]] && [ "$OXIA_SETTLE" = 1 ]; then
    oxia_settle_and_freeze "$release"
    kc create configmap "$release-release" >/dev/null 2>&1
    echo "   oxia: workers released"
  fi

  expected=$(kc get deploy "$worker" -o jsonpath='{.spec.replicas}' 2>/dev/null); [ -z "$expected" ] && expected=1
  echo "   waiting for $expected worker pod(s) to finish (timeout ${PER_BACKEND_TIMEOUT}s)..."
  deadline=$((SECONDS + PER_BACKEND_TIMEOUT)); finished=0; pods=""
  while [ $SECONDS -lt $deadline ]; do
    pods=$(kc get pods -l "app=$worker" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
    n=$(echo "$pods" | wc -w | tr -d ' ')
    fin=0
    for p in $pods; do
      # Not grep -q: it exits at the first match, and under pipefail the SIGPIPE that kubectl
      # then gets (exit 141) fails the pipeline, so a finished worker whose log has lines
      # after the marker goes uncounted and the run times out. Plain grep reads to EOF.
      kc logs "$p" -c "$worker" --request-timeout=60s 2>/dev/null | grep "All workloads finished" >/dev/null && fin=$((fin + 1))
    done
    [ "$n" -ge "$expected" ] && [ "$fin" -ge "$n" ] && { finished=1; break; }
    # fail fast on unrecoverable image states (wrong/missing image) instead of waiting the full timeout
    bad=$(kc get pods -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.state.waiting.reason}{" "}{end}{range .status.initContainerStatuses[*]}{.state.waiting.reason}{" "}{end}{end}' 2>/dev/null)
    if echo "$bad" | grep -qE 'ImagePullBackOff|ErrImagePull|InvalidImageName'; then
      echo "!! unrecoverable image state: $(echo "$bad" | tr ' ' '\n' | grep -vE '^$' | sort -u | tr '\n' ' ')"
      break
    fi
    # CrashLoopBackOff can be transient on a spot/autoscaling cluster (node preemption, OOM during
    # scale-up); only bail once a container has genuinely crash-looped several times.
    if echo "$bad" | grep -q 'CrashLoopBackOff'; then
      maxrestart=$(kc get pods -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.restartCount}{" "}{end}{end}' 2>/dev/null | tr ' ' '\n' | grep -E '^[0-9]+$' | sort -rn | head -1)
      if [ "${maxrestart:-0}" -ge "$CRASH_RESTART_LIMIT" ]; then
        echo "!! persistent crash loop (restarts=$maxrestart, limit=$CRASH_RESTART_LIMIT), dumping crashing pods before sweep:"
        for cp in $(kc get pods --no-headers 2>/dev/null | awk '$4+0>=1{print $1}'); do
          echo "---- $cp ----"
          kc get pod "$cp" -o jsonpath='{range .status.containerStatuses[*]}{.name}: r={.restartCount} reason={.lastState.terminated.reason} exit={.lastState.terminated.exitCode}{"\n"}{end}' 2>/dev/null
          kc logs "$cp" --all-containers --previous --tail=18 2>&1 | tail -18
        done
        break
      fi
    fi
    sleep 10
  done
  if [ "$finished" != 1 ]; then
    # Keep what the finished workers produced: a single worker whose log can't be read
    # (e.g. a transient kubelet failure on its node) would otherwise discard the whole run.
    echo "!! workers did not finish in time (${fin:-0} of ${n:-0} finished); gathering what is there"
    kc get pods 2>&1 | tail -8
  fi

  echo "   gathering results from $(echo "$pods" | wc -w | tr -d ' ') pods"
  got=0
  for p in $pods; do
    # Bounded: a stalled log stream once hung this step for 11 minutes.
    kc logs "$p" -c "$worker" --request-timeout=60s > "$RESULTS/$p.log" 2>/dev/null
    if kubectl --context "$CTX" --request-timeout=120s cp "$NS/$p:/results/$p.jsonl" "$RESULTS/$p.jsonl" -c "$worker" >/dev/null 2>&1; then
      echo "   + $p"; got=$((got + 1))
    else
      echo "   - $p (no results)"
    fi
  done
  [ "$got" -gt 0 ] && ok+=("$backend")
  sweep
done

echo ""
echo "==================== report ===================="
echo "backends with results: ${ok[*]:-none}"
ls "$RESULTS"/*.jsonl >/dev/null 2>&1 || { echo "no results gathered, nothing to report"; exit 1; }
java -jar "$JAR" report --results-dir "$RESULTS" --out "$OUT"
echo "open $OUT/report.html"
