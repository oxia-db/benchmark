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
  for cm in $(kc get configmap -o name 2>/dev/null | grep -- '-status$'); do
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
      etcd)      sc_set="--set etcd.persistence.storageClass=$STORAGE_CLASS" ;;
      zookeeper) sc_set="--set zookeeper.persistence.storageClass=$STORAGE_CLASS" ;;
      consul)    sc_set="--set consul.server.storageClass=$STORAGE_CLASS" ;;
      tikv)      sc_set="--set tikv.storageClassName=$STORAGE_CLASS --set tikv.pd.storageClassName=$STORAGE_CLASS" ;;
    esac
  fi
  # Custom oxia server build (repo:tag), e.g. a release candidate under test.
  img_set=""
  if [[ "$backend" == oxia* ]] && [ -n "$OXIA_IMAGE" ]; then
    img_set="--set oxia-cluster.image.repository=${OXIA_IMAGE%%:*} --set oxia-cluster.image.tag=${OXIA_IMAGE##*:} --set oxia-cluster.image.pullPolicy=Always"
  fi
  if ! hl install "$release" "$CHART" --create-namespace \
        -f "$CHART/comparison/$backend.yaml" \
        --set-file "workloadsYaml=$WORKLOADS" \
        --set "workers[0].image=$IMAGE" \
        --set "workers[0].imagePullPolicy=$IMAGE_PULL_POLICY" \
        --set "workers[0].memory=${WORKER_MEM:-896Mi}" \
        ${WORKER_CPU:+--set workers[0].cpu=$WORKER_CPU} \
        $repl_set $sc_set $img_set >/dev/null 2>&1; then
    echo "!! helm install failed, skipping"; sweep; continue
  fi

  # On a reserved/tainted pool (e.g. a GKE spot pool), pin every pod to it and tolerate its
  # taints, then recycle any pods that started before the patch so they reschedule there.
  if [ -n "$NODE_POOL" ]; then
    sched='{"spec":{"template":{"spec":{"nodeSelector":{"cloud.google.com/gke-nodepool":"'"$NODE_POOL"'"},"tolerations":[{"key":"nodepool","operator":"Exists","effect":"NoSchedule"},{"key":"cloud.google.com/gke-spot","operator":"Exists","effect":"NoSchedule"}]}}}}'
    for res in $(kc get deploy,statefulset -o name 2>/dev/null); do kc patch "$res" --type merge -p "$sched" >/dev/null 2>&1; done
    kc delete pods --all --grace-period=0 >/dev/null 2>&1
  fi

  expected=$(kc get deploy "$worker" -o jsonpath='{.spec.replicas}' 2>/dev/null); [ -z "$expected" ] && expected=1
  echo "   waiting for $expected worker pod(s) to finish (timeout ${PER_BACKEND_TIMEOUT}s)..."
  deadline=$((SECONDS + PER_BACKEND_TIMEOUT)); finished=0; pods=""
  while [ $SECONDS -lt $deadline ]; do
    pods=$(kc get pods -l "app=$worker" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
    n=$(echo "$pods" | wc -w | tr -d ' ')
    fin=0
    for p in $pods; do
      kc logs "$p" -c "$worker" 2>/dev/null | grep -q "All workloads finished" && fin=$((fin + 1))
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
    echo "!! workers did not finish in time, skipping"; kc get pods 2>&1 | tail -8; sweep; continue
  fi

  echo "   gathering results from $(echo "$pods" | wc -w | tr -d ' ') pods"
  got=0
  for p in $pods; do
    if kubectl --context "$CTX" cp "$NS/$p:/results/$p.jsonl" "$RESULTS/$p.jsonl" -c "$worker" >/dev/null 2>&1; then
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
