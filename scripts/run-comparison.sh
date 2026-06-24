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
WORKLOADS="${WORKLOADS:-conf/workload-comparison.yaml}"
RESULTS="${RESULTS:-comparison-results}"
OUT="${OUT:-comparison-report}"
PER_BACKEND_TIMEOUT="${PER_BACKEND_TIMEOUT:-600}"

if [ "$#" -gt 0 ]; then BACKENDS=("$@"); else BACKENDS=(oxia etcd zookeeper redis consul tikv); fi

JAR=$(ls build/libs/oxia-benchmark-*-all.jar 2>/dev/null | head -1)
[ -z "$JAR" ] && { echo "Build the jar first: ./gradlew shadowJar"; exit 1; }

kc() { kubectl --context "$CTX" -n "$NS" "$@"; }
hl() { helm --kube-context "$CTX" -n "$NS" "$@"; }

# The oxia-cluster subchart names everything after the release name, so oxia is
# installed as "bench-oxia" to get bench-oxia-0 / bench-oxia-coordinator. Other
# backends' charts already include the backend name, so they share release "bench".
release_for() { [ "$1" = oxia ] && echo "bench-oxia" || echo "bench"; }
worker_for()  { [ "$1" = oxia ] && echo "bench-oxia-worker" || echo "bench-$1-worker"; }

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
  if ! hl install "$release" "$CHART" --create-namespace \
        -f "$CHART/comparison/$backend.yaml" \
        --set-file "workloadsYaml=$WORKLOADS" \
        --set "workers[0].image=$IMAGE" \
        --set "workers[0].imagePullPolicy=Never" \
        --set "workers[0].memory=${WORKER_MEM:-896Mi}" >/dev/null 2>&1; then
    echo "!! helm install failed, skipping"; sweep; continue
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
    # fail fast on unrecoverable pod states (bad image, crash loop) instead of waiting the full timeout
    bad=$(kc get pods -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.state.waiting.reason}{" "}{end}{range .status.initContainerStatuses[*]}{.state.waiting.reason}{" "}{end}{end}' 2>/dev/null)
    if echo "$bad" | grep -qE 'ImagePullBackOff|ErrImagePull|CrashLoopBackOff|InvalidImageName'; then
      echo "!! unrecoverable pod state: $(echo "$bad" | tr ' ' '\n' | grep -vE '^$' | sort -u | tr '\n' ' ')"
      break
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
