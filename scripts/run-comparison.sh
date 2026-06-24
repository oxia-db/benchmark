#!/usr/bin/env bash
#
# Cross-driver comparison: deploy each backend one at a time, run identical
# workloads with 3 worker pods (a distributed load generator), gather the
# per-worker results, tear it down, then build one comparison report.
#
# Usage:  scripts/run-comparison.sh [backend ...]      (default: all six)
# Env:    KUBE_CONTEXT=kind-matteo  NS=bench  RELEASE=bench
#         IMAGE=oxia/benchmark:local  RESULTS=comparison-results  OUT=comparison-report
#         PER_BACKEND_TIMEOUT=600
#
# Prereqs: the worker image is built and loaded into the cluster, and the report
# jar is built (./gradlew shadowJar). See README "Cross-driver comparison".

set -uo pipefail

CHART="charts/benchmark-stack"
CTX="${KUBE_CONTEXT:-kind-matteo}"
NS="${NS:-bench}"
RELEASE="${RELEASE:-bench}"
IMAGE="${IMAGE:-oxia/benchmark:local}"
RESULTS="${RESULTS:-comparison-results}"
OUT="${OUT:-comparison-report}"
PER_BACKEND_TIMEOUT="${PER_BACKEND_TIMEOUT:-600}"

if [ "$#" -gt 0 ]; then BACKENDS=("$@"); else BACKENDS=(oxia etcd zookeeper redis consul tikv); fi

JAR=$(ls build/libs/oxia-benchmark-*-all.jar 2>/dev/null | head -1)
[ -z "$JAR" ] && { echo "Build the jar first: ./gradlew shadowJar"; exit 1; }

KC="kubectl --context $CTX"
HELM="helm --kube-context $CTX -n $NS"
ok=()

cleanup() {
  $HELM uninstall "$RELEASE" >/dev/null 2>&1
  $KC -n "$NS" delete pvc --all >/dev/null 2>&1
  for _ in $(seq 1 40); do
    [ -z "$($KC -n "$NS" get pods -o name 2>/dev/null)" ] && break
    sleep 3
  done
}

rm -rf "$RESULTS"; mkdir -p "$RESULTS"
echo ">> building chart dependencies"
helm dependency build "$CHART" >/dev/null 2>&1

for backend in "${BACKENDS[@]}"; do
  echo ""
  echo "==================== $backend ===================="
  cleanup
  if ! $HELM install "$RELEASE" "$CHART" --create-namespace \
        -f "$CHART/comparison/$backend.yaml" \
        -f "$CHART/comparison/workloads.yaml" \
        --set "workers[0].image=$IMAGE" \
        --set "workers[0].imagePullPolicy=Never" >/dev/null 2>&1; then
    echo "!! helm install failed, skipping"; cleanup; continue
  fi

  worker="$RELEASE-$backend-worker"
  echo "   waiting for 3 worker pods to finish (timeout ${PER_BACKEND_TIMEOUT}s)..."
  deadline=$((SECONDS + PER_BACKEND_TIMEOUT)); done=0; pods=""
  while [ $SECONDS -lt $deadline ]; do
    pods=$($KC -n "$NS" get pods -l "app=$worker" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
    n=$(echo "$pods" | wc -w | tr -d ' ')
    fin=0
    for p in $pods; do
      $KC -n "$NS" logs "$p" -c "$worker" 2>/dev/null | grep -q "All workloads finished" && fin=$((fin + 1))
    done
    [ "$n" -ge 3 ] && [ "$fin" -ge "$n" ] && { done=1; break; }
    sleep 10
  done
  if [ "$done" != 1 ]; then
    echo "!! workers did not finish in time, skipping"; $KC -n "$NS" get pods 2>&1 | tail -8; cleanup; continue
  fi

  echo "   gathering results from $(echo "$pods" | wc -w | tr -d ' ') pods"
  got=0
  for p in $pods; do
    if $KC cp "$NS/$p:/results/$p.jsonl" "$RESULTS/$p.jsonl" -c "$worker" >/dev/null 2>&1; then
      echo "   + $p"; got=$((got + 1))
    else
      echo "   - $p (no results)"
    fi
  done
  [ "$got" -gt 0 ] && ok+=("$backend")
  cleanup
done

echo ""
echo "==================== report ===================="
echo "backends with results: ${ok[*]:-none}"
ls "$RESULTS"/*.jsonl >/dev/null 2>&1 || { echo "no results gathered, nothing to report"; exit 1; }
java -jar "$JAR" report --results-dir "$RESULTS" --out "$OUT"
echo "open $OUT/report.html"
