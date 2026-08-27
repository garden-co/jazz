#!/usr/bin/env bash
# Run replayable, individual seeded simulation/oracle cases.  This intentionally
# does not use JAZZ_SEED_COUNT: every seed has its own watchdog and receipt.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

usage() {
  cat <<'EOF'
Usage: dev/gates/run-continuous-simulation-soak.sh [options]

  --output DIR          Receipt directory (default: target/simulation-soak)
  --shard-index N       One-based deterministic shard (default: 1)
  --shard-count N       Number of shards (default: 1)
  --sync-seeds N        Number of sync seeds (default: 100)
  --sync-commits N      Commits per sync seed (default: 200)
  --differential-seeds N  Number of differential seeds (default: 50)
  --differential-steps N  Steps per differential seed (default: 20)
  --churn-depths CSV    Aggregate churn depths (default: 10,1000)
  --watchdog-seconds N  Per-seed watchdog (default: 80)
  --budget-seconds N    Cumulative shard budget; stops cleanly at boundary
                        (default: 19800)
  --reporting-reserve-seconds N  Time reserved for summary/artifacts (default: 300)

Replay a recorded failure by copying its `replay` command from summary.json.
EOF
}

output="target/simulation-soak"; shard_index=1; shard_count=1
sync_seeds=100; sync_commits=200; differential_seeds=50; differential_steps=20
churn_depths="10,1000"; watchdog_seconds=80; budget_seconds=19800; reporting_reserve_seconds=300
while (($#)); do
  case "$1" in
    --output) output="$2"; shift 2;; --shard-index) shard_index="$2"; shift 2;;
    --shard-count) shard_count="$2"; shift 2;; --sync-seeds) sync_seeds="$2"; shift 2;;
    --sync-commits) sync_commits="$2"; shift 2;; --differential-seeds) differential_seeds="$2"; shift 2;;
    --differential-steps) differential_steps="$2"; shift 2;; --churn-depths) churn_depths="$2"; shift 2;;
    --watchdog-seconds) watchdog_seconds="$2"; shift 2;; --budget-seconds) budget_seconds="$2"; shift 2;; --reporting-reserve-seconds) reporting_reserve_seconds="$2"; shift 2;; -h|--help) usage; exit 0;;
    *) usage >&2; exit 2;;
  esac
done
for number in "$shard_index" "$shard_count" "$sync_seeds" "$sync_commits" "$differential_seeds" "$differential_steps" "$watchdog_seconds" "$budget_seconds" "$reporting_reserve_seconds"; do
  [[ "$number" =~ ^[1-9][0-9]*$ ]] || { echo "positive integer required: $number" >&2; exit 2; }
done
(( shard_index <= shard_count )) || { echo "shard index exceeds shard count" >&2; exit 2; }
(( sync_seeds <= 1000 && differential_seeds <= 1000 )) || { echo "seed counts are capped at 1000" >&2; exit 2; }
(( sync_commits <= 10000 && differential_steps <= 1000 )) || { echo "workload depth exceeds driver cap" >&2; exit 2; }
(( watchdog_seconds <= 900 && budget_seconds <= 19800 && reporting_reserve_seconds <= 1800 )) || { echo "watchdog/budget exceeds driver cap" >&2; exit 2; }
[[ "$churn_depths" =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]] || { echo "invalid churn depths" >&2; exit 2; }

# A whole shard must fit even if every selected case consumes its watchdog.
# This makes the workflow's six-hour budget a provable bound, not a hope.
assigned_cases=0
for count in "$sync_seeds" "$differential_seeds"; do
  for ((i=0; i<count; i++)); do (( i % shard_count == shard_index - 1 )) && ((assigned_cases+=1)); done
done
(( assigned_cases * (watchdog_seconds + 30) + reporting_reserve_seconds <= budget_seconds )) || { echo "worst-case shard time including kill grace/reporting reserve exceeds budget" >&2; exit 2; }

mkdir -p "$output/logs"
sha="$(git rev-parse HEAD)"
config="$output/config.env"
manifest="$output/manifest.jsonl"
summary="$output/summary.json"
printf '%s\n' "sha=$sha" "shard_index=$shard_index" "shard_count=$shard_count" "sync_seeds=$sync_seeds" "sync_commits=$sync_commits" "differential_seeds=$differential_seeds" "differential_steps=$differential_steps" "churn_depths=$churn_depths" "watchdog_seconds=$watchdog_seconds" "budget_seconds=$budget_seconds" > "$config"
: > "$manifest"

# Keep seed generation aligned with the tests' deterministic extension scheme.
seed_for() { local suite="$1" index="$2"; if [[ "$suite" == sync ]]; then local fixed=(11 29 47 83 32676 40595 2234158 3715011 4372288); if (( index < ${#fixed[@]} )); then echo "${fixed[index]}"; else echo $((1000 + (index-${#fixed[@]}) * 7919)); fi; else local fixed=(11 29 47 4372288 7777013); if (( index < ${#fixed[@]} )); then echo "${fixed[index]}"; else echo $((9000 + (index-${#fixed[@]}) * 7919)); fi; fi; }
failures=0; ran=0; budget_exhausted=false; deadline=$(( $(date +%s) + budget_seconds ))
# Test-only deterministic clock seam: validates incomplete-run receipts without
# sleeping or relying on host scheduling. It is not used by the workflow.
[[ "${JAZZ_SOAK_TEST_EXHAUST_BUDGET:-}" == 1 ]] && deadline=$(date +%s)
run_seed() {
  local suite="$1" seed="$2" test_name env_args test_args log status replay
  if [[ "$suite" == sync ]]; then
    test_name="node::tests::sync::convergence_and_fates::m3_seeded_sync_interleavings_converge_against_oracle"
    env_args=("JAZZ_SEED=$seed" "JAZZ_COMMIT_COUNT=$sync_commits")
    test_args=(--exact)
  else
    test_name="node::tests::harness::m3_maintained_one_shot_differential_oracle"
    env_args=("JAZZ_SEED=$seed" "JAZZ_DIFFERENTIAL_STEP_COUNT=$differential_steps" "JAZZ_DIFFERENTIAL_CHURN_DEPTHS=$churn_depths")
    test_args=(--exact --ignored)
  fi
  log="$output/logs/${suite}-seed-${seed}.log"
  replay="CARGO_TARGET_DIR=\${CARGO_TARGET_DIR:-target} timeout --kill-after=30s ${case_timeout}s env ${env_args[*]} cargo test -p jazz --lib --no-default-features --features testing,transport-compression-zstd ${test_name} -- ${test_args[*]}"
  set +e
  timeout --kill-after=30s "${case_timeout}s" env "${env_args[@]}" cargo test -p jazz --lib --no-default-features --features testing,transport-compression-zstd "$test_name" -- "${test_args[@]}" >"$log" 2>&1
  status=$?
  set -e
  if (( status == 124 || status == 137 )); then result=timeout; else result=passed; (( status == 0 )) || result=failed; fi
  printf '{"suite":"%s","seed":%s,"status":"%s","exit_code":%s,"config":{"sync_commits":%s,"differential_steps":%s,"churn_depths":"%s","watchdog_seconds":%s,"effective_timeout_seconds":%s},"log":"%s","replay":"%s"}\n' "$suite" "$seed" "$result" "$status" "$sync_commits" "$differential_steps" "$churn_depths" "$watchdog_seconds" "$case_timeout" "logs/${suite}-seed-${seed}.log" "$replay" >> "$manifest"
  ((ran+=1)); if [[ "$result" != passed ]]; then ((failures+=1)); fi
  echo "$suite seed=$seed status=$result replay: $replay"
}
for suite in sync differential; do
  count="$sync_seeds"; [[ "$suite" == differential ]] && count="$differential_seeds"
  for ((i=0; i<count; i++)); do
    (( i % shard_count == shard_index - 1 )) || continue
    remaining=$(( deadline - $(date +%s) - reporting_reserve_seconds ))
    if (( remaining <= 30 )); then budget_exhausted=true; break 2; fi
    case_timeout=$watchdog_seconds; (( case_timeout > remaining - 30 )) && case_timeout=$((remaining - 30))
    # Deterministic test seam for the manifest/replay timeout contract.
    [[ "${JAZZ_SOAK_TEST_EFFECTIVE_TIMEOUT:-}" =~ ^[1-9][0-9]*$ ]] && case_timeout="$JAZZ_SOAK_TEST_EFFECTIVE_TIMEOUT"
    run_seed "$suite" "$(seed_for "$suite" "$i")"
  done
done
if "$budget_exhausted"; then
  printf '{"suite":"soak","seed":0,"status":"budget_exhausted","exit_code":3,"config":{"sync_commits":%s,"differential_steps":%s,"churn_depths":"%s","watchdog_seconds":%s,"effective_timeout_seconds":%s},"log":null,"replay":"Re-run the trusted workflow with a larger bounded budget."}\n' "$sync_commits" "$differential_steps" "$churn_depths" "$watchdog_seconds" "$watchdog_seconds" >> "$manifest"
fi
node -e 'const fs=require("fs"); const [manifest,summary,sha,budget]=process.argv.slice(1); const cases=fs.readFileSync(manifest,"utf8").trim().split("\n").filter(Boolean).map(JSON.parse); fs.writeFileSync(summary, JSON.stringify({schema_version:1,sha,budget_exhausted:budget==="true",cases,failures:cases.filter(x=>x.status!=="passed")},null,2)+"\n")' "$manifest" "$summary" "$sha" "$budget_exhausted"
echo "Simulation soak summary: $summary ($ran cases, $failures failures, budget_exhausted=$budget_exhausted)"
if "$budget_exhausted"; then exit 3; fi
(( failures == 0 ))
