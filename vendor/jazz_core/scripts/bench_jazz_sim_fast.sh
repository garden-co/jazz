#!/usr/bin/env bash
set -euo pipefail

fast_benches=(
  s1_saas
  s2_canvas
  s3_permissions
  s4_order_processing
  s5_durable_stream
  s6_text_traces
  s7_migrations
  s9_durable_execution
)

usage() {
  cat <<'EOF'
usage: scripts/bench_jazz_sim_fast.sh [--dry-run] [--phase <name>] <bench-name> [bench-args...]
       scripts/bench_jazz_sim_fast.sh [--dry-run] --all-fast
       scripts/bench_jazz_sim_fast.sh [--dry-run] --encoded-wire-canary

Runs a jazz-sim benchmark with interactive defaults:
  JAZZ_BENCH_PROFILE=fast cargo bench -p jazz-sim --bench <bench-name> --quiet

examples:
  scripts/bench_jazz_sim_fast.sh s2_canvas
  scripts/bench_jazz_sim_fast.sh --all-fast
  scripts/bench_jazz_sim_fast.sh --encoded-wire-canary
  scripts/bench_jazz_sim_fast.sh --dry-run s2_canvas
  scripts/bench_jazz_sim_fast.sh --phase abi_direct_surface s1_saas

options:
  --all-fast             run the main jazz-sim benches that fit the fast profile
  --encoded-wire-canary  run short encoded sync checks over wire_frames transport
  --dry-run              print commands without running them
  --phase <name>         set JAZZ_BENCH_PHASES=<name> for one named bench
  --help                 show this help
EOF
}

dry_run=false
all_fast=false
encoded_wire_canary=false
phase=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all-fast)
      all_fast=true
      shift
      ;;
    --encoded-wire-canary)
      encoded_wire_canary=true
      shift
      ;;
    --dry-run)
      dry_run=true
      shift
      ;;
    --phase)
      if [[ $# -lt 2 ]]; then
        echo "--phase requires a phase name" >&2
        usage >&2
        exit 2
      fi
      phase="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

run_bench() {
  local env_args=()
  while [[ $# -gt 0 && "$1" == *=* ]]; do
    env_args+=("$1")
    shift
  done

  local bench_name="$1"
  shift

  if [[ "$dry_run" == true ]]; then
    if [[ ${#env_args[@]} -gt 0 ]]; then
      printf '%q ' "${env_args[@]}"
    fi
    printf 'JAZZ_BENCH_PROFILE=%q cargo bench -p jazz-sim --bench %q --quiet' \
      "${JAZZ_BENCH_PROFILE:-fast}" \
      "$bench_name"
    if [[ $# -gt 0 ]]; then
      printf ' %q' "$@"
    fi
    printf '\n'
  else
    env ${env_args+"${env_args[@]}"} \
      JAZZ_BENCH_PROFILE="${JAZZ_BENCH_PROFILE:-fast}" \
      cargo bench -p jazz-sim --bench "$bench_name" --quiet "$@"
  fi
}

if [[ "$all_fast" == true && "$encoded_wire_canary" == true ]]; then
  echo "choose either --all-fast or --encoded-wire-canary" >&2
  exit 2
fi

if [[ -n "$phase" && ( "$all_fast" == true || "$encoded_wire_canary" == true ) ]]; then
  echo "--phase is only supported with a single named bench" >&2
  exit 2
fi

if [[ "$all_fast" == true ]]; then
  if [[ $# -gt 0 ]]; then
    echo "usage: scripts/bench_jazz_sim_fast.sh --all-fast [--dry-run]" >&2
    exit 2
  fi

  for bench_name in "${fast_benches[@]}"; do
    run_bench "$bench_name"
  done
  exit 0
fi

if [[ "$encoded_wire_canary" == true ]]; then
  if [[ $# -gt 0 ]]; then
    echo "usage: scripts/bench_jazz_sim_fast.sh --encoded-wire-canary [--dry-run]" >&2
    exit 2
  fi

  run_bench JAZZ_S2_TRANSPORT_CODEC=wire_frames s2_canvas
  run_bench JAZZ_S1_RECONNECT_TRANSPORT_CODEC=wire_frames s1_saas
  exit 0
fi

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

bench_name="$1"
shift

if [[ -n "$phase" ]]; then
  run_bench JAZZ_BENCH_PHASES="$phase" "$bench_name" "$@"
else
  run_bench "$bench_name" "$@"
fi
