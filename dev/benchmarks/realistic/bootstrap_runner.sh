#!/usr/bin/env bash
set -euo pipefail
umask 077
RUNNER_TOKEN="${RUNNER_TOKEN:-}"
export -n RUNNER_TOKEN

readonly RUST_VERSION=1.93.1
readonly RUSTUP_VERSION=1.28.2
readonly RUSTUP_URL=https://static.rust-lang.org/rustup/archive/1.28.2/x86_64-unknown-linux-gnu/rustup-init
readonly RUSTUP_SHA256=20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c
readonly NODE_VERSION=24.13.0
readonly NODE_URL=https://nodejs.org/dist/v24.13.0/node-v24.13.0-linux-x64.tar.gz
readonly NODE_SHA256=6223aad1a81f9d1e7b682c59d12e2de233f7b4c37475cd40d1c89c42b737ffa8
readonly RUNNER_VERSION=2.337.0
readonly RUNNER_URL_PINNED=https://github.com/actions/runner/releases/download/v2.337.0/actions-runner-linux-x64-2.337.0.tar.gz
readonly RUNNER_SHA256=70920811a4f8ad4328818682bca5c6469c1c942fab52448868071d0063816613
readonly WASM_PACK_VERSION=0.13.1
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd -P)"
readonly HELPER="${SCRIPT_DIR}/bootstrap_runner_helper.py"
source "${SCRIPT_DIR}/bootstrap_runner_orchestration.sh"

fail() { printf '%s\n' "$1" >&2; exit 1; }

[[ "${BOOTSTRAP_TEST_MODE:-0}" == 0 || "${BOOTSTRAP_TEST_MODE:-0}" == 1 ]] || fail 'BOOTSTRAP_TEST_MODE must be 0 or 1'
if [[ "${BOOTSTRAP_TEST_MODE:-0}" == 1 ]]; then
  [[ "${EUID}" != 0 ]] || fail 'bootstrap test mode is forbidden for root'
  [[ -n "${BOOTSTRAP_TEST_ROOT:-}" && "${BOOTSTRAP_TEST_ROOT}" == /tmp/* ]] || fail 'bootstrap test root must be a harness-created sandbox beneath /tmp'
  [[ -d "${BOOTSTRAP_TEST_ROOT}" && ! -L "${BOOTSTRAP_TEST_ROOT}" ]] || fail 'bootstrap test root must be an existing non-symlink directory'
  test_root="$(realpath -e -- "${BOOTSTRAP_TEST_ROOT}")" || fail 'bootstrap test root is invalid'
  [[ "${test_root}" == /tmp/* && "$(stat -c %u -- "${test_root}")" == "${EUID}" ]] || fail 'bootstrap test root must be owned by the invoking user beneath /tmp'
  [[ -f "${test_root}/.bootstrap-test-fixture" && ! -L "${test_root}/.bootstrap-test-fixture" ]] || fail 'bootstrap test root is missing its harness fixture marker'
  [[ "$(cat -- "${test_root}/.bootstrap-test-fixture")" == BUG-117-fixture-v1 ]] || fail 'bootstrap test fixture marker is invalid'
  while IFS= read -r -d '' entry; do
    [[ ! -L "${entry}" ]] || [[ "$(realpath -m -- "${entry}")" == "${test_root}/"* ]] || fail 'bootstrap test fixture contains a symlink escape'
  done < <(find "${test_root}" -mindepth 1 -print0)
  ROOT="${test_root}"
elif [[ -n "${BOOTSTRAP_TEST_ROOT:-}" ]]; then
  fail 'BOOTSTRAP_TEST_ROOT requires BOOTSTRAP_TEST_MODE=1'
else
  ROOT=''
fi
rooted() { printf '%s%s' "${ROOT}" "$1"; }
INSTALL_OWNER=root
[[ -z "${ROOT}" ]] || INSTALL_OWNER="$(id -un)"
[[ "${EUID}" == 0 || -n "${ROOT}" ]] || fail 'bootstrap_runner.sh must run as root'
readonly ROOT INSTALL_OWNER
owner_uid="$(id -u "${INSTALL_OWNER}")"
validate_trusted_directory() {
  local directory="$1" metadata owner mode
  [[ -d "${directory}" && ! -L "${directory}" ]] || fail "trusted directory is missing or symlinked: ${directory}"
  metadata="$(stat -c '%u %a' -- "${directory}")" || fail "cannot inspect trusted directory: ${directory}"
  read -r owner mode <<< "${metadata}"
  [[ "${owner}" == "${owner_uid}" ]] || fail "trusted directory has an unexpected owner: ${directory}"
  (( (8#${mode} & 0022) == 0 )) || fail "trusted directory is writable by group or others: ${directory}"
}
validate_root_manifest() {
  local manifest="$1" metadata owner mode
  [[ -f "${manifest}" && ! -L "${manifest}" ]] || fail 'integrity manifest is missing or unsafe'
  metadata="$(stat -c '%u %a' -- "${manifest}")" || fail 'cannot inspect integrity manifest'
  read -r owner mode <<< "${metadata}"
  [[ "${owner}" == "${owner_uid}" ]] || fail 'integrity manifest has an unexpected owner'
  (( (8#${mode} & 0222) == 0 )) || fail 'integrity manifest is writable'
}

seal_toolchain_manifest() {
  local directory="$1" manifest="$2" staged="${2}.new"
  [[ -d "${directory}" && ! -L "${directory}" ]] || fail 'installed Rust tool state is incomplete'
  [[ ! -e "${manifest}" && ! -L "${manifest}" && ! -e "${staged}" && ! -L "${staged}" ]] || fail 'toolchain integrity manifest already exists'
  python3 "${HELPER}" manifest "${directory}" "${staged}" || fail 'could not seal installed Rust tool state'
  chown "${INSTALL_OWNER}:${INSTALL_OWNER}" "${staged}"
  chmod 0444 "${staged}"
  mv -- "${staged}" "${manifest}"
}
verify_toolchain_manifests() {
  local cargo_bin_dir="$1" rustup_home_dir="$2" integrity_dir="$3"
  local cargo_manifest="$4" rustup_manifest="$5" rustup_executable="$6"
  validate_trusted_directory "${integrity_dir}"
  [[ -d "${cargo_bin_dir}" && -x "${rustup_executable}" && -d "${rustup_home_dir}" ]] || fail 'existing Rust tool state is incomplete'
  [[ -f "${cargo_manifest}" && ! -L "${cargo_manifest}" ]] || fail 'Rust tool binary manifest is missing or unsafe'
  [[ -f "${rustup_manifest}" && ! -L "${rustup_manifest}" ]] || fail 'Rust toolchain manifest is missing or unsafe'
  validate_root_manifest "${cargo_manifest}"
  validate_root_manifest "${rustup_manifest}"
  python3 "${HELPER}" verify "${cargo_bin_dir}" "${cargo_manifest}" || fail 'existing Rust tool binaries are partial or modified'
  python3 "${HELPER}" verify "${rustup_home_dir}" "${rustup_manifest}" || fail 'existing Rust toolchain is partial or modified'
}

validate_runtime_state() {
  local path item metadata owner group mode runner_gid
  runner_gid="$(id -g "${INSTALL_OWNER}")"
  validate_trusted_directory "${RUNNER_ROOT%/*}"
  if [[ -L "${RUNNER_ROOT}" || -e "${RUNNER_ROOT}" ]]; then validate_trusted_directory "${RUNNER_ROOT}"; fi
  if [[ -L "${runner_state}" || -e "${runner_state}" ]]; then
    [[ ! -L "${runner_state}" && -d "${runner_state}" ]] || fail 'runner runtime state path is unsafe'
    metadata="$(stat -c '%u %g %a' -- "${runner_state}")" || fail 'cannot inspect runner runtime state'
    read -r owner group mode <<< "${metadata}"
    [[ "${owner}" == "${runner_uid}" ]] || fail 'runner runtime state has an unexpected owner'
    (( (8#${mode} & 0022) == 0 )) || fail 'runner runtime state is writable by group or others'
  fi
  runuser -u "${RUNNER_USER}" -- env python3 "${HELPER}" check-state "${runner_state}" "${runner_uid}" "${runner_gid}" "${owner_uid}" || fail 'runner runtime state directories are unsafe'
  for item in .runner .credentials .credentials_rsaparams .service .env .path; do
    path="${runner_state}/${item}"
    if [[ -L "${path}" || -e "${path}" ]]; then
      [[ ! -L "${path}" && -f "${path}" ]] || fail "runner runtime state entry is unsafe: ${item}"
      metadata="$(stat -c '%u %g %a' -- "${path}")" || fail "cannot inspect runner runtime state entry: ${item}"
      read -r owner group mode <<< "${metadata}"
      [[ "${owner}" == "${runner_uid}" ]] || fail "runner runtime state entry has an unexpected owner: ${item}"
      (( (8#${mode} & 0022) == 0 )) || fail "runner runtime state entry is writable by group or others: ${item}"
    fi
  done
}
readonly CLOUD_INSTANCE_ID="$(rooted /var/lib/cloud/data/instance-id)"
readonly SYS_VENDOR="$(rooted /sys/devices/virtual/dmi/id/sys_vendor)"
readonly SYS_PRODUCT="$(rooted /sys/devices/virtual/dmi/id/product_name)"
readonly RUNNER_ROOT="$(rooted /var/lib/actions-runner)"
readonly OPT_ROOT="$(rooted /opt)"
readonly NODE_ROOT="$(rooted /opt/node-v${NODE_VERSION})"
readonly TEMP_ROOT="$(rooted /var/tmp)"
[[ "${RUNNER_USER:-ubuntu}" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]] || fail 'RUNNER_USER is invalid'
RUNNER_USER="${RUNNER_USER:-ubuntu}"
RUNNER_URL="${RUNNER_URL:-https://github.com/garden-co/jazz2}"
RUNNER_LABELS="${RUNNER_LABELS:-jazz-bench}"
INSTALL_WASM_PACK="${INSTALL_WASM_PACK:-1}"
INSTALL_SSM_AGENT="${INSTALL_SSM_AGENT:-auto}"
[[ "${RUNNER_URL}" =~ ^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/?$ ]] || fail 'RUNNER_URL must be an HTTPS GitHub repository URL'
[[ "${RUNNER_LABELS}" =~ ^[A-Za-z0-9_.-]+(,[A-Za-z0-9_.-]+)*$ ]] || fail 'RUNNER_LABELS contains invalid characters'
[[ "${INSTALL_WASM_PACK}" == 0 || "${INSTALL_WASM_PACK}" == 1 ]] || fail 'INSTALL_WASM_PACK must be 0 or 1'
[[ "${INSTALL_SSM_AGENT}" == auto || "${INSTALL_SSM_AGENT}" == 0 || "${INSTALL_SSM_AGENT}" == 1 || "${INSTALL_SSM_AGENT}" == true || "${INSTALL_SSM_AGENT}" == false || "${INSTALL_SSM_AGENT}" == yes || "${INSTALL_SSM_AGENT}" == no ]] || fail 'INSTALL_SSM_AGENT must be auto, 0, or 1'

if [[ -n "${RUNNER_NAME:-}" ]]; then
  [[ "${RUNNER_NAME}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$ ]] || fail 'RUNNER_NAME is invalid'
  runner_name="${RUNNER_NAME}"
else
  instance_id=''
  if [[ -r "${CLOUD_INSTANCE_ID}" ]]; then
    IFS= read -r instance_id < "${CLOUD_INSTANCE_ID}" || true
  fi
  if [[ -z "${instance_id}" ]]; then instance_id="$(hostname)"; fi
  [[ "${instance_id}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$ ]] || fail 'could not derive a safe RUNNER_NAME'
  runner_name="benchmark-runner-${instance_id}"
fi

account="$(getent passwd "${RUNNER_USER}" || true)"
if [[ -z "${account}" ]]; then
  useradd --create-home --shell "$(rooted /bin/bash)" -- "${RUNNER_USER}"
  account="$(getent passwd "${RUNNER_USER}" || true)"
fi
[[ -n "${account}" ]] || fail 'runner account lookup failed'
IFS=: read -r account_name _ _ _ _ runner_home _ <<< "${account}"
[[ "${account_name}" == "${RUNNER_USER}" && "${runner_home}" == /* && "${runner_home}" != *$'\n'* ]] || fail 'runner account has an invalid home directory'
runner_uid="$(id -u "${RUNNER_USER}")"
[[ "${runner_uid}" != 0 ]] || fail 'RUNNER_USER must resolve to a non-root UID'


cargo_bin="${runner_home}/.cargo/bin"
rustup_home="${runner_home}/.rustup"
rustup_bin="${cargo_bin}/rustup"
wasm_pack_bin="${cargo_bin}/wasm-pack"
toolchain_integrity_dir="${RUNNER_ROOT}/.toolchain-integrity"
cargo_bin_manifest="${toolchain_integrity_dir}/cargo-bin.json"
rustup_home_manifest="${toolchain_integrity_dir}/rustup-home.json"
toolchain_state_present=0
toolchain_state_verified=0
validate_toolchain_state_paths() {
  local managed_directory
  for managed_directory in "${cargo_bin}" "${rustup_home}"; do
    [[ ! -L "${managed_directory}" ]] || fail 'existing Rust tool state directory is unsafe'
    [[ ! -e "${managed_directory}" || -d "${managed_directory}" ]] || fail 'existing Rust tool state is incomplete'
  done
}
runner_toolchain_state_exists() {
  if [[ -e "${rustup_home}" || -e "${rustup_bin}" || -L "${rustup_bin}" || -e "${wasm_pack_bin}" || -L "${wasm_pack_bin}" ]]; then
    return 0
  fi
  [[ -d "${cargo_bin}" ]] && [[ -n "$(find "${cargo_bin}" -mindepth 1 -print -quit)" ]]
}
validate_toolchain_state_paths
if runner_toolchain_state_exists; then toolchain_state_present=1; fi
toolchain_integrity_present=0
if [[ -e "${toolchain_integrity_dir}" || -L "${toolchain_integrity_dir}" ]]; then
  toolchain_integrity_present=1
fi
install_ssm="$(resolve_install_ssm "${INSTALL_SSM_AGENT}" "${SYS_VENDOR}" "${SYS_PRODUCT}")" || fail 'INSTALL_SSM_AGENT is invalid'
# Existing configured runners do not need a second registration token.
runner_state="${RUNNER_ROOT}/${RUNNER_USER}"
validate_runtime_state
if [[ ! -f "${runner_state}/.runner" ]]; then
  [[ -n "${RUNNER_TOKEN}" ]] || fail 'RUNNER_TOKEN is required to configure a new runner'
fi
runner_url_canonical="${RUNNER_URL%/}"
runner_url_canonical="${runner_url_canonical,,}"
if [[ -e "${runner_state}" ]]; then
  [[ -d "${runner_state}" && ! -L "${runner_state}" ]] || fail 'runner runtime state path is unsafe'
  if [[ -e "${runner_state}/.runner" ]]; then
    [[ -f "${runner_state}/.runner" && ! -L "${runner_state}/.runner" ]] || fail 'existing runner configuration is unsafe'
    [[ -f "${runner_state}/.credentials" && ! -L "${runner_state}/.credentials" ]] || fail 'existing runner credentials are incomplete'
    if ! jq -e --arg url "${runner_url_canonical}" --arg name "${runner_name}" \
      '(.agentName == $name) and ([.serverUrl, .gitHubUrl] | any((sub("/$"; "") | ascii_downcase) == $url)) and (.workFolder == "_work")' \
      "${runner_state}/.runner" >/dev/null 2>&1; then
      fail 'existing runner configuration does not match requested runner settings'
    fi
  elif [[ -e "${runner_state}/.credentials" || -e "${runner_state}/.credentials_rsaparams" || -e "${runner_state}/.service" || -e "${runner_state}/.env" || -e "${runner_state}/.path" ]]; then
    fail 'partial runner state found; refusing configuration or service activation'
  fi
fi

runner_package="${OPT_ROOT}/actions-runner/${RUNNER_VERSION}"
runner_manifest="${runner_package}/.bootstrap-manifest"
runner_state="${RUNNER_ROOT}/${RUNNER_USER}"
os_deps_marker="${RUNNER_ROOT}/.os-dependencies-v1"
readonly SYSTEMD_UNIT_DIR="$(rooted /etc/systemd/system)"
readonly SYSTEMD_MOUNTINFO="$(rooted /proc/self/mountinfo)"
readonly JAZZ_RUNNER_UNIT="$(runner_service_name "${runner_url_canonical}" "${runner_name}")"
legacy_runner_unit="$(legacy_runner_service_name "${runner_url_canonical}" "${runner_name}")" ||
  fail 'legacy runner unit identity is ambiguous; inspect services and reprovision manually'
readonly LEGACY_RUNNER_UNIT="${legacy_runner_unit}"
legacy_runner_identity_unambiguous=0
if legacy_runner_service_identity_is_unambiguous "${runner_url_canonical}" "${runner_name}"; then
  legacy_runner_identity_unambiguous=1
fi
readonly LEGACY_RUNNER_IDENTITY_UNAMBIGUOUS="${legacy_runner_identity_unambiguous}"
readonly UNIT_OWNER_UID="${owner_uid}"
RUNNER_STATE="${runner_state}"
readonly RUNNER_STATE
validate_trusted_directory "${OPT_ROOT}"
if [[ -e "${OPT_ROOT}/actions-runner" || -L "${OPT_ROOT}/actions-runner" ]]; then validate_trusted_directory "${OPT_ROOT}/actions-runner"; fi
if [[ -e "${runner_package}" || -L "${runner_package}" ]]; then
  validate_trusted_directory "${runner_package}"
  validate_root_manifest "${runner_manifest}"
  [[ -z "$(find "${runner_package}" \( -type f -o -type d \) \( ! -user "${INSTALL_OWNER}" -o -perm /022 \) -print -quit)" ]] || fail 'existing runner package is not root-owned and immutable to non-root users'
  python3 "${HELPER}" verify "${runner_package}" "${runner_manifest}" --exclude-runtime-links || fail 'existing runner package is partial or does not match its manifest'
  [[ -x "${runner_package}/config.sh" && -x "${runner_package}/bin/Runner.Listener" && -x "${runner_package}/runsvc.sh" ]] || fail 'existing runner package is incomplete'
fi
  [[ ! -e "${runner_package}/svc.sh" && ! -L "${runner_package}/svc.sh" ]] || fail 'runner package contains an unexpected svc.sh'
if [[ -e "${NODE_ROOT}" || -L "${NODE_ROOT}" ]]; then
  validate_trusted_directory "${NODE_ROOT}"
  validate_root_manifest "${NODE_ROOT}/.bootstrap-manifest"
  [[ -z "$(find "${NODE_ROOT}" \( -type f -o -type d \) \( ! -user "${INSTALL_OWNER}" -o -perm /022 \) -print -quit)" ]] || fail 'existing Node installation is not root-owned and immutable to non-root users'
  python3 "${HELPER}" verify "${NODE_ROOT}" "${NODE_ROOT}/.bootstrap-manifest" || fail 'existing Node installation is partial or does not match its manifest'
  [[ -x "${NODE_ROOT}/bin/node" ]] || fail 'existing Node installation is incomplete'
fi

runner_units_output="$(resolve_runner_units "${JAZZ_RUNNER_UNIT}" "${LEGACY_RUNNER_UNIT}" \
  "${SYSTEMD_UNIT_DIR}" "${RUNNER_USER}" "${runner_package}" "${runner_home}/actions-runner" \
  "${OPT_ROOT}/actions-runner" "${LEGACY_RUNNER_IDENTITY_UNAMBIGUOUS}")" || fail 'runner unit identity is ambiguous'
runner_units=()
if [[ -n "${runner_units_output}" ]]; then mapfile -t runner_units <<< "${runner_units_output}"; fi
legacy_runner_present=0
runner_unit_disable_failed=0
runner_unit_stop_error=''
for unit in "${runner_units[@]}"; do
  if [[ "${unit}" == "${LEGACY_RUNNER_UNIT}" ]]; then legacy_runner_present=1; fi
  # Disable all validated units before stopping any, so interruption during a
  # later stop cannot leave another verified runner enabled for normal boot.
  if ! systemctl disable "${unit}"; then runner_unit_disable_failed=1; fi
done
for unit in "${runner_units[@]}"; do
  if ! stop_and_prove_runner_unit "${unit}" "${SYSTEMD_MOUNTINFO}"; then
    if [[ -z "${runner_unit_stop_error}" ]]; then runner_unit_stop_error="${STOP_ERROR}"; fi
  fi
done
if [[ "${runner_unit_disable_failed}" == 1 ]]; then
  if [[ "${legacy_runner_present}" == 1 ]]; then
    fail 'verified legacy runner service could not be disabled; no persistent tools were run; boot state is uncertain and controlled reprovision is required'
  fi
  fail 'verified runner service could not be disabled; no persistent tools were run; boot state is uncertain'
fi
if [[ -n "${runner_unit_stop_error}" ]]; then
  if [[ "${legacy_runner_present}" == 1 ]]; then
    fail "${runner_unit_stop_error}; controlled reprovision is required"
  fi
  fail "${runner_unit_stop_error}"
fi
if [[ "${legacy_runner_present}" == 1 ]]; then
  fail 'verified legacy runner service was disabled, stopped, and quiesced; controlled reprovision is required before bootstrap'
fi
validate_toolchain_state_paths
toolchain_state_present=0
if runner_toolchain_state_exists; then toolchain_state_present=1; fi
toolchain_integrity_present=0
if [[ -e "${toolchain_integrity_dir}" || -L "${toolchain_integrity_dir}" ]]; then
  toolchain_integrity_present=1
fi
if [[ "${toolchain_integrity_present}" == 1 ]]; then
  [[ "${toolchain_state_present}" == 1 ]] || fail 'toolchain integrity manifests exist without installed state'
  verify_toolchain_manifests "${cargo_bin}" "${rustup_home}" "${toolchain_integrity_dir}" \
    "${cargo_bin_manifest}" "${rustup_home_manifest}" "${rustup_bin}"
  toolchain_state_verified=1
elif [[ "${toolchain_state_present}" == 1 ]]; then
  fail 'existing Rust tool state has no protected integrity manifests'
fi
if [[ "${toolchain_state_verified}" == 1 && "${INSTALL_WASM_PACK}" == 1 && ! -x "${wasm_pack_bin}" ]]; then
  fail 'existing wasm-pack installation is incomplete'
fi

offline_ready=0
if [[ -f "${os_deps_marker}" && -x "${NODE_ROOT}/bin/node" && "${toolchain_state_verified}" == 1 && -d "${runner_package}" ]]; then
  if [[ "${install_ssm:-0}" == 1 ]] && ! systemctl is-active --quiet snap.amazon-ssm-agent.amazon-ssm-agent.service; then
    offline_ready=0
  else
    offline_ready=1
  fi
fi

work_tmp="$(mktemp -d "${TEMP_ROOT}/jazz-runner-bootstrap.XXXXXXXX")"
runner_package_open=0
runner_config_pid=''
runner_config_launching=0
runner_signal_pending=0
seal_runner_package_directory() {
  local seal_error=0
  chown "${INSTALL_OWNER}:${INSTALL_OWNER}" "${runner_package}" || seal_error=1
  chmod 0555 "${runner_package}" || seal_error=1
  if (( seal_error == 0 )); then runner_package_open=0; fi
  return "${seal_error}"
}
stop_runner_config() {
  local attempt=0
  [[ -n "${runner_config_pid:-}" ]] || return 0
  kill -TERM -- "-${runner_config_pid}" 2>/dev/null || true
  while (( attempt < 10 )); do
    if ! kill -0 -- "-${runner_config_pid}" 2>/dev/null; then break; fi
    sleep 0.05
    attempt=$((attempt + 1))
  done
  if kill -0 -- "-${runner_config_pid}" 2>/dev/null; then
    kill -KILL -- "-${runner_config_pid}" 2>/dev/null || true
  fi
  wait "${runner_config_pid}" 2>/dev/null || true
  runner_config_pid=''
}
bootstrap_exit_cleanup() {
  local exit_status=$?
  trap - EXIT
  trap '' HUP INT TERM
  stop_runner_config
  if [[ "${runner_package_open:-0}" == 1 ]]; then
    rm -f -- "${runner_package}/svc.sh" || exit_status=1
    seal_runner_package_directory || exit_status=1
  fi
  if [[ -n "${work_tmp:-}" && -d "${work_tmp}" ]]; then
    chmod 0700 -- "${work_tmp}" || exit_status=1
    rm -rf -- "${work_tmp}" || exit_status=1
  fi
  exit "${exit_status}"
}
bootstrap_handle_signal() {
  local exit_status="$1"
  if [[ "${runner_config_launching}" == 1 ]]; then
    if [[ "${runner_signal_pending}" == 0 ]]; then runner_signal_pending="${exit_status}"; fi
    return
  fi
  stop_runner_config
  exit "${exit_status}"
}
trap bootstrap_exit_cleanup EXIT
trap 'bootstrap_handle_signal 129' HUP
trap 'bootstrap_handle_signal 130' INT
trap 'bootstrap_handle_signal 143' TERM
chmod 0711 "${work_tmp}"
rustup_file="${work_tmp}/rustup-init"
node_file="${work_tmp}/node.tar.gz"
runner_file="${work_tmp}/runner.tar.gz"

fetch_pinned() {
  local url="$1" output="$2" expected="$3"
  curl --fail --silent --show-error --location --output "${output}" "${url}"
  python3 "${HELPER}" verify-download "${output}" "${expected}" || fail 'download integrity check failed'
}

if [[ "${offline_ready}" != 1 ]]; then
  # Authenticate all binary archives before any installer or extractor consumes them.
  fetch_pinned "${RUSTUP_URL}" "${rustup_file}" "${RUSTUP_SHA256}"
  fetch_pinned "${NODE_URL}" "${node_file}" "${NODE_SHA256}"
  if [[ ! -d "${runner_package}" ]]; then
    fetch_pinned "${RUNNER_URL_PINNED}" "${runner_file}" "${RUNNER_SHA256}"
  fi

  install_verified_os_dependencies "${install_ssm}" "${INSTALL_OWNER}" "${RUNNER_ROOT}" "${os_deps_marker}" "$(rooted /dev/null)"
fi

if [[ ! -x "${NODE_ROOT}/bin/node" ]]; then
  node_install="${NODE_ROOT}"
  [[ ! -e "${node_install}" && ! -L "${node_install}" ]] || fail 'existing Node installation is partial; refusing replacement'
  node_stage_parent="$(mktemp -d "${OPT_ROOT}/.node-stage.XXXXXXXX")"
  node_stage="${node_stage_parent}/package"
  python3 "${HELPER}" extract "${node_file}" "${node_stage}"
  node_tree="${node_stage}/node-v${NODE_VERSION}-linux-x64"
  [[ -d "${node_tree}" && ! -L "${node_tree}" ]] || fail 'pinned Node archive has an unexpected layout'
  PATH="${node_tree}/bin:${PATH}" "${node_tree}/bin/corepack" enable
  chown -R "${INSTALL_OWNER}:${INSTALL_OWNER}" "${node_tree}"
  find "${node_tree}" -type d -exec chmod 0555 {} +
  find "${node_tree}" -type f -exec chmod a-w {} +
  python3 "${HELPER}" manifest "${node_tree}" "${node_tree}/.bootstrap-manifest"
  chown "${INSTALL_OWNER}:${INSTALL_OWNER}" "${node_tree}/.bootstrap-manifest"
  chmod 0444 "${node_tree}/.bootstrap-manifest"
  [[ ! -e "${node_install}" && ! -L "${node_install}" ]] || fail 'Node installation appeared during installation; refusing replacement'
  mv -- "${node_tree}" "${node_install}"
  rmdir -- "${node_stage}"
  rmdir -- "${node_stage_parent}"
fi
export PATH="${NODE_ROOT}/bin:${PATH}"

if [[ ! -d "${runner_package}" ]]; then
  install -d -o "${INSTALL_OWNER}" -g "${INSTALL_OWNER}" -m 0755 "${OPT_ROOT}/actions-runner"
  validate_trusted_directory "${OPT_ROOT}/actions-runner"
  runner_stage_parent="$(mktemp -d "${OPT_ROOT}/actions-runner/.stage.XXXXXXXX")"
  runner_stage="${runner_stage_parent}/package"
  python3 "${HELPER}" extract "${runner_file}" "${runner_stage}"
  [[ -x "${runner_stage}/config.sh" && -x "${runner_stage}/bin/Runner.Listener" && -x "${runner_stage}/runsvc.sh" ]] || fail 'pinned runner archive has an unexpected layout'
  chown -R "${INSTALL_OWNER}:${INSTALL_OWNER}" "${runner_stage}"
  find "${runner_stage}" -type d -exec chmod 0555 {} +
  find "${runner_stage}" -type f -exec chmod a-w {} +
  python3 "${HELPER}" manifest "${runner_stage}" "${runner_stage}/.bootstrap-manifest"
  chown "${INSTALL_OWNER}:${INSTALL_OWNER}" "${runner_stage}/.bootstrap-manifest"
  chmod 0444 "${runner_stage}/.bootstrap-manifest"
  [[ ! -e "${runner_package}" && ! -L "${runner_package}" ]] || fail 'runner package appeared during installation; refusing replacement'
  mv -- "${runner_stage}" "${runner_package}"
  rmdir -- "${runner_stage_parent}"
fi


if [[ "${offline_ready}" == 1 ]]; then
  [[ "$("${NODE_ROOT}/bin/node" --version)" == "v${NODE_VERSION}" ]] || fail 'existing Node installation is not the pinned version'
fi
if [[ "${toolchain_state_verified}" == 1 ]]; then
  rustup_list="$(runuser -u "${RUNNER_USER}" -- env HOME="${runner_home}" PATH="${cargo_bin}:${PATH}" "${rustup_bin}" toolchain list)"
  rust_toolchain_found=0
  while IFS= read -r toolchain; do
    case "${toolchain}" in "${RUST_VERSION}"|"${RUST_VERSION}-"*) rust_toolchain_found=1 ;; esac
  done <<< "${rustup_list}"
  [[ "${rust_toolchain_found}" == 1 ]] || fail 'existing Rust toolchain is not the pinned version'
  installed_targets="$(runuser -u "${RUNNER_USER}" -- env HOME="${runner_home}" PATH="${cargo_bin}:${PATH}" "${rustup_bin}" target list --installed --toolchain "${RUST_VERSION}")"
  [[ "${installed_targets}" == *wasm32-unknown-unknown* ]] || fail 'existing Rust wasm target is missing'
  if [[ "${INSTALL_WASM_PACK}" == 1 ]]; then
    wasm_pack_version="$(runuser -u "${RUNNER_USER}" -- env HOME="${runner_home}" PATH="${cargo_bin}:${PATH}" "${wasm_pack_bin}" --version)"
    [[ "${wasm_pack_version}" == "wasm-pack ${WASM_PACK_VERSION}" ]] || fail 'existing wasm-pack is not the pinned version'
  fi
fi


# Runtime state stays user-owned and separate from the root-owned, manifest-verified package.
validate_runtime_state
install -d -o "${INSTALL_OWNER}" -g "${INSTALL_OWNER}" -m 0755 "${RUNNER_ROOT}"
if [[ ! -L "${runner_state}" && ! -e "${runner_state}" ]]; then install -d -o "${RUNNER_USER}" -g "${INSTALL_OWNER}" -m 0700 "${runner_state}"; fi
chown "${RUNNER_USER}:${INSTALL_OWNER}" "${runner_state}"
chmod 2700 "${runner_state}"
for item in .runner .credentials .credentials_rsaparams .env .path _work _diag _temp; do
  runtime_link="${runner_package}/${item}"
  if [[ -L "${runtime_link}" ]]; then
    [[ "$(readlink -- "${runtime_link}")" == "${runner_state}/${item}" ]] || fail 'runner package contains an unsafe runtime-state link'
  elif [[ -e "${runtime_link}" ]]; then
    fail "runner package has unexpected mutable state at ${item}"
  else
    ln -s "${runner_state}/${item}" "${runtime_link}"
  fi
done
runuser -u "${RUNNER_USER}" -- env python3 "${HELPER}" prepare-state "${runner_state}" "${runner_uid}" "$(id -g "${INSTALL_OWNER}")" "${owner_uid}" || fail 'runner runtime state directories are unsafe'

if [[ "${toolchain_state_verified}" != 1 ]]; then
  # Rustup is consumed only after its publisher-pinned digest passed.
  chmod 0700 "${rustup_file}"
  chown "${RUNNER_USER}" "${rustup_file}"
  runuser -u "${RUNNER_USER}" -- env HOME="${runner_home}" PATH="${cargo_bin}:${PATH}" "${rustup_file}" -y --no-modify-path --default-toolchain "${RUST_VERSION}" --profile minimal
  runuser -u "${RUNNER_USER}" -- env HOME="${runner_home}" PATH="${cargo_bin}:${PATH}" "${rustup_bin}" target add wasm32-unknown-unknown --toolchain "${RUST_VERSION}"
  if [[ "${INSTALL_WASM_PACK}" == 1 ]]; then
    install_verified_wasm_pack "${RUNNER_USER}" "${runner_home}" "${cargo_bin}:${PATH}" "${WASM_PACK_VERSION}"
  fi

  install -d -o "${INSTALL_OWNER}" -g "${INSTALL_OWNER}" -m 0755 "${toolchain_integrity_dir}"
  validate_trusted_directory "${toolchain_integrity_dir}"
  seal_toolchain_manifest "${cargo_bin}" "${cargo_bin_manifest}"
  seal_toolchain_manifest "${rustup_home}" "${rustup_home_manifest}"
fi
if [[ ! -f "${runner_state}/.runner" ]]; then
  config_log="${work_tmp}/config.log"
  runner_gid="$(id -g "${RUNNER_USER}")"
  runner_package_open=1
  chown "${INSTALL_OWNER}:${runner_gid}" "${runner_package}"
  chmod 1770 "${runner_package}"
  config_status=0
  # A signal between spawn and PID capture must wait until cleanup can own
  # the new process group; recording it here does not discard cancellation.
  runner_config_launching=1
  set -m
  (
    export ACTIONS_RUNNER_INPUT_TOKEN="${RUNNER_TOKEN}"
    unset RUNNER_TOKEN
    # Keep the recorded launcher directly waitable, without an intermediate shell.
    cd -- "${runner_package}" &&
      exec runuser -u "${RUNNER_USER}" -- env HOME="${runner_home}" PATH="${runner_home}/.cargo/bin:${PATH}" \
        "${runner_package}/config.sh" --unattended --disableupdate --url "${RUNNER_URL}" \
        --name "${runner_name}" --labels "${RUNNER_LABELS}" --work "_work"
  ) >"${config_log}" 2>&1 &
  runner_config_pid=$!
  set +m
  runner_config_launching=0
  unset RUNNER_TOKEN
  if [[ "${runner_signal_pending}" != 0 ]]; then
    bootstrap_handle_signal "${runner_signal_pending}"
  fi
  wait "${runner_config_pid}" || config_status=$?
  runner_config_pid=''
  if (( config_status != 0 )); then
    fail 'runner configuration failed; diagnostic output suppressed to protect the registration token'
  fi
  runner_state_file="${runner_state}/.runner"
  generated_svc="${runner_package}/svc.sh"
  generated_svc_present=0
  if [[ -e "${generated_svc}" || -L "${generated_svc}" ]]; then
    if [[ ! -f "${generated_svc}" || -L "${generated_svc}" || ! -x "${generated_svc}" ]]; then
      fail 'runner generated an unsafe svc.sh'
    fi
    svc_owner="$(stat -c %u -- "${generated_svc}")"
    if [[ "${svc_owner}" != "${runner_uid}" ]]; then
      fail 'runner generated an untrusted svc.sh'
    fi
    if ! mv -- "${generated_svc}" "${work_tmp}/generated-svc"; then
      fail 'could not isolate runner-generated svc.sh'
    fi
    generated_svc_present=1
  fi
  seal_runner_package_directory
  python3 "${HELPER}" verify "${runner_package}" "${runner_manifest}" --exclude-runtime-links ||
    fail 'runner configuration modified the pinned package payload'
  if [[ "${generated_svc_present}" == 1 ]]; then
    rm -f -- "${work_tmp}/generated-svc"
  fi
  [[ -f "${runner_state_file}" ]] || fail 'runner configuration did not create runner state'
fi

install_jazz_runner_unit "${JAZZ_RUNNER_UNIT}" "${SYSTEMD_UNIT_DIR}" "${RUNNER_USER}" \
  "${runner_package}" "${runner_name}"
if ! systemctl start "${JAZZ_RUNNER_UNIT}" >"${work_tmp}/service-start.log" 2>&1; then
  cleanup_error=0
  if ! stop_and_prove_runner_unit "${JAZZ_RUNNER_UNIT}" "${SYSTEMD_MOUNTINFO}"; then cleanup_error=1; fi
  if (( cleanup_error == 0 )); then
    systemctl disable "${JAZZ_RUNNER_UNIT}" || cleanup_error=1
    rm -f -- "$(runner_unit_file "${JAZZ_RUNNER_UNIT}" "${SYSTEMD_UNIT_DIR}")" || cleanup_error=1
    systemctl daemon-reload || cleanup_error=1
  fi
  if (( cleanup_error != 0 )); then
    fail 'runner service start failed; cleanup could not be proven complete'
  fi
  fail 'runner service start failed; service was stopped and unit removed'
fi
if [[ "${SKIP_HARDENING:-0}" != 1 && -x "${SCRIPT_DIR}/harden_runner.sh" ]]; then
  "${SCRIPT_DIR}/harden_runner.sh"
fi
