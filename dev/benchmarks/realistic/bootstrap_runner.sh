#!/usr/bin/env bash
set -euo pipefail
umask 077
RUNNER_TOKEN="${RUNNER_TOKEN:-}"
export -n RUNNER_TOKEN
unset ACTIONS_RUNNER_INPUT_TOKEN

readonly RUST_VERSION=1.93.1
readonly RUSTUP_VERSION=1.28.2
readonly RUSTUP_URL="https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/x86_64-unknown-linux-gnu/rustup-init"
readonly RUSTUP_SHA256=20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c
readonly NODE_VERSION=24.13.0
readonly NODE_URL="https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.gz"
readonly NODE_SHA256=6223aad1a81f9d1e7b682c59d12e2de233f7b4c37475cd40d1c89c42b737ffa8
readonly RUNNER_VERSION=2.337.0
readonly RUNNER_URL_PINNED="https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
readonly RUNNER_SHA256=70920811a4f8ad4328818682bca5c6469c1c942fab52448868071d0063816613
readonly WASM_PACK_VERSION=0.13.1
readonly WASM_PACK_URL="https://github.com/wasm-bindgen/wasm-pack/releases/download/v${WASM_PACK_VERSION}/wasm-pack-v${WASM_PACK_VERSION}-x86_64-unknown-linux-musl.tar.gz"
readonly WASM_PACK_SHA256=c539d91ccab2591a7e975bcf82c82e1911b03335c80aa83d67ad25ed2ad06539
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd -P)"
readonly NODE_ROOT="/opt/node-v${NODE_VERSION}"

fail() { printf '%s\n' "$1" >&2; exit 1; }
fetch_pinned() {
  local url="$1" output="$2" expected="$3"
  curl --fail --silent --show-error --location --output "${output}" "${url}"
  printf '%s  %s\n' "${expected}" "${output}" | sha256sum --check --status || fail 'download integrity check failed'
}

[[ "${EUID}" == 0 ]] || fail 'bootstrap_runner.sh must run as root'
RUNNER_USER="${RUNNER_USER:-ubuntu}"
RUNNER_URL="${RUNNER_URL:-https://github.com/garden-co/jazz}"
RUNNER_LABELS="${RUNNER_LABELS:-jazz-bench}"
INSTALL_WASM_PACK="${INSTALL_WASM_PACK:-1}"
INSTALL_SSM_AGENT="${INSTALL_SSM_AGENT:-auto}"
[[ "${RUNNER_USER}" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]] || fail 'RUNNER_USER is invalid'
[[ "${RUNNER_URL}" =~ ^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/?$ ]] || fail 'RUNNER_URL must be an HTTPS GitHub repository URL'
[[ "${RUNNER_LABELS}" =~ ^[A-Za-z0-9_.-]+(,[A-Za-z0-9_.-]+)*$ ]] || fail 'RUNNER_LABELS contains invalid characters'
[[ "${INSTALL_WASM_PACK}" == 0 || "${INSTALL_WASM_PACK}" == 1 ]] || fail 'INSTALL_WASM_PACK must be 0 or 1'
[[ -n "${RUNNER_TOKEN}" ]] || fail 'RUNNER_TOKEN is required to configure a new runner'

should_install_ssm_agent=0
case "${INSTALL_SSM_AGENT}" in
  1|true|yes) should_install_ssm_agent=1 ;;
  0|false|no) ;;
  auto)
    if { [[ -r /sys/devices/virtual/dmi/id/sys_vendor ]] && grep -qi amazon /sys/devices/virtual/dmi/id/sys_vendor; } ||
      { [[ -r /sys/devices/virtual/dmi/id/product_name ]] && grep -qi amazon /sys/devices/virtual/dmi/id/product_name; }; then
      should_install_ssm_agent=1
    fi
    ;;
  *) fail 'INSTALL_SSM_AGENT must be auto, 0, or 1' ;;
esac

instance_id="$(cat /var/lib/cloud/data/instance-id 2>/dev/null || hostname)"
RUNNER_NAME="${RUNNER_NAME:-benchmark-runner-${instance_id}}"
[[ "${RUNNER_NAME}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$ ]] || fail 'RUNNER_NAME is invalid'
if ! id -u "${RUNNER_USER}" >/dev/null 2>&1; then
  useradd --create-home --shell /bin/bash -- "${RUNNER_USER}"
fi
[[ "$(id -u "${RUNNER_USER}")" != 0 ]] || fail 'RUNNER_USER must resolve to a non-root UID'
account="$(getent passwd "${RUNNER_USER}")"
IFS=: read -r account_name _ _ _ _ runner_home _ <<< "${account}"
[[ "${account_name}" == "${RUNNER_USER}" && "${runner_home}" == /* ]] || fail 'runner account has an invalid home directory'
runner_group="$(id -gn "${RUNNER_USER}")"
runner_dir="${runner_home}/actions-runner"
[[ ! -e "${runner_dir}" && ! -L "${runner_dir}" ]] || fail 'runner directory already exists; provision a fresh host'

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y \
  build-essential curl git jq unzip ca-certificates pkg-config libssl-dev libclang-dev libicu74 \
  xvfb libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libgbm1 \
  libasound2t64 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
  libxrandr2 libgtk-3-0 libpango-1.0-0 libcairo2 libatspi2.0-0 \
  linux-tools-common linux-tools-generic
if [[ "${should_install_ssm_agent}" == 1 ]]; then
  apt-get install -y snapd
  systemctl enable --now snapd.socket
  snap wait system seed.loaded
  if ! snap list amazon-ssm-agent >/dev/null 2>&1; then
    snap install amazon-ssm-agent --classic
  fi
  systemctl enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service
fi

work_tmp="$(mktemp -d)"
trap 'rm -rf -- "${work_tmp}"' EXIT
chmod 0711 "${work_tmp}"
fetch_pinned "${RUSTUP_URL}" "${work_tmp}/rustup-init" "${RUSTUP_SHA256}"
fetch_pinned "${NODE_URL}" "${work_tmp}/node.tar.gz" "${NODE_SHA256}"
fetch_pinned "${RUNNER_URL_PINNED}" "${work_tmp}/runner.tar.gz" "${RUNNER_SHA256}"
if [[ "${INSTALL_WASM_PACK}" == 1 ]]; then
  fetch_pinned "${WASM_PACK_URL}" "${work_tmp}/wasm-pack.tar.gz" "${WASM_PACK_SHA256}"
fi

# Only verified downloads are executed or extracted. Rustup verifies the Rust
# toolchain components against the release checksums during installation.
chmod 0755 "${work_tmp}/rustup-init"
runuser -u "${RUNNER_USER}" -- env HOME="${runner_home}" \
  "${work_tmp}/rustup-init" -y --no-modify-path --default-toolchain "${RUST_VERSION}" --profile minimal
runuser -u "${RUNNER_USER}" -- env HOME="${runner_home}" \
  "${runner_home}/.cargo/bin/rustup" target add wasm32-unknown-unknown --toolchain "${RUST_VERSION}"

install -d -m 0755 "${NODE_ROOT}"
tar xzf "${work_tmp}/node.tar.gz" --strip-components=1 -C "${NODE_ROOT}"
export PATH="${NODE_ROOT}/bin:${PATH}"
"${NODE_ROOT}/bin/corepack" enable

if [[ "${INSTALL_WASM_PACK}" == 1 ]]; then
  tar xzf "${work_tmp}/wasm-pack.tar.gz" -C "${work_tmp}"
  install -o "${RUNNER_USER}" -g "${runner_group}" -m 0755 \
    "${work_tmp}/wasm-pack-v${WASM_PACK_VERSION}-x86_64-unknown-linux-musl/wasm-pack" \
    "${runner_home}/.cargo/bin/wasm-pack"
fi

install -d -o "${RUNNER_USER}" -g "${runner_group}" -m 0755 "${runner_dir}"
chmod 0644 "${work_tmp}/runner.tar.gz"
runuser -u "${RUNNER_USER}" -- tar xzf "${work_tmp}/runner.tar.gz" -C "${runner_dir}"
if ! (
  export ACTIONS_RUNNER_INPUT_TOKEN="${RUNNER_TOKEN}"
  unset RUNNER_TOKEN
  cd -- "${runner_dir}" || exit 1
  runuser -u "${RUNNER_USER}" -- env HOME="${runner_home}" PATH="${runner_home}/.cargo/bin:${PATH}" \
    ./config.sh --unattended --disableupdate --url "${RUNNER_URL}" \
    --name "${RUNNER_NAME}" --labels "${RUNNER_LABELS}" --work _work
) >"${work_tmp}/config.log" 2>&1; then
  fail 'runner configuration failed; diagnostic output suppressed to protect the registration token'
fi
unset RUNNER_TOKEN

# Use the service installer generated by the pinned runner release.
(
  cd -- "${runner_dir}"
  ./svc.sh install "${RUNNER_USER}"
  ./svc.sh start
)
if [[ "${SKIP_HARDENING:-0}" != 1 && -x "${SCRIPT_DIR}/harden_runner.sh" ]]; then
  "${SCRIPT_DIR}/harden_runner.sh"
fi
