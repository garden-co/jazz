#!/usr/bin/env bash
set -euo pipefail

RUNNER_USER="${RUNNER_USER:-ubuntu}"
RUNNER_URL="${RUNNER_URL:-https://github.com/garden-co/jazz2}"
RUNNER_TOKEN="${RUNNER_TOKEN:-}"
RUNNER_LABELS="${RUNNER_LABELS:-jazz-bench}"
RUNNER_VERSION="${RUNNER_VERSION:-}"
NODE_MAJOR="${NODE_MAJOR:-22}"
INSTALL_WASM_PACK="${INSTALL_WASM_PACK:-1}"

if [[ -z "${RUNNER_TOKEN}" ]]; then
  echo "RUNNER_TOKEN is required" >&2
  exit 1
fi

RUNNER_HOME="$(eval echo "~${RUNNER_USER}")"
RUNNER_DIR="${RUNNER_HOME}/actions-runner"
INSTANCE_ID_FILE="/var/lib/cloud/data/instance-id"
RUNNER_NAME="${RUNNER_NAME:-benchmark-runner-$(cat "${INSTANCE_ID_FILE}" 2>/dev/null || hostname)}"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y \
  build-essential curl git jq unzip ca-certificates pkg-config libssl-dev \
  xvfb libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libgbm1 \
  libasound2t64 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
  libxrandr2 libgtk-3-0 libpango-1.0-0 libcairo2 libatspi2.0-0 \
  linux-tools-common linux-tools-generic snapd

systemctl enable --now snapd.socket || true
snap wait system seed.loaded || true
snap install amazon-ssm-agent --classic || true
systemctl enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service || true

sudo -u "${RUNNER_USER}" -H bash -lc '
  set -euo pipefail
  if [[ ! -x "$HOME/.cargo/bin/rustup" ]]; then
    curl https://sh.rustup.rs -sSf | sh -s -- -y
  fi
  source "$HOME/.cargo/env"
  rustup toolchain install stable
  rustup default stable
  rustup target add wasm32-unknown-unknown
'

curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | bash -
apt-get install -y nodejs

# Corepack needs root here so it can install the pnpm shims under /usr/bin.
corepack enable

if [[ "${INSTALL_WASM_PACK}" == "1" ]]; then
  sudo -u "${RUNNER_USER}" -H bash -lc '
    set -euo pipefail
    source "$HOME/.cargo/env"
    cargo install wasm-pack --locked || true
  '
fi

cpupower frequency-set -g performance || true

sudo -u "${RUNNER_USER}" -H bash -lc "
  set -euo pipefail
  source \"\$HOME/.cargo/env\"
  mkdir -p \"${RUNNER_DIR}\"
  cd \"${RUNNER_DIR}\"

  if [[ -z \"${RUNNER_VERSION}\" ]]; then
    resolved_version=\$(curl -fsSL https://api.github.com/repos/actions/runner/releases/latest | jq -r .tag_name | sed 's/^v//')
  else
    resolved_version=\"${RUNNER_VERSION}\"
  fi

  if [[ ! -x bin/Runner.Listener ]]; then
    curl -fsSLo actions-runner.tar.gz \
      \"https://github.com/actions/runner/releases/download/v\${resolved_version}/actions-runner-linux-x64-\${resolved_version}.tar.gz\"
    tar xzf actions-runner.tar.gz
    rm -f actions-runner.tar.gz
  fi

  if [[ ! -f .runner ]]; then
    ./config.sh --unattended \
      --url \"${RUNNER_URL}\" \
      --token \"${RUNNER_TOKEN}\" \
      --name \"${RUNNER_NAME}\" \
      --labels \"${RUNNER_LABELS}\" \
      --work \"_work\" \
      --replace
  fi

  ./env.sh
"

# Current runner releases generate svc.sh after config.sh completes.
if [[ ! -f "${RUNNER_DIR}/svc.sh" ]]; then
  echo "Expected ${RUNNER_DIR}/svc.sh after runner configuration" >&2
  exit 1
fi

if [[ ! -f "${RUNNER_DIR}/.service" ]]; then
  (
    cd "${RUNNER_DIR}"
    ./svc.sh install "${RUNNER_USER}"
  )
fi

(
  cd "${RUNNER_DIR}"
  ./svc.sh start
)
