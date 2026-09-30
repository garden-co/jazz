#!/usr/bin/env bash

resolve_install_ssm() {
  local requested="$1" vendor_file="$2" product_file="$3"
  case "${requested}" in
    1|true|yes) printf '1' ;;
    0|false|no) printf '0' ;;
    auto)
      if { [[ -r "${vendor_file}" ]] && grep -qi amazon "${vendor_file}"; } || { [[ -r "${product_file}" ]] && grep -qi amazon "${product_file}"; }; then
        printf '1'
      else
        printf '0'
      fi
      ;;
    *) return 1 ;;
  esac
}

install_verified_os_dependencies() {
  local install_ssm="$1" install_owner="$2" runner_root="$3" os_deps_marker="$4" dev_null="$5"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y \
    build-essential curl git jq unzip ca-certificates pkg-config libssl-dev libclang-dev \
    xvfb libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libgbm1 \
    libasound2t64 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgtk-3-0 libpango-1.0-0 libcairo2 libatspi2.0-0 \
    linux-tools-common linux-tools-generic

  if [[ "${install_ssm}" == 1 ]]; then
    apt-get install -y snapd
    systemctl enable --now snapd.socket
    snap wait system seed.loaded
    snap install amazon-ssm-agent --classic
    systemctl enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service
  fi
  install -d -o "${install_owner}" -g "${install_owner}" -m 0755 "${runner_root}"
  install -o "${install_owner}" -g "${install_owner}" -m 0444 "${dev_null}" "${os_deps_marker}"
}

install_verified_wasm_pack() {
  local runner_user="$1" runner_home="$2" path="$3" version="$4"
  runuser -u "${runner_user}" -- env HOME="${runner_home}" PATH="${path}" cargo install wasm-pack --version "${version}" --locked
}
