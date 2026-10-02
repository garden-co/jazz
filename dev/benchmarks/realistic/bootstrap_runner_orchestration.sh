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
runner_service_name() {
  local canonical_url="$1" name="$2"
  printf 'jazz-benchmark-runner-%s.service' "$(
    printf '%s\0%s' "${canonical_url%/}" "${name}" | sha256sum | cut -d' ' -f1
  )"
}

legacy_runner_service_name() {
  local url="$1" name="$2" repo
  repo="${url#https://github.com/}"
  repo="${repo%/}"
  repo="${repo//\//-}"
  repo="${repo//[^A-Za-z0-9_.-]/-}"
  name="${name//[^A-Za-z0-9_.-]/-}"
  local unit="actions.runner.${repo}.${name}.service"
  (( ${#unit} <= 150 )) || return 1
  printf '%s' "${unit}"
}
legacy_runner_service_identity_is_unambiguous() {
  local url="$1" name="$2" repo suffix
  repo="${url#https://github.com/}"
  repo="${repo%/}"
  repo="${repo//\//-}"
  repo="${repo//[^A-Za-z0-9_.-]/-}"
  suffix="${repo}.${name}"
  [[ "${repo//[^-]/}" == "-" && "${suffix//[^.]/}" == "." ]]
}


runner_unit_file() {
  local unit="$1" unit_dir="$2"
  printf '%s/%s' "${unit_dir}" "${unit}"
}

validate_runner_unit_file() {
  local file="$1" expected_user="$2" expected_pkg="$3" legacy_template="$4"
  local metadata owner mode line killmode_count directive_count directive
  [[ -f "${file}" && ! -L "${file}" ]] || return 1
  metadata="$(stat -c '%u %a' -- "${file}")" || return 1
  read -r owner mode <<< "${metadata}"
  [[ "${owner}" == "${UNIT_OWNER_UID}" ]] || return 1
  (( (8#${mode} & 022) == 0 )) || return 1
  [[ "$(grep -Fc '[Unit]' "${file}")" == 1 &&
    "$(grep -Fc '[Service]' "${file}")" == 1 &&
    "$(grep -Fc '[Install]' "${file}")" == 1 &&
    "$(grep -Ec '^User=' "${file}")" == 1 &&
    "$(grep -Ec '^WorkingDirectory=' "${file}")" == 1 &&
    "$(grep -Ec '^ExecStart=' "${file}")" == 1 ]] || return 1
  if ! grep -Fqx "User=${expected_user}" "${file}" ||
    ! grep -Fqx "WorkingDirectory=${expected_pkg}" "${file}" ||
    ! grep -Fqx "ExecStart=${expected_pkg}/runsvc.sh" "${file}"; then
    return 1
  fi
  [[ "$(grep -Ec '^Description=' "${file}")" == 1 ]] || return 1
  for directive in After KillSignal TimeoutStopSec WantedBy; do
    directive_count="$(grep -Ec "^${directive}=" "${file}")"
    (( directive_count <= 1 )) || return 1
  done
  killmode_count="$(grep -Ec '^KillMode=' "${file}")"
  if [[ "${legacy_template}" == 1 ]]; then
    [[ "${killmode_count}" == 1 ]] && grep -Fqx 'KillMode=process' "${file}" || return 1
  else
    [[ "${killmode_count}" == 0 ]] || return 1
  fi
  while IFS= read -r line || [[ -n "${line}" ]]; do
    case "${line}" in
      ''|'[Unit]'|'[Service]'|'[Install]'|'After=network-online.target') ;;
      'Description=GitHub Actions Runner ('*|'Description=Jazz benchmark runner '*) ;;
      "User=${expected_user}"|"WorkingDirectory=${expected_pkg}") ;;
      "ExecStart=${expected_pkg}/runsvc.sh"|'KillMode=process') ;;
      'KillSignal=SIGTERM'|'TimeoutStopSec=5min'|'WantedBy=multi-user.target') ;;
      *) return 1 ;;
    esac
  done < "${file}"
}
validate_versioned_runner_executable() {
  local package="$1" root="$2" version metadata owner mode
  [[ "${package}" == "${root}/"* ]] || return 1
  version="${package#${root}/}"
  [[ "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
  [[ -d "${package}" && ! -L "${package}" ]] || return 1
  metadata="$(stat -c '%u %a' -- "${package}")" || return 1
  read -r owner mode <<< "${metadata}"
  [[ "${owner}" == "${UNIT_OWNER_UID}" ]] || return 1
  (( (8#${mode} & 022) == 0 )) || return 1
  [[ -f "${package}/runsvc.sh" && ! -L "${package}/runsvc.sh" && -x "${package}/runsvc.sh" ]] || return 1
  metadata="$(stat -c '%u %a' -- "${package}/runsvc.sh")" || return 1
  read -r owner mode <<< "${metadata}"
  [[ "${owner}" == "${UNIT_OWNER_UID}" ]] && (( (8#${mode} & 022) == 0 ))
}


systemd_cgroup_path() {
  local control_group="$1" mountinfo="$2" mountpoint
  [[ -z "${control_group}" ]] && return 0
  [[ "${control_group}" == /* && "${control_group}" != *"//"* &&
    "${control_group}" != *"/../"* && "${control_group}" != */.. &&
    "${control_group}" != *"/./"* && "${control_group}" != */. ]] || return 1
  if [[ -n "${ROOT}" ]]; then
    printf '%s/sys/fs/cgroup%s' "${ROOT}" "${control_group}"
    return 0
  fi
  mountpoint="$(
    awk '{
      for (i = 1; i <= NF; i++) if ($i == "-") {
        if ($(i + 1) == "cgroup2" && $4 == "/" && $5 == "/sys/fs/cgroup") { print $5; exit }
      }
    }' "${mountinfo}"
  )"
  [[ "${mountpoint}" == /sys/fs/cgroup ]] || return 1
  printf '%s%s' "${mountpoint}" "${control_group}"
}

runner_cgroup_empty() {
  local control_group="$1" mountinfo="$2" cgroup events key value extra populated= frozen_seen=0
  CGROUP_ERROR='runner cgroup state is malformed'
  cgroup="$(systemd_cgroup_path "${control_group}" "${mountinfo}")" || return 1
  [[ -z "${cgroup}" ]] && return 0
  if [[ ! -e "${cgroup}" && ! -L "${cgroup}" ]]; then return 0; fi
  events="${cgroup}/cgroup.events"
  [[ -r "${events}" && -f "${events}" && ! -L "${events}" ]] || return 1
  while read -r key value extra; do
    [[ -z "${extra:-}" ]] || return 1
    case "${key}:${value}" in
      populated:0|populated:1)
        [[ -z "${populated}" ]] || return 1
        populated="${value}"
        ;;
      frozen:0|frozen:1)
        (( frozen_seen == 0 )) || return 1
        frozen_seen=1
        ;;
      *) return 1 ;;
    esac
  done < "${events}" || return 1
  [[ -n "${populated}" ]] || return 1
  if [[ "${populated}" == 1 ]]; then
    CGROUP_ERROR='runner cgroup is not empty'
    return 1
  fi
  return 0
}

stop_and_prove_runner_unit() {
  local unit="$1" mountinfo="$2" state substate control_group
  if ! systemctl stop "${unit}"; then STOP_ERROR='runner service stop failed'; return 1; fi
  state="$(systemctl show "${unit}" --property=ActiveState --value)" || { STOP_ERROR='runner service state is unknown after stop'; return 1; }
  substate="$(systemctl show "${unit}" --property=SubState --value)" || { STOP_ERROR='runner service state is unknown after stop'; return 1; }
  if [[ "${state}" != inactive || ( "${substate}" != dead && !( -n "${ROOT}" && "${substate}" == inactive ) ) ]]; then
    STOP_ERROR='runner service did not become inactive after stop'
    return 1
  fi
  control_group="$(systemctl show "${unit}" --property=ControlGroup --value)" || { STOP_ERROR='runner service cgroup is unknown after stop'; return 1; }
  if ! runner_cgroup_empty "${control_group}" "${mountinfo}"; then
    STOP_ERROR="${CGROUP_ERROR:-runner cgroup is not empty}"
    return 1
  fi
  return 0
}

systemd_exec_paths() {
  local serialized="$1" executable
  while [[ "${serialized}" =~ path=([^[:space:];}]+) ]]; do
    executable="${BASH_REMATCH[1]}"
    printf '%s\n' "${executable}"
    serialized="${serialized#*path=${executable}}"
  done
}

resolve_runner_units() {
  local expected="$1" legacy="$2" unit_dir="$3" runner_user="$4" runner_pkg="$5" legacy_pkg="$6" actions_runner_root="$7" legacy_identity_unambiguous="$8"
  local names=() candidates=() exec_paths=() candidate candidate_pkg legacy_template
  local manager_user manager_working_directory manager_exec_start manager_killmode manager_dropins
  local manager_exec_path inventory_has_package unit_files active_units listed_line versioned_pkg lifecycle_property lifecycle_command
  unit_files="$(systemctl list-unit-files --type=service --no-legend --no-pager)" ||
    fail 'runner manager unit inventory is unknown'
  active_units="$(systemctl list-units --type=service --all --no-legend --no-pager)" ||
    fail 'runner manager unit inventory is unknown'
  while IFS= read -r listed_line; do
    candidate="${listed_line%%[[:space:]]*}"
    [[ "${candidate}" == *.service ]] && names+=("${candidate}")
  done < <(printf '%s\n%s\n' "${unit_files}" "${active_units}" | sort -u)
  for candidate in "${names[@]}"; do
    manager_exec_start="$(systemctl show "${candidate}" --property=ExecStart --value)" ||
      fail 'runner manager unit inventory is unknown'
    mapfile -t exec_paths < <(systemd_exec_paths "${manager_exec_start}")
    inventory_has_package=0
    if [[ "${manager_exec_start}" == *"${runner_pkg}/"* ||
      "${manager_exec_start}" == *"${legacy_pkg}/"* ||
      "${manager_exec_start}" == *"${actions_runner_root}/"* ]]; then inventory_has_package=1; fi
    if [[ "${candidate}" == "${expected}" || "${candidate}" == "${legacy}" ]]; then
      inventory_has_package=1
    fi
    [[ "${inventory_has_package}" == 1 ]] || continue
    [[ "${candidate}" == "${expected}" || "${candidate}" == "${legacy}" ]] ||
      fail 'runner unit identity is ambiguous'
    if [[ "${candidate}" == "${legacy}" && "${legacy_identity_unambiguous}" != 1 ]]; then
      fail 'legacy runner unit identity is ambiguous; inspect services and reprovision manually'
    fi
    candidate_pkg="${runner_pkg}"
    legacy_template=0
    [[ "${#exec_paths[@]}" == 1 ]] || fail 'runner unit identity is ambiguous'
    manager_exec_path="${exec_paths[0]}"
    if [[ "${candidate}" == "${legacy}" ]]; then
      legacy_template=1
      if [[ "${manager_exec_path}" == "${legacy_pkg}/runsvc.sh" ]]; then
        candidate_pkg="${legacy_pkg}"
      else
        versioned_pkg="${manager_exec_path%/runsvc.sh}"
        [[ "${manager_exec_path}" == "${versioned_pkg}/runsvc.sh" &&
          "${versioned_pkg}" == "${actions_runner_root}/"* ]] || fail 'runner unit identity is ambiguous'
        validate_versioned_runner_executable "${versioned_pkg}" "${actions_runner_root}" ||
          fail 'runner unit identity is ambiguous'
        candidate_pkg="${versioned_pkg}"
      fi
    fi
    [[ "${manager_exec_path}" == "${candidate_pkg}/runsvc.sh" ]] || fail 'runner unit identity is ambiguous'
    validate_runner_unit_file "$(runner_unit_file "${candidate}" "${unit_dir}")" \
      "${runner_user}" "${candidate_pkg}" "${legacy_template}" || fail 'runner unit identity is ambiguous'
    manager_killmode="$(systemctl show "${candidate}" --property=KillMode --value)" || fail 'runner unit identity is ambiguous'
    manager_dropins="$(systemctl show "${candidate}" --property=DropInPaths --value)" || fail 'runner unit identity is ambiguous'
    [[ -z "${manager_dropins}" ]] || fail 'runner unit identity is ambiguous'
    for lifecycle_property in ExecStartPre ExecStartPost ExecStop ExecStopPost; do
      lifecycle_command="$(systemctl show "${candidate}" --property="${lifecycle_property}" --value)" ||
        fail 'runner unit identity is ambiguous'
      [[ -z "${lifecycle_command}" ]] || fail 'runner unit identity is ambiguous'
    done
    if [[ "${legacy_template}" == 1 ]]; then
      [[ "${manager_killmode}" == process ]] || fail 'runner unit identity is ambiguous'
    else
      [[ "${manager_killmode}" == control-group ||
        ( -n "${ROOT}" && -z "${manager_killmode}" ) ]] || fail 'runner unit identity is ambiguous'
    fi
    manager_user="$(systemctl show "${candidate}" --property=User --value)" || fail 'runner unit identity is ambiguous'
    manager_working_directory="$(systemctl show "${candidate}" --property=WorkingDirectory --value)" || fail 'runner unit identity is ambiguous'
    [[ "${manager_user}" == "${runner_user}" &&
      "${manager_working_directory}" == "${candidate_pkg}" ]] || fail 'runner unit identity is ambiguous'
    candidates+=("${candidate}")
  done
  if [[ -f "${RUNNER_STATE}/.service" && ${#candidates[@]} -eq 0 ]]; then
    fail 'runner service marker has no matching manager unit'
  fi
  if ((${#candidates[@]})); then printf '%s\n' "${candidates[@]}"; fi
}

install_jazz_runner_unit() {
  local unit="$1" unit_dir="$2" runner_user="$3" runner_pkg="$4" runner_name="$5" tmp
  install -d -o "${INSTALL_OWNER}" -g "${INSTALL_OWNER}" -m 0755 "${unit_dir}"
  validate_trusted_directory "${unit_dir}"
  tmp="$(mktemp "${unit_dir}/.${unit}.XXXXXXXX")"
  {
    # Render the v2.337.0 unit template with KillMode deliberately removed:
    # systemd's default control-group mode is required for quiescence proof.
    printf '[Unit]\nDescription=GitHub Actions Runner (%s)\nAfter=network-online.target\n\n[Service]\nExecStart=%s/runsvc.sh\nUser=%s\nWorkingDirectory=%s\nKillSignal=SIGTERM\nTimeoutStopSec=5min\n\n[Install]\nWantedBy=multi-user.target\n' \
      "${runner_name}" "${runner_pkg}" "${runner_user}" "${runner_pkg}"
  } > "${tmp}"
  chown "${INSTALL_OWNER}:${INSTALL_OWNER}" "${tmp}"
  chmod 0644 "${tmp}"
  mv -f -- "${tmp}" "$(runner_unit_file "${unit}" "${unit_dir}")"
  systemctl daemon-reload
  systemctl enable "${unit}"
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
