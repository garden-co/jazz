#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root" >&2
  exit 1
fi

SERVICES_TO_DISABLE=(
  irqbalance.service
  cron.service
  ModemManager.service
  multipathd.service
  multipathd.socket
  udisks2.service
  unattended-upgrades.service
  fwupd.service
  fwupd-refresh.service
  snapd.service
  snapd.socket
)

TIMERS_TO_DISABLE=(
  apt-daily.timer
  apt-daily-upgrade.timer
  snapd.snap-repair.timer
  fwupd-refresh.timer
)

disable_and_mask_unit() {
  local unit="$1"
  systemctl disable --now "${unit}" || true
  systemctl mask "${unit}" || true
}

disable_timer() {
  local unit="$1"
  systemctl disable --now "${unit}" || true
}

set_performance_governor() {
  local governor
  for governor in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [[ -f "${governor}" ]] || continue
    echo performance > "${governor}" || true
  done
}

disable_smt_if_supported() {
  local smt_control="/sys/devices/system/cpu/smt/control"
  if [[ -w "${smt_control}" ]]; then
    echo off > "${smt_control}" || true
  fi
}

disable_cpu_boost_if_supported() {
  if [[ -w /sys/devices/system/cpu/cpufreq/boost ]]; then
    echo 0 > /sys/devices/system/cpu/cpufreq/boost || true
  fi
}

install_tuning_unit() {
  cat > /usr/local/sbin/benchmark-runner-tune.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

for governor in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  [[ -f "${governor}" ]] || continue
  echo performance > "${governor}" || true
done

if [[ -w /sys/devices/system/cpu/cpufreq/boost ]]; then
  echo 0 > /sys/devices/system/cpu/cpufreq/boost || true
fi

if [[ -w /sys/devices/system/cpu/smt/control ]]; then
  echo off > /sys/devices/system/cpu/smt/control || true
fi
EOF
  chmod 0755 /usr/local/sbin/benchmark-runner-tune.sh

  cat > /etc/systemd/system/benchmark-runner-tuning.service <<'EOF'
[Unit]
Description=Apply benchmark runner CPU tuning
After=multi-user.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/benchmark-runner-tune.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable benchmark-runner-tuning.service
  systemctl start benchmark-runner-tuning.service
}

for service in "${SERVICES_TO_DISABLE[@]}"; do
  disable_and_mask_unit "${service}"
done

for timer in "${TIMERS_TO_DISABLE[@]}"; do
  disable_timer "${timer}"
done

set_performance_governor
disable_cpu_boost_if_supported
disable_smt_if_supported
install_tuning_unit

echo "==== tuning summary ===="
echo "smt_control=$(cat /sys/devices/system/cpu/smt/control 2>/dev/null || echo unavailable)"
echo "online_cpus=$(cat /sys/devices/system/cpu/online 2>/dev/null || echo unavailable)"
echo "governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unavailable)"
echo "boost=$(cat /sys/devices/system/cpu/cpufreq/boost 2>/dev/null || echo unavailable)"
systemctl --no-pager --plain --type=service --state=running | \
  egrep "actions\\.runner|amazon-ssm-agent|chrony|irqbalance|snapd|fwupd|ModemManager|multipathd|udisks2|unattended-upgrades|cron" || true
