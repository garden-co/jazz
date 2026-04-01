# Hetzner Dedicated Runner Setup

Use this when the benchmark runner lives on a dedicated Hetzner box instead of EC2.

## Current host profile

As installed on March 9, 2026:

- Host: Hetzner dedicated server
- OS: Ubuntu 24.04.3 LTS
- Kernel: `6.8.0-101-generic`
- CPU: AMD Ryzen 5 3600
- Topology after hardening: `6` online CPUs (`0-5`), `1` thread per core, single NUMA node
- Storage: `2 x 512GB NVMe` in `mdadm` RAID1
- Governor: `performance`
- CPU boost: disabled
- SMT: disabled

This is a materially better benchmark box than the old EC2 runner because it avoids VM scheduling noise and EBS variance.

## Install the OS

In Hetzner rescue mode, install Ubuntu 24.04 onto both NVMe drives with software RAID1:

- `DRIVE1 /dev/nvme0n1`
- `DRIVE2 /dev/nvme1n1`
- `SWRAID 1`
- `SWRAIDLEVEL 1`
- `BOOTLOADER grub`
- `HOSTNAME benchmark-runner`
- `PART /boot/efi esp 256M`
- `PART /boot ext3 1024M`
- `PART / ext4 all`
- `IMAGE /root/images/Ubuntu-2404-noble-amd64-base.tar.gz`

If you need password-based first boot from rescue mode, include `FORCE_PASSWORD 1`.

## Bootstrap the runner

Use the checked-in bootstrap script from the repo:

```bash
sudo RUNNER_TOKEN="<repo registration token>" \
  RUNNER_URL="https://github.com/garden-co/jazz2" \
  RUNNER_USER=runner \
  RUNNER_NAME="benchmark-runner-hetzner" \
  RUNNER_LABELS="jazz-bench,hetzner" \
  INSTALL_SSM_AGENT=0 \
  benchmarks/realistic/bootstrap_runner.sh
```

The bootstrap script will:

- create the runner user if it does not exist
- install Rust, Node, `pnpm`, `wasm-pack`, and `libclang`
- install and register the GitHub Actions runner as a systemd service
- skip AWS SSM installation automatically on non-AWS hardware
- apply `benchmarks/realistic/harden_runner.sh` unless `SKIP_HARDENING=1`

## Hardening choices

Run the hardening script directly if you need to re-apply tuning:

```bash
sudo benchmarks/realistic/harden_runner.sh
```

The script currently enforces:

- CPU governor `performance`
- CPU boost disabled
- SMT disabled
- `irqbalance`, `cron`, `snapd`, `fwupd`, `ModemManager`, `multipathd`, `udisks2`, and `unattended-upgrades` disabled or masked
- boot-time reapplication via `benchmark-runner-tuning.service`

On this host, leave CPU `0` for the OS and pin benchmark processes to `1-5`.

## Validation checklist

After bootstrap or reboot, verify:

```bash
uname -r
cat /sys/devices/system/cpu/smt/control
cat /sys/devices/system/cpu/online
cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
cat /sys/devices/system/cpu/cpufreq/boost
systemctl is-active actions.runner.garden-co-jazz2.benchmark-runner-hetzner.service
```

Expected output on the current box:

- kernel `6.8.0-101-generic`
- SMT `off`
- online CPUs `0-5`
- governor `performance`
- boost `0`
- runner service `active`
