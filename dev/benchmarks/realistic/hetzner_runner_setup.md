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

The bootstrap supports a fresh Ubuntu 24.04 x64 host. It pins Rust `1.93.1`,
rustup-init `1.28.2`, Node `24.13.0`, Actions Runner `2.337.0`, and
wasm-pack `0.13.1`. Each downloaded installer or archive must match the SHA-256
digest in `bootstrap_runner.sh` before it is executed or extracted. Rustup
checks the Rust toolchain components against their release checksums.

Keep the versions and digests together when updating them. Node and runner
digests come from their official release checksums; the wasm-pack digest is
pinned from the official [release archive](https://github.com/wasm-bindgen/wasm-pack/releases/tag/v0.13.1).
The script uses no floating releases or `curl | sh`.

Run bootstrap as root. Rust, wasm-pack and the runner are installed for the
non-root runner account; Node and its Corepack shims are installed under
`/opt/node-v24.13.0/bin`. The runner's generated `svc.sh` installs and starts
the standard `actions.runner.*.service` service. Hardening runs afterwards
unless `SKIP_HARDENING=1`.

The registration token is passed only to configuration through
`ACTIONS_RUNNER_INPUT_TOKEN`, never through command arguments. Configuration
output is suppressed to keep the token out of failure diagnostics. A failed
download check, required installation, configuration or service action aborts
bootstrap.

Bootstrap refuses an existing `~/actions-runner` directory. Use a fresh host
for reprovisioning or after a partial installation. This script does not
upgrade or migrate an existing runner. Runner updates are disabled; review and
update the pinned runner release before GitHub's update deadline.

`INSTALL_SSM_AGENT=auto` installs SSM on detected AWS hardware and skips it
elsewhere. Use `1` to require it, or `0` to skip it explicitly. SSM is installed
through Ubuntu's apt/snap repositories; requested installation failures abort
setup.

Use the checked-in script:

```bash
read -rsp "GitHub registration token: " RUNNER_TOKEN
printf '\n'
export RUNNER_TOKEN
sudo --preserve-env=RUNNER_TOKEN env \
  RUNNER_URL="https://github.com/garden-co/jazz" \
  RUNNER_USER=runner \
  RUNNER_NAME="benchmark-runner-hetzner" \
  RUNNER_LABELS="jazz-bench,hetzner" \
  INSTALL_SSM_AGENT=0 \
  dev/benchmarks/realistic/bootstrap_runner.sh
bootstrap_status=$?
unset RUNNER_TOKEN
exit "${bootstrap_status}"
```

Run the focused checks with
`node --test dev/benchmarks/realistic/bootstrap_runner.test.mjs`.
They use miniature archives and command stubs in temporary directories. They
cover checksum failures, non-root installation commands, token handling and
required installation failures; they do not validate a live systemd host.

Before reprovisioning real runners, bootstrap one disposable host with
`RUNNER_NAME=bootstrap-smoke-<unique-id>`, `RUNNER_LABELS=jazz-bootstrap-smoke`
and `SKIP_HARDENING=1`. Keep its label separate from `jazz-bench` so it does
not take production benchmark jobs. Check that its GitHub registration is
online and that its `actions.runner.*.service` is active. As the runner user,
check `rustc --version`, `rustup target list --installed`, `node --version`,
`pnpm --version` and `wasm-pack --version` with
`/opt/node-v24.13.0/bin` and `~/.cargo/bin` on `PATH`. Confirm the pinned
versions and the `wasm32-unknown-unknown` target, then reboot and check the
service again. Remove the disposable GitHub registration and host when done.
Reprovision real hosts one at a time after this check succeeds.

## Hardening choices

Run the hardening script directly if you need to re-apply tuning:

```bash
sudo dev/benchmarks/realistic/harden_runner.sh
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
systemctl list-units 'actions.runner.*.service'
```

Expected output on the current box:

- kernel `6.8.0-101-generic`
- SMT `off`
- online CPUs `0-5`
- governor `performance`
- boost `0`
- runner service `active`
