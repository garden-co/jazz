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

The checked-in bootstrap pins Rust `1.93.1`, Node `24.13.0` (official Node.js tarball SHA-256 `6223aad1a81f9d1e7b682c59d12e2de233f7b4c37475cd40d1c89c42b737ffa8`), Actions Runner `2.337.0` (SHA-256 `70920811a4f8ad4328818682bca5c6469c1c942fab52448868071d0063816613`), rustup-init `1.28.2` (SHA-256 `20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c`), and wasm-pack `0.13.1`. Downloaded archives must match their fixed publisher hashes before execution/extraction; versions do not float to `latest`.

Bootstrap fails closed. The runner account must resolve to a non-root UID.
Hashes are checked before archive extraction or execution; reuse requires the
root-owned manifest to match every immutable runner-package entry.
An empty runner `~/.cargo/bin` directory is not treated as an installed
toolchain. Bootstrap rechecks tool paths after all verified runners are
disabled, stopped and proven quiescent. If a job adds a tool during shutdown,
the recheck finds unmanifested state and aborts before executing it.

Before bootstrap runs persistent Rustup or wasm-pack, it enumerates manager
units and validates each matching runner unit, registration identity,
root-owned unit file, executable and drop-ins. It disables all exact verified
candidates before stopping any, then requires each to be inactive/dead with an
empty cgroup-v2 `cgroup.events`. A disable, stop or cgroup-proof failure aborts
before persistent tools run. If disabling fails, boot activation is uncertain.
If stopping or proving an empty cgroup fails, a runner may remain active during
the current boot; bootstrap does not claim that it stopped.
Units still loaded in systemd remain candidates even if their on-disk files
have disappeared. A missing unit definition is unverifiable: bootstrap rejects
it before stopping the runner or running persistent tools.

Unknown or unverified units are left untouched; the runner-writable
`.service` marker is never authoritative. After a successful disable, a failure
before unit installation leaves the normal boot link removed. A successful
bootstrap enables the managed Jazz unit again. `systemctl disable` removes
normal enablement links; explicit starts and dependency-driven activation are
outside this guarantee.

Bootstrap installs, repairs, enables and starts a deterministic root-owned Jazz
`jazz-benchmark-runner-<sha256>.service` unit. It never executes runner
`svc.sh`.

During configuration, runner v2.337.0 may create `svc.sh`. Bootstrap temporarily
opens the package directory in mode `1770` to the runner's primary group. It
then moves the expected `svc.sh` out, restores root ownership and mode `0555`,
checks the pinned package manifest, and removes the temporary copy. If config
is interrupted, the exit cleanup removes `svc.sh` and reseals the directory.
Other package changes fail the post-config manifest check or the next bootstrap.

The Jazz unit follows the pinned v2.337.0 template with `KillMode=process`
intentionally omitted, so systemd's control-group default allows a full
cgroup-empty proof. Upstream legacy unit names join the repository slug and
runner name with `-` and `.`, so dots or hyphens can make them ambiguous.
Bootstrap mutates a legacy unit only when its name uniquely identifies the
requested repository and runner. It disables, stops and quiesces verified
upstream or prior-version units, including those under
`/opt/actions-runner/<version>`, then requires controlled reprovision without
migrating or restarting them. Ambiguous or otherwise unverified units are left
untouched.

`INSTALL_SSM_AGENT=0` explicitly disables AWS SSM installation on Hetzner. `auto` also skips SSM on non-AWS hosts; set `INSTALL_SSM_AGENT=1` only on AWS when installation via signed apt/snap repositories is intended, and then failure is fatal.

Use the checked-in bootstrap script from the repo:

```bash
read -rsp "GitHub registration token: " RUNNER_TOKEN
printf '\n'
export RUNNER_TOKEN
sudo --preserve-env=RUNNER_TOKEN env \
  RUNNER_URL="https://github.com/garden-co/jazz2" \
  RUNNER_USER=runner \
  RUNNER_NAME="benchmark-runner-hetzner" \
  RUNNER_LABELS="jazz-bench,hetzner" \
  INSTALL_SSM_AGENT=0 \
  dev/benchmarks/realistic/bootstrap_runner.sh
bootstrap_status=$?
unset RUNNER_TOKEN
exit "${bootstrap_status}"
```

The registration token is read silently and remains environment data, not an
argument. Bootstrap v2.337.0 passes it to `config.sh` using the runner's
supported `ACTIONS_RUNNER_INPUT_TOKEN` secret input, then removes the variable;
the token is not present in the configuration process's argv or failure logs.

After a successful bootstrap, it:

- creates the runner user if needed and installs Rust, Node, `pnpm`, `wasm-pack`, and `libclang`
- configures the runner and installs/starts its root-managed Jazz systemd unit
- skips AWS SSM installation on non-AWS hardware
- applies `dev/benchmarks/realistic/harden_runner.sh` after runner service start unless `SKIP_HARDENING=1`

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
systemctl list-units 'jazz-benchmark-runner-*.service'
```

Expected output on the current box:

- kernel `6.8.0-101-generic`
- SMT `off`
- online CPUs `0-5`
- governor `performance`
- boost `0`
- runner service `active`
