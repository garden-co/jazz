# AWS EC2 Self-Hosted Runner Setup

Use this for predictable benchmark runs with absolute-number logging.

## Why this setup

- Fixed machine type and fixed labels.
- One benchmark job at a time.
- Same toolchain versions every run.
- Benchmark artifacts captured for later delta rendering.

## 1. Create the EC2 instance

Recommended stable config:

- Instance type: `c7i.2xlarge` (avoid burstable `t*` types)
- OS: Ubuntu 24.04 LTS
- Disk: 150GB gp3
- Security group: no inbound rules, outbound internet allowed
- Access: SSM via `AmazonSSMManagedInstanceCore`

Keep this instance dedicated to benchmarks.

Tag all created resources so they are easy to distinguish from Pulumi-managed infrastructure:

- `ManagedBy=benchmark-runner`
- `Component=benchmark-runner`
- `Name=benchmark-runner-*`

## 2. Bootstrap the instance

The bootstrap pins Rust `1.93.1`, Node `24.13.0` (official Node.js tarball SHA-256 `6223aad1a81f9d1e7b682c59d12e2de233f7b4c37475cd40d1c89c42b737ffa8`), and Actions Runner `2.337.0` (SHA-256 `70920811a4f8ad4328818682bca5c6469c1c942fab52448868071d0063816613`). Rust is bootstrapped with rustup-init `1.28.2` (SHA-256 `20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c`); wasm-pack is pinned to `0.13.1`. Each downloaded binary archive is checked against its fixed digest before execution or extraction. Keep these pins and publisher hashes in sync with reviewed upstream releases; the script intentionally does not select `latest`.

Bootstrap fails closed: a failed hash check, required tool install, requested SSM
install/enable, runner configuration, or systemd action aborts setup. The runner
account must resolve to a non-root UID. Root-owned manifests cover the Node tree
and immutable runner-package payload (runtime links are excluded). Rust tool
binaries in `.cargo/bin` and the Rustup-managed `.rustup` tree are sealed with
root-owned manifests under `/var/lib/actions-runner/.toolchain-integrity`.
An empty runner `~/.cargo/bin` directory is not treated as an installed
toolchain. Bootstrap rechecks tool paths after all verified runners are
disabled, stopped and proven quiescent. If a job adds a tool during shutdown,
the recheck finds unmanifested state and aborts before executing it.
Before bootstrap executes persistent Rustup or wasm-pack, it enumerates manager
units and validates every matching runner unit, registration identity,
root-owned unit file, executable and drop-ins. It disables all exact verified
candidates before stopping any, then requires each to be inactive/dead with an
empty cgroup-v2 `cgroup.events`. A disable, stop or cgroup-proof failure aborts
before persistent tools run. If disabling fails, boot activation is uncertain.
If stopping or proving an empty cgroup fails, a runner may remain active during
the current boot; bootstrap does not claim that it stopped.

Unknown or unverified units are left untouched; `.service` files in runner
state are never authoritative. After a successful disable, a failure before
unit installation leaves the normal boot link removed. A successful bootstrap
enables the managed Jazz unit again. `systemctl disable` removes normal
enablement links; explicit starts and dependency-driven activation are outside
this guarantee. Missing, partial, unmanifested or modified state is rejected
rather than upgraded or overwritten. Mutable Cargo registry/cache data is
outside these manifests.

Bootstrap owns a deterministic `jazz-benchmark-runner-<sha256>.service` unit,
derived from the canonical GitHub URL and exact runner name. It installs and
repairs that root-owned unit and starts it through systemd; it never executes
runner-generated `svc.sh`.

During configuration, runner v2.337.0 may create `svc.sh`. Bootstrap temporarily
opens the package directory in mode `1770` to the runner's primary group. It
then moves the expected `svc.sh` out, restores root ownership and mode `0555`,
checks the pinned package manifest, and removes the temporary copy. If config
is interrupted, the exit cleanup removes `svc.sh` and reseals the directory.
Other package changes fail the post-config manifest check or the next bootstrap.

The Jazz unit follows the pinned v2.337.0 template with `KillMode=process`
intentionally omitted: systemd's default control-group mode lets bootstrap prove
the whole cgroup is empty. Upstream legacy unit names join the repository slug
and runner name with `-` and `.`, so dots or hyphens can make them ambiguous.
Bootstrap mutates a legacy unit only when its name uniquely identifies the
requested repository and runner. It disables, stops and quiesces verified
legacy units, then requires controlled reprovision without migrating or
restarting them. Ambiguous or otherwise unverifiable services are left
untouched and setup fails.

Registration tokens are read silently and passed as opaque environment data.
For runner v2.337.0, bootstrap exports the supported
`ACTIONS_RUNNER_INPUT_TOKEN` only within the configuration process group; setup
and cleanup commands do not inherit it. The token is never included in command-line
arguments and is removed from the parent shell after configuration starts.
Failure diagnostics suppress configuration output.

The private on-disk manifest is canonical JSON format 1. Its exact bytes are pinned by `bootstrap_runner_manifest_v1.json`; the verifier fails closed on unknown formats. An incompatible manifest change requires a format-version bump, corresponding verifier support, and a revised byte fixture. A complete pinned installation can be rerun without downloading tool archives. Registration tokens are treated as opaque data and omitted from failure diagnostics.

`INSTALL_SSM_AGENT=1` requests installation through Ubuntu's signed apt/snap repositories and is required to succeed. `INSTALL_SSM_AGENT=auto` (the default) installs it on detected AWS hardware and otherwise skips it; use `0` to disable it explicitly. Apt and snap repository trust remains the operating system's signed repository configuration.

Use the checked-in bootstrap script rather than copy-pasting one-off commands:

```bash
read -rsp "GitHub registration token: " RUNNER_TOKEN
printf '\n'
export RUNNER_TOKEN
sudo --preserve-env=RUNNER_TOKEN env \
  RUNNER_URL="https://github.com/garden-co/jazz2" \
  RUNNER_USER=ubuntu \
  INSTALL_SSM_AGENT=1 \
  dev/benchmarks/realistic/bootstrap_runner.sh
bootstrap_status=$?
unset RUNNER_TOKEN
exit "${bootstrap_status}"
```

This script handles the details that bit us on the first live setup:

- enables `corepack` as `root` so `pnpm` shims can be written under `/usr/bin`
- installs `libclang-dev` so RocksDB bindgen builds work without ad-hoc workflow `sudo`
- uses `/var/lib/cloud/data/instance-id` for naming, which works with IMDSv2 required
- creates/updates the root-owned Jazz systemd unit; `config.sh` runs first and
  any generated `svc.sh` is checked for its file type, executable bit and runner
  ownership, then discarded without execution. The remaining package payload
  is verified against the pinned manifest.
- applies `dev/benchmarks/realistic/harden_runner.sh` after systemd starts the runner unless `SKIP_HARDENING=1`

## 3. Register the GitHub runner

Create a repo registration token from GitHub:

- `Settings -> Actions -> Runners -> New self-hosted runner`
- or use the API/CLI and pass the token into `RUNNER_TOKEN`

Use these labels:

- `self-hosted`
- `linux`
- `x64`
- `jazz-bench`

## 4. Pin machine behavior for stability

Use the checked-in hardening script for the deterministic baseline:

```bash
sudo dev/benchmarks/realistic/harden_runner.sh
```

It currently does three concrete things:

- disables SMT in-guest when `/sys/devices/system/cpu/smt/control` is available
- disables noisy background services and timers (`irqbalance`, `snapd`, `fwupd`, `ModemManager`, `multipathd`, `udisks2`, `unattended-upgrades`, `cron`, apt timers)
- installs a one-shot `benchmark-runner-tuning.service` so SMT-off and performance governor are re-applied on boot

If you need a lighter-touch machine for debugging, skip this step or run bootstrap with `SKIP_HARDENING=1`.

You can still try the performance governor manually:

```bash
sudo cpupower frequency-set -g performance || true
```

Disable unattended package upgrades during benchmark windows.

## 5. Run workflow

Use `.github/workflows/benchmarks.yml`.

- Nightly and `main` push native benchmarks run automatically.
- PR benchmarks run only when the PR has the `benchmark` label.
- Browser benchmarks run when the workflow includes the browser job.

Artifacts include absolute JSON results plus machine/toolchain metadata.

## 6. Cost optimization options

Always-on is simplest and most stable. If you need lower cost:

- stop/start on schedule, but keep the same instance and same EBS volume
- still run only one benchmark at a time
- avoid changing instance type or AMI between runs
