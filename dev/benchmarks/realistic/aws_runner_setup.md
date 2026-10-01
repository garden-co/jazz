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
install/enable, runner configuration, or service action stops setup before runner
configuration/service activation. Root-owned manifests cover the Node tree and
the runner package's immutable entries (runtime links are excluded). Rust tool
binaries in `.cargo/bin` and the Rustup-managed `.rustup` tree are sealed with
root-owned manifests under `/var/lib/actions-runner/.toolchain-integrity`;
bootstrap verifies both before executing persistent Rustup or wasm-pack. On a
rerun with an existing runner service, bootstrap stops the service before
executing persistent Rust tools, then verifies both manifests again to catch
changes made by an in-flight job. A stop or post-stop verification failure
aborts setup and leaves the runner service stopped; the service starts only
after bootstrap succeeds. Missing, partial, unmanifested, or modified state is
rejected rather than upgraded or overwritten. Mutable Cargo registry/cache
data is outside these manifests.

The private on-disk manifest is canonical JSON format 1. Its exact bytes are pinned by `bootstrap_runner_manifest_v1.json`; the verifier fails closed on unknown formats. An incompatible manifest change requires a format-version bump, corresponding verifier support, and a revised byte fixture. A complete pinned installation can be rerun without downloading tool archives. Registration tokens are passed as opaque data and are suppressed from failure diagnostics.

`INSTALL_SSM_AGENT=1` requests installation through Ubuntu's signed apt/snap repositories and is required to succeed. `INSTALL_SSM_AGENT=auto` (the default) installs it on detected AWS hardware and otherwise skips it; use `0` to disable it explicitly. Apt and snap repository trust remains the operating system's signed repository configuration.

Use the checked-in bootstrap script rather than copy-pasting one-off commands:

```bash
sudo RUNNER_TOKEN="<repo registration token>" \
  RUNNER_URL="https://github.com/garden-co/jazz2" \
  RUNNER_USER=ubuntu \
  INSTALL_SSM_AGENT=1 \
  dev/benchmarks/realistic/bootstrap_runner.sh
```

This script handles the details that bit us on the first live setup:

- enables `corepack` as `root` so `pnpm` shims can be written under `/usr/bin`
- installs `libclang-dev` so RocksDB bindgen builds work without ad-hoc workflow `sudo`
- uses `/var/lib/cloud/data/instance-id` for naming, which works with IMDSv2 required
- installs the GitHub runner service after `config.sh`, which is when current releases generate `svc.sh`
- applies `dev/benchmarks/realistic/harden_runner.sh` unless `SKIP_HARDENING=1`

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
