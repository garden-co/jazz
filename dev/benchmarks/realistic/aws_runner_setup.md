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
  RUNNER_USER=ubuntu \
  INSTALL_SSM_AGENT=1 \
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
