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

Use the checked-in bootstrap script rather than copy-pasting one-off commands:

```bash
sudo RUNNER_TOKEN="<repo registration token>" \
  RUNNER_URL="https://github.com/garden-co/jazz2" \
  RUNNER_USER=ubuntu \
  benchmarks/realistic/bootstrap_runner.sh
```

This script handles the details that bit us on the first live setup:

- enables `corepack` as `root` so `pnpm` shims can be written under `/usr/bin`
- uses `/var/lib/cloud/data/instance-id` for naming, which works with IMDSv2 required
- installs the GitHub runner service after `config.sh`, which is when current releases generate `svc.sh`
- applies `benchmarks/realistic/harden_runner.sh` unless `SKIP_HARDENING=1`

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
sudo benchmarks/realistic/harden_runner.sh
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
