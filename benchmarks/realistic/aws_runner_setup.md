# AWS EC2 Self-Hosted Runner Setup (Minimal)

Use this for predictable benchmark runs with absolute-number logging.

## Why this setup

- Fixed machine type and fixed labels.
- One benchmark job at a time.
- Same toolchain versions every run.
- Benchmark artifacts captured for later delta rendering.

## 1. Create the EC2 instance

Recommended minimal stable config:

- Instance type: `c7i.2xlarge` (avoid burstable `t*` types)
- OS: Ubuntu 24.04 LTS
- Disk: 150GB gp3
- Security group: SSH from admin IP only, outbound internet allowed

Keep this instance dedicated to benchmarks.

## 2. Install system dependencies

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential curl git jq unzip ca-certificates pkg-config libssl-dev \
  xvfb libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libgbm1 \
  libasound2t64 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
  libxrandr2 libgtk-3-0 libpango-1.0-0 libcairo2 libatspi2.0-0
```

## 3. Install Rust + Node toolchains

```bash
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
rustup toolchain install stable
rustup target add wasm32-unknown-unknown

curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt-get install -y nodejs
corepack enable
```

Optional (faster browser bench builds):

```bash
cargo install wasm-pack --locked
```

## 4. Register the GitHub runner

In GitHub: `Settings -> Actions -> Runners -> New self-hosted runner` and follow generated commands.

When configuring labels, include:

- `self-hosted`
- `linux`
- `x64`
- `jazz-bench`

Install as a service:

```bash
sudo ./svc.sh install
sudo ./svc.sh start
```

## 5. Pin machine behavior for stability

Use performance governor:

```bash
sudo apt-get install -y linux-tools-common linux-tools-generic
sudo cpupower frequency-set -g performance || true
```

Disable unattended package upgrades during benchmark windows.

## 6. Run workflow

Use `.github/workflows/benchmarks.yml`.

- Nightly native benchmarks run automatically.
- Browser benchmark runs on manual dispatch when `include_browser=true`.

Artifacts include absolute JSON results plus machine/toolchain metadata.

## 7. Cost optimization options

Always-on is simplest and most stable. If you need lower cost:

- stop/start on schedule, but keep the same instance and same EBS volume
- still run only one benchmark at a time
- avoid changing instance type or AMI between runs
