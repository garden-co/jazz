# Linux Node native packages

The GNU NAPI packages support **Linux x64 and ARM64 with glibc >= 2.34 and
libstdc++ providing GLIBCXX_3.4.29 / CXXABI_1.3.13**. This includes the AL2023
userspace used by Vercel builds and Debian Bookworm. Alpine/musl is not a GNU
NAPI target; the standalone CLI has a separate support matrix.

`dev/artifacts/linux-napi/build.sh linux-x64-gnu` (or `linux-arm64-gnu`) builds
inside the pinned AL2023 image on a host of the corresponding architecture.
The runner's own Ubuntu userspace is not linked into the binding. Image base
indexes, Rust and Node versions are pinned in `Dockerfile`; the AL2023 package
repository release is pinned by the base image. Compiler/libc versions, image
identity and recipe identity are recorded in the producer manifest.

The baseline intentionally uses GCC11: a bounded Bullseye/GCC10 build trial
failed on RocksDB11.1.1's C++20 `using enum`. Moving to Ubuntu22.04 would still
require glibc2.35, above AL2023's glibc2.34. Details and follow-up belong in
[#3048](https://github.com/garden-co/jazz/issues/3048) and
[#2675](https://github.com/garden-co/jazz/issues/2675).

Cargo caches live under `target/linux-napi-<recipe-sha256>-<architecture>` and
CI cache restore keys include that recipe and architecture. Never seed them
with native C/C++ products from a newer host baseline. Normal package staging
still verifies sealed binary hashes and the shared source fingerprint.

A hosted deployment receipt must distinguish the build from the running
function: import `jazz-tools/backend` during framework build, then invoke an
HTTP route that opens the native backend and writes/reads synthetic data.
Record `process.arch`, Node version and
`process.report.getReport().header.glibcVersionRuntime`. A persistence smoke
may use `/tmp` and reopen within the same invocation; this proves native
storage loading, not durable serverless storage across requests.
