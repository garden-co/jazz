# Blacksmith CI image experiment

This experiment runs the existing `lint`, `test-rust`, and `test-ts` workloads
on independent Blacksmith VMs. It does not change the required `CI` workflow or
the Latitude runner.

The image preinstalls the pinned Node, pnpm, Rust, WASM, test, and Playwright
toolchain. Every job still gets an isolated checkout, Cargo target directory,
Turbo working directory, sccache local cache, and sccache daemon socket.

Cross-VM reuse is explicit:

- sccache uses a small job-local disk cache backed by the isolated S3 prefix
  `jazz-ci/v1/experiments/blacksmith-v1`. The experiment branch assumes a role
  whose object permissions are limited to that prefix. A production
  workflow must use the read-only role for `pull_request` and reserve the write
  role for trusted pushes.
  Remote write failures are observable in sccache statistics but do not fail a
  correct build when the job-local cache remains healthy.
- Turbo remote caching is deliberately disabled until the experiment has a
  dedicated least-privilege Vercel Remote Cache token. The broader deployment
  token used by benchmark workflows must not be exposed to build subprocesses.
  Turbo stores content-addressed outputs and logs declared by `turbo.json`; it
  does not share mutable working directories.
- pnpm and Rust download/build caches use concurrency-safe GitHub Actions cache
  entries, transparently accelerated by Blacksmith.

The workflow is intentionally separate and manually dispatchable. Its image
tag and S3 key prefix are versioned so experiments cannot silently change the
cache contract used by a later production workflow.
