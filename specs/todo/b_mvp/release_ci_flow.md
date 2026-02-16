# Release CI Flow — TODO (MVP)

PR CI is now optimized for fast feedback, including debug-mode builds for some native artifacts.
MVP needs a dedicated release CI path that validates release artifacts before publish.

## Goals

- Keep PR CI fast and developer-friendly.
- Add a separate release-focused workflow that always uses release build settings.
- Catch packaging and artifact regressions before publish.

## MVP Scope

- Add a new workflow (`.github/workflows/release-ci.yml`) with at least:
  - `workflow_dispatch` trigger for manual verification.
  - tag-based trigger for release candidates/final tags (for example `v*`).
- Build release artifacts in clean CI:
  - `jazz-cli` via `cargo build --release`.
  - `groove-wasm` via `wasm-pack build --release`.
  - `jazz-napi` via `napi build --release` (explicitly bypass debug CI toggle).
- Run basic smoke checks against produced artifacts.
- Upload build artifacts (and optional checksums) for inspectability.

## Non-Goals (MVP)

- Full cross-platform release matrix/signing/notarization.
- End-to-end publish automation to npm/crates from this workflow.
- Extra browser E2E coverage (already covered by PR CI).

## Design Notes

- Keep `ci.yml` optimized for PR validation speed.
- Put all release-only correctness checks in `release-ci.yml`.
- Ensure publish workflows either:
  1. reuse artifacts produced by `release-ci`, or
  2. rerun equivalent release-build validations.

## Acceptance Criteria

- Release workflow is runnable manually from GitHub Actions.
- Release workflow runs automatically on release tags.
- Workflow produces release artifacts for CLI, WASM, and N-API.
- A broken release artifact build fails the workflow.
- PR CI remains unchanged in speed-oriented behavior.

## Follow-Ups

- Add cross-platform release matrix (Linux/macOS/Windows) once publishing targets are finalized.
- Add provenance/signing and checksum verification gates.
