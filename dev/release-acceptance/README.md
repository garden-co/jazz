# Packaged release acceptance preparation

This harness tests a **disposable local CLI server** and installed packages from
an external project. It does not publish packages, provision Cloud, or test
installed devices. A passing local result is not complete release acceptance.
See [the release runbook](../RELEASE_RUNBOOK.md) for the remaining gates.

## Scaffold the candidate

Save a config outside the repository, replacing every placeholder:

```json
{
  "phase": "final-preview",
  "sourceSha": "<40-hex-versioned-candidate-sha>",
  "packageVersion": "2.0.0-alpha.56",
  "createJazzSpec": "https://pkg.pr.new/garden-co/jazz/create-jazz@<40-hex-versioned-candidate-sha>",
  "parent": "/absolute/new/external/parent",
  "name": "alpha56-starter",
  "starter": "react-localfirst"
}
```

Run `node dev/release-acceptance/scaffold.mjs /absolute/scaffold-config.json`.
This invokes the exact preview `create-jazz` executable with selfhosted hosting
and checks the resulting Jazz dependency pin. For a published release, set
`phase` to `published` and `createJazzSpec` to `create-jazz@2.0.0-alpha.56`;
that path invokes `npm create jazz@2.0.0-alpha.56`. It never requests a tenant.
The preview mode is the equivalent executable, not a claim to have exercised a
registry version that has not been published.

Inspect the scaffold's pins, install its exact candidate dependencies, and
capture installation logs/lockfile. Run its production build and browser
acceptance separately. The scaffold wrapper does not claim these steps passed.

## Run CLI and persistent-client acceptance

Install the packed candidate packages and matching native payloads in a fresh
external project. No workspace symlinks are allowed. Save config outside the
repository:

```json
{
  "phase": "final-preview",
  "sourceSha": "<40-hex-versioned-candidate-sha>",
  "packageVersion": "2.0.0-alpha.56",
  "previewRun": "<workflow-run-url-or-id>",
  "project": "/absolute/external/project",
  "cli": "/absolute/hash-verified/jazz-tools-linux-x64",
  "cliSha256": "<sha256>",
  "nativeFingerprint": "<nativeArtifactFingerprint-from-verified-manifest>",
  "cliArtifact": {
    "runId": 123456789,
    "artifactId": 987654321,
    "archivePath": "/absolute/original-cli-artifact.zip"
  },
  "output": "/absolute/new/private/receipt-directory",
  "packages": {
    "jazz-tools": {
      "tarball": "/absolute/jazz-tools.tgz",
      "sha256": "<sha256>"
    },
    "jazz-napi": { "tarball": "/absolute/jazz-napi.tgz", "sha256": "<sha256>" },
    "jazz-wasm": { "tarball": "/absolute/jazz-wasm.tgz", "sha256": "<sha256>" },
    "@garden-co/jazz-napi-linux-x64-gnu": {
      "tarball": "/absolute/native-payload.tgz",
      "sha256": "<sha256>"
    }
  }
}
```

Run `node dev/release-acceptance/run.mjs /absolute/config.json`.
The CLI must be executable. Use `phase: baseline` for historical mechanics
checks; those may have older package versions. Final-preview/published runs
require alpha56 manifests. The script checks CLI and tarball hashes and compares
every installed package file against extracted tarball bytes. Unexpected files
are rejected except nested dependency directories; pin native payload packages
as well. Before loading Jazz, every verified package directory is checked to
ensure its Jazz imports resolve to the verified package directories. Unchecked
nested Jazz copies are rejected, including overrides below a dist directory.
Linux additionally verifies the selected native producer manifest's
source, release profile, fingerprint and binary digest. No same-version package
substitution is accepted.

Final modes require `cliArtifact`: the successful producer run ID, CLI artifact
ID and **original** GitHub artifact ZIP. The runner uses `gh auth token` (or its
`GH_TOKEN` configuration) only in memory to fetch fresh metadata from the fixed
GitHub API repository `garden-co/jazz`. Caller-authored metadata is not accepted.
No token is written to a receipt or passed on a command line.

The source SHA must match both run and artifact metadata. Approved callers are
`preview-build.yml` for same-repository pull requests, and
`preview-jazz-tools-alpha-release.yml` or `publish-jazz-tools-alpha.yml` for
push/workflow_dispatch. The PR caller explicitly passes the event's head SHA to
the build checkout; the authenticated run/artifact head and repository identities
are authoritative. GitHub may return absent or empty `pull_requests` associations;
these do not invalidate an otherwise exact run. Nonempty associations must be
consistent. No lookup of the PR's current, possibly advanced head is performed.
A merge revision is not silently substituted for the candidate head. The standalone reusable
`build-jazz-packages.yml` has no independent run. `previewRun` must name this
actual producer run ID or URL, including when packages came from a reused run.

The API's `sha256:` artifact digest must match the original ZIP; it must contain
exactly the expected platform CLI binary. Those bytes must equal both the
configured executable and the already verified packed jazz-tools/bin/native
binary. This uses the current publisher's evidence and requires no new producer
manifest or workflow change. Save the original ZIP, for example:

```bash
# Root downloads the chosen artifact; do not replace it with a reconstructed ZIP.
gh api --hostname github.com "/repos/garden-co/jazz/actions/artifacts/$ARTIFACT_ID/zip" > "$ARCHIVE_PATH"
```

`gh run download` extracts files and cannot supply the original ZIP digest proof.
The runner requires `unzip`, bounds compressed/uncompressed sizes, and reads the
single entry without extracting archive paths. It saves the authenticated API
metadata and a sanitized digest/run/artifact summary in
`cli-artifact-provenance.json`. The console provenance record contains that
summary. Expired, failed, mismatched or unverifiable artifacts fail closed.
Baseline permits absent `cliArtifact`, explicitly logs that CLI producer
provenance was not verified, and makes no final source-binding claim. These
checks detect artifact substitution; they are not a hostile same-user filesystem
boundary.

The output path must not already exist. The script generates private credentials,
starts the real CLI with fresh persistent server storage, validates and deploys
both schema and permissions (twice), then starts separate child processes for:

1. Ordinary local-first CRUD: update and delete a specific row while another
   remains. Backend-authority control confirms default-deny read filtering.
2. Separate fresh readers compare complete Text and typed JSON at 65,536,
   65,537 and 800,000 serialized bytes.
3. Disconnected ordinary writer commits locally, independent connected reader
   confirms absence at authority, then client process closes without syncing.
4. CLI stops. A new process restores the same retained account and persistent
   client store, reads the pending row, retained row and deletion while offline.
5. CLI restarts with the same server data directory. Another new process
   reconnects; an independent fresh reader checks persisted data and exactly one
   pending row by ID.

Local state and credentials stay in the private output directory (0700/0600).
Do not publish raw logs or config. Generated fixture source remains in the
external project for review. Cleanup stops owned children but preserves all
stores and receipts. Commands run in owned POSIX process groups. SIGINT/SIGTERM
stop each group (including npm descendants), escalate to SIGKILL after 1.5 seconds,
and allow up to 1.5 further seconds to reap the direct child before exiting
130/143. Windows is rejected rather than claiming unsupported tree cleanup.
Each operation has a 20-second bound; child phases have a
120-second bound and the whole run 300 seconds. A timeout fails the run.

The file account store has exactly one process owner at a time; it is a synthetic
acceptance store, not a production multi-process credential-store implementation.
The process restart is graceful; this does not claim abrupt crash durability.

## Explicit remaining gates

The runner prints `NOT_RUN` for separate acceptance gates, even after local
PASS: scaffold, external JWT/negative auth, denied-write settlement, deterministic
in-flight cancellation, real old-wire-v2 client rejection with no mutation,
browser/worker, Android/iOS, Cloud version/digest pinning, sleep/wake and cleanup.
Attach the independent scaffold receipt when run; its absence is not inferred
from local CLI success. Session-close races do not prove in-flight cancellation.

Cloud needs a new disposable **preview** tenant only after infra supplies the
matching image. Root verifies tenant status version, immutable digest and preview
lifecycle, publishes schema AND permissions, configures an inline public JWT key
for a locally controlled issuer, runs backend/local-first/external-JWT CRUD and
negative auth, pending writes/reconnect/reopen/sleep/wake in both region paths,
then deletes the tenant. Root owns secrets and external writes. This local runner
intentionally accepts no external endpoint or Cloud credentials.

Installed RN device receipts require the matching platform payload and wrapper,
installed binary hash and fresh launch nonce. The existing device harness has
explicit reconnect coverage debt; a green offline SQLite receipt is not proof
of full network recovery. Do not replace device receipts with host/source tests.

## Focused harness contract tests

Run `node --test dev/release-acceptance/*.test.mjs`. Synthetic packed
packages and an npm stub exercise nested dependency rejection, exact preview
locators, and SIGINT/SIGTERM cleanup including TERM-resistant descendants. They
make no network requests and do not count as package or Cloud acceptance.
The CLI provenance tests inject API fixture responses and generated tiny ZIPs
to check source/workflow/run/digest/entry binding and executable/package equality.

## Mixed-version wire acceptance

`mixed-version.mjs` runs real `jazz-tools server` binaries and real Node
clients from two installed versions against each other (the runbook's
"current client vs candidate server, candidate client vs current server, and
the candidate pair"). Prepare two external projects, one with the published
release (`npm i jazz-tools@<current>`) and one with the candidate's packed
`jazz-tools`, `jazz-napi` and `jazz-wasm` tarballs (use npm `overrides` so the
candidate packages win over the registry). Pair each with its native CLI: the
published one is `node_modules/jazz-tools/bin/native/jazz-tools-linux-x64`
(chmod +x), the candidate is `cargo build --release -p jazz-cli --bin jazz-tools`.

```json
{
  "output": "/absolute/new/output-dir",
  "versions": {
    "old": {
      "project": "/abs/old-project",
      "cli": "/abs/old/jazz-tools-linux-x64"
    },
    "new": {
      "project": "/abs/new-project",
      "cli": "/abs/jazz/target/release/jazz-tools"
    }
  }
}
```

Run `node dev/release-acceptance/mixed-version.mjs /absolute/config.json`.
Optional keys: `only` (cell names), `skipLarge` (skip the 800KB value checks),
`largeSizes`. Each cell uses a fresh server store, deploys schema (mixed cells
deploy with the other version's CLI), and drives two client processes through
global-tier insert/update/delete, remote point reads, subscriptions in both
directions, 800KB chunked values, the legacy `"edge"` tier from old clients,
disconnect/offline write/reconnect, a server restart or in-place server
upgrade on the same store with a write made while it was down, and fresh
clients. The `large-values-*` cells probe fresh subscribers against tables
holding large rows; the `edge` cells check that retired server edges fail
explicitly. Results are written to `<output>/results.json`; set
`JAZZ_MIXED_TRACE=1` for per-command client traces and `RUST_LOG` for server
logs.
