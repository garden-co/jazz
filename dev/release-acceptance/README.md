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
  "cliProducer": {
    "path": "/absolute/producer-manifest.json",
    "sha256": "<manifest-sha256>",
    "artifact": "<CLI-artifact-file-in-manifest>"
  },
  "output": "/absolute/new/private/receipt-directory",
  "packages": {
    "jazz-tools": { "tarball": "/absolute/jazz-tools.tgz", "sha256": "<sha256>" },
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
as well. Linux additionally verifies the selected native producer manifest's
source, release profile, fingerprint and binary digest. No same-version package
substitution is accepted.

Final modes require `cliProducer`, pointing to an authoritative producer manifest
whose `git.head` is the candidate source and whose `artifacts` contains the CLI
`file` and `sha256`. Its own digest is pinned in the input. Obtain it from the
verified producer workflow, not a hand-authored reconstruction. If the publisher
provides no such CLI source/digest receipt, final mode intentionally fails closed;
resolve that provenance gap before final acceptance. Baseline permits its absence
and does not claim a verified CLI source binding. Archive workflow/manifests and
installation logs alongside private receipts. These checks detect accidental
artifact substitution; they are not a hostile same-user filesystem boundary.

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
stores and receipts. Each operation has a 20-second bound; child phases have a
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
