# Packaged release acceptance preparation

This harness tests a **disposable local core** and installed packages from
an external project. It does not publish packages, provision Cloud, or test
installed devices. A passing local result is not complete release acceptance.
See [the release runbook](../RELEASE_RUNBOOK.md) for the remaining gates.

## Run core and persistent-client acceptance

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
  "nativeFingerprint": "<nativeArtifactFingerprint-from-verified-manifest>",
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
Use `phase: baseline` for historical mechanics checks; those may have older
package versions (the backend scenario runs `createJazz` with `remote()`, and
falls back to a backend-secret session only for baseline packages that predate
it). Final-preview/published runs require alpha56 manifests. The script checks
tarball hashes and compares every installed package file against extracted
tarball bytes. Unexpected files are rejected except nested dependency
directories; pin native payload packages as well. Before loading Jazz, every
verified package directory is checked to ensure its Jazz imports resolve to the
verified package directories. Unchecked nested Jazz copies are rejected,
including overrides below a dist directory. Linux additionally verifies the
selected native producer manifest's source, release profile, fingerprint and
binary digest. No same-version package substitution is accepted.

The core under test is that verified native payload: `core.mjs` loads the
installed `jazz-napi` and runs its `serveCore`, the production server shell a
`vercel()` Sandbox runs, with Node owning the signals. There is no separate
server binary to bind to a producer run; jazz-tools no longer ships one.

The output path must not already exist. The script generates private credentials,
starts the core with fresh persistent server storage, validates and deploys
both schema and permissions (twice), then starts separate child processes for:

1. Ordinary local-first CRUD: update and delete a specific row while another
   remains. Backend-authority control confirms default-deny read filtering.
2. Separate fresh readers compare complete Text and typed JSON at 65,536,
   65,537 and 800,000 serialized bytes.
3. Disconnected ordinary writer commits locally, independent connected reader
   confirms absence at authority, then client process closes without syncing.
4. The core stops. A new process restores the same retained account and persistent
   client store, reads the pending row, retained row and deletion while offline.
5. The core restarts with the same server data directory. Another new process
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
PASS: external JWT/negative auth, denied-write settlement, deterministic
in-flight cancellation, real old-wire-v2 client rejection with no mutation,
browser/worker, Android/iOS, Cloud version/digest pinning, sleep/wake and cleanup.
Session-close races do not prove in-flight cancellation.

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

## Mixed-version wire acceptance

`mixed-version.mjs` runs real `jazz-tools server` binaries and real Node
clients from two installed versions against each other (the runbook's
"current client vs candidate server, candidate client vs current server, and
the candidate pair"). Prepare two external projects, one with the published
release (`npm i jazz-tools@<current>`) and one with the candidate's packed
`jazz-tools`, `jazz-napi` and `jazz-wasm` tarballs (use npm `overrides` so the
candidate packages win over the registry). Pair each with its native server
binary. For alpha.59 or older it is
`node_modules/jazz-tools/bin/native/jazz-tools-linux-x64` (chmod +x). From
alpha.60 jazz-tools ships no server binary: build it from the version's tag with
`cargo build --release -p jazz-cli --bin jazz-tools`, the internal build Jazz
Cloud runs.

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
Optional keys: `only` (cell names; naming an opt-in cell runs it),
`skipLarge` (skip the 800KB value checks), `largeSizes`, `deadlineMinutes`.

Checks that only make sense for some version pairs are opt-in:

- `oversized: { "count": 4800, "size": 60000, "batch": 20, "readerMinutes": 15 }`:
  runs the `oversized-first-sync-*` cells, where a fresh whole-table subscriber's
  first snapshot exceeds the routed payload limit (#3520). These take a long time;
  raise `deadlineMinutes` (150 was enough for the alpha.57 run).

Every other cell runs by default. That includes the `rolling-upgrade-*` cells,
which restart the old server as the new version on the same store and port;
in-place upgrade applies to any version pair.

Each cell uses a fresh server store and deploys schema (mixed cells deploy with
the other version's CLI). It then drives two client processes through
global-tier insert/update/delete, remote point reads, subscriptions in both
directions, 800KB chunked values, disconnect/offline write/reconnect, a server
restart or in-place server upgrade on the same store with a write made while it
was down, and fresh clients. The `large-values-*` cells probe fresh subscribers
against tables holding large rows. The `exclusive-*` cells run exclusive
transactions that read a row by id or by query, update or upsert an existing
row, or only insert. They expect a conflict once another client changed the row
first, and from an old client on a new server, which cannot check its reads.

Unknown config keys and unknown cell names in `only` are rejected, a run where
no cell ran fails, and every skipped cell gets a `skip` entry with its reason.
To keep a known, tracked failure from turning the run red, list it in
`knownFailures` as `{"<cell>:<check>": "#NNNN"}`; it is recorded as
`known-fail` and does not affect the exit code. If the whole-run deadline
fires, a `deadline` failure and `results.json` are written before exit.

Results are written to `<output>/results.json`; set `JAZZ_MIXED_TRACE=1` for
per-command client traces and `RUST_LOG` for server logs.

The client driver and CLI calls assume the current API: `createJazzClient`
with a persistent driver, the `"global"` durability tier,
`jazz-tools server --bound-port-file --allow-local-first-auth` and
the `deploy-fixture.mjs` catalogue deploy. A release that renames any of these needs
matching edits to `mixed-version.mjs` and `mixed-version-client.mjs`.
