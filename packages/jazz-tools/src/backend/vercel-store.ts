// The `vercel()` store: the app's core runs in a persistent Vercel Sandbox
// in the app's own Vercel project, with its data on a Drive. The backend
// creates the Sandbox on first use, wakes it when a request needs it and lets
// it stop when the app goes quiet. The function's own Vercel OIDC token is
// the only credential: the Sandbox SDK authenticates with it, and the core
// trusts exactly that workload (its issuer, audience and subject) and admits
// nothing else, so the app sets no Jazz env vars and no secret exists.
// Wake handling is adapted from garden-co/infra's v2 sync proxy (ADR 0004).

import { createHash, randomUUID } from "node:crypto";
import type { JazzCoreSocketEvent } from "jazz-napi";
import {
  connectedBlobStore,
  resolveVercelBlob,
  resolveVercelS3,
  vercelBlob,
  type S3BlobStore,
  type VercelBlob,
  type VercelBlobStore,
  type VercelS3,
} from "./blobs.js";
import {
  fetchFor,
  inProcessWebSocketFor,
  registerInProcessTransport,
} from "../runtime/in-process-transport.js";
import type {
  DeferredConnectWebSocket,
  WebSocketConstructor,
} from "../runtime/native-runtime/websocket.js";
import { backendNative } from "./backend-native.js";
import { optionalPeerImportError } from "./optional-peer.js";
import { logStoreOpened } from "./store-log.js";

export const VERCEL_CORE_PORT = 1625;

/** What a `vercel()` namespace function is called with, for each deployment. */
export interface VercelNamespaceContext {
  /**
   * The deployment's Vercel environment: `"production"`, `"preview"` or a
   * custom environment's slug (`VERCEL_TARGET_ENV`, else `VERCEL_ENV`).
   */
  environment: string;
  /** The deployment's Git branch (`VERCEL_GIT_COMMIT_REF`); `undefined` without one. */
  branch: string | undefined;
}

/**
 * A core's name within its Vercel project: its Sandbox is
 * `jazz-<project>-<namespace>`. A string names one core; a function picks
 * one per deployment.
 */
export type VercelNamespace = string | ((context: VercelNamespaceContext) => string | undefined);

export interface VercelStoreOptions {
  /**
   * Which core this deployment runs: its Sandbox is
   * `jazz-<VERCEL_PROJECT_ID>-<namespace>` (lowercase, characters other than
   * `a-z`, `0-9` and `-` become `-`). Required. The usual choices:
   *
   * - `({ environment }) => environment`: one core per environment
   *   (production, preview, each custom environment).
   * - `({ environment, branch }) => environment === "production" ? "production" : perBranch(branch)`:
   *   production's core, and one per preview branch, so a branch's schema
   *   and permissions never apply to another branch's data. These are the
   *   names earlier versions gave cores by default.
   *
   * A deployment only reaches cores of its own environment: a core trusts
   * the environment whose deployment started it first. So a plain string,
   * one core for every environment, serves only the environment that
   * deploys first, and the others are refused: give each environment a
   * namespace of its own, as the functions above do.
   */
  namespace: VercelNamespace;
  /** Replaced by `namespace`; passing it throws, saying how to migrate. */
  name?: never;
  /**
   * Not an option: the core generates its app id on its Drive at first
   * start and keeps it there.
   */
  appId?: never;
  /**
   * The Vercel region the core runs and keeps its data in: its Sandbox's
   * region and its Drive's. Required. Fixed when the core is created;
   * moving an existing core to another region isn't supported yet, so a
   * different `region` for an existing core throws. `"iad1"` is where
   * earlier versions created every core. Pick the region your functions run
   * in: a request from a function elsewhere crosses regions to the core.
   * See https://vercel.com/docs/sandbox/concepts/regions.
   */
  region: string;
  /**
   * Where large values' bytes go:
   * - `vercelBlob()`: the project's Blob store, written by the core
   *   through `/jazz`. The default whenever a Blob store is connected to
   *   the project (`BLOB_STORE_ID` is set).
   * - `s3()`: a bucket connected through the Vercel Marketplace.
   * - `"sqlite"`: the core's database file on its Drive, which grows with
   *   every large value. The default without a Blob store.
   *
   * A core may move from `"sqlite"` to either of the others, never back or
   * between them.
   */
  blobs?: VercelBlobStore | S3BlobStore | "sqlite";
  /** vCPUs for the core (2 GB of memory each). Defaults to 1. Fixed at creation. */
  vcpus?: number;
  /** Drive size in bytes. Defaults to 10 GiB. Fixed at creation. */
  maxDriveBytes?: number;
  /**
   * How long the core stays awake after its last request or socket.
   * Defaults to 5 minutes; a stopped core wakes in about a second.
   */
  idleMs?: number;
  /**
   * Where the core comes from: an npm tarball of
   * `@garden-co/jazz-napi-linux-x64-gnu`, the Node addon the Sandbox runs
   * it with. Defaults to the registry's tarball of the installed version
   * (for a release candidate, the tarball attached to its GitHub release);
   * set it for a build that is neither. A download that fails is reported
   * by the function.
   */
  coreTarballUrl?: string;
  /** Vercel credentials for scripts outside a Vercel function. Defaults to the function's OIDC token. */
  credentials?: { token: string; teamId: string; projectId: string };
  /**
   * The backend's credential to the core: a Vercel OIDC token of this
   * project and environment. Defaults to the function's own
   * (`getVercelOidcToken()` from `@vercel/functions/oidc`); pass one for
   * scripts outside a Vercel function.
   */
  backendCredential?: () => string | Promise<string>;
  /** @internal A stand-in for `@vercel/sandbox`, for tests. */
  sdk?: SandboxSdk;
}

/** What the store needs of `@vercel/sandbox` (injected in tests). */
export interface SandboxSdk {
  Sandbox: {
    get(params: Record<string, unknown>): Promise<SandboxHandle>;
    create(params: Record<string, unknown>): Promise<SandboxHandle>;
  };
  Drive: {
    get(params: Record<string, unknown>): Promise<{ readonly region: string }>;
    getOrCreate(params: Record<string, unknown>): Promise<{ readonly region: string }>;
  };
}

export interface SandboxHandle {
  readonly status: string;
  /** The region the Sandbox is configured to run in. */
  readonly region: string;
  domain(port: number): string;
  currentSession(): {
    startedAt: Date;
    timeout: number;
    extendTimeout(ms: number): Promise<void>;
  };
  /** A detached command resolves once it started; any other once it finished. */
  runCommand(params: {
    cmd: string;
    args: string[];
    detached?: boolean;
  }): Promise<SandboxCommandResult | void>;
  writeFiles(files: { path: string; content: Buffer }[]): Promise<void>;
  readFileToBuffer(file: { path: string }): Promise<Buffer | null>;
  stop(): Promise<void>;
}

/** What a command run in the Sandbox (`@vercel/sandbox`'s `CommandFinished`) reports. */
export interface SandboxCommandResult {
  readonly exitCode: number | null;
  stdout(): Promise<string>;
}

/** The shape `createJazz` drives a core through (see `Core` in create-jazz.ts). */
export interface VercelCore {
  /** The app id the core runs as, kept on its Drive. */
  readonly appId: string;
  readonly serverUrl: string;
  readonly adminSecret: string;
  readonly backendSecret: string;
  /** `access` is what the backend states next to its credential (see `Core`). */
  handle(
    method: string,
    path: string,
    headers: string[][],
    body: Uint8Array,
    access?: "admin" | "backend" | "session",
  ): Promise<{ status: number; headers: string[][]; body: Uint8Array }>;
  openSocket(
    headers: string[][],
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
    access?: "admin" | "backend" | "session",
  ): {
    send(data: Uint8Array): boolean;
    close(): void;
    /** Resolves once the session reaches the core (see `CoreSocket.opened`). */
    readonly opened?: Promise<void>;
  };
  close(): Promise<void>;
}

/** A core reached at `url` with this backend credential: `remote()`'s core. */
export type OpenRemote = (target: {
  appId: string;
  url: string;
  backendCredential: () => string | Promise<string>;
}) => VercelCore;

/** The workload a core trusts: the claims of the function's own OIDC token. */
export interface TrustedWorkload {
  issuer: string;
  audience: string;
  subject: string;
}

const since = (t0: number) => Math.round(performance.now() - t0);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const DEFAULT_IDLE_MS = 5 * 60_000;
const WAKE_DEADLINE_MS = 60_000;
// A session reported as running may be seconds from its timeout; reuse it
// only if it can be extended.
const MIN_LEFT_MS = 30_000;
// Vercel's router can answer SANDBOX_NOT_LISTENING for minutes while the
// server listens; a fresh session routes, so a session is recycled once its
// core has listened inside the Sandbox for this long while the router said
// it doesn't. A core still opening its store answers the same from outside,
// for as long as the open takes (tens of seconds for a large store), so the
// router's answer alone never recycles a session (#6366).
const NOT_LISTENING_RECYCLE_MS = 12_000;
// How long a wake is trusted before the next request checks again.
const ENSURE_FRESH_MS = 10_000;
/**
 * Vercel's router sets this on an answer it gave itself (the Sandbox isn't
 * listening, or is stopping): the request never reached the core.
 */
const ROUTER_ERROR_HEADER = "x-vercel-error";
/** A wake's log names this step when it found the core still opening its store. */
const OPENING_STEP = "waited for its core to open its store";
const KEEPALIVE_MS = 30_000;
// A session that answers `/health` while connections to its core keep
// ending without opening (none opening in between), for this long and this
// many times, is stopped, so the next wake starts a fresh one. A core that
// is restarting opens connections again well within this.
const UNREACHED_RECYCLE_MS = 20_000;
const UNREACHED_RECYCLE_COUNT = 2;

// After a start that can't succeed, requests fail fast for this long
// instead of each waking the Sandbox into the same failure.
const START_FAILURE_HOLD_MS = 30_000;

/** What the core in a Sandbox is doing, from `coreStateScript` (`unknown`: the check failed). */
type CoreState =
  | { kind: "listening" | "opening"; upSeconds: number }
  | { kind: "starting" | "down" }
  | { kind: "unknown"; why: string };

/** A core start that gave up (the start script said why); retrying won't help. */
class CoreStartError extends Error {}

const isWakeConflict = (error: unknown) => /\b409\b|already attached|conflict/i.test(String(error));

/**
 * Whether a lookup (`Sandbox.get`, `Drive.get`) failed because nothing has
 * that name: `@vercel/sandbox`'s `APIError` with a 404 response. Any other
 * failure (a 5xx, the network) says nothing about whether it exists.
 */
export const isNotFound = (error: unknown): boolean => {
  const status = (error as { response?: { status?: unknown } })?.response?.status;
  if (typeof status === "number") return status === 404;
  return /\b404\b/.test(String((error as { message?: unknown })?.message ?? error));
};

/**
 * A lookup that failed for another reason than "not found": retried until
 * the wake's deadline, never read as a missing core (which would create one).
 */
class LookupError extends Error {}

/**
 * Whether `Sandbox.create` failed because a Sandbox of that name exists:
 * another instance won the race to create it. `@vercel/sandbox` throws an
 * `APIError` whose `response` is the API's answer and whose message carries
 * the API's own ("Status code 4xx is not ok: A sandbox with the name '…'
 * already exists for this project"); the status may be 409 or a 4xx the
 * API uses for invalid requests, so the API's message decides.
 */
const isNameTaken = (error: unknown) => {
  const e = error as { response?: { status?: unknown }; json?: { error?: { message?: unknown } } };
  const status = typeof e?.response?.status === "number" ? e.response.status : undefined;
  if (status !== undefined && (status < 400 || status >= 500)) return false;
  const said = [String(error), String(e?.json?.error?.message ?? "")].join(" ");
  return /already exists/i.test(said);
};

async function healthy(base: string, timeoutMs: number, diag: Record<string, number>) {
  const deadline = performance.now() + timeoutMs;
  let pause = 50;
  while (performance.now() < deadline) {
    let why: string;
    try {
      const response = await fetch(`${base}/health`, {
        signal: AbortSignal.timeout(2000),
      });
      if (response.ok) return true;
      const routerError = response.headers.get("x-vercel-error");
      why = `http ${response.status}${routerError ? ` ${routerError}` : ""}`;
    } catch (error) {
      const e = error as { name?: string; cause?: { code?: string } };
      why = e.name === "TimeoutError" ? "timeout" : (e.cause?.code ?? e.name ?? "error");
    }
    diag[why] = (diag[why] ?? 0) + 1;
    await sleep(pause);
    pause = Math.min(pause * 2, 500);
  }
  return false;
}

/**
 * Whether version `a` is newer than `b`. A `-` starts a prerelease of what
 * precedes it, at any depth, so `2.0.0-alpha.64-rc.1` is a candidate for
 * `2.0.0-alpha.64` and sorts before it, as `2.0.0-alpha.64` does before
 * `2.0.0`. Dot-separated parts compare numerically when both are numbers;
 * a number sorts before a word. The start script's `newer` orders the same
 * way (GNU `sort -V` with every `-` as `~`).
 */
export function newerVersion(a: string, b: string): boolean {
  return compareVersions(a, b) > 0;
}

const PRERELEASE = Symbol("prerelease");
type VersionPart = number | string | typeof PRERELEASE;

function versionParts(version: string): VersionPart[] {
  const parts: VersionPart[] = [];
  version.split("-").forEach((segment, i) => {
    if (i > 0) parts.push(PRERELEASE);
    for (const part of segment.split(".")) {
      parts.push(/^\d+$/.test(part) ? Number(part) : part);
    }
  });
  return parts;
}

function compareVersions(a: string, b: string): number {
  const x = versionParts(a);
  const y = versionParts(b);
  for (let i = 0; ; i++) {
    const p = x[i];
    const q = y[i];
    if (p === undefined && q === undefined) return 0;
    // One ended: a prerelease marker on the other sorts before the end
    // (`alpha.64-rc.1` < `alpha.64`); any other part sorts after it.
    if (p === undefined) return q === PRERELEASE ? 1 : -1;
    if (q === undefined) return p === PRERELEASE ? -1 : 1;
    if (p === q) continue;
    if (p === PRERELEASE) return -1;
    if (q === PRERELEASE) return 1;
    if (typeof p === "number" && typeof q === "number") return p > q ? 1 : -1;
    if (typeof p === "number") return -1;
    if (typeof q === "number") return 1;
    return p > q ? 1 : -1;
  }
}

/**
 * Writes the tarball at argv[1] to stdout (the Sandbox image has node but
 * no curl). On failure it says why on stderr, and writes to the file at
 * argv[2] whether retrying can help: a 4xx is permanent, except 408, 425
 * and 429; those, a 5xx and a network error are transient.
 *
 * @internal
 */
export const DOWNLOAD_CORE =
  "const [url, kindFile] = process.argv.slice(1);" +
  " const fail = (kind, why) => { require('node:fs').writeFileSync(kindFile, kind); console.error(why); process.exit(1); };" +
  " fetch(url).then(async (r) => {" +
  " if (!r.ok) return fail(r.status >= 400 && r.status < 500 && ![408, 425, 429].includes(r.status) ? 'permanent' : 'transient', 'HTTP ' + r.status);" +
  " process.stdout.write(Buffer.from(await r.arrayBuffer()));" +
  " }).catch((e) => fail('transient', e.message + (e.cause ? ': ' + (e.cause.code || e.cause.message) : '')));";

/** `s` as one single-quoted shell word. */
const shellQuote = (s: string) => `'${s.replaceAll("'", `'\\''`)}'`;

/** The addon's file in its npm tarball. */
const CORE_ADDON = "package/jazz-napi.linux-x64-gnu.node";

/**
 * Runs the core in the Sandbox's Node: `node -e` with the addon, the app id
 * and the core's options, in that order. Node owns the process's signals: a
 * SIGTERM asks the core to drain, and the process exits once it has. The
 * handlers are in place before the core starts opening its store, and each
 * signal is logged with how long after the launch it came, so a stop while
 * the core still opens (it drains once open) shows in /data/server.log. The
 * addon's path stays in the process's command line, which is how a later
 * start recognises it.
 *
 * @internal
 */
export const CORE_LAUNCHER =
  "const [addon, appId, options] = process.argv.slice(1);" +
  " let core; let stopping = false;" +
  " for (const signal of ['SIGTERM', 'SIGINT']) process.once(signal, () => {" +
  " console.error(new Date().toISOString() + ' jazz: ' + signal + ' ' + Math.round(process.uptime()) + ' s after the core launched; asking it to stop (a core still opening its store stops once it has opened)');" +
  " stopping = true; core?.stop(); });" +
  " core = require(addon).serveCore({ ...JSON.parse(options), appId });" +
  " if (stopping) core.stop();" +
  " core.closed().then(() => process.exit(0), (error) => { console.error(error); process.exit(1); });";

/**
 * What the core in a Sandbox is doing, as seen from inside it: one line,
 *
 * - `listening <s>`: its process runs (the pid file names a process running
 *   one of our cores, as the start script checks) and its port accepts
 *   connections on localhost, `<s>` seconds after it launched;
 * - `opening <s>`: its process runs but doesn't listen yet (it is opening
 *   its store);
 * - `starting`: no core runs, but a start holds the start lock (it is
 *   downloading the core, or stopping another version's);
 * - `down`: nothing runs or starts.
 *
 * @internal
 */
export function coreStateScript(
  /** @internal Prefix for /data and /tmp, for tests. */
  root = "",
  port = VERCEL_CORE_PORT,
): string {
  return `set -uo pipefail
data=${shellQuote(`${root}/data`)}
run=${shellQuote(`${root}/tmp`)}
pid=$(cat "$run/jazz.pid" 2>/dev/null || true)
cmdline=$(tr '\\0' ' ' 2>/dev/null < "/proc/$pid/cmdline" || true)
if [ -n "$pid" ] && [[ "$cmdline" == *"$data/bin/jazz-napi-"* ]]; then
  launched=$(cat "$run/jazz.launched" 2>/dev/null || true)
  age=$(( $(date +%s) - \${launched:-$(date +%s)} ))
  if timeout 2 bash -c 'exec 3<>/dev/tcp/127.0.0.1/${port}' 2>/dev/null; then
    echo "listening $age"
  else
    echo "opening $age"
  fi
elif ! flock -n "$run/jazz.lock" true 2>/dev/null; then
  echo starting
else
  echo down
fi
`;
}

/**
 * Claims the recycle of the session started at `$1` (its start in
 * milliseconds): exits 0 for the first caller only, across every waiter and
 * function instance, since all of them reach the same Sandbox. Claims of
 * earlier sessions (a resumed snapshot keeps /tmp) are dropped.
 *
 * The claim is a file bash creates under `noclobber` (`set -C`), which opens
 * it with O_CREAT|O_EXCL: exactly one creator wins, whatever coreutils the
 * image ships. `mkdir` isn't such a claim everywhere: uutils' mkdir (the
 * default coreutils since Ubuntu 25.10) exits 0 when it loses the race to a
 * concurrent mkdir of the same path. Claims of releases that used a
 * directory (still empty) are dropped by `find -delete` just the same.
 *
 * @internal
 */
export function claimRecycleScript(
  /** @internal Prefix for /tmp, for tests. */
  root = "",
): string {
  return `set -u
run=${shellQuote(`${root}/tmp`)}
find "$run" -maxdepth 1 -name 'jazz-recycled-*' ! -name "jazz-recycled-$1" -delete 2>/dev/null
set -C
{ : > "$run/jazz-recycled-$1"; } 2>/dev/null
`;
}

/**
 * Gives back the claim on the recycle of the session started at `$1`, after
 * a stop that failed, so another waiter can try.
 *
 * @internal
 */
export function releaseRecycleScript(
  /** @internal Prefix for /tmp, for tests. */
  root = "",
): string {
  return `set -u
rm -f ${shellQuote(`${root}/tmp`)}/"jazz-recycled-$1" 2>/dev/null || true
`;
}

/**
 * Start the core: install the core addon for `version` on the Drive,
 * generate the app id once, and run the newest core version
 * this Drive has seen, so an instance from an older deployment never
 * downgrades data a newer one already wrote. A different running version is
 * stopped first; the same one is left alone.
 */
export function startScript(
  version: string,
  tarballUrl: string,
  trust: TrustedWorkload,
  blobs?: VercelS3 | VercelBlob,
  /** @internal Prefix for /data and /tmp, for tests. */
  root = "",
): string {
  const trustLine = `${trust.issuer} ${trust.audience} ${trust.subject}`;
  return `#!/bin/bash
# Written by jazz-tools' vercel() store for each server start.
set -euo pipefail
# The function names each start by its script's file name, and trusts only
# an error file that names one of its own starts (never a stale one).
start_id=$(basename "$0" .sh)
rm -f "$0"
data=${shellQuote(`${root}/data`)}
run=${shellQuote(`${root}/tmp`)}
# Starts are serialized; the server itself runs without the lock.
exec 9>"$run/jazz.lock"
flock 9
# Version order: GNU sort -V with every "-" as "~" sorts a prerelease
# before what it is a prerelease of, at any depth (alpha.64-rc.1 before
# alpha.64 before the release), as newerVersion does in TypeScript.
newer() {
  [ "$1" != "$2" ] && [ "$(printf '%s\\n%s\\n' "\${1//-/\\~}" "\${2//-/\\~}" | sort -V | tail -1)" = "\${1//-/\\~}" ]
}
version=${shellQuote(version)}
trust=${shellQuote(trustLine)}
# A start that gives up says why in this version's error file, which the
# function reads and reports instead of waiting for a server that won't
# come: "<start id> permanent: …" when retrying can't help (the function then stops
# the Sandbox), "<start id> transient: …" when a later start may succeed.
error_file="$run/jazz-$version.error"
rm -f "$error_file"
fail() {
  echo "jazz: $2" >&2
  printf '%s %s: %s\\n' "$start_id" "$1" "$2" > "$error_file"
  exit 1
}
trap 'fail transient "the start script failed at line $LINENO"' ERR
# The first start pins the workload this core trusts; a start for another
# workload never retargets it.
pinned=$(cat "$data/trust" 2>/dev/null || true)
if [ -n "$pinned" ] && [ "$pinned" != "$trust" ]; then
  fail permanent "this core trusts $pinned; not retargeting it to $trust (to re-pin after a rename, delete $data/trust)"
fi
[ -n "$pinned" ] || { printf '%s\\n' "$trust" > "$data/trust.tmp" && mv "$data/trust.tmp" "$data/trust"; }
highest=$(cat "$data/version" 2>/dev/null || true)
if [ -n "$highest" ] && newer "$highest" "$version"; then
  # A newer deployment owns this Drive: run its core, never ours.
  if [ ! -s "$data/bin/jazz-napi-$highest.node" ]; then
    fail permanent "$highest owns this Drive but its core is missing; not starting $version"
  fi
  version=$highest
fi
addon=$data/bin/jazz-napi-$version.node
# Fetch before stopping anything, so a failed download leaves the old core up.
if [ ! -s "$addon" ]; then
  mkdir -p "$data/bin"
  url=${shellQuote(tarballUrl)}
  rm -f "$run/download.kind"
  if ! node -e ${shellQuote(DOWNLOAD_CORE)} "$url" "$run/download.kind" 2>"$run/download.err" \\
    | tar xz -O ${CORE_ADDON} 2>>"$run/download.err" > "$addon.tmp"; then
    rm -f "$addon.tmp"
    # No kind: the download arrived but isn't the core's tarball.
    fail "$(cat "$run/download.kind" 2>/dev/null || echo permanent)" "could not download the core for $version from $url ($(head -c 300 "$run/download.err" | tr '\\n' ' ')); set vercel({ coreTarballUrl }) to a tarball of @garden-co/jazz-napi-linux-x64-gnu for this version"
  fi
  [ -s "$addon.tmp" ] || fail permanent "the core download for $version from $url was empty"
  mv "$addon.tmp" "$addon"
fi
# The pid file may be stale (a resumed snapshot): trust it only if that
# process runs one of our cores. Its stderr is silenced before the input
# redirect, which fails for a missing pid file or a process that is gone.
pid=$(cat "$run/jazz.pid" 2>/dev/null || true)
cmdline=$(tr '\\0' ' ' 2>/dev/null < "/proc/$pid/cmdline" || true)
if [ -n "$pid" ] && [[ "$cmdline" == *"$data/bin/jazz-napi-"* ]]; then
  [ "$(cat "$run/jazz.version" 2>/dev/null)" = "$version" ] && exit 0
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 150); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
  kill -KILL "$pid" 2>/dev/null || true
fi
if [ ! -s "$data/app-id" ]; then
  app_id=$(cat /proc/sys/kernel/random/uuid)
  printf '%s\\n' "$app_id" > "$data/app-id.tmp" && mv "$data/app-id.tmp" "$data/app-id"
fi
app_id=$(cat "$data/app-id")
export NODE_ENV=production
mkdir -p "$data/store"
printf '%s\\n' "$version" > "$data/version.tmp" && mv "$data/version.tmp" "$data/version"
# Older cores are never started again (see above).
find "$data/bin" -name 'jazz-napi-*' ! -name "jazz-napi-$version.node" -delete
printf '%s\\n' "$version" > "$run/jazz.version"
echo $$ > "$run/jazz.pid"
# When the core launched, for coreStateScript: a large store takes a while to open.
date +%s > "$run/jazz.launched"
exec 9>&-
# The core admits only the deploying function's own workload.${blobsSetup(blobs)}
exec node -e ${shellQuote(CORE_LAUNCHER)} "$addon" "$app_id" ${shellQuote(
    JSON.stringify(coreOptions(`${root}/data/store`, trust, blobs)),
  )}
`;
}

/**
 * The environment a core with `s3()` starts with: it assumes the
 * Marketplace's role with the function's OIDC token, which every request
 * carries. With `vercelBlob()` that token is the Blob credential itself.
 */
function blobsSetup(blobs: VercelS3 | VercelBlob | undefined): string {
  if (!blobs) {
    return "\n# Large values stay in its SQLite file (they can move to Vercel Blob or S3 later).";
  }
  if ("storeId" in blobs) {
    return "\n# Large values go to the project's Blob store, with the function's own OIDC token.";
  }
  return `
# Large values go to S3, under the role this project's functions assume.
export S3_BUCKET_NAME=${shellQuote(blobs.bucket)} AWS_REGION=${shellQuote(blobs.region)} AWS_ROLE_ARN=${shellQuote(blobs.roleArn)}`;
}

/**
 * The Vercel environment (`production`, `preview` or a custom one's slug)
 * whose functions this core trusts, from the token's subject.
 */
export function environmentOf(trust: TrustedWorkload): string | undefined {
  return /:environment:([^:]+)$/.exec(trust.subject)?.[1];
}

/** The core's options, but its app id, which the Drive keeps. */
function coreOptions(
  dataDir: string,
  trust: TrustedWorkload,
  blobs: VercelS3 | VercelBlob | undefined,
): Record<string, unknown> {
  // A bucket or Blob store is claimed for the core's environment: one
  // environment's OIDC token reaches the whole store, so preview and
  // production never share one.
  const environment = environmentOf(trust);
  const claim = environment === undefined ? {} : { environment };
  return {
    port: VERCEL_CORE_PORT,
    dataDir,
    blobs: !blobs
      ? { kind: "sqlite" }
      : "storeId" in blobs
        ? {
            kind: "vercel-blob",
            tokenFromWorkload: true,
            storeId: blobs.storeId,
            ...claim,
            ...(blobs.prefix === undefined ? {} : { prefix: blobs.prefix }),
          }
        : {
            kind: "s3",
            roleFromWorkload: true,
            ...claim,
            ...(blobs.endpoint === undefined ? {} : { endpoint: blobs.endpoint }),
            ...(blobs.prefix === undefined ? {} : { prefix: blobs.prefix }),
          },
    trustedWorkload: { issuer: trust.issuer, audience: trust.audience, subject: trust.subject },
  };
}

/**
 * Said once per process: a Next.js server bundles each route on its own, so
 * a module-level flag would say it once per route that opens Jazz.
 */
const WARNED_NO_BLOB_STORE = Symbol.for("jazz-tools.vercel.warnedNoBlobStore");
const warnedOnce = globalThis as { [WARNED_NO_BLOB_STORE]?: true };

/**
 * Where a `vercel()` core keeps large values: as asked, or by default the
 * project's Blob store when one is connected, and its SQLite file when not.
 */
export function resolveCoreBlobs(
  options: Pick<VercelStoreOptions, "blobs" | "region">,
  env: Record<string, string | undefined>,
  warn: (message: string) => void = console.warn,
): VercelS3 | VercelBlob | undefined {
  const blobs = options.blobs ?? (connectedBlobStore(env) ? vercelBlob() : undefined);
  if (blobs === undefined && !warnedOnce[WARNED_NO_BLOB_STORE]) {
    warnedOnce[WARNED_NO_BLOB_STORE] = true;
    warn(
      "vercel(): no Blob store is connected to this project, so large values stay in the core's SQLite file. Connect one (Storage tab in the Vercel dashboard) and they go there from then on.",
    );
  }
  if (blobs === undefined || blobs === "sqlite") return undefined;
  if (blobs.kind === "vercel-blob") return resolveVercelBlob(blobs, env);
  return resolveVercelS3(blobs, options.region, env, warn);
}

/**
 * Refuse a Blob store that another environment claimed, so a preview
 * deployment given production's store (an API- or CLI-created store is
 * connected to every environment) fails at start and says why. The core
 * checks the same claim, but only at its first large value, since no token
 * reaches it before a request does, and then it only retries in the
 * background while writes are acknowledged. A store nobody claimed yet, or
 * a read that fails, is left to the core's own check.
 */
export async function refuseOtherEnvironmentsBlobStore(
  storeId: string,
  environment: string | undefined,
  token: string,
  fetchImpl: typeof fetch = fetch,
): Promise<void> {
  if (environment === undefined) return;
  const id = storeId.replace(/^store_/, "").toLowerCase();
  const response = await fetchImpl(
    `https://${id}.private.blob.vercel-storage.com/${BLOB_ENVIRONMENT_MARKER}?cache=0`,
    { headers: { authorization: `Bearer ${token}` } },
  ).catch(() => undefined);
  if (!response?.ok) return;
  const claimed = (await response.text().catch(() => "")).trim();
  if (claimed && claimed !== environment) {
    throw new Error(
      `vercel(): Blob store ${storeId} serves the ${claimed} environment, not ${environment}; ` +
        "connect a separate Blob store to each environment (a store connected through the API or " +
        "CLI is connected to all of them)",
    );
  }
}

/** Where a core claims a bucket or Blob store for its environment (jazz-server's `ENVIRONMENT_OBJECT`). */
const BLOB_ENVIRONMENT_MARKER = ".jazz-environment";

/** The workload an OIDC token names: whoever holds such a token is trusted. */
export function workloadOf(token: string): TrustedWorkload {
  let claims: { iss?: unknown; aud?: unknown; sub?: unknown };
  try {
    claims = JSON.parse(Buffer.from(token.split(".")[1] ?? "", "base64url").toString("utf8"));
  } catch {
    throw new Error("vercel(): the backend credential is not an OIDC token");
  }
  const audience = Array.isArray(claims.aud) ? claims.aud[0] : claims.aud;
  if (
    typeof claims.iss !== "string" ||
    typeof audience !== "string" ||
    typeof claims.sub !== "string"
  ) {
    throw new Error("vercel(): the backend credential's token has no iss, aud or sub");
  }
  return { issuer: claims.iss, audience, subject: claims.sub };
}

const expiry = (token: string): number => {
  try {
    const { exp } = JSON.parse(Buffer.from(token.split(".")[1] ?? "", "base64url").toString());
    return typeof exp === "number" ? exp * 1000 : 0;
  } catch {
    return 0;
  }
};
let latestToken: string | undefined;

/**
 * The function's OIDC token. Vercel hands a fresh one to each request; work
 * outside a request (reconnects, the first open at cold start) uses the
 * freshest one seen, or `VERCEL_OIDC_TOKEN`.
 */
async function functionOidcToken(): Promise<string> {
  let fetched: string | undefined;
  let failure: unknown;
  try {
    // A literal inside `try`, like `importSandboxSdk`'s.
    const { getVercelOidcToken } = (await backendNative().importPeer("@vercel/functions/oidc")) as {
      getVercelOidcToken(): Promise<string>;
    };
    fetched = await getVercelOidcToken();
  } catch (error) {
    failure = error;
    fetched = process.env.VERCEL_OIDC_TOKEN;
  }
  for (const token of [fetched, latestToken]) {
    if (token && (!latestToken || expiry(token) > expiry(latestToken))) latestToken = token;
  }
  if (latestToken && expiry(latestToken) > Date.now()) return latestToken;
  throw new Error(
    `vercel(): no current Vercel OIDC token (enable OIDC for the project, or pass backendCredential)` +
      (failure ? `: ${String(failure)}` : ""),
  );
}

/**
 * A deployment only ever starts or reaches its own environment's core.
 * Custom environments run with VERCEL_ENV=preview and their own slug in
 * VERCEL_TARGET_ENV and in the token's subject.
 */
export function refuseOtherEnvironment(
  trust: TrustedWorkload,
  env: Record<string, string | undefined>,
): void {
  const target = env.VERCEL_TARGET_ENV ?? env.VERCEL_ENV;
  if (target && !trust.subject.endsWith(`:environment:${target}`)) {
    throw new Error(
      `vercel(): this ${target} deployment's OIDC token is for ${trust.subject}; refusing to start its core`,
    );
  }
}

/** Whether this process runs in a Vercel deployment (not `vercel dev` or a pulled env). */
export function onVercelDeployment(env: Record<string, string | undefined> = process.env): boolean {
  return !!env.VERCEL && (env.VERCEL_ENV === "production" || env.VERCEL_ENV === "preview");
}

/**
 * Whether this process runs on Vercel's serverless hosts: no lasting disk,
 * sockets only through `upgradeWebSocket`. Only `vercel dev` (VERCEL_ENV
 * development) is a local machine; a VERCEL flag without VERCEL_ENV counts
 * as a host, the safer default for refusing local disk.
 */
export function onVercelHost(env: Record<string, string | undefined> = process.env): boolean {
  return !!env.VERCEL && env.VERCEL_ENV !== "development";
}

/** Vercel's limit on a Sandbox name. */
export const SANDBOX_NAME_MAX = 128;
/** Vercel's limit on a Drive name. */
export const DRIVE_NAME_MAX = 64;

// Project ids are mixed case with an underscore; names stay [a-z0-9-].
const safeProject = (project: string) => project.toLowerCase().replace(/[^a-z0-9]+/g, "-");
// Runs of other characters become one "-"; a "-" stays as it is, so
// `perBranch`'s names pass through unchanged.
const safeNamespace = (namespace: string) => namespace.toLowerCase().replace(/[^a-z0-9-]+/g, "-");
/** A project id as Vercel issues them (`prj_` and 28 characters), when the real one isn't known yet. */
const TYPICAL_PROJECT_ID = `prj_${"x".repeat(28)}`;

const PER_BRANCH_RECIPE =
  'namespace: ({ environment, branch }) => environment === "production" ? "production" : perBranch(branch)';

/** Why `vercel()` needs a namespace, with the usual choices to copy. */
export function missingNamespaceError(): Error {
  return new Error(
    "vercel(): pass `namespace`, which names this deployment's core (its Sandbox is jazz-<project>-<namespace>). The usual choices:\n" +
      '  vercel({ namespace: ({ environment }) => environment, region: "iad1" }) // one core per Vercel environment\n' +
      `  vercel({ ${PER_BRANCH_RECIPE}, region: "iad1" }) // production's core and one per preview branch, the cores earlier versions created (perBranch is exported by jazz-tools/backend)`,
  );
}

/** Vercel's list of the regions a Sandbox can run in. */
const SANDBOX_REGIONS_DOCS = "https://vercel.com/docs/sandbox/concepts/regions";

/** Why `vercel()` needs a region, with the one that keeps existing cores where they are. */
export function missingRegionError(): Error {
  return new Error(
    "vercel(): pass `region`, the Vercel region this deployment's core runs and keeps its data in (its Sandbox and its Drive, fixed when the core is created). " +
      "Earlier versions created every core in iad1; to keep existing cores where they are, pass:\n" +
      '  vercel({ namespace, region: "iad1" })\n' +
      `For a new app, pick the region your functions run in (see ${SANDBOX_REGIONS_DOCS}).`,
  );
}

/** Check `region` when `vercel()` is called, so `next dev` reports it too. */
export function checkRegionOption(options: { region?: unknown }): string {
  if (options.region === undefined) throw missingRegionError();
  if (typeof options.region !== "string" || !options.region) {
    throw new Error(
      `vercel(): region must be a Vercel region such as "iad1" (see ${SANDBOX_REGIONS_DOCS}); got ${JSON.stringify(options.region)}`,
    );
  }
  return options.region;
}

/**
 * An existing core (its Sandbox, or its Drive) is in `actual`, not the
 * configured region: refused, since nothing would move it.
 */
export function refuseRegionMove(what: string, actual: string, configured: string): void {
  if (actual === configured) return;
  throw new Error(
    `vercel(): ${what} is in ${actual}, but \`region\` is ${JSON.stringify(configured)}. ` +
      "Moving a core to another region isn't supported yet: its data would have to be copied (https://github.com/garden-co/jazz/issues/5441). " +
      `To keep using this core, pass \`region: ${JSON.stringify(actual)}\`.`,
  );
}

/** Said once per process, like the Blob store warning. */
const WARNED_FUNCTION_REGION = Symbol.for("jazz-tools.vercel.warnedFunctionRegion");
const warnedRegion = globalThis as { [WARNED_FUNCTION_REGION]?: true };

/**
 * Warn once per process when this function runs in another region than
 * its core: every request then crosses regions.
 */
export function warnFunctionRegion(
  core: string,
  region: string,
  env: Record<string, string | undefined>,
  warn: (message: string) => void = console.warn,
): void {
  const functionRegion = env.VERCEL_REGION;
  if (!functionRegion || functionRegion === region || warnedRegion[WARNED_FUNCTION_REGION]) return;
  warnedRegion[WARNED_FUNCTION_REGION] = true;
  warn(
    `vercel(): this function runs in ${functionRegion}, but its core ${core} is in ${region}, so every request to the core makes a round trip between the two regions. ` +
      `Run your functions in ${region} (the project's Function Region setting).`,
  );
}

/** `name` became `namespace`; refused with the migration rather than ignored. */
/** What happens to a core no namespace reaches, and where to ask for it back. */
const UNREACHABLE =
  "Its data stays on that Sandbox and its Drive, but this version opens another core: if you need it, open an issue at https://github.com/garden-co/jazz/issues/new and we'll add a way to reach it, or rename it for you.";

export function refuseVercelName(options: {
  name?: unknown;
  credentials?: { projectId?: string };
}): void {
  if (!("name" in options)) return;
  const defaults =
    "To keep the cores earlier versions created by default (production's and one per preview branch), pass " +
    `vercel({ ${PER_BRANCH_RECIPE}, region: "iad1" }) with perBranch from jazz-tools/backend.`;
  const project = options.credentials?.projectId ?? process.env.VERCEL_PROJECT_ID;
  const name = options.name;
  // The Sandbox a namespace names: "jazz-", the project id lowercased with
  // each run of other characters as "-", "-", and the namespace.
  const rule =
    "A core is now always the Sandbox jazz-<project>-<namespace>, where <project> is the project id lowercased with each run of characters other than a-z and 0-9 as one `-` (prj_AbC1 is prj-abc1).";
  let keep: string;
  if (typeof name !== "string") {
    keep = `${rule} A Sandbox you named \`jazz-<project>-<rest>\` is \`namespace: "<rest>"\`.`;
  } else if (!project) {
    keep = `${rule} If ${JSON.stringify(name)} is jazz-<project>-<rest> for this project, pass \`namespace: "<rest>"\`; a name of any other form can't be reached by a namespace any more. ${UNREACHABLE}`;
  } else {
    const prefix = `jazz-${safeProject(project)}-`;
    const rest = name.startsWith(prefix) ? name.slice(prefix.length) : undefined;
    keep =
      rest && safeNamespace(rest) === rest && /[a-z0-9]/.test(rest)
        ? `To keep the core ${name}, pass \`namespace: ${JSON.stringify(rest)}\`.`
        : `${rule} The core ${name} is not of that form (for this project it would start with ${prefix}), so no namespace reaches it any more. ${UNREACHABLE}`;
  }
  throw new Error(`vercel(): \`name\` is now \`namespace\`. ${keep} ${defaults}`);
}

/**
 * The namespace earlier versions gave a preview branch's core by default:
 * `preview-<slug>-<hash>`, the branch's first 24 safe characters and a hash
 * of its full name, so `feat/a` and `feat-a` get cores of their own. For a
 * deployment without a branch (from the CLI outside a Git checkout, or
 * through the API) it is `""`, which `vercel()` refuses: those deployments
 * would all share one core, so give them a namespace of their own.
 */
export function perBranch(branch: string | undefined): string {
  if (!branch) return "";
  const slug = branch
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 24);
  const hash = createHash("sha256").update(branch).digest("hex").slice(0, 8);
  return `preview-${slug}-${hash}`;
}

/** The context a namespace function gets for the deployment `env` describes. */
export function namespaceContext(env: Record<string, string | undefined>): VercelNamespaceContext {
  return {
    environment: env.VERCEL_TARGET_ENV || env.VERCEL_ENV || "development",
    branch: env.VERCEL_GIT_COMMIT_REF || undefined,
  };
}

/**
 * Check a namespace that is already a string (`vercel({ namespace })` at
 * call time, or a function's result): what it becomes in the Sandbox's
 * name, refused when empty or too long for `project`.
 */
export function checkNamespace(
  namespace: unknown,
  project: string,
  context?: VercelNamespaceContext,
): string {
  if (namespace !== undefined && namespace !== null && typeof namespace !== "string") {
    throw new Error(
      `vercel(): namespace must be a string, or a function returning one; got ${typeof namespace}`,
    );
  }
  if (!namespace) {
    // Deploys from the CLI outside a Git checkout, or through the API, have
    // an empty branch: `perBranch` gives them no namespace, since they'd
    // all share one core.
    if (context && context.environment !== "production" && context.branch === undefined) {
      throw new Error(
        `vercel(): this ${context.environment} deployment has no Git branch (VERCEL_GIT_COMMIT_REF is empty, ` +
          "as for deploys from the CLI outside a Git checkout or through the API), and its namespace function returned none; " +
          'return a namespace for it, e.g. `perBranch(branch) || "cli-previews"`',
      );
    }
    throw new Error(
      context
        ? `vercel(): the namespace function returned no namespace for this ${context.environment} deployment (branch ${context.branch})`
        : "vercel(): namespace is empty",
    );
  }
  const safe = safeNamespace(namespace);
  if (!/[a-z0-9]/.test(safe)) {
    throw new Error(`vercel(): namespace ${JSON.stringify(namespace)} has no letters or digits`);
  }
  const prefix = `jazz-${safeProject(project)}-`;
  const max = SANDBOX_NAME_MAX - prefix.length;
  if (safe.length > max) {
    throw new Error(
      `vercel(): namespace ${JSON.stringify(namespace)} is ${safe.length} characters; with this project's prefix ${prefix} it may have at most ${max} (Vercel's Sandbox names have at most ${SANDBOX_NAME_MAX})`,
    );
  }
  return safe;
}

/**
 * Check a string namespace when `vercel()` is called, so `next dev` reports
 * it too: against the project's id when it's known, else a typical one.
 */
export function checkNamespaceOption(options: {
  namespace?: unknown;
  credentials?: { projectId: string };
}): void {
  if (options.namespace === undefined) throw missingNamespaceError();
  if (typeof options.namespace === "function") return;
  checkNamespace(
    options.namespace,
    options.credentials?.projectId ?? process.env.VERCEL_PROJECT_ID ?? TYPICAL_PROJECT_ID,
  );
}

/**
 * This deployment's core: `jazz-<project>-<namespace>`, with the namespace
 * the option names for it. Cores are kept apart by name, not secured from
 * each other: every preview deployment's OIDC token names the same
 * workload (`…:environment:preview`), so any preview function can reach
 * any preview core. Production is its own workload.
 */
export function coreName(
  project: string,
  namespace: VercelNamespace | undefined,
  env: Record<string, string | undefined>,
): { name: string; namespace: string } {
  if (namespace === undefined) throw missingNamespaceError();
  const context = namespaceContext(env);
  const chosen = typeof namespace === "function" ? namespace(context) : namespace;
  const safe = checkNamespace(
    chosen,
    project,
    typeof namespace === "function" ? context : undefined,
  );
  return { name: `jazz-${safeProject(project)}-${safe}`, namespace: safe };
}

/**
 * The Drive a core keeps its data on: `<name>-data`, or, for a name too
 * long for Vercel's Drive names, its first part and a hash of the whole.
 */
export function driveName(name: string): string {
  const drive = `${name}-data`;
  if (drive.length <= DRIVE_NAME_MAX) return drive;
  const hash = createHash("sha256").update(name).digest("hex").slice(0, 8);
  const head = name.slice(0, DRIVE_NAME_MAX - "-data".length - hash.length - 1);
  return `${head}-${hash}-data`;
}

/**
 * The core addon's tarball for a jazz-tools version: npm's for a published
 * version, the GitHub release asset for a release candidate (rc builds are
 * attached to their release and never reach npm).
 */
export function defaultCoreTarballUrl(version: string): string {
  const name = "jazz-napi-linux-x64-gnu";
  // The versions dev/artifacts/rc-tarballs.mjs stamps on a candidate.
  if (/^2\.0\.0-(alpha|beta)\.[0-9]+(\.[0-9]+)?-rc\.[0-9]+$/.test(version)) {
    return `https://github.com/garden-co/jazz/releases/download/v${version}/garden-co-${name}-${version}.tgz`;
  }
  return `https://registry.npmjs.org/@garden-co/${name}/-/${name}-${version}.tgz`;
}

/**
 * The installed jazz-tools version, as a JSON import of the package's own
 * `package.json` export. Bundlers (webpack, Turbopack) inline it, and Node
 * loads it as a module. Neither reads a path at runtime, unlike `readFile`
 * of a URL resolved against `import.meta.url` (webpack rewrites that URL
 * into one `fs` refuses) or `createRequire` from `import.meta.url`
 * (bundlers bake the build machine's path into it, so a deployed function
 * can't find it).
 */
async function installedVersion(): Promise<string> {
  const packageJson = (await import("jazz-tools/package.json", { with: { type: "json" } })) as {
    default: { version: string };
  };
  return packageJson.default.version;
}

export interface OpenVercelCoreOptions {
  options: VercelStoreOptions;
  openRemote: OpenRemote;
  sdk?: SandboxSdk;
  version?: string;
  env?: Record<string, string | undefined>;
  /** Reads the Blob store's environment claim (tests). */
  fetch?: typeof fetch;
}

/**
 * `@vercel/sandbox`, an optional peer dependency. The import is a literal
 * inside `try`: bundlers (Turbopack, webpack) then bundle the package when
 * it's installed, and turn a missing one into an error at runtime instead
 * of failing every route that imports jazz-tools (an app on `sqlite()`
 * never needs it). A `.catch()` on the import isn't enough for that.
 */
async function importSandboxSdk(): Promise<SandboxSdk> {
  try {
    return (await backendNative().importPeer("@vercel/sandbox")) as unknown as SandboxSdk;
  } catch (error) {
    throw optionalPeerImportError("vercel()", "@vercel/sandbox", error, {
      requireEsm: SANDBOX_REQUIRE_ESM_HINT,
    });
  }
}

/**
 * @vercel/sandbox 4.1.0 to 5.0.0 depend on @vercel/drives 1.0.0, whose
 * CommonJS build require()s the ES-only @workflow/serde. A function Vercel
 * bundles (Next.js) never runs that require(); a plain `api/` function, which
 * Vercel only traces, does, and fails on Vercel's runtime.
 */
const SANDBOX_REQUIRE_ESM_HINT =
  "@vercel/sandbox 4.1.0 to 5.0.0 do this through @vercel/drives in a function Vercel doesn't bundle " +
  "(a plain Node function in api/; Next.js bundles it): install @vercel/sandbox@4.0.0.";

export async function openVercelCore(params: OpenVercelCoreOptions): Promise<VercelCore> {
  const { options, openRemote } = params;
  refuseVercelAppId(options);
  const env = params.env ?? process.env;
  const sdk = params.sdk ?? options.sdk ?? (await importSandboxSdk());
  const version = params.version ?? (await installedVersion());
  const tarballUrl = options.coreTarballUrl ?? defaultCoreTarballUrl(version);
  const project = options.credentials?.projectId ?? env.VERCEL_PROJECT_ID;
  refuseVercelName(options);
  if (options.namespace === undefined) throw missingNamespaceError();
  const region = checkRegionOption(options);
  if (!project) {
    throw new Error(
      "vercel(): VERCEL_PROJECT_ID is not set; enable system environment variables, or pass credentials with its projectId",
    );
  }
  const { name, namespace } = coreName(project, options.namespace, env);
  const drive = driveName(name);
  warnFunctionRegion(name, region, env);
  const creds = options.credentials ?? {};
  const idleMs = options.idleMs ?? DEFAULT_IDLE_MS;
  const backendCredential = options.backendCredential ?? functionOidcToken;
  // The core trusts the workload this function's token names, so every
  // instance of this project and environment reaches it, and nothing else.
  const token = await backendCredential();
  const trust = workloadOf(token);
  // Skipped for a `backendCredential` of your own, which names whatever
  // workload you chose.
  if (!options.backendCredential) refuseOtherEnvironment(trust, env);
  const trustLine = `${trust.issuer} ${trust.audience} ${trust.subject}`;
  const blobs = resolveCoreBlobs(options, env);
  if (blobs && environmentOf(trust) === undefined) {
    console.warn(
      `vercel(): the core's workload ${trust.subject} names no Vercel environment, so its blob store is not claimed for one; make sure no other environment uses that store`,
    );
  }
  // Read while the core wakes; refused before anything is served.
  const storeChecked =
    blobs && "storeId" in blobs
      ? refuseOtherEnvironmentsBlobStore(blobs.storeId, environmentOf(trust), token, params.fetch)
      : undefined;
  void storeChecked?.catch(() => {});
  const script = Buffer.from(startScript(version, tarballUrl, trust, blobs));

  /** This instance's recent starts, by id (its script's file name). */
  const starts: string[] = [];
  const startServer = async (sbx: SandboxHandle) => {
    // A fresh path each time: bash reads a script as it runs, so one that is
    // running must never be rewritten.
    const id = `jazz-start-${randomUUID()}`;
    starts.push(id);
    if (starts.length > 20) starts.shift();
    const path = `/tmp/${id}.sh`;
    await sbx.writeFiles([{ path, content: script }]);
    await sbx.runCommand({
      cmd: "bash",
      args: ["-lc", `exec bash ${path} >>/data/server.log 2>&1`],
      detached: true,
    });
  };

  /**
   * Why this version's last start in `sbx` gave up, if it was one of this
   * instance's starts (see `fail` in the start script); a file left by an
   * earlier session or another instance is never read as this one's. A
   * permanent cause never comes up on its own; a transient one may on the
   * next start.
   */
  const startError = async (sbx: SandboxHandle) => {
    const text = (
      await sbx.readFileToBuffer({ path: `/tmp/jazz-${version}.error` }).catch(() => null)
    )
      ?.toString("utf8")
      .trim();
    const match = text ? /^(\S+) (permanent|transient): (.*)$/s.exec(text) : null;
    if (!match || !starts.includes(match[1]!)) return undefined;
    return { permanent: match[2] === "permanent", why: match[3]! };
  };

  /**
   * Stop the Sandbox rather than keep it awake retrying a start that can't
   * succeed. Other instances may be watching the same Sandbox: only the
   * session this one saw fail is stopped, never a newer one, and never one
   * where another version's core is running or starting (an older
   * deployment's, still in use).
   */
  const giveUp = async (sbx: SandboxHandle, why: string): Promise<never> => {
    const watched = sbx.currentSession().startedAt.getTime();
    const now = await getRunning();
    const other = now ? await runningVersion(now) : undefined;
    if (
      now &&
      now.currentSession().startedAt.getTime() === watched &&
      (!other || other === version)
    ) {
      await now.stop().catch(() => {});
    }
    throw new CoreStartError(
      `vercel(): core ${name} could not start: ${why} (log: /data/server.log)`,
    );
  };

  /** The server version running in `sbx`, if any. */
  const runningVersion = async (sbx: SandboxHandle) =>
    (await sbx.readFileToBuffer({ path: "/tmp/jazz.version" }).catch(() => null))
      ?.toString("utf8")
      .trim() || undefined;

  const getRunning = async (): Promise<SandboxHandle | null> => {
    const sbx = await sdk.Sandbox.get({ name, resume: false, ...creds }).catch(() => null);
    if (sbx?.status !== "running") return null;
    const session = sbx.currentSession();
    const left = session.startedAt.getTime() + session.timeout - Date.now();
    if (left < MIN_LEFT_MS) {
      const extended = await session.extendTimeout(idleMs).then(
        () => true,
        () => false,
      );
      if (!extended) return null;
    }
    return sbx;
  };

  /**
   * The core's Sandbox, or `null` only when Vercel says there is none (a
   * 404). Any other failure is a `LookupError`: read as "no Sandbox", it
   * would provision a core, in the configured region, next to one that
   * exists.
   */
  const findSandbox = async (): Promise<SandboxHandle | null> => {
    try {
      return await sdk.Sandbox.get({ name, resume: false, ...creds });
    } catch (error) {
      if (isNotFound(error)) return null;
      throw new LookupError(`vercel(): could not look up core ${name}: ${String(error)}`);
    }
  };

  /**
   * Create the Sandbox and start its core, or `null` when another instance
   * created it first (concurrent first requests on a cold deploy): the
   * caller then waits for that one, like any other wake.
   */
  const provision = async (): Promise<SandboxHandle | null> => {
    // An existing Drive (say, of a deleted Sandbox) keeps its region, which
    // can't change: a core there is refused like an existing Sandbox, before
    // `getOrCreate` is asked for one in the configured region.
    const existing = await sdk.Drive.get({ name: drive, ...creds }).catch((error: unknown) => {
      if (isNotFound(error)) return null;
      throw new LookupError(
        `vercel(): could not look up core ${name}'s Drive ${drive}: ${String(error)}`,
      );
    });
    if (existing) refuseRegionMove(`core ${name}'s Drive ${drive}`, existing.region, region);
    const created = await sdk.Drive.getOrCreate({
      name: drive,
      region,
      maxSize: options.maxDriveBytes ?? 10 * 1024 ** 3,
      ...creds,
    });
    refuseRegionMove(`core ${name}'s Drive ${drive}`, created.region, region);
    const sbx = await sdk.Sandbox.create({
      name,
      region,
      image: "vercel/sandbox/node:24",
      ports: [VERCEL_CORE_PORT],
      resources: { vcpus: options.vcpus ?? 1 },
      timeout: idleMs,
      persistent: true,
      keepLastSnapshots: { count: 1 },
      mounts: { "/data": { drive, mode: "read-write" } },
      ...creds,
    }).catch((error: unknown) => {
      if (isNameTaken(error)) return null;
      throw error;
    });
    if (sbx) await startServer(sbx);
    return sbx;
  };

  const refuseForeignPin = async (sbx: SandboxHandle) => {
    const pinned = (await sbx.readFileToBuffer({ path: "/data/trust" }).catch(() => null))
      ?.toString("utf8")
      .trim();
    if (pinned && pinned !== trustLine) {
      throw new Error(
        `vercel(): core ${name} trusts ${pinned.split(" ").at(-1)}, not this deployment (${trust.subject}). ` +
          "After a team or project rename, or a change of OIDC issuer mode, delete /data/trust in the Sandbox to re-pin it.",
      );
    }
  };

  /**
   * Stop the session `sbx` belongs to, so the next wake starts a fresh one.
   * Other instances may be watching the same session; only the session this
   * one saw fail is stopped, never a newer one. Says how it went: `gone`
   * when that session no longer runs (stopped, or replaced by a newer one),
   * and the error when Vercel refused to stop it.
   */
  const stopSession = async (
    sbx: SandboxHandle,
  ): Promise<{ stopped: true } | { gone: true } | { failed: unknown }> => {
    const watched = sbx.currentSession().startedAt.getTime();
    const now = await getRunning();
    if (!now || now.currentSession().startedAt.getTime() !== watched) return { gone: true };
    try {
      await now.stop();
      return { stopped: true };
    } catch (error) {
      return { failed: error };
    }
  };

  /** What the core in `sbx` is doing, as seen from inside the Sandbox (see `coreStateScript`). */
  const coreState = async (sbx: SandboxHandle): Promise<CoreState> => {
    let said: string;
    try {
      const done = await sbx.runCommand({
        cmd: "bash",
        args: ["-c", coreStateScript(), "jazz-core-state"],
      });
      said = done ? (await done.stdout()).trim() : "";
    } catch (error) {
      return { kind: "unknown", why: String(error) };
    }
    const match = /^(listening|opening) (\d+)$/.exec(said);
    if (match) return { kind: match[1] as "listening" | "opening", upSeconds: Number(match[2]) };
    if (said === "starting" || said === "down") return { kind: said };
    return { kind: "unknown", why: `the core's state check said ${JSON.stringify(said)}` };
  };

  /**
   * Stop the session `sbx` belongs to because Vercel's router doesn't reach
   * the core in it, so the next wake starts a fresh one. Only a session
   * whose core listens inside the Sandbox is stopped: one still opening its
   * store looks the same from outside, and stopping it throws its open away.
   * Each session is stopped for this at most once, by whichever waiter of
   * any instance claims it first. Returns whether this call stopped it.
   */
  const recycle = async (sbx: SandboxHandle, why: string): Promise<boolean> => {
    const state = await coreState(sbx);
    if (state.kind !== "listening") return false;
    const session = sbx.currentSession().startedAt.getTime();
    const claim = await sbx
      .runCommand({
        cmd: "bash",
        args: ["-c", claimRecycleScript(), "jazz-claim-recycle", String(session)],
      })
      .catch(() => undefined);
    if (claim?.exitCode !== 0) return false;
    console.warn(
      `jazz: vercel(): stopping core ${name}'s session, whose core listens, so the next wake starts a fresh one: ${why}`,
    );
    const stop = await stopSession(sbx);
    if ("failed" in stop) {
      // Left claimed, the session could never be recycled again: every
      // later waiter would wait out its deadline. Give the claim back.
      await sbx
        .runCommand({
          cmd: "bash",
          args: ["-c", releaseRecycleScript(), "jazz-release-recycle", String(session)],
        })
        .catch(() => undefined);
      console.warn(
        `jazz: vercel(): could not stop core ${name}'s session (${String(stop.failed)}); released its recycle for the next waiter`,
      );
      return false;
    }
    return true;
  };

  /** Resume (or create) the Sandbox and wait until the server answers. */
  const wake = async (): Promise<{ sbx: SandboxHandle; base: string }> => {
    const t0 = performance.now();
    const deadline = t0 + WAKE_DEADLINE_MS;
    const healthFails: Record<string, number> = {};
    let recycled = false;
    let restarted = false;
    /** What the core was doing when this wake last looked inside its Sandbox. */
    let lastState: CoreState | undefined;
    /** The last transient start failure, for the error if the wake times out. */
    let lastTransient: string | undefined;
    /** What this wake had to do, for its log line. */
    const steps = new Set<string>();
    /** Done: say how long it took, once per wake, if the core wasn't simply answering. */
    const woke = (sbx: SandboxHandle, base: string) => {
      if (steps.size > 0) {
        console.info(
          `jazz: vercel(): core ${name} woke in ${since(t0)} ms (${[...steps].join(", ")})`,
        );
      }
      return { sbx, base };
    };
    let sbx = await getRunning();
    for (;;) {
      while (!sbx) {
        try {
          const existing = await findSandbox();
          // Before resuming, which starts its server: a core elsewhere is
          // never woken.
          if (existing) refuseRegionMove(`core ${name}`, existing.region, region);
          steps.add(existing ? "resumed" : "created");
          sbx = existing
            ? await sdk.Sandbox.get({
                name,
                resume: true,
                ...creds,
                onResume: startServer,
              })
            : await provision();
          if (!sbx) {
            // Another instance is creating it: wait for that Sandbox,
            // resuming it if it isn't running yet.
            if (performance.now() > deadline) {
              throw new Error(
                `vercel(): core ${name} was being created by another instance and did not come up in ${since(t0)} ms`,
              );
            }
            await sleep(250);
            sbx = await getRunning();
          }
        } catch (error) {
          const retry = isWakeConflict(error) || error instanceof LookupError;
          if (!retry || performance.now() > deadline) throw error;
          await sleep(500);
          sbx = await getRunning();
        }
      }
      // A Sandbox is found by name only, and `region` applies only when it
      // is created: a core elsewhere is refused, not silently used.
      refuseRegionMove(`core ${name}`, sbx.region, region);
      // A core pinned to another workload never becomes ours: say so now,
      // not after the health checks time out.
      await refuseForeignPin(sbx);
      let base = sbx.domain(VERCEL_CORE_PORT);
      let ok = await healthy(base, 300, healthFails);
      if (!ok) {
        steps.add("started its server");
        await startServer(sbx);
      }
      /** Since when this session's core has listened while the router said it doesn't. */
      let unroutedSince: number | undefined;
      while (!ok && sbx && performance.now() < deadline) {
        const fails: Record<string, number> = {};
        ok = await healthy(base, 5_000, fails);
        for (const [why, n] of Object.entries(fails))
          healthFails[why] = (healthFails[why] ?? 0) + n;
        if (ok) break;
        const failure = await startError(sbx);
        if (failure?.permanent) await giveUp(sbx, failure.why);
        if (failure) lastTransient = failure.why;
        // Only the Sandbox itself tells a core that is still opening its
        // store from one the router doesn't reach: both answer "not
        // listening" from outside.
        const state = await coreState(sbx);
        lastState = state;
        const refused = Object.keys(fails).some((k) => k.includes("SANDBOX_NOT_LISTENING"));
        if (state.kind === "listening" && refused) {
          unroutedSince ??= performance.now();
          if (!recycled && performance.now() - unroutedSince >= NOT_LISTENING_RECYCLE_MS) {
            const why = `Vercel's router answered SANDBOX_NOT_LISTENING for ${since(unroutedSince)} ms while it listened`;
            if (await recycle(sbx, why)) {
              recycled = true;
              steps.add("recycled a session the router didn't reach");
              sbx = null;
              break;
            }
          }
        } else {
          unroutedSince = undefined;
        }
        if (state.kind === "opening" && !steps.has(OPENING_STEP)) {
          steps.add(OPENING_STEP);
          console.info(
            `jazz: vercel(): core ${name} is opening its store (its process launched ${state.upSeconds} s ago); waiting for it to listen`,
          );
        }
        sbx = await getRunning();
        if (sbx) {
          base = sbx.domain(VERCEL_CORE_PORT);
          // A core that runs, or a start under way, needs no other start.
          if (state.kind === "down" || state.kind === "unknown") await startServer(sbx);
        }
      }
      if (ok && sbx) {
        // An instance of a newer deployment restarts an older server once;
        // an older instance uses whatever newer server it finds.
        const running = await runningVersion(sbx);
        if (!running || !newerVersion(version, running)) return woke(sbx, base);
        if (restarted) {
          throw new Error(`vercel(): core ${name} still runs ${running}, not ${version}`);
        }
        restarted = true;
        steps.add(`restarted ${running} on ${version}`);
        await startServer(sbx);
        while (performance.now() < deadline) {
          const now = await runningVersion(sbx);
          if (now && !newerVersion(version, now) && (await healthy(base, 1_000, healthFails))) {
            return woke(sbx, base);
          }
          // The older core stays up for the deployments still using it.
          const failure = await startError(sbx);
          if (failure?.permanent) {
            throw new CoreStartError(
              `vercel(): core ${name} could not start ${version}: ${failure.why}`,
            );
          }
          if (failure) {
            // A later start may succeed: try again.
            lastTransient = failure.why;
            await startServer(sbx);
            await sleep(1_000);
          }
          await sleep(250);
        }
      }
      if (performance.now() > deadline) {
        if (lastState?.kind === "opening") {
          throw new Error(
            `vercel(): core ${name} is still opening its store: its process launched ${lastState.upSeconds} s ago and doesn't listen yet; ` +
              `gave up waiting after ${since(t0)} ms, and the core keeps opening (log: /data/server.log)`,
          );
        }
        throw new Error(
          `vercel(): core ${name} did not become healthy in ${since(t0)} ms (${JSON.stringify(healthFails)})` +
            (lastTransient ? `; its last start failed: ${lastTransient}` : ""),
        );
      }
    }
  };

  let live:
    | { sbx: SandboxHandle; base: string; appId: string; core: VercelCore; checkedAt: number }
    | undefined;
  let pending: Promise<NonNullable<typeof live>> | undefined;
  let lastActivity = Date.now();
  let sockets = 0;
  let failed: { at: number; error: CoreStartError } | undefined;
  /**
   * The core `live` names stopped answering (Vercel's router refused a
   * request, or a socket to it failed): the next `ensure` wakes it, health
   * check included, instead of trusting it for `ENSURE_FRESH_MS`.
   */
  let suspect = false;
  const distrust = () => {
    suspect = true;
  };
  /**
   * Since when connections to the core have ended without ever opening,
   * with none opening since, and how many. A core can answer `/health` and
   * still be out of reach for connections (Vercel's router stopped passing
   * them to its session): waking it then finds it healthy at the same
   * address, so only a fresh session gets connections through again.
   *
   * Only connections made to a session count, and only against that
   * session: one that was still waiting for a wake never tried any, and a
   * wake that brings up a new session starts its count afresh (#6028).
   */
  let unreached: { since: number; count: number } | undefined;
  const connectionUnreached = () => {
    distrust();
    unreached = unreached
      ? { since: unreached.since, count: unreached.count + 1 }
      : { since: Date.now(), count: 1 };
  };
  const connectionReached = () => {
    unreached = undefined;
  };
  /** Whether the session `live` names keeps refusing connections it answers `/health` for. */
  const unreachable = () =>
    unreached !== undefined &&
    unreached.count >= UNREACHED_RECYCLE_COUNT &&
    Date.now() - unreached.since >= UNREACHED_RECYCLE_MS;

  const ensure = (): Promise<NonNullable<typeof live>> => {
    lastActivity = Date.now();
    if (live && !suspect && Date.now() - live.checkedAt < ENSURE_FRESH_MS)
      return Promise.resolve(live);
    if (failed && Date.now() - failed.at < START_FAILURE_HOLD_MS)
      return Promise.reject(failed.error);
    pending ??= (async () => {
      try {
        if (live && !suspect && (await getRunning())) {
          live.checkedAt = Date.now();
          return live;
        }
        if (live && unreachable()) {
          // Its `/health` would pass again: start a fresh session instead,
          // unless another waiter already did, or its core doesn't listen
          // (then the wake below waits for it).
          const why = `${unreached!.count} connections in ${Date.now() - unreached!.since} ms ended without reaching the core, which answers /health`;
          unreached = undefined;
          await recycle(live.sbx, why);
        }
        const before = live?.sbx.currentSession().startedAt.getTime();
        const { sbx, base } = await wake();
        suspect = false;
        // Connections that failed to reach an earlier session say nothing
        // about this one: stopping it for them would throw away the wake.
        if (sbx.currentSession().startedAt.getTime() !== before) unreached = undefined;
        const coreAppId =
          live?.base === base
            ? live.appId
            : (await sbx.readFileToBuffer({ path: "/data/app-id" }))?.toString("utf8").trim();
        if (!coreAppId) throw new Error(`vercel(): core ${name} has no app id on its Drive`);
        await refuseForeignPin(sbx);
        if (live?.base === base) {
          live.sbx = sbx;
          live.checkedAt = Date.now();
          return live;
        }
        const replaced = live?.core;
        live = {
          sbx,
          base,
          appId: coreAppId,
          core: openRemote({ appId: coreAppId, url: base, backendCredential }),
          checkedAt: Date.now(),
        };
        if (replaced) void replaced.close().catch(() => {});
        failed = undefined;
        return live;
      } catch (error) {
        if (error instanceof CoreStartError) failed = { at: Date.now(), error };
        throw error;
      } finally {
        pending = undefined;
      }
    })();
    return pending;
  };

  // Keep the core awake while browsers are connected or requests are recent;
  // once quiet, its session timeout stops it.
  // Each instance reads the session fresh and only tops it up to `idleMs`
  // from now, so many instances don't stack their extensions.
  const keepalive = setInterval(() => {
    if (!live) return;
    if (sockets === 0 && Date.now() - lastActivity > idleMs) return;
    // Attempts that never reach the core aren't use: a session no
    // connection gets through to is left to time out, not kept awake.
    if (unreached) return;
    void sdk.Sandbox.get({ name, resume: false, ...creds })
      .then((sbx) => {
        if (sbx.status !== "running") return;
        const session = sbx.currentSession();
        const left = session.startedAt.getTime() + session.timeout - Date.now();
        if (left < idleMs - KEEPALIVE_MS) return session.extendTimeout(idleMs - left);
      })
      .catch(() => {});
  }, KEEPALIVE_MS);
  (keepalive as { unref?: () => void }).unref?.();

  const ensured = ensure();
  let first: Awaited<typeof ensured>;
  try {
    [first] = await Promise.all([ensured, storeChecked]);
  } catch (error) {
    clearInterval(keepalive);
    // The wake may still be under way: close the core it comes up with.
    void ensured.then(({ core }) => core.close()).catch(() => {});
    throw error;
  }
  logStoreOpened(`jazz: store vercel namespace ${JSON.stringify(namespace)} (core ${name})`);
  // Clients in this process (the backend's own session) reach the core through
  // an in-process origin, so each of their requests and reconnects wakes it;
  // they go on through the current remote core's own in-process origin, which
  // presents the backend credential.
  const serverUrl = `http://jazz-vercel-${randomUUID()}.invalid`;
  const unregister = registerInProcessTransport(serverUrl, {
    fetch: async (input, init) => {
      const request = new Request(input, init);
      const url = new URL(request.url);
      const send = async (sent: Request) => {
        const inner = (await ensure()).core.serverUrl;
        return fetchFor(inner)(new Request(`${inner}${url.pathname}${url.search}`, sent));
      };
      // Kept to send again only when that costs little: a clone tees the
      // body, buffering a streamed upload whole while the first send runs.
      const again = resendable(request, init?.body) ? request.clone() : undefined;
      const response = await send(request);
      if (!response.headers.has(ROUTER_ERROR_HEADER)) return response;
      // Refused before it reached the core: wake it (for the next request
      // too) and send it again.
      distrust();
      if (!again) return response;
      void response.body?.cancel().catch(() => {});
      return send(again);
    },
    WebSocket: wakingWebSocket(async () => (await ensure()).core.serverUrl, {
      dropped: distrust,
      unreached: connectionUnreached,
      reached: connectionReached,
    }),
  });

  return {
    appId: first.appId,
    serverUrl,
    adminSecret: first.core.adminSecret,
    backendSecret: first.core.backendSecret,
    async handle(method, path, headers, body, access) {
      const response = await (await ensure()).core.handle(method, path, headers, body, access);
      if (!response.headers.some(([name]) => name!.toLowerCase() === ROUTER_ERROR_HEADER))
        return response;
      // Refused before it reached the core (a core stopping or restarting
      // while this instance still trusted it): wake it and send it again.
      distrust();
      return (await ensure()).core.handle(method, path, headers, body, access);
    },
    openSocket(headers, onEvent, access) {
      sockets++;
      let inner: ReturnType<VercelCore["openSocket"]> | undefined;
      let closed = false;
      const queued: Uint8Array[] = [];
      const done = () => {
        if (!closed) sockets--;
        closed = true;
      };
      ensure().then(
        ({ core }) => {
          if (closed) return;
          try {
            let reached = false;
            inner = core.openSocket(
              headers,
              (error, event) => {
                if (error || event.kind === "close") done();
                // A connection that never reached the core counts toward
                // recycling its session; a dropped one only wakes it: the
                // client reconnects, and its next session finds it awake.
                if (event.kind === "close" && inner?.opened && !reached) connectionUnreached();
                else if (event.kind === "close" && event.code === 1006) distrust();
                onEvent(error, event);
              },
              access,
            );
            void inner.opened?.then(() => {
              reached = true;
              connectionReached();
            });
          } catch (error) {
            done();
            onEvent(error instanceof Error ? error : new Error(String(error)), {
              kind: "close",
              code: 1011,
              reason: "core unavailable",
            } as JazzCoreSocketEvent);
            return;
          }
          for (const data of queued.splice(0)) inner.send(data);
        },
        (error: unknown) => {
          done();
          onEvent(error instanceof Error ? error : new Error(String(error)), {
            kind: "close",
            code: 1011,
            reason: "core unavailable",
          } as JazzCoreSocketEvent);
        },
      );
      return {
        send(data) {
          if (closed) return false;
          lastActivity = Date.now();
          if (inner) return inner.send(data);
          queued.push(data);
          return true;
        },
        close() {
          done();
          inner?.close();
        },
      };
    },
    async close() {
      clearInterval(keepalive);
      unregister();
      await live?.core.close();
    },
  };
}

/** The largest request body kept to send again after the router refused it. */
const RESEND_MAX_BYTES = 1024 * 1024;

/** Whether `request` has no body, or a small one already in memory or of stated size. */
function resendable(request: Request, given?: unknown): boolean {
  if (request.body === null) return true;
  const size =
    typeof given === "string"
      ? Buffer.byteLength(given)
      : given instanceof ArrayBuffer || ArrayBuffer.isView(given)
        ? given.byteLength
        : Number(request.headers.get("content-length") ?? NaN);
  return Number.isFinite(size) && size <= RESEND_MAX_BYTES;
}

/**
 * A WebSocket that wakes the core, then connects to wherever it now runs.
 *
 * A connection to the core that fails before it opens (the core is stopping,
 * restarting, or Vercel's router refuses the upgrade) is tried again once the
 * core is awake, until the wake deadline: the client hears of it only as a
 * slower open. Nothing is replayed, since a client sends nothing before
 * `open`. A connection that opened and then dropped is reported as it is:
 * the client's own reconnect, and the query machinery above it, take over.
 *
 * `hooks` hears how each connection went: `dropped` for a connection to the
 * core that closed abnormally (the core may be down), `unreached` once a
 * connection ends without ever having opened (retries spent, or the client
 * gave up waiting) after a connection to the core was made (not while it
 * only waited for the wake), and `reached` once one opens.
 */
function wakingWebSocket(
  resolveOrigin: () => Promise<string>,
  hooks: { dropped: () => void; unreached: () => void; reached: () => void },
): WebSocketConstructor {
  return class WakingSocket implements DeferredConnectWebSocket {
    /** The client's handshake timeout runs only while a connection to the core is under way. */
    readonly defersConnect = true;
    binaryType: "arraybuffer" | "blob" = "arraybuffer";
    readyState = 0;
    private socket: WebSocket | undefined;
    private readonly listeners = new Map<string, Array<(event?: any) => void>>();
    private readonly path: string;
    private readonly deadline = performance.now() + WAKE_DEADLINE_MS;
    private pause = 100;
    /** Whether a connection to the core was made, not just waited for. */
    private tried = false;

    constructor(url: string) {
      this.path = new URL(url).pathname + new URL(url).search;
      this.connect();
    }

    private connect(): void {
      resolveOrigin().then(
        (origin) => {
          if (this.readyState >= 2) return;
          const Socket = inProcessWebSocketFor(origin) ?? globalThis.WebSocket;
          const socket = new Socket(`${origin.replace(/^http/, "ws")}${this.path}`) as WebSocket;
          socket.binaryType = "arraybuffer";
          this.socket = socket;
          this.tried = true;
          this.emit("connecting");
          socket.addEventListener("open", () => {
            if (this.socket !== socket || this.readyState !== 0) return;
            this.readyState = 1;
            hooks.reached();
            this.emit("open");
          });
          socket.addEventListener("message", (event) => {
            if (this.socket === socket) this.emit("message", { data: event.data });
          });
          socket.addEventListener("error", (event) => {
            // A connection that never opened is retried, or reports its close.
            if (this.socket === socket && this.readyState === 1) this.emit("error", event);
          });
          socket.addEventListener("close", (event) => {
            if (this.socket !== socket) return;
            if (event.code === 1006) hooks.dropped();
            if (this.readyState === 0 && event.code === 1006 && this.retry()) return;
            this.finish(event.code, event.reason, this.readyState === 0);
          });
        },
        (error: unknown) => {
          this.emit("error", error);
          this.finish(1011, "core unavailable");
        },
      );
    }

    /** Connect again after a connection that never opened, if there is time left. */
    private retry(): boolean {
      if (performance.now() + this.pause > this.deadline) return false;
      this.socket = undefined;
      // Waking again, under the wake deadline rather than the handshake timeout.
      this.emit("waking");
      setTimeout(() => {
        if (this.readyState === 0) this.connect();
      }, this.pause);
      this.pause = Math.min(this.pause * 2, 1_000);
      return true;
    }

    addEventListener(type: string, listener: (event?: any) => void): void {
      const list = this.listeners.get(type) ?? [];
      list.push(listener);
      this.listeners.set(type, list);
    }

    send(data: Uint8Array | string): void {
      if (this.readyState !== 1 || !this.socket) throw new Error("socket is not open");
      this.socket.send(data as Uint8Array<ArrayBuffer>);
    }

    close(): void {
      if (this.readyState >= 2) return;
      this.socket?.close();
      // A client gives up on a connection that doesn't open in time (its
      // handshake timeout): that one never reached the core either, if it
      // tried. One given up while the core was still waking never did.
      this.finish(1000, "client closed", this.readyState === 0 && this.tried);
    }

    private emit(type: string, event?: unknown): void {
      for (const listener of this.listeners.get(type) ?? []) listener(event);
    }

    /** End this socket; `unreached` if it ends without having reached the core. */
    private finish(code: number, reason: string, unreached = false): void {
      if (this.readyState >= 3) return;
      this.readyState = 3;
      if (unreached) hooks.unreached();
      this.emit("close", { code, reason });
    }
  };
}

/** The core keeps its own app id; one passed from JavaScript is refused, not ignored. */
export function refuseVercelAppId(options: object): void {
  if ("appId" in options) {
    throw new Error(
      "vercel(): appId is not an option: the core keeps its own app id on its Drive.",
    );
  }
}
