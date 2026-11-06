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
  BrowserWebSocket,
  WebSocketConstructor,
} from "../runtime/native-runtime/websocket.js";
import { optionalPeerImportError } from "./optional-peer.js";

export const VERCEL_CORE_PORT = 1625;

export interface VercelStoreOptions {
  /**
   * Sandbox name. Defaults to `jazz-<VERCEL_PROJECT_ID>-production` for
   * production and `jazz-<VERCEL_PROJECT_ID>-preview-<branch>-<hash>` for
   * each Git branch's previews. Required for a preview deployment without
   * a Git branch (from the CLI outside a Git checkout, or through the API).
   */
  name?: string;
  /**
   * Not an option: the core generates its app id on its Drive at first
   * start and keeps it there.
   */
  appId?: never;
  /** Sandbox region. Defaults to `iad1`. Fixed when the Sandbox is created. */
  region?: string;
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
  Drive: { getOrCreate(params: Record<string, unknown>): Promise<unknown> };
}

export interface SandboxHandle {
  readonly status: string;
  domain(port: number): string;
  currentSession(): {
    startedAt: Date;
    timeout: number;
    extendTimeout(ms: number): Promise<void>;
  };
  runCommand(params: { cmd: string; args: string[]; detached?: boolean }): Promise<unknown>;
  writeFiles(files: { path: string; content: Buffer }[]): Promise<void>;
  readFileToBuffer(file: { path: string }): Promise<Buffer | null>;
  stop(): Promise<void>;
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
  ): { send(data: Uint8Array): boolean; close(): void };
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
// server listens; a fresh session routes, so recycle after this long.
const NOT_LISTENING_RECYCLE_MS = 12_000;
// How long a wake is trusted before the next request checks again.
const ENSURE_FRESH_MS = 10_000;
const KEEPALIVE_MS = 30_000;

// After a start that can't succeed, requests fail fast for this long
// instead of each waking the Sandbox into the same failure.
const START_FAILURE_HOLD_MS = 30_000;

/** A core start that gave up (the start script said why); retrying won't help. */
class CoreStartError extends Error {}

const isWakeConflict = (error: unknown) => /\b409\b|already attached|conflict/i.test(String(error));

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

/** Whether semver `a` is newer than `b` (prereleases sort before their release). */
export function newerVersion(a: string, b: string): boolean {
  const split = (v: string) => {
    const [main = "", pre] = v.split(/-(.*)/s, 2);
    return { main: main.split(".").map(Number), pre: pre?.split(".") };
  };
  const x = split(a);
  const y = split(b);
  for (let i = 0; i < 3; i++) {
    if ((x.main[i] ?? 0) !== (y.main[i] ?? 0)) return (x.main[i] ?? 0) > (y.main[i] ?? 0);
  }
  if (!x.pre || !y.pre) return !x.pre && !!y.pre;
  for (let i = 0; i < Math.max(x.pre.length, y.pre.length); i++) {
    const p = x.pre[i];
    const q = y.pre[i];
    if (p === q) continue;
    if (p === undefined) return false;
    if (q === undefined) return true;
    const numeric = /^\d+$/;
    if (numeric.test(p) && numeric.test(q)) return Number(p) > Number(q);
    if (numeric.test(p) !== numeric.test(q)) return !numeric.test(p);
    return p > q;
  }
  return false;
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

/** The addon's file in its npm tarball. */
const CORE_ADDON = "package/jazz-napi.linux-x64-gnu.node";

/**
 * Runs the core in the Sandbox's Node: `node -e` with the addon, the app id
 * and the core's options, in that order. Node owns the process's signals: a
 * SIGTERM asks the core to drain, and the process exits once it has. The addon's path stays in the
 * process's command line, which is how a later start recognises it.
 *
 * @internal
 */
export const CORE_LAUNCHER =
  "const [addon, appId, options] = process.argv.slice(1);" +
  " const core = require(addon).serveCore({ ...JSON.parse(options), appId });" +
  " for (const signal of ['SIGTERM', 'SIGINT']) process.once(signal, () => core.stop());" +
  " core.closed().then(() => process.exit(0), (error) => { console.error(error); process.exit(1); });";

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
  const q = (s: string) => `'${s.replaceAll("'", `'\\''`)}'`;
  const trustLine = `${trust.issuer} ${trust.audience} ${trust.subject}`;
  return `#!/bin/bash
# Written by jazz-tools' vercel() store for each server start.
set -euo pipefail
# The function names each start by its script's file name, and trusts only
# an error file that names one of its own starts (never a stale one).
start_id=$(basename "$0" .sh)
rm -f "$0"
data=${q(`${root}/data`)}
run=${q(`${root}/tmp`)}
# Starts are serialized; the server itself runs without the lock.
exec 9>"$run/jazz.lock"
flock 9
# Semver order: GNU sort -V with "~" sorts a prerelease before its release.
newer() {
  [ "$1" != "$2" ] && [ "$(printf '%s\\n%s\\n' "\${1/-/\\~}" "\${2/-/\\~}" | sort -V | tail -1)" = "\${1/-/\\~}" ]
}
version=${q(version)}
trust=${q(trustLine)}
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
  url=${q(tarballUrl)}
  rm -f "$run/download.kind"
  if ! node -e ${q(DOWNLOAD_CORE)} "$url" "$run/download.kind" 2>"$run/download.err" \\
    | tar xz -O ${CORE_ADDON} 2>>"$run/download.err" > "$addon.tmp"; then
    rm -f "$addon.tmp"
    # No kind: the download arrived but isn't the core's tarball.
    fail "$(cat "$run/download.kind" 2>/dev/null || echo permanent)" "could not download the core for $version from $url ($(head -c 300 "$run/download.err" | tr '\\n' ' ')); set vercel({ coreTarballUrl }) to a tarball of @garden-co/jazz-napi-linux-x64-gnu for this version"
  fi
  [ -s "$addon.tmp" ] || fail permanent "the core download for $version from $url was empty"
  mv "$addon.tmp" "$addon"
fi
# The pid file may be stale (a resumed snapshot): trust it only if that
# process runs one of our cores.
pid=$(cat "$run/jazz.pid" 2>/dev/null || true)
cmdline=$(tr '\\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)
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
exec 9>&-
# The core admits only the deploying function's own workload.${blobsSetup(blobs, q)}
exec node -e ${q(CORE_LAUNCHER)} "$addon" "$app_id" ${q(
    JSON.stringify(coreOptions(`${root}/data/store`, trust, blobs)),
  )}
`;
}

/**
 * The environment a core with `s3()` starts with: it assumes the
 * Marketplace's role with the function's OIDC token, which every request
 * carries. With `vercelBlob()` that token is the Blob credential itself.
 */
function blobsSetup(blobs: VercelS3 | VercelBlob | undefined, q: (s: string) => string): string {
  if (!blobs) {
    return "\n# Large values stay in its SQLite file (they can move to Vercel Blob or S3 later).";
  }
  if ("storeId" in blobs) {
    return "\n# Large values go to the project's Blob store, with the function's own OIDC token.";
  }
  return `
# Large values go to S3, under the role this project's functions assume.
export S3_BUCKET_NAME=${q(blobs.bucket)} AWS_REGION=${q(blobs.region)} AWS_ROLE_ARN=${q(blobs.roleArn)}`;
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

let warnedNoBlobStore = false;

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
  if (blobs === undefined && !warnedNoBlobStore) {
    warnedNoBlobStore = true;
    warn(
      "vercel(): no Blob store is connected to this project, so large values stay in the core's SQLite file. Connect one (Storage tab in the Vercel dashboard) and they go there from then on.",
    );
  }
  if (blobs === undefined || blobs === "sqlite") return undefined;
  if (blobs.kind === "vercel-blob") return resolveVercelBlob(blobs, env);
  return resolveVercelS3(blobs, options.region ?? "iad1", env);
}

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
    const { getVercelOidcToken } = (await import(/* @vite-ignore */ "@vercel/functions/oidc")) as {
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

/**
 * The core for this deployment: one for production, and one per Git branch
 * for previews, so a branch's schema and permissions never apply to
 * another branch's data. This keeps honest branches apart; it is not a
 * security boundary: every preview deployment's OIDC token names the same
 * workload (`…:environment:preview`), so any preview function can reach any
 * branch's core. Production is its own workload.
 */
export function defaultCoreName(project: string, env: Record<string, string | undefined>): string {
  const target = env.VERCEL_ENV ?? "development";
  // Project ids are mixed case with an underscore; names stay [a-z0-9-].
  const safe = (part: string) => part.toLowerCase().replace(/[^a-z0-9]+/g, "-");
  if (target !== "preview") return `jazz-${safe(project)}-${safe(target)}`;
  // Deploys from the CLI outside a Git checkout, or through the API, have an
  // empty branch. They'd all share one core, so ask for a name instead.
  const branch = env.VERCEL_GIT_COMMIT_REF;
  if (!branch) {
    throw new Error(
      "vercel(): this preview deployment has no Git branch (VERCEL_GIT_COMMIT_REF is empty, " +
        "as for deploys from the CLI outside a Git checkout or through the API); " +
        "pass vercel({ name }) to choose its core",
    );
  }
  const slug = branch
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 24);
  const hash = createHash("sha256").update(branch).digest("hex").slice(0, 8);
  return `jazz-${safe(project)}-preview-${slug}-${hash}`;
}

/**
 * The core addon's tarball for a jazz-tools version: npm's for a published
 * version, the GitHub release asset for a release candidate (rc builds are
 * attached to their release and never reach npm).
 */
export function defaultCoreTarballUrl(version: string): string {
  const name = "jazz-napi-linux-x64-gnu";
  if (/^2\.0\.0-alpha\.[0-9]+-rc\.[0-9]+$/.test(version)) {
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
    return (await import(/* @vite-ignore */ "@vercel/sandbox")) as unknown as SandboxSdk;
  } catch (error) {
    throw optionalPeerImportError("vercel()", "@vercel/sandbox", error);
  }
}

export async function openVercelCore(params: OpenVercelCoreOptions): Promise<VercelCore> {
  const { options, openRemote } = params;
  refuseVercelAppId(options);
  const env = params.env ?? process.env;
  const sdk = params.sdk ?? options.sdk ?? (await importSandboxSdk());
  const version = params.version ?? (await installedVersion());
  const tarballUrl = options.coreTarballUrl ?? defaultCoreTarballUrl(version);
  const project = options.credentials?.projectId ?? env.VERCEL_PROJECT_ID;
  if (!options.name && !project) {
    throw new Error(
      "vercel(): VERCEL_PROJECT_ID is not set; enable system environment variables or pass a name",
    );
  }
  const name = options.name ?? defaultCoreName(project!, env);
  const creds = options.credentials ?? {};
  const idleMs = options.idleMs ?? DEFAULT_IDLE_MS;
  const backendCredential = options.backendCredential ?? functionOidcToken;
  // The core trusts the workload this function's token names, so every
  // instance of this project and environment reaches it, and nothing else.
  const trust = workloadOf(await backendCredential());
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

  const provision = async () => {
    await sdk.Drive.getOrCreate({
      name: `${name}-data`,
      region: options.region ?? "iad1",
      maxSize: options.maxDriveBytes ?? 10 * 1024 ** 3,
      ...creds,
    });
    const sbx = await sdk.Sandbox.create({
      name,
      region: options.region ?? "iad1",
      image: "vercel/sandbox/node:24",
      ports: [VERCEL_CORE_PORT],
      resources: { vcpus: options.vcpus ?? 1 },
      timeout: idleMs,
      persistent: true,
      keepLastSnapshots: { count: 1 },
      mounts: { "/data": { drive: `${name}-data`, mode: "read-write" } },
      ...creds,
    });
    await startServer(sbx);
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

  /** Resume (or create) the Sandbox and wait until the server answers. */
  const wake = async (): Promise<{ sbx: SandboxHandle; base: string }> => {
    const t0 = performance.now();
    const deadline = t0 + WAKE_DEADLINE_MS;
    const healthFails: Record<string, number> = {};
    let recycled = false;
    let restarted = false;
    /** The last transient start failure, for the error if the wake times out. */
    let lastTransient: string | undefined;
    let sbx = await getRunning();
    for (;;) {
      while (!sbx) {
        try {
          const existing = await sdk.Sandbox.get({
            name,
            resume: false,
            ...creds,
          }).catch(() => null);
          sbx = existing
            ? await sdk.Sandbox.get({
                name,
                resume: true,
                ...creds,
                onResume: startServer,
              })
            : await provision();
        } catch (error) {
          if (!isWakeConflict(error) || performance.now() > deadline) throw error;
          await sleep(500);
          sbx = await getRunning();
        }
      }
      // A core pinned to another workload never becomes ours: say so now,
      // not after the health checks time out.
      await refuseForeignPin(sbx);
      let base = sbx.domain(VERCEL_CORE_PORT);
      let ok = await healthy(base, 300, healthFails);
      if (!ok) await startServer(sbx);
      const healthT0 = performance.now();
      while (!ok && sbx && performance.now() < deadline) {
        ok = await healthy(base, 5_000, healthFails);
        const failure = ok ? undefined : await startError(sbx);
        if (failure?.permanent) await giveUp(sbx, failure.why);
        if (failure) lastTransient = failure.why;
        const notListening = Object.keys(healthFails).some((k) =>
          k.includes("SANDBOX_NOT_LISTENING"),
        );
        if (
          !ok &&
          notListening &&
          !recycled &&
          performance.now() - healthT0 > NOT_LISTENING_RECYCLE_MS
        ) {
          recycled = true;
          // Other instances may be watching the same session; only the
          // session this one saw fail is stopped, never a newer one.
          const watched = sbx.currentSession().startedAt.getTime();
          const now = await getRunning();
          if (now && now.currentSession().startedAt.getTime() === watched) {
            await now.stop().catch(() => {});
          }
          sbx = null;
          break;
        }
        if (!ok) {
          sbx = await getRunning();
          if (sbx) {
            base = sbx.domain(VERCEL_CORE_PORT);
            await startServer(sbx);
          }
        }
      }
      if (ok && sbx) {
        // An instance of a newer deployment restarts an older server once;
        // an older instance uses whatever newer server it finds.
        const running = await runningVersion(sbx);
        if (!running || !newerVersion(version, running)) return { sbx, base };
        if (restarted) {
          throw new Error(`vercel(): core ${name} still runs ${running}, not ${version}`);
        }
        restarted = true;
        await startServer(sbx);
        while (performance.now() < deadline) {
          const now = await runningVersion(sbx);
          if (now && !newerVersion(version, now) && (await healthy(base, 1_000, healthFails))) {
            return { sbx, base };
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

  const ensure = (): Promise<NonNullable<typeof live>> => {
    lastActivity = Date.now();
    if (live && Date.now() - live.checkedAt < ENSURE_FRESH_MS) return Promise.resolve(live);
    if (failed && Date.now() - failed.at < START_FAILURE_HOLD_MS)
      return Promise.reject(failed.error);
    pending ??= (async () => {
      try {
        if (live && (await getRunning())) {
          live.checkedAt = Date.now();
          return live;
        }
        const { sbx, base } = await wake();
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

  const first = await ensure();
  // Clients in this process (the backend's own session) reach the core through
  // an in-process origin, so each of their requests and reconnects wakes it;
  // they go on through the current remote core's own in-process origin, which
  // presents the backend credential.
  const serverUrl = `http://jazz-vercel-${randomUUID()}.invalid`;
  const unregister = registerInProcessTransport(serverUrl, {
    fetch: async (input, init) => {
      const request = new Request(input, init);
      const url = new URL(request.url);
      const inner = (await ensure()).core.serverUrl;
      return fetchFor(inner)(new Request(`${inner}${url.pathname}${url.search}`, request));
    },
    WebSocket: wakingWebSocket(async () => (await ensure()).core.serverUrl),
  });

  return {
    appId: first.appId,
    serverUrl,
    adminSecret: first.core.adminSecret,
    backendSecret: first.core.backendSecret,
    async handle(method, path, headers, body, access) {
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
            inner = core.openSocket(
              headers,
              (error, event) => {
                if (error || event.kind === "close") done();
                onEvent(error, event);
              },
              access,
            );
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

/** A WebSocket that wakes the core, then connects to wherever it now runs. */
function wakingWebSocket(resolveOrigin: () => Promise<string>): WebSocketConstructor {
  return class WakingSocket implements BrowserWebSocket {
    binaryType: "arraybuffer" | "blob" = "arraybuffer";
    readyState = 0;
    private socket: WebSocket | undefined;
    private readonly listeners = new Map<string, Array<(event?: any) => void>>();

    constructor(url: string) {
      const path = new URL(url).pathname + new URL(url).search;
      resolveOrigin().then(
        (origin) => {
          if (this.readyState >= 2) return;
          const Socket = inProcessWebSocketFor(origin) ?? globalThis.WebSocket;
          const socket = new Socket(`${origin.replace(/^http/, "ws")}${path}`) as WebSocket;
          socket.binaryType = "arraybuffer";
          this.socket = socket;
          socket.addEventListener("open", () => {
            this.readyState = 1;
            this.emit("open");
          });
          socket.addEventListener("message", (event) => this.emit("message", { data: event.data }));
          socket.addEventListener("error", (event) => this.emit("error", event));
          socket.addEventListener("close", (event) => this.finish(event.code, event.reason));
        },
        (error: unknown) => {
          this.emit("error", error);
          this.finish(1011, "core unavailable");
        },
      );
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
      this.finish(1000, "client closed");
    }

    private emit(type: string, event?: unknown): void {
      for (const listener of this.listeners.get(type) ?? []) listener(event);
    }

    private finish(code: number, reason: string): void {
      if (this.readyState >= 3) return;
      this.readyState = 3;
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
