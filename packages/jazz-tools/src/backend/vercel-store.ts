// The `vercel()` store: the app's core runs in a persistent Vercel Sandbox
// in the app's own Vercel project, with its data on a Drive. The backend
// creates the Sandbox on first use, wakes it when a request needs it and lets
// it stop when the app goes quiet. The Sandbox SDK authenticates with the
// function's own OIDC token, so the app sets no Jazz env vars: the core's
// secrets are generated inside the Sandbox and read back through the SDK.
// Wake handling is adapted from garden-co/infra's v2 sync proxy (ADR 0004).

import { createHash, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import type { JazzCoreSocketEvent } from "jazz-napi";
import { registerInProcessTransport } from "../runtime/in-process-transport.js";
import type {
  BrowserWebSocket,
  WebSocketConstructor,
} from "../runtime/native-runtime/websocket.js";

export const VERCEL_CORE_PORT = 1625;

export interface VercelStoreOptions {
  /**
   * Sandbox name. Defaults to `jazz-<VERCEL_PROJECT_ID>-production` for
   * production and `jazz-<VERCEL_PROJECT_ID>-preview-<branch>-<hash>` for
   * each Git branch's previews.
   */
  name?: string;
  /**
   * The core's app id. Defaults to one the core generates on its Drive at
   * first start and keeps, like its secrets. Set it only to keep the id of
   * an existing app.
   */
  appId?: string;
  /** Sandbox region. Defaults to `iad1`. Fixed when the Sandbox is created. */
  region?: string;
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
   * Where the core's server binary comes from: an npm tarball of
   * `jazz-tools`. Defaults to the registry's tarball of the installed version.
   */
  serverTarballUrl?: string;
  /** Vercel credentials for scripts outside a Vercel function. Defaults to the function's OIDC token. */
  credentials?: { token: string; teamId: string; projectId: string };
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
  /** The app id the core runs as (see `VercelStoreOptions.appId`). */
  readonly appId?: string;
  readonly serverUrl: string;
  readonly adminSecret: string;
  readonly backendSecret: string;
  handle(
    method: string,
    path: string,
    headers: string[][],
    body: Uint8Array,
  ): Promise<{ status: number; headers: string[][]; body: Uint8Array }>;
  openSocket(
    headers: string[][],
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
  ): { send(data: Uint8Array): boolean; close(): void };
  close(): Promise<void>;
}

/** A core reached at `url` with these secrets: `remote()`'s core. */
export type OpenRemote = (target: {
  appId: string;
  url: string;
  adminSecret: string;
  backendSecret: string;
}) => VercelCore;

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
 * Start the core: install the server binary for `version` on the Drive,
 * generate the app id and secrets once, and run the newest server version
 * this Drive has seen, so an instance from an older deployment never
 * downgrades data a newer one already wrote. A different running version is
 * stopped first; the same one is left alone.
 */
export function startScript(
  appId: string | undefined,
  version: string,
  tarballUrl: string,
): string {
  const q = (s: string) => `'${s.replaceAll("'", `'\\''`)}'`;
  return `#!/bin/bash
# Written by jazz-tools' vercel() store for each server start.
set -euo pipefail
# Starts are serialized; the server itself runs without the lock.
exec 9>/tmp/jazz.lock
flock 9
# Semver order: GNU sort -V with "~" sorts a prerelease before its release.
newer() {
  [ "$1" != "$2" ] && [ "$(printf '%s\\n%s\\n' "\${1/-/\\~}" "\${2/-/\\~}" | sort -V | tail -1)" = "\${1/-/\\~}" ]
}
version=${q(version)}
highest=$(cat /data/version 2>/dev/null || true)
if [ -n "$highest" ] && [ -x "/data/bin/jazz-tools-$highest" ] && newer "$highest" "$version"; then
  version=$highest
fi
if [ -f /tmp/jazz.pid ] && kill -0 "$(cat /tmp/jazz.pid)" 2>/dev/null; then
  [ "$(cat /tmp/jazz.version 2>/dev/null)" = "$version" ] && exit 0
  pid=$(cat /tmp/jazz.pid)
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 150); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
  kill -KILL "$pid" 2>/dev/null || true
fi
bin=/data/bin/jazz-tools-$version
if [ ! -x "$bin" ]; then
  mkdir -p /data/bin
  # The Sandbox image has node but no curl.
  node -e 'fetch(process.argv[1]).then(async (r) => { if (!r.ok) throw new Error(r.status); process.stdout.write(Buffer.from(await r.arrayBuffer())); })' ${q(tarballUrl)} \\
    | tar xz -O package/bin/native/jazz-tools-linux-x64 > "$bin.tmp"
  chmod 755 "$bin.tmp" && mv "$bin.tmp" "$bin"
fi
if [ ! -f /data/secrets.env ]; then
  umask 077
  secret() { head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '='; }
  app_id=${appId ? q(appId) : "$(cat /proc/sys/kernel/random/uuid)"}
  printf 'JAZZ_APP_ID=%s\\nJAZZ_ADMIN_SECRET=%s\\nJAZZ_BACKEND_SECRET=%s\\n' "$app_id" "$(secret)" "$(secret)" > /data/secrets.env.tmp
  mv /data/secrets.env.tmp /data/secrets.env
fi
set -a; . /data/secrets.env; set +a
export NODE_ENV=production
mkdir -p /data/store
printf '%s\n' "$version" > /data/version.tmp && mv /data/version.tmp /data/version
# Older servers are never started again (see above).
find /data/bin -name 'jazz-tools-*' ! -name "jazz-tools-$version" -delete
printf '%s\n' "$version" > /tmp/jazz.version
echo $$ > /tmp/jazz.pid
exec 9>&-
exec "$bin" server "$JAZZ_APP_ID" --port ${VERCEL_CORE_PORT} --data-dir /data/store
`;
}

function parseSecrets(text: string): {
  appId: string;
  adminSecret: string;
  backendSecret: string;
} {
  const values = new Map(
    text
      .split("\n")
      .map((line) => line.split("=", 2) as [string, string])
      .filter(([, value]) => value),
  );
  const adminSecret = values.get("JAZZ_ADMIN_SECRET");
  const backendSecret = values.get("JAZZ_BACKEND_SECRET");
  const appId = values.get("JAZZ_APP_ID");
  if (!appId || !adminSecret || !backendSecret) {
    throw new Error("vercel(): the core's secrets are missing");
  }
  return { appId, adminSecret, backendSecret };
}

/** Whether this process runs in a Vercel deployment (not `vercel dev` or a pulled env). */
export function onVercelDeployment(env: Record<string, string | undefined> = process.env): boolean {
  return !!env.VERCEL && (env.VERCEL_ENV === "production" || env.VERCEL_ENV === "preview");
}

/**
 * The core for this deployment: one for production, and one per Git branch
 * for previews, so a branch's schema and permissions never apply to
 * another branch's data.
 */
export function defaultCoreName(project: string, env: Record<string, string | undefined>): string {
  const target = env.VERCEL_ENV ?? "development";
  if (target !== "preview") return `jazz-${project}-${target}`;
  const branch = env.VERCEL_GIT_COMMIT_REF ?? env.VERCEL_DEPLOYMENT_ID ?? "unknown";
  const slug = branch
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 24);
  const hash = createHash("sha256").update(branch).digest("hex").slice(0, 8);
  return `jazz-${project}-preview-${slug}-${hash}`;
}

async function installedVersion(): Promise<string> {
  const packageJson = JSON.parse(
    await readFile(new URL("../../package.json", import.meta.url), "utf8"),
  ) as { version: string };
  return packageJson.version;
}

export interface OpenVercelCoreOptions {
  /** Used when the store has no `appId` of its own. */
  appId?: string;
  options: VercelStoreOptions;
  openRemote: OpenRemote;
  sdk?: SandboxSdk;
  version?: string;
  env?: Record<string, string | undefined>;
}

export async function openVercelCore(params: OpenVercelCoreOptions): Promise<VercelCore> {
  const { options, openRemote } = params;
  const appId = options.appId ?? params.appId;
  const env = params.env ?? process.env;
  const sdk =
    params.sdk ??
    options.sdk ??
    ((await import(/* @vite-ignore */ "@vercel/sandbox" as string).catch(() => {
      throw new Error("vercel() needs the @vercel/sandbox package: npm install @vercel/sandbox");
    })) as SandboxSdk);
  const version = params.version ?? (await installedVersion());
  const tarballUrl =
    options.serverTarballUrl ?? `https://registry.npmjs.org/jazz-tools/-/jazz-tools-${version}.tgz`;
  const project = options.credentials?.projectId ?? env.VERCEL_PROJECT_ID;
  if (!options.name && !project) {
    throw new Error(
      "vercel(): VERCEL_PROJECT_ID is not set; enable system environment variables or pass a name",
    );
  }
  const name = options.name ?? defaultCoreName(project!, env);
  const creds = options.credentials ?? {};
  const idleMs = options.idleMs ?? DEFAULT_IDLE_MS;
  const script = Buffer.from(startScript(appId, version, tarballUrl));

  const startServer = async (sbx: SandboxHandle) => {
    // A fresh path each time: bash reads a script as it runs, so one that is
    // running must never be rewritten.
    const path = `/tmp/jazz-start-${randomUUID()}.sh`;
    await sbx.writeFiles([{ path, content: script }]);
    await sbx.runCommand({
      cmd: "bash",
      args: ["-lc", `exec bash ${path} >>/data/server.log 2>&1`],
      detached: true,
    });
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

  /** Resume (or create) the Sandbox and wait until the server answers. */
  const wake = async (): Promise<{ sbx: SandboxHandle; base: string }> => {
    const t0 = performance.now();
    const deadline = t0 + WAKE_DEADLINE_MS;
    const healthFails: Record<string, number> = {};
    let recycled = false;
    let restarted = false;
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
      let base = sbx.domain(VERCEL_CORE_PORT);
      let ok = await healthy(base, 300, healthFails);
      if (!ok) await startServer(sbx);
      const healthT0 = performance.now();
      while (!ok && sbx && performance.now() < deadline) {
        ok = await healthy(base, 5_000, healthFails);
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
          await sleep(250);
        }
      }
      if (performance.now() > deadline) {
        throw new Error(
          `vercel(): core ${name} did not become healthy in ${since(t0)} ms (${JSON.stringify(healthFails)})`,
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

  const ensure = (): Promise<NonNullable<typeof live>> => {
    lastActivity = Date.now();
    if (live && Date.now() - live.checkedAt < ENSURE_FRESH_MS) return Promise.resolve(live);
    pending ??= (async () => {
      try {
        if (live && (await getRunning())) {
          live.checkedAt = Date.now();
          return live;
        }
        const { sbx, base } = await wake();
        const secrets =
          live?.base === base
            ? {
                appId: live.appId,
                adminSecret: live.core.adminSecret,
                backendSecret: live.core.backendSecret,
              }
            : parseSecrets(
                (await sbx.readFileToBuffer({ path: "/data/secrets.env" }))?.toString("utf8") ?? "",
              );
        if (appId && secrets.appId !== appId) {
          throw new Error(`vercel(): core ${name} belongs to app ${secrets.appId}, not ${appId}`);
        }
        const replaced = live?.core;
        live = {
          sbx,
          base,
          appId: secrets.appId,
          core: openRemote({
            appId: secrets.appId,
            url: base,
            adminSecret: secrets.adminSecret,
            backendSecret: secrets.backendSecret,
          }),
          checkedAt: Date.now(),
        };
        if (replaced) void replaced.close().catch(() => {});
        return live;
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
  // an in-process origin, so each of their requests and reconnects wakes it.
  const serverUrl = `http://jazz-vercel-${randomUUID()}.invalid`;
  const unregister = registerInProcessTransport(serverUrl, {
    fetch: async (input, init) => {
      const request = new Request(input, init);
      const url = new URL(request.url);
      const { base } = await ensure();
      return fetch(new Request(`${base}${url.pathname}${url.search}`, request));
    },
    WebSocket: wakingWebSocket(async () => (await ensure()).base),
  });

  return {
    appId: first.appId,
    serverUrl,
    adminSecret: first.core.adminSecret,
    backendSecret: first.core.backendSecret,
    async handle(method, path, headers, body) {
      return (await ensure()).core.handle(method, path, headers, body);
    },
    openSocket(headers, onEvent) {
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
            inner = core.openSocket(headers, (error, event) => {
              if (error || event.kind === "close") done();
              onEvent(error, event);
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

/** A WebSocket that wakes the core, then connects to wherever it now runs. */
function wakingWebSocket(resolveBase: () => Promise<string>): WebSocketConstructor {
  return class WakingSocket implements BrowserWebSocket {
    binaryType: "arraybuffer" | "blob" = "arraybuffer";
    readyState = 0;
    private socket: WebSocket | undefined;
    private readonly listeners = new Map<string, Array<(event?: any) => void>>();

    constructor(url: string) {
      const path = new URL(url).pathname + new URL(url).search;
      resolveBase().then(
        (base) => {
          if (this.readyState >= 2) return;
          const socket = new globalThis.WebSocket(`${base.replace(/^http/, "ws")}${path}`);
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
