// The `vercel()` store: the app's core runs in a persistent Vercel Sandbox
// in the app's own Vercel project, with its data on a Drive. The backend
// creates the Sandbox on first use, wakes it when a request needs it and lets
// it stop when the app goes quiet. The Sandbox SDK authenticates with the
// function's own OIDC token, so the app sets no Jazz env vars: the core's
// secrets are generated inside the Sandbox and read back through the SDK.
// Wake handling is adapted from garden-co/infra's v2 sync proxy (ADR 0004).

import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import type { JazzCoreSocketEvent } from "jazz-napi";
import { registerInProcessTransport } from "../runtime/in-process-transport.js";
import type {
  BrowserWebSocket,
  WebSocketConstructor,
} from "../runtime/native-runtime/websocket.js";

export const VERCEL_CORE_PORT = 1625;

export interface VercelStoreOptions {
  /** Sandbox name. Defaults to `jazz-<appId>-<VERCEL_ENV>`, so previews share one core. */
  name?: string;
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
    await sleep(20);
  }
  return false;
}

/** Install the server binary for `version` on the Drive, generate secrets once, run. */
export function startScript(appId: string, version: string, tarballUrl: string): string {
  const q = (s: string) => `'${s.replaceAll("'", `'\\''`)}'`;
  return `#!/bin/bash
# Written by jazz-tools' vercel() store before each server start.
set -euo pipefail
bin=/data/bin/jazz-tools-${version}
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
  printf 'JAZZ_ADMIN_SECRET=%s\\nJAZZ_BACKEND_SECRET=%s\\n' "$(secret)" "$(secret)" > /data/secrets.env.tmp
  mv /data/secrets.env.tmp /data/secrets.env
fi
set -a; . /data/secrets.env; set +a
export NODE_ENV=production
mkdir -p /data/store
exec "$bin" server ${q(appId)} --port ${VERCEL_CORE_PORT} --data-dir /data/store
`;
}

function parseSecrets(text: string): {
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
  if (!adminSecret || !backendSecret) throw new Error("vercel(): the core's secrets are missing");
  return { adminSecret, backendSecret };
}

async function installedVersion(): Promise<string> {
  const packageJson = JSON.parse(
    await readFile(new URL("../../package.json", import.meta.url), "utf8"),
  ) as { version: string };
  return packageJson.version;
}

export interface OpenVercelCoreOptions {
  appId: string;
  options: VercelStoreOptions;
  openRemote: OpenRemote;
  sdk?: SandboxSdk;
  version?: string;
  env?: Record<string, string | undefined>;
}

export async function openVercelCore(params: OpenVercelCoreOptions): Promise<VercelCore> {
  const { appId, options, openRemote } = params;
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
  const name = options.name ?? `jazz-${appId}-${env.VERCEL_ENV ?? "development"}`;
  const creds = options.credentials ?? {};
  const idleMs = options.idleMs ?? DEFAULT_IDLE_MS;
  const script = Buffer.from(startScript(appId, version, tarballUrl));

  const startServer = async (sbx: SandboxHandle) => {
    await sbx.writeFiles([{ path: "/data/start.sh", content: script }]);
    // Several instances may wake the same core; only the lock holder starts it.
    await sbx.runCommand({
      cmd: "bash",
      args: ["-lc", "exec flock -n /tmp/jazz.lock bash /data/start.sh >>/data/server.log 2>&1"],
      detached: true,
    });
  };

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
          await sbx.stop().catch(() => {});
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
      if (ok && sbx) return { sbx, base };
      if (performance.now() > deadline) {
        throw new Error(
          `vercel(): core ${name} did not become healthy in ${since(t0)} ms (${JSON.stringify(healthFails)})`,
        );
      }
    }
  };

  let live: { sbx: SandboxHandle; base: string; core: VercelCore; checkedAt: number } | undefined;
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
                adminSecret: live.core.adminSecret,
                backendSecret: live.core.backendSecret,
              }
            : parseSecrets(
                (await sbx.readFileToBuffer({ path: "/data/secrets.env" }))?.toString("utf8") ?? "",
              );
        live = {
          sbx,
          base,
          core: openRemote({ url: base, ...secrets }),
          checkedAt: Date.now(),
        };
        return live;
      } finally {
        pending = undefined;
      }
    })();
    return pending;
  };

  // Keep the core awake while browsers are connected or requests are recent;
  // once quiet, its session timeout stops it.
  const keepalive = setInterval(() => {
    if (!live) return;
    if (sockets === 0 && Date.now() - lastActivity > idleMs) return;
    const session = live.sbx.currentSession();
    const left = session.startedAt.getTime() + session.timeout - Date.now();
    if (left < idleMs) session.extendTimeout(idleMs).catch(() => {});
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
          inner = core.openSocket(headers, (error, event) => {
            if (error || event.kind === "close") done();
            onEvent(error, event);
          });
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
