// Runs a Jazz Durable Object Worker in local workerd (Miniflare), the way a
// deployment binds it: `JAZZ` (class `JazzCore`, SQLite-backed) and
// `JAZZ_CORE_SECRET`. No Cloudflare account involved.
import { readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare, type WorkerOptions } from "miniflare";

const here = dirname(fileURLToPath(import.meta.url));

export interface DurableObjectCore {
  /** The Worker's origin, on a loopback port. */
  readonly url: string;
  readonly secret: string;
  readonly miniflare: Miniflare;
  /** The app id the Worker's object keeps (`GET /app`). */
  appId(): Promise<string>;
  evict(): Promise<void>;
  dispose(): Promise<void>;
}

export async function startDurableObjectCore(
  options: {
    /** The Worker module (default: the mock core). */
    script?: string;
    /** The Worker's `JAZZ_CORE_SECRET`; `null` for none (the object makes its own). */
    secret?: string | null;
    /** A directory the object's storage outlives a restart in. */
    persist?: string;
    /** The app's backend object too (class `JazzBackend`, as `JAZZ_BACKEND`). */
    backend?: boolean;
    /** More bindings for the Worker (test switches). */
    bindings?: Record<string, string>;
    outboundService?: WorkerOptions["outboundService"];
  } = {},
): Promise<DurableObjectCore> {
  const script = options.script ?? join(here, "mock-core.mjs");
  const secret =
    options.secret === null ? "" : (options.secret ?? "do-test-core-secret-".repeat(2));
  const miniflare = new Miniflare({
    // Each module listed, as wrangler uploads a bundle: Miniflare's own
    // scan refuses the bundle's non-literal dynamic imports (Node-only
    // fallbacks a Worker never runs).
    modulesRoot: dirname(script),
    modules: workerModules(script),
    compatibilityDate: "2026-04-01",
    durableObjects: {
      JAZZ: { className: "JazzCore", useSQLite: true },
      ...(options.backend ? { JAZZ_BACKEND: { className: "JazzBackend", useSQLite: true } } : {}),
    },
    compatibilityFlags: ["nodejs_compat"],
    ...(options.persist ? { durableObjectsPersist: options.persist } : {}),
    outboundService: options.outboundService,
    bindings: { ...options.bindings, ...(secret && { JAZZ_CORE_SECRET: secret }) },
    host: "127.0.0.1",
    port: 0,
  });
  const url = (await miniflare.ready).origin;
  return {
    url,
    secret,
    miniflare,
    async appId() {
      const response = await fetch(`${url}/app`, {
        headers: { "x-jazz-backend-credential": secret },
      });
      if (!response.ok) throw new Error(`GET /app: ${response.status}`);
      return ((await response.json()) as { appId: string }).appId;
    },
    // The unnamed Worker routes core traffic to JAZZ.idFromName("jazz").
    evict: () =>
      miniflare.unsafeEvictDurableObject("", "JazzCore", { name: "jazz", webSockets: "hibernate" }),
    dispose: () => miniflare.dispose(),
  };
}

/** The Worker's modules: `script` first, then the other modules beside it. */
export function workerModules(script: string) {
  const dir = dirname(script);
  const others = readdirSync(dir, { recursive: true }) as string[];
  const modules = [{ type: "ESModule" as const, path: script }];
  for (const file of others) {
    const path = join(dir, file);
    if (path === script) continue;
    if (/\.m?js$/.test(file)) modules.push({ type: "ESModule", path });
    else if (file.endsWith(".wasm")) modules.push({ type: "CompiledWasm" as never, path });
  }
  return modules;
}
