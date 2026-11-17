// Runs a Jazz Durable Object Worker in local workerd (Miniflare), the way a
// deployment binds it: `JAZZ` (class `JazzCore`, SQLite-backed) and
// `JAZZ_CORE_SECRET`. No Cloudflare account involved.
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const here = dirname(fileURLToPath(import.meta.url));

export interface DurableObjectCore {
  /** The Worker's origin, on a loopback port. */
  readonly url: string;
  readonly secret: string;
  readonly miniflare: Miniflare;
  /** The app id the Worker's object keeps (`GET /app`). */
  appId(): Promise<string>;
  dispose(): Promise<void>;
}

export async function startDurableObjectCore(
  options: {
    /** The Worker module (default: the mock core). */
    script?: string;
    secret?: string;
    /** A directory the object's storage outlives a restart in. */
    persist?: string;
  } = {},
): Promise<DurableObjectCore> {
  const script = options.script ?? join(here, "mock-core.mjs");
  const secret = options.secret ?? "do-test-core-secret-".repeat(2);
  const miniflare = new Miniflare({
    modules: true,
    scriptPath: script,
    modulesRoot: dirname(script),
    modulesRules: [
      { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
      { type: "CompiledWasm", include: ["**/*.wasm"] },
    ],
    compatibilityDate: "2026-04-01",
    durableObjects: { JAZZ: { className: "JazzCore", useSQLite: true } },
    ...(options.persist ? { durableObjectsPersist: options.persist } : {}),
    bindings: { JAZZ_CORE_SECRET: secret },
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
    dispose: () => miniflare.dispose(),
  };
}
