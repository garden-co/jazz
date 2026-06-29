import { createRequire } from "node:module";
import { loadEnvFileIntoProcessEnv } from "./env-file.js";
import { wireInspectorOverlay, type OverlayDevServer } from "./inspector-overlay/serve.js";
import { ManagedDevRuntime } from "./managed-runtime.js";
import type { TelemetryOptions } from "../runtime/sync-telemetry.js";

// jazz-tools contains a dynamic `import("jazz-wasm")` that we intentionally
// keep out of Vite's dep optimizer (wasm-bindgen output breaks esbuild's
// pre-bundling). With pnpm's strict install layout, a bare `jazz-wasm`
// specifier left in a consumer bundle won't resolve at runtime because the
// package isn't hoisted to the project root. We resolve jazz-wasm from this
// module's location — where it IS a direct dependency of jazz-tools — and
// return it as an absolute path, so the plugin can alias the bare specifier
// without forcing the consumer to add jazz-wasm to their own package.json.
export function resolveJazzWasmEntry(): string | null {
  try {
    return createRequire(import.meta.url).resolve("jazz-wasm");
  } catch {
    return null;
  }
}

export interface JazzViteUserConfig {
  ssr?: { external?: true | string[] };
  optimizeDeps?: { exclude?: string[] };
}

// Shared Vite config merge used by both the Vite and SvelteKit plugins so the
// wasm/ssr/optimizeDeps shape is maintained in one place.
export function buildJazzViteConfig(config: JazzViteUserConfig) {
  const existingSsr = config.ssr?.external;
  const existingExclude = config.optimizeDeps?.exclude ?? [];
  const jazzWasmEntry = resolveJazzWasmEntry();
  // `ssr.external: true` means "externalize everything", so jazz-napi is
  // already covered — preserve the bool rather than coercing to an array.
  const ssrExternal: true | string[] =
    existingSsr === true ? true : Array.from(new Set([...(existingSsr ?? []), "jazz-napi"]));
  return {
    worker: { format: "es" as const },
    optimizeDeps: { exclude: Array.from(new Set([...existingExclude, "jazz-wasm"])) },
    ssr: { external: ssrExternal },
    ...(jazzWasmEntry
      ? { resolve: { alias: [{ find: /^jazz-wasm$/, replacement: jazzWasmEntry }] } }
      : {}),
  };
}

export interface JazzServerOptions {
  port?: number;
  adminSecret?: string;
  appId?: string;
  allowLocalFirstAuth?: boolean;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
}

export interface JazzPluginOptions {
  server?: boolean | string | JazzServerOptions;
  adminSecret?: string;
  schemaDir?: string;
  appId?: string;
  telemetry?: TelemetryOptions;
  /**
   * The in-app inspector overlay (a floating toggle that opens the embedded
   * inspector) is served during dev by default. Set to `false` to disable it.
   */
  inspector?: boolean;
}

const LOG_PREFIX = "[jazz]";

// Minimal subset of Vite's ViteDevServer — redeclared here to keep this
// module zero-dep on Vite's public types. Extends OverlayDevServer so the
// middleware shape is shared with ./sveltekit.ts in one place.
export interface ViteDevServer extends OverlayDevServer {
  config: {
    root: string;
    command: string;
    env?: Record<string, string>;
    server?: {
      port?: number;
      host?: string | boolean;
      https?: unknown;
    };
  };
  httpServer: { once(event: string, cb: () => void): void } | null;
  ws: {
    send(payload: { type: string; err?: { message: string; stack?: string } }): void;
  };
  restart?(forceOptimize?: boolean): Promise<void>;
}

export function jazzPlugin(options: JazzPluginOptions = {}) {
  // Vite only surfaces VITE_*-prefixed vars to the client bundle, so the
  // scaffolder writes the two client-facing keys under the VITE_ prefix.
  // Use the same names here so process.env lookups match what's in `.env`.
  const runtime = new ManagedDevRuntime({
    appId: "VITE_JAZZ_APP_ID",
    serverUrl: "VITE_JAZZ_SERVER_URL",
    telemetryCollectorUrl: "VITE_JAZZ_TELEMETRY_COLLECTOR_URL",
  });

  return {
    name: "jazz",

    config(config: JazzViteUserConfig) {
      return buildJazzViteConfig(config);
    },

    async configureServer(viteServer: ViteDevServer) {
      if (viteServer.config.command !== "serve") return;

      if (options.server === false) {
        return;
      }

      // Vite does not populate process.env from .env for unprefixed
      // keys, so the managed runtime's env-driven cloud-mode check would
      // otherwise never fire. Backfill before reading.
      loadEnvFileIntoProcessEnv(viteServer.config.root);

      const schemaDir = options.schemaDir ?? viteServer.config.root;

      let managed;
      try {
        managed = await runtime.initialize({
          ...options,
          schemaDir,
          onSchemaPush: () => {
            viteServer.ws.send({ type: "full-reload" });
          },
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        viteServer.ws.send({
          type: "error",
          err: {
            message: `${LOG_PREFIX} initialization failed: ${message}`,
            stack: error instanceof Error ? error.stack : undefined,
          },
        });
        return;
      }

      // Vite only exposes VITE_*-prefixed keys to the client bundle via
      // import.meta.env. process.env gets the same values via the managed
      // runtime's own write below.
      viteServer.config.env ??= {};
      viteServer.config.env.VITE_JAZZ_APP_ID = managed.appId;
      viteServer.config.env.VITE_JAZZ_SERVER_URL = managed.serverUrl;
      if (managed.telemetryCollectorUrl) {
        viteServer.config.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL = managed.telemetryCollectorUrl;
      }
      if (options.inspector !== false) wireInspectorOverlay(viteServer);

      viteServer.httpServer?.once("close", async () => {
        await runtime.dispose();
      });
    },
  };
}
