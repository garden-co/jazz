import { createRequire } from "node:module";
import { resolve } from "node:path";
import { loadEnvFileIntoProcessEnv, resolveViteEnvDir, type ViteEnvConfig } from "./env-file.js";
import { buildInspectorLink } from "./inspector-link.js";
import { wireInspectorOverlay, type OverlayDevServer } from "./inspector-overlay/serve.js";
import { ManagedDevRuntime } from "./managed-runtime.js";
import { RuntimeOwners } from "./runtime-owners.js";
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

export interface JazzServerOptions {
  port?: number;
  /** Concrete IP address used for both listener binding and the advertised URL. */
  host?: string;
  adminSecret?: string;
  appId?: string;
  allowLocalFirstAuth?: boolean;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  jwtIssuer?: string;
  jwtAudience?: string;
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
// module zero-dep on Vite's public types. Exported for sibling plugins
// (./sveltekit.ts) to share, so the shape is maintained in one place.
export interface ViteDevServer {
  config: {
    root: string;
    command: string;
    mode?: string;
    /** Vite resolves this from the user config before configureServer runs. */
    envDir?: string | false;
    env?: Record<string, string>;
    server?: {
      port?: number;
      host?: string | boolean;
      https?: unknown;
    };
  };
  httpServer: { once(event: string, cb: () => void): void } | null;
  middlewares?: OverlayDevServer["middlewares"];
  ws: {
    send(payload: { type: string; err?: { message: string; stack?: string } }): void;
  };
  restart?(forceOptimize?: boolean): Promise<void>;
}

// Vite only surfaces VITE_*-prefixed vars to the client bundle, so the
// scaffolder writes the two client-facing keys under the VITE_ prefix.
// Use the same names here so process.env lookups match what's in `.env`.
//
// The runtime is process-wide so the plugin instances Vite creates on a
// dev-server restart adopt the running runtime instead of racing the old
// server for its port or attaching to a URL that is about to be stopped.
const runtime = new ManagedDevRuntime({
  appId: "VITE_JAZZ_APP_ID",
  serverUrl: "VITE_JAZZ_SERVER_URL",
  telemetryCollectorUrl: "VITE_JAZZ_TELEMETRY_COLLECTOR_URL",
});
const owners = new RuntimeOwners<ViteDevServer>(() => runtime.dispose());

export function jazzPlugin(options: JazzPluginOptions = {}) {
  // Identity of this plugin instance in the shared runtime's owner set.
  const holder = {};
  let envLoaded = false;

  async function ensureEnvLoaded(envDir: string | false, mode: string): Promise<void> {
    if (envLoaded) return;
    await loadEnvFileIntoProcessEnv(envDir, mode);
    envLoaded = true;
  }

  return {
    name: "jazz",

    config(
      config: {
        root?: string;
        envDir?: ViteEnvConfig["envDir"];
        envFile?: ViteEnvConfig["envFile"];
        ssr?: { external?: true | string[] };
        optimizeDeps?: { exclude?: string[] };
      },
      env?: { command?: string; mode?: string },
    ) {
      const existingSsr = config.ssr?.external;
      const existingExclude = config.optimizeDeps?.exclude ?? [];
      const jazzWasmEntry = resolveJazzWasmEntry();
      // `ssr.external: true` means "externalize everything", so jazz-napi is
      // already covered — preserve the bool rather than coercing to an array.
      const ssrExternal: true | string[] =
        existingSsr === true ? true : Array.from(new Set([...(existingSsr ?? []), "jazz-napi"]));
      const merged = {
        optimizeDeps: { exclude: Array.from(new Set([...existingExclude, "jazz-wasm"])) },
        ssr: { external: ssrExternal },
        ...(jazzWasmEntry
          ? { resolve: { alias: [{ find: /^jazz-wasm$/, replacement: jazzWasmEntry }] } }
          : {}),
      };
      if (env?.command !== "serve" || options.server === false) {
        return merged;
      }
      const root = resolve(config.root ?? process.cwd());
      return ensureEnvLoaded(resolveViteEnvDir(root, config), env.mode ?? "development").then(
        () => {
          runtime.prepareEnv({
            ...options,
            schemaDir: options.schemaDir ?? root,
          });
          return merged;
        },
      );
    },

    async configureServer(viteServer: ViteDevServer) {
      if (viteServer.config.command !== "serve") return;

      if (options.server === false) {
        return;
      }

      // Vite does not populate process.env from env files for unprefixed keys,
      // so the managed runtime's env-driven cloud-mode check would otherwise
      // never fire. The config hook normally loads them first; this is the
      // fallback for direct callers that skip that hook.
      await ensureEnvLoaded(
        viteServer.config.envDir ?? viteServer.config.root,
        viteServer.config.mode ?? "development",
      );

      const schemaDir = options.schemaDir ?? viteServer.config.root;

      let managed;
      await owners.acquire(holder);
      try {
        managed = await runtime.initialize({
          ...options,
          schemaDir,
          // The runtime keeps this callback across Vite restarts, so it
          // resolves the live dev servers from the owner set on every push.
          onSchemaPush: () => {
            for (const server of owners.servers()) server.ws.send({ type: "full-reload" });
          },
        });
      } catch (error) {
        await owners.release(holder).catch(() => undefined);
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
      owners.attachServer(holder, viteServer);

      viteServer.config.env ??= {};
      viteServer.config.env.VITE_JAZZ_APP_ID = managed.appId;
      viteServer.config.env.VITE_JAZZ_SERVER_URL = managed.serverUrl;
      if (managed.telemetryCollectorUrl) {
        viteServer.config.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL = managed.telemetryCollectorUrl;
      }
      console.log(
        `${LOG_PREFIX} Open the inspector: ${buildInspectorLink(managed.serverUrl, managed.appId)}`,
      );

      if (options.inspector !== false) wireInspectorOverlay(viteServer);

      // On a restart Vite closes the old httpServer only after the new
      // server's plugins have adopted the runtime; the last owner disposes it.
      viteServer.httpServer?.once("close", async () => {
        await owners.release(holder);
      });
    },
  };
}

export async function __resetJazzVitePluginForTests(): Promise<void> {
  owners.resetForTests();
  await runtime.resetForTests();
}
