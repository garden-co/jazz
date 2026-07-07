import { join, resolve } from "node:path";
import { loadEnvFileIntoProcessEnv } from "./env-file.js";
import { wireInspectorOverlay } from "./inspector-overlay/serve.js";
import { ManagedDevRuntime, type ManagedRuntime } from "./managed-runtime.js";
import { buildJazzViteConfig } from "./vite.js";
import type {
  JazzServerOptions as BaseJazzServerOptions,
  JazzPluginOptions as BaseJazzPluginOptions,
  ViteDevServer,
} from "./vite.js";

const LOG_PREFIX = "[jazz]";

// SvelteKit extends the shared Vite-plugin options with server-side concerns.
// backendSecret is injected into process.env so server-side SvelteKit code
// (+page.server.ts, +server.ts, hooks) can read it. A Vite+React SPA has no
// server side and would have nothing to consume the env var, which is why
// the field lives here and not on the base Vite plugin options.
export interface JazzServerOptions extends BaseJazzServerOptions {
  backendSecret?: string;
}

export interface JazzPluginOptions extends Omit<BaseJazzPluginOptions, "server"> {
  server?: boolean | string | JazzServerOptions;
}

interface ViteServerConfigLike {
  port?: number;
  host?: string | boolean;
  https?: unknown;
}

interface ViteUserConfigLike {
  root?: string;
  server?: ViteServerConfigLike;
  ssr?: { external?: true | string[] };
  optimizeDeps?: { exclude?: string[] };
}

interface ViteConfigEnvLike {
  command: "build" | "serve";
  mode?: string;
}

function resolveOrigin(server: ViteServerConfigLike | undefined): string {
  const port = server?.port ?? 5173;
  const hostOpt = server?.host;
  // Vite's `server.host` accepts boolean (true = listen on all, undefined/false
  // = localhost) or a string hostname. Only use the string form for building
  // the origin — the boolean form still means the dev URL is localhost.
  const host = typeof hostOpt === "string" ? hostOpt : "localhost";
  const scheme = server?.https ? "https" : "http";
  return `${scheme}://${host}:${port}`;
}

const runtime = new ManagedDevRuntime({
  appId: "PUBLIC_JAZZ_APP_ID",
  serverUrl: "PUBLIC_JAZZ_SERVER_URL",
  telemetryCollectorUrl: "PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
});

export function jazzSvelteKit(options: JazzPluginOptions = {}) {
  // Set once configureServer runs. The schema watcher's reload/error callbacks
  // read it lazily: the runtime is started in the `config` hook (before any
  // dev server exists), but watch pushes only fire later, by which point this
  // is populated. Initial-push callbacks during `config` see `null` and no-op,
  // which is correct — there's no browser to reload yet.
  let viteServerRef: ViteDevServer | null = null;
  let managed: ManagedRuntime | null = null;

  function buildInitOptions(serverConfig: ViteServerConfigLike | undefined, root: string) {
    const schemaDir = options.schemaDir ?? join(root, "src", "lib");
    const serverOpt = options.server ?? true;

    // Extract backendSecret from the server config object (SvelteKit-only option).
    const serverConfigObj: JazzServerOptions =
      typeof serverOpt === "object" && serverOpt !== null ? serverOpt : {};
    const backendSecret = serverConfigObj.backendSecret ?? process.env.BACKEND_SECRET;

    return {
      server: resolveServerWithJwks(serverOpt, serverConfig),
      schemaDir,
      envDir: root,
      adminSecret: options.adminSecret,
      appId: options.appId,
      telemetry: options.telemetry,
      backendSecret,
      onSchemaError: (error: Error) => {
        viteServerRef?.ws.send({
          type: "error",
          err: {
            message: `${LOG_PREFIX} schema push failed: ${error.message}`,
            stack: error.stack,
          },
        });
      },
      onSchemaPush: () => {
        viteServerRef?.ws.send({ type: "full-reload" });
      },
    };
  }

  // Start the managed runtime and populate process.env exactly once. Called
  // from the `config` hook in real usage (so SvelteKit's later env capture
  // sees the vars on the first pass — no restart needed); also reachable from
  // configureServer for direct/programmatic callers that never run `config`.
  // runtime.initialize() is idempotent, but we additionally cache here so a
  // second caller does not re-run the env-file backfill or re-resolve jwks.
  async function ensureInitialised(
    serverConfig: ViteServerConfigLike | undefined,
    root: string,
  ): Promise<ManagedRuntime> {
    if (managed) return managed;
    loadEnvFileIntoProcessEnv(root);
    managed = await runtime.initialize(buildInitOptions(serverConfig, root));
    return managed;
  }

  return {
    name: "jazz-sveltekit",
    // `enforce: "pre"` puts this plugin's `config` hook in Vite's pre bucket,
    // which runs before SvelteKit's `vite-plugin-sveltekit-setup` (a normal-
    // bucket plugin whose `config({order:'pre'})` hook captures env via
    // loadEnv into the static `$env/dynamic/public` module). Vite awaits each
    // config hook in order, so by the time SvelteKit captures env, the runtime
    // has started and PUBLIC_JAZZ_* are in process.env — first paint is
    // correct with no dev-server restart. Mirrors how the Next plugin awaits
    // runtime.initialize() inside the next-config factory.
    enforce: "pre" as const,

    config(config: ViteUserConfigLike, env?: ViteConfigEnvLike) {
      const merged = buildJazzViteConfig(config);
      if (env?.command !== "serve" || options.server === false) {
        return merged;
      }
      const root = config.root ? resolve(config.root) : process.cwd();
      return ensureInitialised(config.server, root).then(() => merged);
    },

    async configureServer(viteServer: ViteDevServer): Promise<void> {
      viteServerRef = viteServer;
      if (viteServer.config.command !== "serve" || options.server === false) return;

      let resolvedRuntime: ManagedRuntime;
      try {
        resolvedRuntime = await ensureInitialised(viteServer.config.server, viteServer.config.root);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        viteServer.ws.send({
          type: "error",
          err: {
            message: `${LOG_PREFIX} initialization failed: ${message}`,
            stack: error instanceof Error ? error.stack : undefined,
          },
        });
        throw error;
      }

      viteServer.config.env ??= {};
      viteServer.config.env.PUBLIC_JAZZ_APP_ID = resolvedRuntime.appId;
      viteServer.config.env.PUBLIC_JAZZ_SERVER_URL = resolvedRuntime.serverUrl;
      if (resolvedRuntime.telemetryCollectorUrl) {
        viteServer.config.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL =
          resolvedRuntime.telemetryCollectorUrl;
      }

      if (options.inspector !== false) wireInspectorOverlay(viteServer);
    },
  };
}

export async function __resetJazzSvelteKitPluginForTests(): Promise<void> {
  await runtime.resetForTests();
}

function resolveServerWithJwks(
  serverOpt: JazzPluginOptions["server"] | true,
  serverConfig: ViteServerConfigLike | undefined,
): BaseJazzPluginOptions["server"] {
  if (serverOpt === false || typeof serverOpt === "string") return serverOpt;

  const serverConfigObj: JazzServerOptions =
    typeof serverOpt === "object" && serverOpt !== null ? serverOpt : {};

  if (serverConfigObj.jwksUrl !== undefined) return serverConfigObj as BaseJazzServerOptions;

  // Pre-resolve jwksUrl from the configured host/port when starting a local
  // server. Precedence: explicit option → APP_ORIGIN env → Vite config →
  // localhost:5173. Known limitation: Vite auto-port-increment means the
  // configured port may differ from the actual one; set APP_ORIGIN explicitly
  // to work around that.
  const appOrigin = process.env.APP_ORIGIN ?? resolveOrigin(serverConfig);
  return {
    ...serverConfigObj,
    jwksUrl: `${appOrigin}/api/auth/jwks`,
  } as BaseJazzServerOptions;
}
