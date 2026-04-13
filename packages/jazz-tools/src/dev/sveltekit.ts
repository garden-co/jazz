import { randomUUID } from "node:crypto";
import { join } from "node:path";
import {
  startLocalJazzServer,
  pushSchemaCatalogue,
  type LocalJazzServerHandle,
} from "./dev-server.js";
import { watchSchema } from "./schema-watcher.js";
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

type ManagedRuntime = {
  appId: string;
  serverUrl: string;
  adminSecret: string;
};

function resolveViteOrigin(viteServer: ViteDevServer): string {
  const server = viteServer.config.server;
  const port = server?.port ?? 5173;
  const hostOpt = server?.host;
  // Vite's `server.host` accepts boolean (true = listen on all, undefined/false
  // = localhost) or a string hostname. Only use the string form for building
  // the origin — the boolean form still means the dev URL is localhost.
  const host = typeof hostOpt === "string" ? hostOpt : "localhost";
  const scheme = server?.https ? "https" : "http";
  return `${scheme}://${host}:${port}`;
}

// A single process-wide shutdown hook fans out to every active plugin
// instance's disposer. Per-plugin hooks would register 3 listeners per
// configureServer call and quickly exceed EventEmitter's default maxListeners
// in test harnesses. This is the only module-level state in the file.
const activeDisposers = new Set<() => Promise<void>>();
let globalShutdownInstalled = false;

function ensureGlobalShutdownHook(): void {
  if (globalShutdownInstalled) return;
  globalShutdownInstalled = true;
  const handler = () => {
    for (const dispose of activeDisposers) {
      dispose().catch(console.error);
    }
  };
  // Only SIGINT/SIGTERM — Node's `exit` event fires synchronously and would
  // abandon any async work in dispose() (notably serverHandle.stop()). The
  // TCP socket closes implicitly when the process exits, so skipping `exit`
  // doesn't leave anything truly orphaned.
  process.once("SIGINT", handler);
  process.once("SIGTERM", handler);
}

export function jazzSvelteKit(options: JazzPluginOptions = {}) {
  let serverHandle: LocalJazzServerHandle | null = null;
  let watcher: { close: () => void } | null = null;
  let initPromise: Promise<ManagedRuntime> | null = null;
  let runtime: ManagedRuntime | null = null;
  let disposerRegistered = false;

  async function disposeRuntime(): Promise<void> {
    watcher?.close();
    watcher = null;
    if (serverHandle) {
      await serverHandle.stop();
      serverHandle = null;
    }
    delete process.env.PUBLIC_JAZZ_APP_ID;
    delete process.env.PUBLIC_JAZZ_SERVER_URL;
    delete process.env.BACKEND_SECRET;
    runtime = null;
    initPromise = null;
    if (disposerRegistered) {
      activeDisposers.delete(disposeRuntime);
      disposerRegistered = false;
    }
  }

  function registerDisposer(): void {
    if (disposerRegistered) return;
    activeDisposers.add(disposeRuntime);
    disposerRegistered = true;
    ensureGlobalShutdownHook();
  }

  async function publishAndWatchSchema(opts: {
    schemaDir: string;
    serverUrl: string;
    appId: string;
    adminSecret: string;
    ws: ViteDevServer["ws"];
  }): Promise<void> {
    const { schemaDir, serverUrl, appId, adminSecret, ws } = opts;

    try {
      await pushSchemaCatalogue({ serverUrl, appId, adminSecret, schemaDir });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.startsWith("Schema file not found")) {
        console.warn(
          `${LOG_PREFIX} no schema.ts found in ${schemaDir} — plugin will not publish a schema. Create ${schemaDir}/schema.ts to enable schema publishing.`,
        );
        return;
      }
      console.error(`${LOG_PREFIX} schema push failed:`, message);
      ws.send({
        type: "error",
        err: {
          message: `${LOG_PREFIX} schema push failed: ${message}`,
          stack: error instanceof Error ? error.stack : undefined,
        },
      });
      throw error;
    }

    console.log(`${LOG_PREFIX} schema published`);

    watcher = watchSchema({
      schemaDir,
      serverUrl,
      appId,
      adminSecret,
      onPush: (hash) => {
        console.log(`${LOG_PREFIX} schema updated (${hash.slice(0, 12)})`);
      },
      onError: (error) => {
        console.error(`${LOG_PREFIX} schema push failed:`, error.message);
        ws.send({
          type: "error",
          err: {
            message: `${LOG_PREFIX} schema push failed: ${error.message}`,
            stack: error.stack,
          },
        });
      },
    });
  }

  async function initializeRuntime(viteServer: ViteDevServer): Promise<ManagedRuntime> {
    if (runtime) return runtime;
    if (initPromise) return initPromise;

    initPromise = (async () => {
      const serverOpt = options.server ?? true;
      let serverUrl: string;
      let appId: string;
      let adminSecret: string;

      if (process.env.PUBLIC_JAZZ_SERVER_URL) {
        // Env-preset branch: connect to an existing server specified in the environment.
        serverUrl = process.env.PUBLIC_JAZZ_SERVER_URL;
        adminSecret = options.adminSecret ?? "";
        appId = process.env.PUBLIC_JAZZ_APP_ID ?? options.appId ?? "";
        if (!adminSecret) {
          throw new Error(
            `${LOG_PREFIX} adminSecret is required when connecting to an existing server`,
          );
        }
        if (!appId) {
          throw new Error(`${LOG_PREFIX} appId is required when connecting to an existing server`);
        }
        console.log(`${LOG_PREFIX} using server from .env: ${serverUrl}`);
      } else if (typeof serverOpt === "string") {
        // String-URL branch: connect to an explicit server URL passed as options.server.
        serverUrl = serverOpt;
        adminSecret = options.adminSecret ?? "";
        appId = options.appId ?? "";
        if (!adminSecret) {
          throw new Error(
            `${LOG_PREFIX} adminSecret is required when connecting to an existing server`,
          );
        }
        if (!appId) {
          throw new Error(`${LOG_PREFIX} appId is required when connecting to an existing server`);
        }
      } else {
        // Local-server branch: start an embedded Jazz server.
        const serverConfig: JazzServerOptions =
          typeof serverOpt === "object" && serverOpt !== null ? serverOpt : {};

        adminSecret =
          serverConfig.adminSecret ?? options.adminSecret ?? `jazz-dev-${randomUUID().slice(0, 8)}`;

        // jwksUrl precedence: explicit option > APP_ORIGIN env > Vite config
        // host/port > hardcoded localhost:5173. Note that this doesn't handle
        // Vite auto-incrementing to a free port when the configured one is
        // taken — users hitting that case should set APP_ORIGIN explicitly.
        const appOrigin =
          serverConfig.jwksUrl !== undefined
            ? null
            : (process.env.APP_ORIGIN ?? resolveViteOrigin(viteServer));

        serverHandle = await startLocalJazzServer({
          appId: serverConfig.appId ?? options.appId ?? randomUUID(),
          port: serverConfig.port ?? 0,
          adminSecret,
          backendSecret: serverConfig.backendSecret,
          allowLocalFirstAuth: serverConfig.allowLocalFirstAuth,
          dataDir: serverConfig.dataDir,
          inMemory: serverConfig.inMemory,
          jwksUrl: serverConfig.jwksUrl ?? `${appOrigin}/api/auth/jwks`,
          catalogueAuthority: serverConfig.catalogueAuthority,
          catalogueAuthorityUrl: serverConfig.catalogueAuthorityUrl,
          catalogueAuthorityAdminSecret: serverConfig.catalogueAuthorityAdminSecret,
        });

        serverUrl = serverHandle.url;
        appId = serverHandle.appId;

        if (serverHandle.backendSecret) {
          process.env.BACKEND_SECRET = serverHandle.backendSecret;
        }
      }

      process.env.PUBLIC_JAZZ_APP_ID = appId;
      process.env.PUBLIC_JAZZ_SERVER_URL = serverUrl;

      viteServer.config.env ??= {};
      viteServer.config.env.PUBLIC_JAZZ_APP_ID = appId;
      viteServer.config.env.PUBLIC_JAZZ_SERVER_URL = serverUrl;

      const schemaDir = options.schemaDir ?? join(viteServer.config.root, "src", "lib");

      try {
        await publishAndWatchSchema({
          schemaDir,
          serverUrl,
          appId,
          adminSecret,
          ws: viteServer.ws,
        });
      } catch (error) {
        await disposeRuntime();
        throw error;
      }

      runtime = { appId, serverUrl, adminSecret };
      return runtime;
    })();

    try {
      return await initPromise;
    } catch (error) {
      initPromise = null;
      throw error;
    }
  }

  return {
    name: "jazz-sveltekit",

    async configureServer(viteServer: ViteDevServer): Promise<void> {
      if (viteServer.config.command !== "serve" || options.server === false) return;

      await initializeRuntime(viteServer);
      registerDisposer();

      viteServer.httpServer?.once("close", () => {
        // EventEmitter discards the return value, so an async throw would
        // become an unhandled rejection. Surface it explicitly instead.
        disposeRuntime().catch((error) => {
          console.error(`${LOG_PREFIX} dispose failed:`, error);
        });
      });
    },
  };
}
