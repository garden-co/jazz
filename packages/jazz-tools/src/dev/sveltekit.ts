import { join } from "node:path";
import { ManagedDevRuntime } from "./managed-runtime.js";
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

export function jazzSvelteKit(options: JazzPluginOptions = {}) {
  const runtime = new ManagedDevRuntime({
    appId: "PUBLIC_JAZZ_APP_ID",
    serverUrl: "PUBLIC_JAZZ_SERVER_URL",
  });

  return {
    name: "jazz-sveltekit",

    async configureServer(viteServer: ViteDevServer): Promise<void> {
      if (viteServer.config.command !== "serve" || options.server === false) return;

      const schemaDir = options.schemaDir ?? join(viteServer.config.root, "src", "lib");
      const serverOpt = options.server ?? true;

      // Extract backendSecret from the server config object (SvelteKit-only option).
      const serverConfig: JazzServerOptions =
        typeof serverOpt === "object" && serverOpt !== null ? serverOpt : {};
      const backendSecret = serverConfig.backendSecret;

      // Pre-resolve jwksUrl from Vite's configured host/port when starting a
      // local server. Precedence: explicit option → APP_ORIGIN env → Vite config
      // → localhost:5173. Known limitation: Vite auto-port-increment means the
      // configured port may differ from the actual one; set APP_ORIGIN explicitly
      // to work around that.
      const resolvedServer = resolveServerWithJwks(serverOpt, viteServer);

      let managed;
      try {
        managed = await runtime.initialize({
          server: resolvedServer,
          schemaDir,
          envDir: viteServer.config.root,
          adminSecret: options.adminSecret,
          appId: options.appId,
          backendSecret,
          onSchemaError: (error) => {
            viteServer.ws.send({
              type: "error",
              err: {
                message: `${LOG_PREFIX} schema push failed: ${error.message}`,
                stack: error.stack,
              },
            });
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
        throw error;
      }

      viteServer.config.env ??= {};
      viteServer.config.env.PUBLIC_JAZZ_APP_ID = managed.appId;
      viteServer.config.env.PUBLIC_JAZZ_SERVER_URL = managed.serverUrl;

      viteServer.httpServer?.once("close", () => {
        runtime.dispose().catch((error) => {
          console.error(`${LOG_PREFIX} dispose failed:`, error);
        });
      });
    },
  };
}

function resolveServerWithJwks(
  serverOpt: JazzPluginOptions["server"] | true,
  viteServer: ViteDevServer,
): BaseJazzPluginOptions["server"] {
  if (serverOpt === false || typeof serverOpt === "string") return serverOpt;

  const serverConfig: JazzServerOptions =
    typeof serverOpt === "object" && serverOpt !== null ? serverOpt : {};

  if (serverConfig.jwksUrl !== undefined) return serverConfig as BaseJazzServerOptions;

  const appOrigin = process.env.APP_ORIGIN ?? resolveViteOrigin(viteServer);
  return { ...serverConfig, jwksUrl: `${appOrigin}/api/auth/jwks` } as BaseJazzServerOptions;
}
