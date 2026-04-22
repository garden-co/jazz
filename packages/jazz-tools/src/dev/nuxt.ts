import { loadEnvFileIntoProcessEnv } from "./env-file.js";
import { ManagedDevRuntime } from "./managed-runtime.js";
import type {
  JazzServerOptions as BaseJazzServerOptions,
  JazzPluginOptions as BaseJazzPluginOptions,
  ViteDevServer,
} from "./vite.js";

const LOG_PREFIX = "[jazz]";

export interface JazzServerOptions extends BaseJazzServerOptions {
  backendSecret?: string;
}

export interface JazzPluginOptions extends Omit<BaseJazzPluginOptions, "server"> {
  server?: boolean | string | JazzServerOptions;
}

function resolveViteOrigin(viteServer: ViteDevServer): string {
  const server = viteServer.config.server;
  const port = server?.port ?? 3000;
  const hostOpt = server?.host;
  const host = typeof hostOpt === "string" ? hostOpt : "localhost";
  const scheme = server?.https ? "https" : "http";
  return `${scheme}://${host}:${port}`;
}

const runtime = new ManagedDevRuntime({
  appId: "NUXT_PUBLIC_JAZZ_APP_ID",
  serverUrl: "NUXT_PUBLIC_JAZZ_SERVER_URL",
});

export function jazzNuxt(options: JazzPluginOptions = {}) {
  return {
    name: "jazz-nuxt",

    config(config: { ssr?: { external?: string[] }; optimizeDeps?: { exclude?: string[] } }) {
      const existingSsr = config.ssr?.external ?? [];
      const existingExclude = config.optimizeDeps?.exclude ?? [];
      return {
        build: { target: "es2020" },
        worker: { format: "es" as const },
        optimizeDeps: { exclude: Array.from(new Set([...existingExclude, "jazz-wasm"])) },
        ssr: { external: Array.from(new Set([...existingSsr, "jazz-napi"])) },
      };
    },

    async configureServer(viteServer: ViteDevServer): Promise<void> {
      if (viteServer.config.command !== "serve" || options.server === false) return;

      loadEnvFileIntoProcessEnv(viteServer.config.root);

      const schemaDir = options.schemaDir ?? viteServer.config.root;
      const serverOpt = options.server ?? true;

      const serverConfig: JazzServerOptions =
        typeof serverOpt === "object" && serverOpt !== null ? serverOpt : {};
      const backendSecret = serverConfig.backendSecret ?? process.env.BACKEND_SECRET;

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

      // viteServer.config.env exposes values via import.meta.env in the
      // client bundle. process.env is what Nitro reads at request time to
      // populate runtimeConfig.public — without this, useRuntimeConfig()
      // returns the empty defaults from nuxt.config.ts.
      viteServer.config.env ??= {};
      viteServer.config.env.NUXT_PUBLIC_JAZZ_APP_ID = managed.appId;
      viteServer.config.env.NUXT_PUBLIC_JAZZ_SERVER_URL = managed.serverUrl;
      process.env.NUXT_PUBLIC_JAZZ_APP_ID = managed.appId;
      process.env.NUXT_PUBLIC_JAZZ_SERVER_URL = managed.serverUrl;

      viteServer.httpServer?.once("close", () => {
        runtime.dispose().catch((error) => {
          console.error(`${LOG_PREFIX} dispose failed:`, error);
        });
      });
    },
  };
}

export async function __resetJazzNuxtPluginForTests(): Promise<void> {
  await runtime.resetForTests();
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
