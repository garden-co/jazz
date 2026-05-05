import { createRequire } from "node:module";
import { isAbsolute, join, resolve } from "node:path";
import { loadEnvFileIntoProcessEnv } from "./env-file.js";
import { buildInspectorLink } from "./inspector-link.js";
import { ManagedDevRuntime } from "./managed-runtime.js";
import type { TelemetryOptions } from "../runtime/sync-telemetry.js";

// Resolve jazz-wasm from the consumer's project root. jazz-wasm is an optional
// peer dependency: if the consumer hasn't installed it, the bare `jazz-wasm`
// specifier inside jazz-tools won't resolve at build time and Vite emits a
// generic "Failed to resolve import" — the friendly runtime error in
// loadWasmModule never gets a chance to fire. Surface a clear install message
// here, before the bundler runs.
export function assertJazzWasmInstalled(consumerRoot: string = process.cwd()): void {
  // createRequire requires an absolute path or file URL, but a caller (e.g.
  // a Vite config setting `root` to a relative path) may legitimately pass
  // a relative root. Anchor it to cwd so resolution still works.
  const absoluteRoot = isAbsolute(consumerRoot) ? consumerRoot : resolve(consumerRoot);
  try {
    createRequire(join(absoluteRoot, "package.json")).resolve("jazz-wasm/package.json");
  } catch (err) {
    throw new Error(
      `[jazz] The "jazz-wasm" peer dependency is required but is not installed in this project.\n` +
        `Install it alongside jazz-tools, e.g.:\n` +
        `  npm install jazz-wasm\n` +
        `  pnpm add jazz-wasm\n` +
        `  yarn add jazz-wasm`,
      { cause: err },
    );
  }
}

export interface JazzServerOptions {
  port?: number;
  adminSecret?: string;
  appId?: string;
  allowLocalFirstAuth?: boolean;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  catalogueAuthority?: "local" | "forward";
  catalogueAuthorityUrl?: string;
  catalogueAuthorityAdminSecret?: string;
}

export interface JazzPluginOptions {
  server?: boolean | string | JazzServerOptions;
  adminSecret?: string;
  schemaDir?: string;
  appId?: string;
  telemetry?: TelemetryOptions;
}

const LOG_PREFIX = "[jazz]";

// Minimal subset of Vite's ViteDevServer — redeclared here to keep this
// module zero-dep on Vite's public types. Exported for sibling plugins
// (./sveltekit.ts) to share, so the shape is maintained in one place.
export interface ViteDevServer {
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

    config(config: {
      root?: string;
      ssr?: { external?: true | string[] };
      optimizeDeps?: { exclude?: string[] };
    }) {
      assertJazzWasmInstalled(config.root);
      const existingSsr = config.ssr?.external;
      const existingExclude = config.optimizeDeps?.exclude ?? [];
      // `ssr.external: true` means "externalize everything", so jazz-napi is
      // already covered — preserve the bool rather than coercing to an array.
      const ssrExternal: true | string[] =
        existingSsr === true ? true : Array.from(new Set([...(existingSsr ?? []), "jazz-napi"]));
      return {
        worker: { format: "es" as const },
        optimizeDeps: { exclude: Array.from(new Set([...existingExclude, "jazz-wasm"])) },
        ssr: { external: ssrExternal },
      };
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
      console.log(
        `${LOG_PREFIX} Open the inspector: ${buildInspectorLink(
          managed.serverUrl,
          managed.appId,
          managed.adminSecret,
        )}`,
      );

      viteServer.httpServer?.once("close", async () => {
        await runtime.dispose();
      });
    },
  };
}
