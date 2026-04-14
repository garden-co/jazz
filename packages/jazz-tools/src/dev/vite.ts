import { ManagedDevRuntime } from "./managed-runtime.js";

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
}

const LOG_PREFIX = "[jazz]";

interface ViteDevServer {
  config: {
    root: string;
    command: string;
    env?: Record<string, string>;
  };
  httpServer: { once(event: string, cb: () => void): void } | null;
  ws: {
    send(payload: { type: string; err?: { message: string; stack?: string } }): void;
  };
}

export function jazzPlugin(options: JazzPluginOptions = {}) {
  const runtime = new ManagedDevRuntime({
    appId: "JAZZ_APP_ID",
    serverUrl: "JAZZ_SERVER_URL",
  });

  return {
    name: "jazz",

    async configureServer(viteServer: ViteDevServer) {
      if (viteServer.config.command !== "serve") return;

      if (options.server === false) {
        return;
      }

      const schemaDir = options.schemaDir ?? viteServer.config.root;

      let managed;
      try {
        managed = await runtime.initialize({ ...options, schemaDir });
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

      viteServer.config.env ??= {};
      viteServer.config.env.JAZZ_APP_ID = managed.appId;
      viteServer.config.env.JAZZ_SERVER_URL = managed.serverUrl;

      viteServer.httpServer?.once("close", async () => {
        await runtime.dispose();
      });
    },
  };
}
