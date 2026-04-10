import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  startLocalJazzServer,
  pushSchemaCatalogue,
  type LocalJazzServerHandle,
} from "./dev-server.js";
import { watchSchema } from "./schema-watcher.js";

export interface JazzServerOptions {
  port?: number;
  adminSecret?: string;
  appId?: string;
  allowAnonymous?: boolean;
  allowDemo?: boolean;
  allowSelfSigned?: boolean;
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

const DEFAULT_PORT = 0;
const LOG_PREFIX = "[jazz]";

async function persistAppIdToEnv(root: string, appId: string): Promise<void> {
  const envPath = join(root, ".env");
  let content = "";
  try {
    content = await readFile(envPath, "utf8");
  } catch {
    // file doesn't exist yet
  }
  if (content.includes("JAZZ_APP_ID=")) return;
  const line = `JAZZ_APP_ID=${appId}\n`;
  await writeFile(envPath, content ? content + line : line);
}

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
  let serverHandle: LocalJazzServerHandle | null = null;
  let watcher: { close: () => void } | null = null;

  return {
    name: "jazz",

    async configureServer(viteServer: ViteDevServer) {
      if (viteServer.config.command !== "serve") return;

      const serverOpt = options.server ?? true;

      if (serverOpt === false) {
        return;
      }

      const schemaDir = options.schemaDir ?? viteServer.config.root;
      const env = viteServer.config.env ?? {};
      let serverUrl: string;
      let adminSecret: string;
      let appId: string;

      if (env.JAZZ_SERVER_URL) {
        serverUrl = env.JAZZ_SERVER_URL;
        adminSecret = options.adminSecret ?? "";
        if (!adminSecret) {
          throw new Error(
            `${LOG_PREFIX} adminSecret is required when connecting to an existing server`,
          );
        }
        appId = env.JAZZ_APP_ID ?? options.appId ?? "";
        if (!appId) {
          throw new Error(`${LOG_PREFIX} appId is required when connecting to an existing server`);
        }
        console.log(`${LOG_PREFIX} using server from .env: ${serverUrl}`);
      } else if (typeof serverOpt === "string") {
        serverUrl = serverOpt;
        adminSecret = options.adminSecret ?? "";
        if (!adminSecret) {
          throw new Error(
            `${LOG_PREFIX} adminSecret is required when connecting to an existing server`,
          );
        }
        appId = options.appId ?? "";
        if (!appId) {
          throw new Error(`${LOG_PREFIX} appId is required when connecting to an existing server`);
        }
      } else {
        const serverConfig = typeof serverOpt === "object" ? serverOpt : {};
        adminSecret = serverConfig.adminSecret ?? `jazz-dev-${randomUUID().slice(0, 8)}`;
        appId = env.JAZZ_APP_ID ?? serverConfig.appId ?? options.appId ?? randomUUID();
        const port = serverConfig.port ?? DEFAULT_PORT;

        serverHandle = await startLocalJazzServer({
          appId,
          port,
          adminSecret,
          allowAnonymous: serverConfig.allowAnonymous,
          allowDemo: serverConfig.allowDemo,
          allowSelfSigned: serverConfig.allowSelfSigned,
          dataDir: serverConfig.dataDir,
          inMemory: serverConfig.inMemory,
          jwksUrl: serverConfig.jwksUrl,
          catalogueAuthority: serverConfig.catalogueAuthority,
          catalogueAuthorityUrl: serverConfig.catalogueAuthorityUrl,
          catalogueAuthorityAdminSecret: serverConfig.catalogueAuthorityAdminSecret,
        });

        serverUrl = serverHandle.url;
        console.log(`${LOG_PREFIX} server started on ${serverUrl}`);
        if (serverHandle.dataDir) {
          console.log(`${LOG_PREFIX} data dir: ${serverHandle.dataDir}`);
        }

        if (!env.JAZZ_APP_ID && !serverConfig.appId && !options.appId) {
          await persistAppIdToEnv(viteServer.config.root, appId);
        }
      }

      console.log(`${LOG_PREFIX} app id: ${appId}`);

      try {
        await pushSchemaCatalogue({
          serverUrl,
          appId,
          adminSecret,
          schemaDir,
        });
        console.log(`${LOG_PREFIX} schema published`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`${LOG_PREFIX} schema push failed:`, message);
        viteServer.ws.send({
          type: "error",
          err: {
            message: `${LOG_PREFIX} schema push failed: ${message}`,
            stack: error instanceof Error ? error.stack : undefined,
          },
        });
      }

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
          viteServer.ws.send({
            type: "error",
            err: {
              message: `${LOG_PREFIX} schema push failed: ${error.message}`,
              stack: error.stack,
            },
          });
        },
      });

      viteServer.config.env ??= {};
      viteServer.config.env.JAZZ_APP_ID = appId;
      viteServer.config.env.JAZZ_SERVER_URL = serverUrl;
      process.env.JAZZ_APP_ID = appId;
      process.env.JAZZ_SERVER_URL = serverUrl;

      viteServer.httpServer?.once("close", async () => {
        watcher?.close();
        watcher = null;
        if (serverHandle) {
          await serverHandle.stop();
          serverHandle = null;
        }
      });
    },
  };
}
