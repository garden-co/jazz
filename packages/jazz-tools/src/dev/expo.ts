import { randomUUID } from "node:crypto";
import {
  pushSchemaCatalogue,
  startLocalJazzServer,
  type LocalJazzServerHandle,
} from "./dev-server.js";
import { watchSchema } from "./schema-watcher.js";
import type { JazzPluginOptions, JazzServerOptions } from "./vite.js";

export type { JazzPluginOptions, JazzServerOptions };

export interface ExpoConfigLike {
  extra?: Record<string, unknown>;
  [key: string]: unknown;
}

const PUBLIC_APP_ID_ENV = "EXPO_PUBLIC_JAZZ_APP_ID";
const PUBLIC_SERVER_URL_ENV = "EXPO_PUBLIC_JAZZ_SERVER_URL";
const LOG_PREFIX = "[jazz]";

type ManagedRuntime = {
  appId: string;
  serverUrl: string;
  adminSecret: string;
};

type ManagedRuntimeConfig = {
  schemaDir: string;
  server: boolean | string | Record<string, unknown>;
  adminSecret: string | null;
  appId: string | null;
  publicServerUrl: string | null;
  publicAppId: string | null;
};

let initPromise: Promise<ManagedRuntime> | null = null;
let initConfigSignature: string | null = null;
let runtime: ManagedRuntime | null = null;
let runtimeConfigSignature: string | null = null;
let serverHandle: LocalJazzServerHandle | null = null;
let watcher: { close: () => void } | null = null;
let shutdownHooksInstalled = false;
let cleanupHandler: (() => void) | null = null;

function normalizeServerOption(
  server: JazzPluginOptions["server"],
): ManagedRuntimeConfig["server"] {
  if (server === undefined || server === true) return true;
  if (server === false || typeof server === "string") return server;
  return Object.keys(server)
    .sort()
    .reduce<Record<string, unknown>>((acc, key) => {
      const value = server[key as keyof JazzServerOptions];
      if (value !== undefined) {
        acc[key] = value;
      }
      return acc;
    }, {});
}

function getManagedRuntimeConfig(options: JazzPluginOptions): ManagedRuntimeConfig {
  return {
    schemaDir: options.schemaDir ?? process.cwd(),
    server: normalizeServerOption(options.server),
    adminSecret: options.adminSecret ?? null,
    appId: options.appId ?? null,
    publicServerUrl: process.env[PUBLIC_SERVER_URL_ENV] ?? null,
    publicAppId: process.env[PUBLIC_APP_ID_ENV] ?? null,
  };
}

function serializeManagedRuntimeConfig(config: ManagedRuntimeConfig): string {
  return JSON.stringify(config);
}

function assertCompatibleManagedRuntime(options: JazzPluginOptions): void {
  const requestedSignature = serializeManagedRuntimeConfig(getManagedRuntimeConfig(options));
  const matchesInitial = initConfigSignature === requestedSignature;
  const matchesRuntime = runtimeConfigSignature === requestedSignature;
  if ((runtime || initPromise) && !matchesInitial && !matchesRuntime) {
    throw new Error(
      `${LOG_PREFIX} conflicting Jazz dev runtime configuration; call __resetJazzExpoPluginForTests() before switching dev options`,
    );
  }
}

async function disposeManagedRuntime(): Promise<void> {
  watcher?.close();
  watcher = null;
  if (serverHandle) {
    await serverHandle.stop();
    serverHandle = null;
  }
  runtime = null;
  initPromise = null;
  initConfigSignature = null;
  runtimeConfigSignature = null;
}

function installShutdownHooks(): void {
  if (shutdownHooksInstalled) return;

  cleanupHandler = () => {
    void disposeManagedRuntime();
  };

  process.once("SIGINT", cleanupHandler);
  process.once("SIGTERM", cleanupHandler);
  process.once("exit", cleanupHandler);
  shutdownHooksInstalled = true;
}

async function initializeManagedRuntime(options: JazzPluginOptions): Promise<ManagedRuntime> {
  assertCompatibleManagedRuntime(options);
  if (runtime) return runtime;
  if (initPromise) return initPromise;

  const requestedConfig = getManagedRuntimeConfig(options);
  const requestedSignature = serializeManagedRuntimeConfig(requestedConfig);
  initConfigSignature = requestedSignature;

  initPromise = (async () => {
    const serverOpt = options.server ?? true;
    const schemaDir = requestedConfig.schemaDir;
    let serverUrl: string;
    let adminSecret: string;
    let appId: string;

    try {
      if (process.env[PUBLIC_SERVER_URL_ENV]) {
        serverUrl = process.env[PUBLIC_SERVER_URL_ENV]!;
        adminSecret = options.adminSecret ?? "";
        appId = process.env[PUBLIC_APP_ID_ENV] ?? options.appId ?? "";
        if (!adminSecret) {
          throw new Error(
            `${LOG_PREFIX} adminSecret is required when connecting to an existing server`,
          );
        }
        if (!appId) {
          throw new Error(`${LOG_PREFIX} appId is required when connecting to an existing server`);
        }
        console.log(`${LOG_PREFIX} using server from env: ${serverUrl}`);
      } else if (typeof serverOpt === "string") {
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
        const serverConfig = typeof serverOpt === "object" ? serverOpt : {};
        adminSecret = serverConfig.adminSecret ?? `jazz-dev-${randomUUID().slice(0, 8)}`;
        appId =
          process.env[PUBLIC_APP_ID_ENV] ?? serverConfig.appId ?? options.appId ?? randomUUID();

        serverHandle = await startLocalJazzServer({
          appId,
          port: serverConfig.port ?? 0,
          adminSecret,
          allowLocalFirstAuth: serverConfig.allowLocalFirstAuth,
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
      }

      console.log(`${LOG_PREFIX} app id: ${appId}`);

      await pushSchemaCatalogue({ serverUrl, appId, adminSecret, schemaDir });
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
        },
      });

      installShutdownHooks();

      process.env[PUBLIC_APP_ID_ENV] = appId;
      process.env[PUBLIC_SERVER_URL_ENV] = serverUrl;

      runtime = { appId, serverUrl, adminSecret };
      runtimeConfigSignature = serializeManagedRuntimeConfig(getManagedRuntimeConfig(options));
      return runtime;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`${LOG_PREFIX} initialization failed:`, message);
      await disposeManagedRuntime();
      throw error;
    }
  })();

  try {
    return await initPromise;
  } catch (error) {
    initPromise = null;
    initConfigSignature = null;
    throw error;
  }
}

export async function withJazzExpo(
  expoConfig: ExpoConfigLike,
  options: JazzPluginOptions = {},
): Promise<ExpoConfigLike> {
  if (process.env.NODE_ENV === "production" || options.server === false) {
    return expoConfig;
  }

  const managed = await initializeManagedRuntime(options);

  return {
    ...expoConfig,
    extra: {
      ...expoConfig.extra,
      jazzAppId: managed.appId,
      jazzServerUrl: managed.serverUrl,
    },
  };
}

export async function __resetJazzExpoPluginForTests(): Promise<void> {
  if (cleanupHandler) {
    process.off("SIGINT", cleanupHandler);
    process.off("SIGTERM", cleanupHandler);
    process.off("exit", cleanupHandler);
  }
  cleanupHandler = null;
  shutdownHooksInstalled = false;
  await disposeManagedRuntime();
}
