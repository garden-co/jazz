import { randomUUID } from "node:crypto";
import {
  pushSchemaCatalogue,
  startLocalJazzServer,
  type LocalJazzServerHandle,
} from "./dev-server.js";
import { watchSchema } from "./schema-watcher.js";

export interface JazzServerOptions {
  port?: number;
  adminSecret?: string;
  backendSecret?: string;
  appId?: string;
  allowAnonymous?: boolean;
  allowDemo?: boolean;
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

export interface NextConfigLike {
  env?: Record<string, string | undefined>;
  serverExternalPackages?: string[];
  [key: string]: unknown;
}

interface NextConfigContextLike {
  defaultConfig: NextConfigLike;
}

type NextConfigFactory = (
  phase: string,
  context: NextConfigContextLike,
) => NextConfigLike | Promise<NextConfigLike>;

type NextConfigInput = NextConfigLike | NextConfigFactory;

type ManagedRuntime = {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  backendSecret?: string;
};

type ManagedRuntimeConfig = {
  schemaDir: string;
  server: boolean | string | Record<string, unknown>;
  adminSecret: string | null;
  appId: string | null;
  publicServerUrl: string | null;
  publicAppId: string | null;
};

const DEVELOPMENT_PHASE = "phase-development-server";
const PUBLIC_APP_ID_ENV = "NEXT_PUBLIC_JAZZ_APP_ID";
const PUBLIC_SERVER_URL_ENV = "NEXT_PUBLIC_JAZZ_SERVER_URL";
const LOG_PREFIX = "[jazz]";

let initPromise: Promise<ManagedRuntime> | null = null;
let initConfigSignature: string | null = null;
let runtime: ManagedRuntime | null = null;
let runtimeConfigSignature: string | null = null;
let serverHandle: LocalJazzServerHandle | null = null;
let watcher: { close: () => void } | null = null;
let shutdownHooksInstalled = false;
let cleanupHandler: (() => void) | null = null;

function mergeServerExternalPackages(existing: string[] | undefined): string[] {
  return Array.from(new Set([...(existing ?? []), "jazz-tools", "jazz-napi"]));
}

async function resolveConfig(
  input: NextConfigInput | undefined,
  phase: string,
  context: NextConfigContextLike,
): Promise<NextConfigLike> {
  if (!input) return {};
  if (typeof input === "function") {
    return (await input(phase, context)) ?? {};
  }
  return input;
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

function resolveSchemaDir(options: JazzPluginOptions): string {
  return options.schemaDir ?? process.cwd();
}

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
    schemaDir: resolveSchemaDir(options),
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
      `${LOG_PREFIX} conflicting Jazz dev runtime configuration; call __resetJazzNextPluginForTests() before switching dev options`,
    );
  }
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
      if (serverOpt === false) {
        throw new Error(`${LOG_PREFIX} server=false should bypass initialization`);
      }

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
        console.log(`${LOG_PREFIX} using server from .env: ${serverUrl}`);
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
          backendSecret: serverConfig.backendSecret,
          allowAnonymous: serverConfig.allowAnonymous,
          allowDemo: serverConfig.allowDemo,
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

      const backendSecret = serverHandle?.backendSecret;

      process.env[PUBLIC_APP_ID_ENV] = appId;
      process.env[PUBLIC_SERVER_URL_ENV] = serverUrl;
      if (backendSecret) {
        process.env.BACKEND_SECRET = backendSecret;
      }

      runtime = { appId, serverUrl, adminSecret, backendSecret };
      runtimeConfigSignature = serializeManagedRuntimeConfig(getManagedRuntimeConfig(options));
      return runtime;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`${LOG_PREFIX} schema push failed:`, message);
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

export function withJazz(
  nextConfig?: NextConfigInput,
  options: JazzPluginOptions = {},
): NextConfigFactory {
  return async (phase, context) => {
    const resolved = await resolveConfig(nextConfig, phase, context);
    const merged: NextConfigLike = {
      ...resolved,
      serverExternalPackages: mergeServerExternalPackages(resolved.serverExternalPackages),
    };

    if (phase !== DEVELOPMENT_PHASE || options.server === false) {
      return merged;
    }

    const managed = await initializeManagedRuntime(options);

    return {
      ...merged,
      env: {
        ...merged.env,
        [PUBLIC_APP_ID_ENV]: managed.appId,
        [PUBLIC_SERVER_URL_ENV]: managed.serverUrl,
        ...(managed.backendSecret ? { BACKEND_SECRET: managed.backendSecret } : {}),
      },
    };
  };
}

export async function __resetJazzNextPluginForTests(): Promise<void> {
  if (cleanupHandler) {
    process.off("SIGINT", cleanupHandler);
    process.off("SIGTERM", cleanupHandler);
    process.off("exit", cleanupHandler);
  }
  cleanupHandler = null;
  shutdownHooksInstalled = false;
  await disposeManagedRuntime();
}
