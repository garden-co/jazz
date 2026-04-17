import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { LocalJazzServerHandle } from "./dev-server.js";
import type { JazzPluginOptions, JazzServerOptions } from "./vite.js";

const LOG_PREFIX = "[jazz]";

export type ManagedRuntime = {
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

export interface ManagedRuntimeEnvKeys {
  appId: string;
  serverUrl: string;
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

async function readEnvAppId(envPath: string, envKey: string): Promise<string | null> {
  try {
    const content = await readFile(envPath, "utf8");
    const match = content.match(new RegExp(`^${envKey}=(.+)$`, "m"));
    return match?.[1]?.trim() ?? null;
  } catch {
    return null;
  }
}

async function persistAppIdToEnv(envPath: string, envKey: string, appId: string): Promise<void> {
  let content = "";
  try {
    content = await readFile(envPath, "utf8");
  } catch {
    // file doesn't exist yet
  }
  if (content.includes(`${envKey}=`)) return;
  const line = `${envKey}=${appId}\n`;
  await mkdir(join(envPath, ".."), { recursive: true });
  await writeFile(envPath, content ? content + line : line);
}

export interface InitializeOptions extends JazzPluginOptions {
  backendSecret?: string;
  /** Directory in which to persist the generated app ID to a .env file. Defaults to schemaDir. */
  envDir?: string;
  /** Called when a schema watch push fails after initialisation. Use this to forward errors to e.g. Vite's HMR overlay. */
  onSchemaError?: (error: Error) => void;
}

export class ManagedDevRuntime {
  private initPromise: Promise<ManagedRuntime> | null = null;
  private initConfigSignature: string | null = null;
  private runtime: ManagedRuntime | null = null;
  private runtimeConfigSignature: string | null = null;
  private serverHandle: LocalJazzServerHandle | null = null;
  private watcher: { close: () => void } | null = null;
  private shutdownHooksInstalled = false;
  private cleanupHandler: (() => void) | null = null;

  constructor(private envKeys: ManagedRuntimeEnvKeys) {}

  private getManagedRuntimeConfig(options: JazzPluginOptions): ManagedRuntimeConfig {
    return {
      schemaDir: options.schemaDir ?? process.cwd(),
      server: normalizeServerOption(options.server),
      adminSecret: options.adminSecret ?? null,
      appId: options.appId ?? null,
      publicServerUrl: process.env[this.envKeys.serverUrl] ?? null,
      publicAppId: process.env[this.envKeys.appId] ?? null,
    };
  }

  private serializeConfig(config: ManagedRuntimeConfig): string {
    return JSON.stringify(config);
  }

  private assertCompatible(options: JazzPluginOptions): void {
    const requestedSignature = this.serializeConfig(this.getManagedRuntimeConfig(options));
    const matchesInitial = this.initConfigSignature === requestedSignature;
    const matchesRuntime = this.runtimeConfigSignature === requestedSignature;
    if ((this.runtime || this.initPromise) && !matchesInitial && !matchesRuntime) {
      throw new Error(
        `${LOG_PREFIX} conflicting Jazz dev runtime configuration; call resetForTests() before switching dev options`,
      );
    }
  }

  async dispose(): Promise<void> {
    this.watcher?.close();
    this.watcher = null;
    if (this.serverHandle) {
      await this.serverHandle.stop();
      this.serverHandle = null;
    }
    this.runtime = null;
    this.initPromise = null;
    this.initConfigSignature = null;
    this.runtimeConfigSignature = null;
  }

  private installShutdownHooks(): void {
    if (this.shutdownHooksInstalled) return;

    this.cleanupHandler = () => {
      void this.dispose();
    };

    process.once("SIGINT", this.cleanupHandler);
    process.once("SIGTERM", this.cleanupHandler);
    process.once("exit", this.cleanupHandler);
    this.shutdownHooksInstalled = true;
  }

  async resetForTests(): Promise<void> {
    if (this.cleanupHandler) {
      process.off("SIGINT", this.cleanupHandler);
      process.off("SIGTERM", this.cleanupHandler);
      process.off("exit", this.cleanupHandler);
    }
    this.cleanupHandler = null;
    this.shutdownHooksInstalled = false;
    await this.dispose();
  }

  async initialize(options: InitializeOptions): Promise<ManagedRuntime> {
    this.assertCompatible(options);
    if (this.runtime) return this.runtime;
    if (this.initPromise) return this.initPromise;

    const requestedConfig = this.getManagedRuntimeConfig(options);
    const requestedSignature = this.serializeConfig(requestedConfig);
    this.initConfigSignature = requestedSignature;

    this.initPromise = (async () => {
      const serverOpt = options.server ?? true;
      const schemaDir = requestedConfig.schemaDir;
      const envPath = join(options.envDir ?? schemaDir, ".env");
      let serverUrl: string;
      let adminSecret: string;
      let appId: string;

      try {
        if (serverOpt === false) {
          throw new Error(`${LOG_PREFIX} server=false should bypass initialization`);
        }

        if (process.env[this.envKeys.serverUrl]) {
          serverUrl = process.env[this.envKeys.serverUrl]!;
          adminSecret = options.adminSecret ?? "";
          appId = process.env[this.envKeys.appId] ?? options.appId ?? "";
          if (!adminSecret) {
            throw new Error(
              `${LOG_PREFIX} adminSecret is required when connecting to an existing server`,
            );
          }
          if (!appId) {
            throw new Error(
              `${LOG_PREFIX} appId is required when connecting to an existing server`,
            );
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
            throw new Error(
              `${LOG_PREFIX} appId is required when connecting to an existing server`,
            );
          }
        } else {
          const serverConfig = typeof serverOpt === "object" ? serverOpt : {};
          adminSecret = serverConfig.adminSecret ?? `jazz-dev-${randomUUID().slice(0, 8)}`;
          const envAppId = await readEnvAppId(envPath, this.envKeys.appId);
          appId =
            process.env[this.envKeys.appId] ??
            envAppId ??
            serverConfig.appId ??
            options.appId ??
            randomUUID();

          const { startLocalJazzServer } = await import("./dev-server.js");
          this.serverHandle = await startLocalJazzServer({
            appId,
            port: serverConfig.port ?? 0,
            adminSecret,
            backendSecret: options.backendSecret,
            allowLocalFirstAuth: serverConfig.allowLocalFirstAuth,
            dataDir: serverConfig.dataDir,
            inMemory: serverConfig.inMemory,
            jwksUrl: serverConfig.jwksUrl,
            catalogueAuthority: serverConfig.catalogueAuthority,
            catalogueAuthorityUrl: serverConfig.catalogueAuthorityUrl,
            catalogueAuthorityAdminSecret: serverConfig.catalogueAuthorityAdminSecret,
          });

          serverUrl = this.serverHandle.url;
          console.log(`${LOG_PREFIX} server started on ${serverUrl}`);
          if (this.serverHandle.dataDir) {
            console.log(`${LOG_PREFIX} data dir: ${this.serverHandle.dataDir}`);
          }
        }

        console.log(`${LOG_PREFIX} app id: ${appId}`);

        await persistAppIdToEnv(envPath, this.envKeys.appId, appId);

        const { pushSchemaCatalogue } = await import("./dev-server.js");
        await pushSchemaCatalogue({ serverUrl, appId, adminSecret, schemaDir });
        console.log(`${LOG_PREFIX} schema published`);

        const { watchSchema } = await import("./schema-watcher.js");
        this.watcher = watchSchema({
          schemaDir,
          serverUrl,
          appId,
          adminSecret,
          onPush: (hash) => {
            console.log(`${LOG_PREFIX} schema updated (${hash.slice(0, 12)})`);
          },
          onError: (error) => {
            console.error(`${LOG_PREFIX} schema push failed:`, error.message);
            options.onSchemaError?.(error);
          },
        });

        this.installShutdownHooks();

        const backendSecret = this.serverHandle?.backendSecret;

        process.env[this.envKeys.appId] = appId;
        process.env[this.envKeys.serverUrl] = serverUrl;
        if (backendSecret) {
          process.env.BACKEND_SECRET = backendSecret;
        }

        this.runtime = { appId, serverUrl, adminSecret, backendSecret };
        this.runtimeConfigSignature = this.serializeConfig(this.getManagedRuntimeConfig(options));
        return this.runtime;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`${LOG_PREFIX} initialization failed:`, message);
        await this.dispose();
        throw error;
      }
    })();

    try {
      return await this.initPromise;
    } catch (error) {
      this.initPromise = null;
      this.initConfigSignature = null;
      throw error;
    }
  }
}
