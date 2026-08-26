import { randomUUID } from "node:crypto";
import {
  closeSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { mkdir } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import type { LocalJazzServerHandle } from "./dev-server.js";
import type { JazzPluginOptions, JazzServerOptions } from "./vite.js";
import { resolveTelemetryCollectorUrl, type TelemetryOptions } from "../runtime/sync-telemetry.js";
import { shortSchemaHash } from "./catalogue.js";

function defaultPersistentDataDir(projectRoot: string): string {
  return join(projectRoot, "node_modules", ".cache", "jazz-dev-server");
}

const LOG_PREFIX = "[jazz]";

function isSchemaPushNetworkError(error: unknown): boolean {
  return error instanceof TypeError && error.message === "fetch failed";
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function warnInitialSchemaPushSkipped(opts: {
  serverUrl: string;
  envServerUrlKey: string | null;
  error: unknown;
}): void {
  const fallback =
    opts.envServerUrlKey === null
      ? "remove the remote server URL option"
      : `comment out ${opts.envServerUrlKey}`;
  console.warn(
    `${LOG_PREFIX} schema auto-push skipped because ${opts.serverUrl} is unreachable (${errorMessage(
      opts.error,
    )}). The dev server will keep using this app and server URL. To use a local Jazz dev server while offline, ${fallback}. Save schema.ts/permissions.ts or restart after reconnecting to publish again.`,
  );
}

function toRelativePath(absPath: string): string {
  const rel = relative(process.cwd(), absPath);
  if (!rel) return ".";
  // fall back to absolute if path escapes cwd
  if (rel.startsWith("..")) return absPath;
  return rel;
}

function printServerStartedBanner(opts: {
  serverUrl: string;
  appId: string;
  dataDir?: string;
  adminSecret?: string;
}): void {
  if (!process.stdout.isTTY) {
    return;
  }

  const useColor = process.env.NO_COLOR === undefined;
  const bold = useColor ? "\x1b[1m" : "";
  const brand = useColor ? "\x1b[38;2;20;106;255m" : ""; // #146aff
  const reset = useColor ? "\x1b[0m" : "";
  const art = [
    "     ██╗ █████╗ ███████╗███████╗",
    "     ██║██╔══██╗╚══███╔╝╚══███╔╝",
    "     ██║███████║  ███╔╝   ███╔╝ ",
    "██   ██║██╔══██║ ███╔╝   ███╔╝  ",
    "╚█████╔╝██║  ██║███████╗███████╗",
    " ╚════╝ ╚═╝  ╚═╝╚══════╝╚══════╝",
  ];
  console.log("");
  for (const line of art) {
    console.log(`${bold}${brand}${line}${reset}`);
  }
  console.log("");
  console.log(
    `${bold}Running a local jazz server on ${reset}${bold}${brand}${opts.serverUrl}${reset}`,
  );
  if (opts.dataDir) {
    console.log(`${bold}Data dir:${reset} ${bold}${brand}${toRelativePath(opts.dataDir)}${reset}`);
  }
  console.log(`${bold}App id:${reset}   ${bold}${brand}${opts.appId}${reset}`);
  if (opts.adminSecret) {
    console.log(`${bold}Admin secret:${reset} ${bold}${brand}${opts.adminSecret}${reset}`);
  }
}

export type ManagedRuntime = {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  backendSecret?: string;
  telemetryCollectorUrl?: string;
};

type ManagedRuntimeConfig = {
  schemaDir: string;
  server: boolean | string | Record<string, unknown>;
  adminSecret: string | null;
  appId: string | null;
  publicServerUrl: string | null;
  publicAppId: string | null;
  publicTelemetryCollectorUrl: string | null;
  telemetry: TelemetryOptions | null;
};

export interface ManagedRuntimeEnvKeys {
  appId: string;
  serverUrl: string;
  telemetryCollectorUrl: string;
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

const lockWaiter = new Int32Array(new SharedArrayBuffer(4));
const dotenvLine =
  /(?:^|^)\s*(?:export\s+)?([\w.-]+)(?:\s*=\s*?|:\s+?)(\s*'(?:\\'|[^'])*'|\s*"(?:\\"|[^"])*"|\s*`(?:\\`|[^`])*`|[^#\r\n]+)?\s*(?:#.*)?(?:$|$)/gm;

// Keep this contract aligned with the dotenv parser used by Vite and Expo:
// export syntax, whitespace, quotes, inline comments, and quoted newlines all
// resolve exactly as they do when the framework later loads the same file.
function parseDotenv(content: string): {
  values: Record<string, string>;
  assignmentLines: Map<string, number[]>;
} {
  const values: Record<string, string> = {};
  const assignmentLines = new Map<string, number[]>();
  const normalized = content.replace(/\r\n?/g, "\n");
  dotenvLine.lastIndex = 0;
  for (const match of normalized.matchAll(dotenvLine)) {
    const key = match[1];
    if (!key) continue;
    const line = normalized.slice(0, match.index).split("\n").length;
    const lines = assignmentLines.get(key) ?? [];
    lines.push(line);
    assignmentLines.set(key, lines);
    let value = (match[2] ?? "").trim();
    const quote = value[0];
    value = value.replace(/^(['"`])([\s\S]*)\1$/gm, "$2");
    if (quote === '"') {
      value = value.replace(/\\n/g, "\n").replace(/\\r/g, "\r");
    }
    values[key] = value;
  }
  return { values, assignmentLines };
}

function readEnvContent(envPath: string): string {
  try {
    return readFileSync(envPath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return "";
    throw error;
  }
}

function parsedEnvValue(content: string, envKey: string): string | null {
  const parsed = parseDotenv(content);
  const assignments = parsed.assignmentLines.get(envKey) ?? [];
  if (assignments.length > 1) {
    throw new Error(
      `${LOG_PREFIX} ${envKey} is assigned more than once in .env (lines ${assignments.join(
        ", ",
      )}). Keep exactly one assignment.`,
    );
  }
  if (assignments.length === 0) return null;
  const value = parsed.values[envKey];
  if (value === undefined) {
    throw new Error(`${LOG_PREFIX} ${envKey} has an invalid .env assignment.`);
  }
  if (value.length === 0) {
    throw new Error(
      `${LOG_PREFIX} ${envKey} is empty in .env. Remove the assignment to generate an app ID, or provide a non-empty value.`,
    );
  }
  return value;
}

function withEnvLock<T>(envPath: string, action: () => T): T {
  const lockPath = `${envPath}.jazz-lock`;
  const deadline = Date.now() + 10_000;
  let descriptor: number | null = null;
  while (descriptor === null) {
    try {
      descriptor = openSync(lockPath, "wx", 0o600);
      writeFileSync(descriptor, `${process.pid}\n`);
      fsyncSync(descriptor);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      if (Date.now() >= deadline) {
        throw new Error(`${LOG_PREFIX} timed out waiting to update .env safely.`);
      }
      Atomics.wait(lockWaiter, 0, 0, 10);
    }
  }
  let result: T | undefined;
  let actionError: unknown;
  try {
    result = action();
  } catch (error) {
    actionError = error;
  }
  try {
    closeSync(descriptor);
    unlinkSync(lockPath);
  } catch (cleanupError) {
    if (actionError === undefined && (cleanupError as NodeJS.ErrnoException).code !== "ENOENT") {
      throw cleanupError;
    }
  }
  if (actionError !== undefined) throw actionError;
  return result as T;
}

function atomicReplaceEnv(envPath: string, content: string, mode: number): void {
  const tempPath = join(dirname(envPath), `.${randomUUID()}.jazz-env.tmp`);
  let descriptor: number | null = null;
  let writeError: unknown;
  try {
    descriptor = openSync(tempPath, "wx", mode);
    writeFileSync(descriptor, content, "utf8");
    fsyncSync(descriptor);
    closeSync(descriptor);
    descriptor = null;
    renameSync(tempPath, envPath);
  } catch (error) {
    writeError = error;
  }
  try {
    if (descriptor !== null) closeSync(descriptor);
    unlinkSync(tempPath);
  } catch (cleanupError) {
    if (writeError === undefined && (cleanupError as NodeJS.ErrnoException).code !== "ENOENT") {
      throw cleanupError;
    }
  }
  if (writeError !== undefined) throw writeError;
}

export function ensureEnvAppId(
  envPath: string,
  envKey: string,
  fallback: string,
  preferred: string | undefined,
): string {
  mkdirSync(dirname(envPath), { recursive: true });
  return withEnvLock(envPath, () => {
    const content = readEnvContent(envPath);
    const existing = parsedEnvValue(content, envKey);
    if (existing !== null) return preferred ?? existing;

    const newline = content.includes("\r\n") ? "\r\n" : "\n";
    const separator = content && !content.endsWith("\n") ? newline : "";
    const next = `${content}${separator}${envKey}=${fallback}${newline}`;
    const mode = (() => {
      try {
        return statSync(envPath).mode & 0o7777;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return 0o600;
        throw error;
      }
    })();
    // Re-read under the interprocess lock immediately before replacement so
    // every cooperating writer derives its update from the latest whole file.
    if (readEnvContent(envPath) !== content) {
      throw new Error(`${LOG_PREFIX} .env changed outside the Jazz writer; retry startup.`);
    }
    atomicReplaceEnv(envPath, next, mode);
    return fallback;
  });
}

export interface InitializeOptions extends JazzPluginOptions {
  backendSecret?: string;
  /** Directory in which to persist the generated app ID to a .env file. Defaults to schemaDir. */
  envDir?: string;
  /** Called when a schema watch push fails after initialisation. Use this to forward errors to e.g. Vite's HMR overlay. */
  onSchemaError?: (error: Error) => void;
  /** Called when the schema watcher successfully pushes an updated schema. Use this to e.g. trigger a Vite full-reload. The initial dev-server push awaits this callback so plugins can write generated artefacts before the host bundler starts compiling. */
  onSchemaPush?: (hash: string) => void | Promise<void>;
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

  prepareEnv(options: InitializeOptions): string | null {
    const serverOpt = options.server ?? true;
    if (serverOpt === false || typeof serverOpt === "string") return null;

    const explicitAdminSecret = options.adminSecret ?? process.env.JAZZ_ADMIN_SECRET ?? null;
    if (process.env[this.envKeys.serverUrl] && explicitAdminSecret) return null;

    const schemaDir = options.schemaDir ?? process.cwd();
    const envPath = join(options.envDir ?? schemaDir, ".env");
    const serverConfig = typeof serverOpt === "object" ? serverOpt : {};
    return ensureEnvAppId(
      envPath,
      this.envKeys.appId,
      process.env[this.envKeys.appId] ?? serverConfig.appId ?? options.appId ?? randomUUID(),
      process.env[this.envKeys.appId],
    );
  }

  private getManagedRuntimeConfig(options: JazzPluginOptions): ManagedRuntimeConfig {
    return {
      schemaDir: options.schemaDir ?? process.cwd(),
      server: normalizeServerOption(options.server),
      adminSecret: options.adminSecret ?? null,
      appId: options.appId ?? null,
      publicServerUrl: process.env[this.envKeys.serverUrl] ?? null,
      publicAppId: process.env[this.envKeys.appId] ?? null,
      publicTelemetryCollectorUrl: process.env[this.envKeys.telemetryCollectorUrl] ?? null,
      telemetry: options.telemetry ?? null,
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
      let usesExistingServer = false;
      let existingServerEnvKey: string | null = null;
      const telemetryCollectorUrl =
        process.env[this.envKeys.telemetryCollectorUrl] ??
        resolveTelemetryCollectorUrl(options.telemetry);
      const preparedAppId = this.prepareEnv(options);

      try {
        if (serverOpt === false) {
          throw new Error(`${LOG_PREFIX} server=false should bypass initialization`);
        }

        // A bare serverUrl env var on its own is treated as our own leftover
        // from a previous run in the same process (Vite HMR restarts,
        // `runtime.resetForTests()` in tests, etc., all re-enter initialize
        // with process.env still set from before). The "external server"
        // path here means "connect to a Jazz dev server someone else is
        // running" — that intent only makes sense if the caller explicitly
        // supplied an adminSecret, so use that as the signal. Otherwise we
        // ignore the env URL and fall through to starting a fresh local
        // server below.
        const explicitAdminSecret = options.adminSecret ?? process.env.JAZZ_ADMIN_SECRET ?? null;
        if (process.env[this.envKeys.serverUrl] && explicitAdminSecret) {
          usesExistingServer = true;
          existingServerEnvKey = this.envKeys.serverUrl;
          serverUrl = process.env[this.envKeys.serverUrl]!;
          adminSecret = explicitAdminSecret;
          appId = process.env[this.envKeys.appId] ?? options.appId ?? "";
          if (!appId) {
            throw new Error(
              `${LOG_PREFIX} appId is required when connecting to an existing server`,
            );
          }
          console.log(`${LOG_PREFIX} using server from env: ${serverUrl}`);
          console.log(`${LOG_PREFIX} app id: ${appId}`);
        } else if (typeof serverOpt === "string") {
          usesExistingServer = true;
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
          console.log(`${LOG_PREFIX} app id: ${appId}`);
        } else {
          const serverConfig = typeof serverOpt === "object" ? serverOpt : {};
          adminSecret =
            serverConfig.adminSecret ??
            options.adminSecret ??
            `jazz-dev-${randomUUID().slice(0, 8)}`;
          appId =
            process.env[this.envKeys.appId] ??
            preparedAppId ??
            serverConfig.appId ??
            options.appId ??
            randomUUID();

          let dataDir = serverConfig.dataDir;
          if (dataDir === undefined && serverConfig.inMemory !== true) {
            const projectRoot = options.envDir ?? schemaDir;
            dataDir = defaultPersistentDataDir(projectRoot);
            await mkdir(dataDir, { recursive: true });
          }

          const { startLocalJazzServer } = await import("./dev-server.js");
          this.serverHandle = await startLocalJazzServer({
            appId,
            port: serverConfig.port ?? 0,
            adminSecret,
            backendSecret: options.backendSecret,
            allowLocalFirstAuth: serverConfig.allowLocalFirstAuth,
            dataDir,
            inMemory: serverConfig.inMemory,
            jwksUrl: serverConfig.jwksUrl,
            telemetryCollectorUrl,
          });

          serverUrl = this.serverHandle.url;
          printServerStartedBanner({
            serverUrl,
            appId,
            dataDir: this.serverHandle.dataDir,
            adminSecret,
          });
        }

        ensureEnvAppId(envPath, this.envKeys.appId, appId, appId);
        if (telemetryCollectorUrl) {
          console.log(`${LOG_PREFIX} telemetry collector: ${telemetryCollectorUrl}`);
        }

        const { deploy } = await import("./catalogue-project.js");
        try {
          const initialDeploy = await deploy({
            serverUrl,
            appId,
            adminSecret,
            schemaDir,
          });
          console.log(`${LOG_PREFIX} schema published`);
          await options.onSchemaPush?.(initialDeploy.schema.hash);
        } catch (error) {
          if (usesExistingServer && isSchemaPushNetworkError(error)) {
            warnInitialSchemaPushSkipped({
              serverUrl,
              envServerUrlKey: existingServerEnvKey,
              error,
            });
          } else {
            throw error;
          }
        }

        const { watchSchema } = await import("./schema-watcher.js");
        this.watcher = watchSchema({
          schemaDir,
          serverUrl,
          appId,
          adminSecret,
          onPush: async (hash) => {
            console.log(`${LOG_PREFIX} schema updated (${shortSchemaHash(hash)})`);
            await options.onSchemaPush?.(hash);
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
        if (telemetryCollectorUrl) {
          process.env[this.envKeys.telemetryCollectorUrl] = telemetryCollectorUrl;
        }
        if (backendSecret) {
          process.env.BACKEND_SECRET = backendSecret;
        }

        this.runtime = { appId, serverUrl, adminSecret, backendSecret, telemetryCollectorUrl };
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
