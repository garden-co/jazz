import { randomUUID } from "node:crypto";
import {
  closeSync,
  constants,
  fstatSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  writeSync,
} from "node:fs";
import { mkdir } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { unlock, waitForLockSync } from "fs-native-extensions";
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

interface EnvFileOperations {
  waitForLock(descriptor: number): void;
  unlock(descriptor: number): void;
  write(descriptor: number, bytes: Uint8Array, offset: number): number;
  fsync(descriptor: number): void;
  /** Test seam for a pathname replacement after open(2), before fstat/lstat. */
  afterOpen?(descriptor: number): void;
}

class EnvPathReplacedError extends Error {
  constructor() {
    super(`${LOG_PREFIX} .env changed while it was being updated; retry startup.`);
  }
}

const defaultEnvFileOperations: EnvFileOperations = {
  waitForLock: waitForLockSync,
  unlock,
  write: (descriptor, bytes, offset) =>
    writeSync(descriptor, bytes, offset, bytes.byteLength - offset),
  fsync: fsyncSync,
};

function assertRegularEnvPath(envPath: string): void {
  try {
    const metadata = lstatSync(envPath);
    if (metadata.isSymbolicLink()) {
      throw new Error(
        `${LOG_PREFIX} refusing to update symlinked .env at ${envPath}; use a regular file instead.`,
      );
    }
    if (!metadata.isFile()) {
      throw new Error(`${LOG_PREFIX} refusing to update non-file .env at ${envPath}.`);
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}

/**
 * The advisory lock belongs to the inode, not the pathname. Revalidate after
 * locking (and before returning) so an atomic rename cannot leave us appending
 * to an unlinked file while reporting success for the replacement at envPath.
 */
function assertEnvPathMatchesDescriptor(envPath: string, descriptor: number): void {
  let pathMetadata;
  try {
    pathMetadata = lstatSync(envPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new EnvPathReplacedError();
    }
    throw error;
  }
  if (pathMetadata.isSymbolicLink()) {
    throw new Error(
      `${LOG_PREFIX} refusing to update symlinked .env at ${envPath}; use a regular file instead.`,
    );
  }
  if (!pathMetadata.isFile()) {
    throw new Error(`${LOG_PREFIX} refusing to update non-file .env at ${envPath}.`);
  }

  const descriptorMetadata = fstatSync(descriptor);
  if (
    !descriptorMetadata.isFile() ||
    pathMetadata.dev !== descriptorMetadata.dev ||
    pathMetadata.ino !== descriptorMetadata.ino
  ) {
    throw new EnvPathReplacedError();
  }
}

function openEnvForAppend(envPath: string, afterOpen?: (descriptor: number) => void): number {
  assertRegularEnvPath(envPath);
  const descriptor = openSync(
    envPath,
    constants.O_CREAT | constants.O_RDWR | constants.O_APPEND | constants.O_NOFOLLOW,
    0o666,
  );
  try {
    afterOpen?.(descriptor);
    assertEnvPathMatchesDescriptor(envPath, descriptor);
    return descriptor;
  } catch (error) {
    closeSync(descriptor);
    throw error;
  }
}

function writeAll(descriptor: number, bytes: Uint8Array, operations: EnvFileOperations): void {
  let offset = 0;
  while (offset < bytes.byteLength) {
    const written = operations.write(descriptor, bytes, offset);
    if (!Number.isSafeInteger(written) || written <= 0) {
      throw new Error(`${LOG_PREFIX} failed to make progress while appending to .env.`);
    }
    offset += written;
  }
}

export function ensureEnvAppId(
  envPath: string,
  envKey: string,
  fallback: string,
  preferred: string | undefined,
  operations: EnvFileOperations = defaultEnvFileOperations,
): string {
  mkdirSync(dirname(envPath), { recursive: true });
  // An external atomic writer can replace the pathname after we have locked
  // its old inode. Retry a few times against the visible replacement; a path
  // that keeps changing remains an explicit startup failure rather than a
  // false success on an unlinked descriptor.
  for (let attempt = 0; attempt < 3; attempt += 1) {
    let descriptor: number | undefined;
    let result: string | undefined;
    let actionError: unknown;
    let locked = false;
    try {
      descriptor = openEnvForAppend(envPath, operations.afterOpen);
      operations.waitForLock(descriptor);
      locked = true;
      // The lock is on the .env inode itself. Revalidate the pathname after
      // acquisition, then reparse so every cooperating process observes the
      // previous writer's complete append on the visible file.
      assertEnvPathMatchesDescriptor(envPath, descriptor);
      const content = readFileSync(descriptor, "utf8");
      const existing = parsedEnvValue(content, envKey);
      if (existing !== null) {
        result = preferred ?? existing;
      } else {
        const newline = content.includes("\r\n") ? "\r\n" : "\n";
        const separator = content && !content.endsWith("\n") ? newline : "";
        const addition = Buffer.from(`${separator}${envKey}=${fallback}${newline}`);
        writeAll(descriptor, addition, operations);
        operations.fsync(descriptor);
        result = fallback;
      }
      // A replacement after the append/fsync would otherwise make this call
      // falsely report an ID that is absent from the visible .env.
      assertEnvPathMatchesDescriptor(envPath, descriptor);
    } catch (error) {
      actionError = error;
    }

    let cleanupError: unknown;
    if (locked && descriptor !== undefined) {
      try {
        operations.unlock(descriptor);
      } catch (error) {
        cleanupError = error;
      }
    }
    if (descriptor !== undefined) {
      try {
        closeSync(descriptor);
      } catch (error) {
        cleanupError ??= error;
      }
    }
    if (actionError !== undefined) {
      if (
        actionError instanceof EnvPathReplacedError &&
        cleanupError === undefined &&
        attempt < 2
      ) {
        continue;
      }
      throw actionError;
    }
    if (cleanupError !== undefined) throw cleanupError;
    return result as string;
  }

  // The loop either returns or throws; this keeps TypeScript's control-flow
  // analysis honest if its bounds change in the future.
  throw new EnvPathReplacedError();
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
  private previousBackendSecret: string | undefined;
  private hadPreviousBackendSecret = false;
  private injectedBackendSecret: string | undefined;
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
    if (
      this.injectedBackendSecret !== undefined &&
      process.env.BACKEND_SECRET === this.injectedBackendSecret
    ) {
      if (this.hadPreviousBackendSecret) {
        process.env.BACKEND_SECRET = this.previousBackendSecret;
      } else {
        delete process.env.BACKEND_SECRET;
      }
    }
    this.previousBackendSecret = undefined;
    this.hadPreviousBackendSecret = false;
    this.injectedBackendSecret = undefined;
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
            jwtIssuer: serverConfig.jwtIssuer,
            jwtAudience: serverConfig.jwtAudience,
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

        this.previousBackendSecret = process.env.BACKEND_SECRET;
        this.hadPreviousBackendSecret = Object.hasOwn(process.env, "BACKEND_SECRET");
        if (backendSecret) {
          process.env.BACKEND_SECRET = backendSecret;
          this.injectedBackendSecret = backendSecret;
        }

        process.env[this.envKeys.appId] = appId;
        process.env[this.envKeys.serverUrl] = serverUrl;
        if (telemetryCollectorUrl) {
          process.env[this.envKeys.telemetryCollectorUrl] = telemetryCollectorUrl;
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
