import { JazzClient, type ConnectRuntimeOptions } from "../runtime/client.js";
import { resolveDefaultPersistentDbName, type DbConfig } from "../runtime/db.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { isModuleNotFoundError } from "../runtime/peer-dep-error.js";
import {
  resolveInitialSyncFlushEvery,
  resolveRuntimeIdentity,
} from "../runtime/runtime-identity.js";
import {
  RuntimeSource,
  type RuntimeClientContext,
  type RuntimeTelemetryContext,
  type RuntimeTokenOptions,
} from "../runtime/runtime-source.js";
import { RnDbShim, type JazzRnModule } from "./native-db.js";
import { importJazzRn } from "./jazz-rn-importer.js";

export interface ReactNativeDbConfig extends DbConfig {
  /** Absolute filesystem directory containing the persistent SQLite database. */
  dataDirectory?: string;
}

/** @internal Preserve the binding's historical filename-safe database naming. */
export function sanitizeReactNativeDbName(dbName: string): string {
  return Array.from(dbName, (character) =>
    /[A-Za-z0-9_-]/.test(character) ? character : "_",
  ).join("");
}

/** @internal Resolve and validate the native SQLite path for a persistent config. */
export function resolveReactNativePersistentPath(config: ReactNativeDbConfig): string {
  const directory = config.dataDirectory?.trim();
  if (!directory) {
    throw new Error("React Native persistent storage requires an absolute dataDirectory path");
  }
  if (!directory.startsWith("/") && !/^[A-Za-z]:[\\/]/.test(directory)) {
    throw new Error(
      `React Native dataDirectory must be an absolute filesystem path, received ${JSON.stringify(directory)}`,
    );
  }

  const withoutTrailingSeparators = directory.replace(/[\\/]+$/, "");
  const base = withoutTrailingSeparators || directory[0];
  const dbName = sanitizeReactNativeDbName(resolveDefaultPersistentDbName(config));
  return base === "/" ? `/${dbName}.db` : `${base}/${dbName}.db`;
}

export class ReactNativeRuntimeSource extends RuntimeSource<ReactNativeDbConfig> {
  private module: JazzRnModule | null = null;

  private get nativeModule(): JazzRnModule {
    if (!this.module) {
      throw new Error("React Native runtime source is not loaded");
    }
    return this.module;
  }

  override async load(_config: ReactNativeDbConfig): Promise<void> {
    if (this.module) return;

    try {
      this.module = (await importJazzRn()) as unknown as JazzRnModule;
    } catch (error) {
      if (!isModuleNotFoundError(error, "jazz-rn")) {
        throw error;
      }
      throw new Error(
        `[jazz-tools] The "jazz-rn" peer dependency is required by jazz-tools/react-native but is not installed. ` +
          `Install it with npm install jazz-rn, pnpm add jazz-rn, or yarn add jazz-rn, then rebuild the native app.`,
        { cause: error },
      );
    }
  }

  override createClient({
    config,
    schema,
    onAuthFailure,
  }: RuntimeClientContext<ReactNativeDbConfig>): JazzClient {
    const persistent = (config.driver?.type ?? "persistent") === "persistent";
    const dbName = persistent ? resolveDefaultPersistentDbName(config) : undefined;
    const persistentPath = persistent ? resolveReactNativePersistentPath(config) : undefined;
    const { node, author } = resolveRuntimeIdentity(config, dbName);
    const runtime = new NativeRuntimeAdapter(
      RnDbShim.forModule(this.nativeModule),
      schema,
      node,
      author,
      1,
      // App clients hold only a partial history. Keeping this false also restores
      // durable pending uploads when a persistent database is reopened (INV-API-30).
      false,
      {
        persistentPath,
        initialSyncFlushEvery: resolveInitialSyncFlushEvery(config),
      },
    );
    const runtimeOptions: ConnectRuntimeOptions = { onAuthFailure };

    return JazzClient.connectWithRuntime(
      runtime,
      {
        appId: config.appId,
        schema,
        driver: config.driver,
        serverUrl: config.serverUrl,
        env: config.env,
        userBranch: config.userBranch,
        jwtToken: config.jwtToken,
        cookieSession: config.cookieSession,
        backendSecret: config.backendSecret,
        adminSecret: config.adminSecret,
        tier: "local",
      },
      runtimeOptions,
    );
  }

  override installTelemetry(
    _context: RuntimeTelemetryContext<ReactNativeDbConfig>,
  ): (() => void) | null {
    return null;
  }

  override mintLocalFirstToken(options: RuntimeTokenOptions): string {
    return this.nativeModule.mintLocalFirstToken(
      options.secret,
      options.audience,
      options.ttlSeconds,
      options.nowSeconds,
    );
  }

  override mintAnonymousToken(options: RuntimeTokenOptions): string {
    return this.nativeModule.mintAnonymousToken(
      options.secret,
      options.audience,
      options.ttlSeconds,
      options.nowSeconds,
    );
  }
}
