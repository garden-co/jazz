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
  type RuntimeTokenModule,
} from "../runtime/runtime-source.js";
import { RnDbShim, type JazzRnModule } from "./native-db.js";
import { importJazzRn } from "./jazz-rn-importer.js";

export interface ReactNativeDbConfig extends DbConfig {
  /** Absolute filesystem directory containing the persistent SQLite database. */
  dataDirectory?: string;
}

function stableDbNameHash(value: string): string {
  // FNV-1a over Unicode code points is small enough for the RN bootstrap path,
  // deterministic across JavaScript engines, and does not need node:crypto.
  let hash = 0x811c9dc5;
  for (const character of value) {
    hash ^= character.codePointAt(0) ?? 0;
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

/** @internal Preserve safe names and put transformed names in a disjoint namespace. */
export function sanitizeReactNativeDbName(dbName: string): string {
  const sanitized = Array.from(dbName, (character) =>
    /[A-Za-z0-9_-]/.test(character) ? character : "_",
  ).join("");
  // `~` is deliberately outside the accepted raw-name alphabet. Without this
  // marker, a safe raw name could equal another raw name's sanitized+hash form.
  return sanitized === dbName ? sanitized : `~${sanitized}-${stableDbNameHash(dbName)}`;
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
      this.connectContext(config, schema),
      runtimeOptions,
    );
  }

  override installTelemetry(
    _context: RuntimeTelemetryContext<ReactNativeDbConfig>,
  ): (() => void) | null {
    return null;
  }

  protected override tokenModule(): RuntimeTokenModule {
    return this.nativeModule;
  }
}
