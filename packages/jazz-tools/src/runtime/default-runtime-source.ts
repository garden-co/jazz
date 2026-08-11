import {
  JazzClient,
  loadWasmModule,
  type ConnectRuntimeOptions,
  type WasmModule,
} from "./client.js";
import { resolveDefaultPersistentDbName, type DbConfig } from "./db.js";
import {
  RuntimeSource,
  type RuntimeClientContext,
  type RuntimeTelemetryContext,
  type RuntimeTokenModule,
} from "./runtime-source.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { PersistentBrowserOpfsRuntime } from "./native-runtime/persistent-browser-runtime.js";
import { installWasmTelemetry } from "./sync-telemetry.js";
import { resolveInitialSyncFlushEvery, resolveRuntimeIdentity } from "./runtime-identity.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

function setGlobalWasmLogLevel(level?: DbConfig["logLevel"]): void {
  (globalThis as any).__JAZZ_WASM_LOG_LEVEL = level ?? DEFAULT_WASM_LOG_LEVEL;
}

export class DefaultRuntimeSource extends RuntimeSource<DbConfig> {
  private module: WasmModule | null = null;

  private get wasmModule(): WasmModule {
    if (!this.module) {
      throw new Error("Default runtime source is not loaded");
    }
    return this.module;
  }

  override async load(config: DbConfig): Promise<void> {
    this.module ??= await loadWasmModule(config.runtimeSources);
  }

  override createClient({
    config,
    schema,
    onAuthFailure,
  }: RuntimeClientContext<DbConfig>): JazzClient {
    setGlobalWasmLogLevel(config.logLevel);

    const runtimeOptions: ConnectRuntimeOptions = {
      onAuthFailure,
    };

    const persistentBrowserDbName =
      isBrowserRuntime() && (config.driver?.type ?? "persistent") === "persistent"
        ? resolveDefaultPersistentDbName(config)
        : undefined;
    const { node, author } = resolveRuntimeIdentity(config, persistentBrowserDbName);
    const flushEvery = resolveInitialSyncFlushEvery(config);
    const mainThreadPeerRuntime = persistentBrowserDbName
      ? new PersistentBrowserOpfsRuntime(
          config.runtimeSources,
          schema,
          persistentBrowserDbName,
          node,
          author,
          flushEvery,
        )
      : new NativeRuntimeAdapter(this.wasmModule.WasmDb, schema, node, author, 1, true, {
          initialSyncFlushEvery: flushEvery,
        });

    return JazzClient.connectWithRuntime(
      mainThreadPeerRuntime,
      this.connectContext(config, schema),
      runtimeOptions,
    );
  }

  override installTelemetry({
    config,
    collectorUrl,
    runtimeThread,
  }: RuntimeTelemetryContext<DbConfig>): (() => void) | null {
    return installWasmTelemetry({
      wasmModule: this.wasmModule,
      collectorUrl,
      appId: config.appId,
      runtimeThread,
    });
  }

  protected override tokenModule(): RuntimeTokenModule {
    return this.wasmModule;
  }
}

function isBrowserRuntime(): boolean {
  return typeof window !== "undefined" && typeof Worker !== "undefined";
}
