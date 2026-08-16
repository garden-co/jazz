import {
  JazzClient,
  loadWasmModule,
  type ConnectRuntimeOptions,
  type WasmModule,
} from "./client.js";
import { resolveDefaultPersistentDbName, type DbConfig } from "./db.js";
import {
  RuntimeSource,
  type BrowserWorkerConnection,
  type BrowserWorkerConnectionContext,
  type BrowserFollowerConnection,
  type BrowserFollowerConnectionContext,
  type RuntimeClientContext,
  type RuntimeTelemetryContext,
  type RuntimeTokenModule,
} from "./runtime-source.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { DedicatedBrowserWorkerConnection } from "./native-runtime/browser-worker-connection.js";
import { MessagePortBrowserFollowerConnection } from "./native-runtime/browser-follower-connection.js";
import { installWasmTelemetry } from "./sync-telemetry.js";
import { resolveClientSessionSync } from "./client-session.js";
import type { WasmSchema } from "../drivers/types.js";
import { resolveInitialSyncFlushEvery, resolveRuntimeIdentity } from "./runtime-identity.js";
import { httpUrlToWs } from "./url.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

function setGlobalWasmLogLevel(level?: DbConfig["logLevel"]): void {
  (globalThis as any).__JAZZ_WASM_LOG_LEVEL = level ?? DEFAULT_WASM_LOG_LEVEL;
}

export class DefaultRuntimeSource extends RuntimeSource<DbConfig> {
  override readonly supportsBrowserWorker = true;
  private module: WasmModule | null = null;
  private ownerRuntime: NativeRuntimeAdapter | null = null;

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

    const browserMode = isPersistentBrowserConfig(config);
    const persistentBrowserDbName = browserMode
      ? resolveDefaultPersistentDbName(config)
      : undefined;
    const { node, author } = resolveRuntimeIdentity(config, persistentBrowserDbName, "main-node");
    const flushEvery = resolveInitialSyncFlushEvery(config);
    const mainThreadPeerRuntime = this.nativeSchemaView(
      schema,
      node,
      author,
      flushEvery,
      !browserMode,
    );
    if (browserMode) {
      mainThreadPeerRuntime.setNonDurableClient();
    }

    return JazzClient.connectWithRuntime(
      mainThreadPeerRuntime,
      this.connectContext(config, schema),
      runtimeOptions,
    );
  }

  override createBrowserWorkerConnection({
    config,
    schema,
    client,
    leadershipId,
    workerLockName,
    onAuthFailure,
    onAuthRestored,
    onFailure,
    onFollowerPortClosed,
  }: BrowserWorkerConnectionContext<DbConfig>): BrowserWorkerConnection {
    const runtime = client.getRuntime();
    if (!(runtime instanceof NativeRuntimeAdapter)) {
      throw new Error("Browser worker connections require the native runtime adapter");
    }
    const dbName = resolveDefaultPersistentDbName(config);
    const { node, author } = resolveRuntimeIdentity(config, dbName);
    return new DedicatedBrowserWorkerConnection(
      runtime,
      {
        runtimeSources: config.runtimeSources,
        schema,
        dbName,
        node,
        author,
        initialSyncFlushEvery: resolveInitialSyncFlushEvery(config),
        appId: config.appId,
        serverUrl: config.serverUrl ? httpUrlToWs(config.serverUrl, config.appId) : undefined,
        authJson: JSON.stringify(runtimeAuth(config)),
        sessionClaims: resolveClientSessionSync(config)?.claims ?? {},
        leadershipId,
        workerLockName,
        logLevel: config.logLevel,
        telemetryCollectorUrl: config.telemetryCollectorUrl,
      },
      { onAuthFailure, onAuthRestored, onFailure, onFollowerPortClosed },
    );
  }

  override createBrowserFollowerConnection({
    config,
    client,
    port,
    onAuthFailure,
    onAuthRestored,
    onFailure,
  }: BrowserFollowerConnectionContext<DbConfig>): BrowserFollowerConnection {
    const runtime = client.getRuntime();
    if (!(runtime instanceof NativeRuntimeAdapter)) {
      throw new Error("Browser follower connections require the native runtime adapter");
    }
    const sessionClaims = resolveClientSessionSync(config)?.claims ?? {};
    const connection = new MessagePortBrowserFollowerConnection(runtime, port, sessionClaims, {
      onAuthFailure,
      onAuthRestored,
      onFailure,
    });
    connection.updateAuth(JSON.stringify(runtimeAuth(config)), sessionClaims);
    return connection;
  }

  private nativeSchemaView(
    schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
    flushEvery: number,
    historyComplete: boolean,
  ): NativeRuntimeAdapter {
    if (!this.ownerRuntime || this.ownerRuntime.isClosed()) {
      this.ownerRuntime = new NativeRuntimeAdapter(
        this.wasmModule.WasmDb,
        schema,
        node,
        author,
        1,
        historyComplete,
        { initialSyncFlushEvery: flushEvery },
      );
      return this.ownerRuntime;
    }
    return Object.keys(schema).length === 0
      ? this.ownerRuntime
      : this.ownerRuntime.registerSchemaView(schema);
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

function isPersistentBrowserConfig(config: DbConfig): boolean {
  return isBrowserRuntime() && (config.driver?.type ?? "persistent") === "persistent";
}

function runtimeAuth(config: DbConfig): Record<string, unknown> {
  return {
    jwt_token: config.jwtToken ?? null,
    ...(config.adminSecret ? { admin_secret: config.adminSecret } : {}),
    ...(config.backendSecret ? { backend_secret: config.backendSecret } : {}),
    ...(config.cookieSession ? { backend_session: config.cookieSession } : {}),
  };
}
