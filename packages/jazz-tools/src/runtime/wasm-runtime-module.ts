import {
  JazzClient,
  loadWasmModule,
  type ConnectSyncRuntimeOptions,
  type WasmModule,
} from "./client.js";
import { ANONYMOUS_JWT_ISSUER, LOCAL_FIRST_JWT_ISSUER } from "./client-session.js";
import type { DbConfig } from "./db.js";
import {
  DbRuntimeModule,
  type DbRuntimeClientContext,
  type DbRuntimeTelemetryContext,
  type RuntimeTokenOptions,
} from "./db-runtime-module.js";
import { installWasmTelemetry } from "./sync-telemetry.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

function setGlobalWasmLogLevel(level?: DbConfig["logLevel"]): void {
  (globalThis as any).__JAZZ_WASM_LOG_LEVEL = level ?? DEFAULT_WASM_LOG_LEVEL;
}

function mintWasmToken(
  wasmModule: WasmModule,
  secret: string,
  issuer: string,
  audience: string,
  ttlSeconds: number,
  nowSeconds: bigint,
): string {
  return wasmModule.WasmRuntime.mintJazzSelfSignedToken(
    secret,
    issuer,
    audience,
    BigInt(ttlSeconds),
    nowSeconds,
  );
}

export class WasmRuntimeModule extends DbRuntimeModule<DbConfig> {
  private get wasmModule(): WasmModule {
    return this.loadedRuntime as WasmModule;
  }

  protected override async loadRuntime(config: DbConfig): Promise<WasmModule> {
    return await loadWasmModule(config.runtimeSources);
  }

  override createClient({
    config,
    schema,
    hasWorker,
    useBinaryEncoding,
    onAuthFailure,
    onBeforeLocalBatchWait,
    onRejectedBatchAcknowledged,
  }: DbRuntimeClientContext<DbConfig>): JazzClient {
    setGlobalWasmLogLevel(config.logLevel);

    const runtimeOptions: ConnectSyncRuntimeOptions = {
      // Worker-bridged runtimes exchange postcard payloads with peers;
      // direct browser/server routing keeps JSON payloads.
      useBinaryEncoding,
      onAuthFailure,
      onBeforeLocalBatchWait,
      onRejectedBatchAcknowledged,
    };

    return JazzClient.connectSync(
      this.wasmModule,
      {
        appId: config.appId,
        schema,
        driver: config.driver,
        // In worker mode, don't connect to server directly — worker handles it.
        serverUrl: hasWorker ? undefined : config.serverUrl,
        env: config.env,
        userBranch: config.userBranch,
        jwtToken: config.jwtToken,
        cookieSession: config.cookieSession,
        adminSecret: config.adminSecret,
        tier: hasWorker ? undefined : "local",
        // Keep worker-bridged browser clients on local durability by default.
        // For direct (non-worker) clients connected to a server, default to edge.
        defaultDurabilityTier: hasWorker ? undefined : config.serverUrl ? "edge" : undefined,
      },
      runtimeOptions,
    );
  }

  override installTelemetry({
    config,
    collectorUrl,
    runtimeThread,
  }: DbRuntimeTelemetryContext<DbConfig>): (() => void) | null {
    return installWasmTelemetry({
      wasmModule: this.wasmModule,
      collectorUrl,
      appId: config.appId,
      runtimeThread,
    });
  }

  override mintLocalFirstToken(options: RuntimeTokenOptions): string {
    return mintWasmToken(
      this.wasmModule,
      options.secret,
      LOCAL_FIRST_JWT_ISSUER,
      options.audience,
      options.ttlSeconds,
      options.nowSeconds,
    );
  }

  override mintAnonymousToken(options: RuntimeTokenOptions): string {
    return mintWasmToken(
      this.wasmModule,
      options.secret,
      ANONYMOUS_JWT_ISSUER,
      options.audience,
      options.ttlSeconds,
      options.nowSeconds,
    );
  }
}
