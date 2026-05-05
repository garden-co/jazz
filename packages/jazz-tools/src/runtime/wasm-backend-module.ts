import {
  JazzClient,
  loadWasmModule,
  type ConnectSyncRuntimeOptions,
  type WasmModule,
} from "./client.js";
import { ANONYMOUS_JWT_ISSUER, LOCAL_FIRST_JWT_ISSUER } from "./client-session.js";
import type { DbConfig } from "./db.js";
import {
  DbBackendModule,
  type DbBackendClientContext,
  type DbBackend,
  type DbBackendCreateContext,
  type DbBackendTelemetryContext,
  type BackendTokenOptions,
} from "./db-backend.js";
import { installWasmTelemetry } from "./sync-telemetry.js";
import { BrowserWasmBackend, shouldUseBrowserWasmBackend } from "./browser-wasm-backend.js";

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

export class WasmBackendModule extends DbBackendModule<DbConfig> {
  private get wasmModule(): WasmModule {
    return this.loadedResources as WasmModule;
  }

  protected override async loadResources(config: DbConfig): Promise<WasmModule> {
    return await loadWasmModule(config.runtimeSources);
  }

  override async createBackend(
    context: DbBackendCreateContext<DbConfig>,
  ): Promise<DbBackend<DbConfig>> {
    if (shouldUseBrowserWasmBackend(context.config)) {
      return await BrowserWasmBackend.create({
        config: context.config,
        host: context.host,
        createClient: (clientContext) => this.createClient(clientContext),
      });
    }

    return await super.createBackend(context);
  }

  override createClient({
    config,
    schema,
    hasWorker,
    useBinaryEncoding,
    onAuthFailure,
    onRejectedBatchAcknowledged,
  }: DbBackendClientContext<DbConfig>): JazzClient {
    setGlobalWasmLogLevel(config.logLevel);

    const runtimeOptions: ConnectSyncRuntimeOptions = {
      // Worker-bridged runtimes exchange postcard payloads with peers;
      // direct browser/server routing keeps JSON payloads.
      useBinaryEncoding,
      onAuthFailure,
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
  }: DbBackendTelemetryContext<DbConfig>): (() => void) | null {
    return installWasmTelemetry({
      wasmModule: this.wasmModule,
      collectorUrl,
      appId: config.appId,
      runtimeThread,
    });
  }

  override mintLocalFirstToken(options: BackendTokenOptions): string {
    return mintWasmToken(
      this.wasmModule,
      options.secret,
      LOCAL_FIRST_JWT_ISSUER,
      options.audience,
      options.ttlSeconds,
      options.nowSeconds,
    );
  }

  override mintAnonymousToken(options: BackendTokenOptions): string {
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
