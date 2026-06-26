import {
  JazzClient,
  loadWasmModule,
  type ConnectRuntimeOptions,
  type WasmModule,
} from "./client.js";
import { LOCAL_FIRST_JWT_ISSUER } from "./client-session.js";
import type { DbConfig } from "./db.js";
import {
  DbRuntimeModule,
  type DbRuntimeClientContext,
  type DbRuntimeTelemetryContext,
  type RuntimeTokenOptions,
} from "./db-runtime-module.js";
import { DirectCoreRuntime } from "./direct-core/runtime.js";
import { installWasmTelemetry } from "./sync-telemetry.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

function setGlobalWasmLogLevel(level?: DbConfig["logLevel"]): void {
  (globalThis as any).__JAZZ_WASM_LOG_LEVEL = level ?? DEFAULT_WASM_LOG_LEVEL;
}

function mintWasmToken(
  wasmModule: WasmModule,
  seedB64: string,
  _issuer: string,
  audience: string,
  ttlSeconds: number,
  nowSeconds: bigint,
): string {
  return wasmModule.mintLocalFirstToken(seedB64, audience, ttlSeconds, nowSeconds);
}

function deterministicBytes(seed: string): Uint8Array {
  let hash = 0x811c9dc5;
  const bytes = new Uint8Array(16);
  for (let round = 0; round < 4; round += 1) {
    for (let i = 0; i < seed.length; i += 1) {
      hash ^= seed.charCodeAt(i) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    const view = new DataView(bytes.buffer);
    view.setUint32(round * 4, hash >>> 0, true);
  }
  return bytes;
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
    durablePeer,
    onAuthFailure,
  }: DbRuntimeClientContext<DbConfig>): JazzClient {
    setGlobalWasmLogLevel(config.logLevel);

    const hasDurablePeer = durablePeer !== null;
    const runtimeOptions: ConnectRuntimeOptions = {
      onAuthFailure,
    };

    const mainThreadPeerRuntime = new DirectCoreRuntime(
      this.wasmModule.WasmDb,
      schema,
      deterministicBytes(
        `${config.appId}:${config.env ?? "dev"}:${config.userBranch ?? "main"}:node`,
      ),
      deterministicBytes(
        `${config.appId}:${config.env ?? "dev"}:${config.userBranch ?? "main"}:author`,
      ),
      1,
      !hasDurablePeer,
    );

    return JazzClient.connectWithRuntime(
      mainThreadPeerRuntime,
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
        tier: hasDurablePeer ? undefined : "local",
        defaultDurabilityTier: hasDurablePeer ? undefined : config.serverUrl ? "edge" : undefined,
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
    return this.wasmModule.mintAnonymousToken(
      options.secret,
      options.audience,
      options.ttlSeconds,
      options.nowSeconds,
    );
  }
}
