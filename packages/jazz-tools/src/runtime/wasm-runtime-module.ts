import {
  JazzClient,
  loadWasmModule,
  type ConnectRuntimeOptions,
  type WasmModule,
} from "./client.js";
import { resolveDefaultPersistentDbName, type DbConfig } from "./db.js";
import {
  DbRuntimeModule,
  type DbRuntimeClientContext,
  type DbRuntimeTelemetryContext,
  type RuntimeTokenOptions,
} from "./db-runtime-module.js";
import { CoreRuntime } from "./core-runtime/runtime.js";
import { PersistentBrowserOpfsRuntime } from "./core-runtime/persistent-browser-runtime.js";
import { installWasmTelemetry } from "./sync-telemetry.js";
import { parseJwtPayload } from "./client-session.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

function setGlobalWasmLogLevel(level?: DbConfig["logLevel"]): void {
  (globalThis as any).__JAZZ_WASM_LOG_LEVEL = level ?? DEFAULT_WASM_LOG_LEVEL;
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

function randomBytes(): Uint8Array {
  const bytes = new Uint8Array(16);
  if (globalThis.crypto?.getRandomValues) {
    globalThis.crypto.getRandomValues(bytes);
    return bytes;
  }
  return deterministicBytes(`${Date.now()}:${Math.random()}`);
}

function uuidBytes(value: string): Uint8Array | null {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) {
    return null;
  }
  const bytes = new Uint8Array(16);
  for (let index = 0; index < 16; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function subjectFromConfig(config: DbConfig): string | null {
  if (config.cookieSession?.user_id) return config.cookieSession.user_id;
  const payload = parseJwtPayload(config.jwtToken ?? "");
  return typeof payload?.sub === "string" && payload.sub.trim() ? payload.sub.trim() : null;
}

function persistentIdentitySeed(config: DbConfig, subject: string | null): string {
  return `${config.appId}:${config.env ?? "dev"}:${config.userBranch ?? "main"}:${subject ?? "anonymous"}`;
}

function authorBytesForSubject(subject: string, fallbackSeed: string): Uint8Array {
  return uuidBytes(subject) ?? deterministicBytes(`${fallbackSeed}:author`);
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
    onAuthFailure,
  }: DbRuntimeClientContext<DbConfig>): JazzClient {
    setGlobalWasmLogLevel(config.logLevel);

    const runtimeOptions: ConnectRuntimeOptions = {
      onAuthFailure,
    };

    const subject = subjectFromConfig(config);
    const persistentBrowserDbName =
      isBrowserRuntime() && (config.driver?.type ?? "persistent") === "persistent"
        ? resolveDefaultPersistentDbName(config)
        : undefined;
    const identitySeed = persistentIdentitySeed(config, subject);
    const node = persistentBrowserDbName
      ? deterministicBytes(`${identitySeed}:${persistentBrowserDbName}:node`)
      : randomBytes();
    const author = subject
      ? authorBytesForSubject(subject, identitySeed)
      : deterministicBytes(`${identitySeed}:author`);
    const mainThreadPeerRuntime = persistentBrowserDbName
      ? new PersistentBrowserOpfsRuntime(
          config.runtimeSources,
          schema,
          persistentBrowserDbName,
          node,
          author,
        )
      : new CoreRuntime(this.wasmModule.WasmDb, schema, node, author, 1, true);

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
        tier: "local",
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
    return this.wasmModule.mintLocalFirstToken(
      options.secret,
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

function isBrowserRuntime(): boolean {
  return typeof window !== "undefined" && typeof Worker !== "undefined";
}
