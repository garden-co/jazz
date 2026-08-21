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
  type RuntimeTokenOptions,
} from "./runtime-source.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { DedicatedBrowserWorkerConnection } from "./native-runtime/browser-worker-connection.js";
import { MessagePortBrowserFollowerConnection } from "./native-runtime/browser-follower-connection.js";
import { installWasmTelemetry } from "./sync-telemetry.js";
import { parseJwtPayload, resolveClientSessionSync } from "./client-session.js";
import type { WasmSchema } from "../drivers/types.js";
import { httpUrlToWs } from "./url.js";
import { authorBytesForSubject, isUsableSubject } from "./author-id.js";

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

function subjectFromConfig(config: DbConfig): string | null {
  if (config.cookieSession?.user_id && isUsableSubject(config.cookieSession.user_id)) {
    return config.cookieSession.user_id;
  }
  const payload = parseJwtPayload(config.jwtToken ?? "");
  return typeof payload?.sub === "string" && isUsableSubject(payload.sub) ? payload.sub : null;
}

function persistentIdentitySeed(config: DbConfig, subject: string | null): string {
  return `${config.appId}:${config.env ?? "dev"}:${config.userBranch ?? "main"}:${subject ?? "anonymous"}`;
}

function initialSyncFlushEvery(config: DbConfig): number {
  const value = config.initialSyncFlushEvery ?? 512;
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error("initialSyncFlushEvery must be a positive integer");
  }
  return value;
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

    const subject = subjectFromConfig(config);
    const identitySeed = persistentIdentitySeed(config, subject);
    // A persistent worker may replay a main-thread-authored transaction after
    // the page has reopened. Keep that logical client's node identity stable
    // for the persistence namespace so the fresh in-memory runtime still owns
    // the transaction's eventual rejection/settlement notifications.
    const node = isPersistentBrowserConfig(config)
      ? deterministicBytes(`${identitySeed}:${resolveDefaultPersistentDbName(config)}:main-node`)
      : randomBytes();
    const author = subject
      ? authorBytesForSubject(subject)
      : deterministicBytes(`${identitySeed}:author`);
    const flushEvery = initialSyncFlushEvery(config);
    const browserMode = isPersistentBrowserConfig(config);
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
    const subject = subjectFromConfig(config);
    const identitySeed = persistentIdentitySeed(config, subject);
    const dbName = resolveDefaultPersistentDbName(config);
    const author = subject
      ? authorBytesForSubject(subject)
      : deterministicBytes(`${identitySeed}:author`);
    return new DedicatedBrowserWorkerConnection(
      runtime,
      {
        runtimeSources: config.runtimeSources,
        schema,
        dbName,
        node: deterministicBytes(`${identitySeed}:${dbName}:node`),
        author,
        initialSyncFlushEvery: initialSyncFlushEvery(config),
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
