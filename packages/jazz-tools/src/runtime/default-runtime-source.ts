import { JazzClient, type ConnectRuntimeOptions } from "./client.js";
import { loadWasmModule, type WasmModule } from "./wasm-loader.js";
import type { AppContext } from "./context.js";
import { resolveDefaultPersistentDbName, type DbConfig } from "./db.js";
import { getTrustedReservedSession, setTrustedReservedSession } from "./db-internal-session.js";
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
import type { NativeSelfSignedClientProof } from "./native-runtime/native-codec.js";
import {
  SharedBrowserForegroundNodeLease,
  SharedBrowserWorkerConnection,
} from "./native-runtime/browser-shared-worker-connection.js";
import { AttachedBrowserWorkerConnection } from "./native-runtime/attached-browser-worker-connection.js";
import { MessagePortBrowserFollowerConnection } from "./native-runtime/browser-follower-connection.js";
import { installWasmTelemetry } from "./sync-telemetry.js";
import {
  ANONYMOUS_JWT_ISSUER,
  internalSessionFromVerifiedReservedJwtPayload,
  LOCAL_FIRST_JWT_ISSUER,
  parseJwtPayload,
  resolveClientInternalSessionSync,
} from "./client-session.js";
import type { WasmSchema } from "../drivers/types.js";
import { httpUrlToWs } from "./url.js";
import { authorBytesForSession, canonicalAuthorSubject } from "./author-id.js";
import {
  createBrowserAuthSessionKey,
  createBrowserStorageOwner,
  createBrowserWorkerFingerprint,
} from "./browser-worker-config.js";
import { getRuntimeSchemaCacheKey } from "../drivers/schema-wire.js";

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

function sessionFromConfig(config: DbConfig) {
  return resolveClientInternalSessionSync({
    ...config,
    trustedReservedSession: getTrustedReservedSession(config),
  });
}

/**
 * Admit the identity carried by a same-origin inspector attachment.
 *
 * The session is used only to bind the main-thread peer to the host worker's
 * identity. Reserved-issuer tokens remain fail-closed: the native constructor
 * verifies the self-signed proof and audience before this peer can open.
 */
export function trustAttachedBrowserWorkerSession(config: DbConfig): void {
  const attachedSession = config.runtimeSources?.browserWorkerSession;
  if (!config.runtimeSources?.browserWorkerPort || !attachedSession) return;

  const payload = parseJwtPayload(config.jwtToken ?? "");
  const authMode =
    payload?.iss === LOCAL_FIRST_JWT_ISSUER
      ? "local-first"
      : payload?.iss === ANONYMOUS_JWT_ISSUER
        ? "anonymous"
        : null;
  if (!authMode) return;

  const tokenSession = internalSessionFromVerifiedReservedJwtPayload(payload ?? {}, authMode);
  if (
    !tokenSession ||
    tokenSession.issuer !== attachedSession.issuer ||
    tokenSession.user_id !== attachedSession.user_id ||
    tokenSession.authMode !== attachedSession.authMode
  ) {
    throw new Error("Attached browser worker session does not match its identity token");
  }

  // Claims come from the token, not the cross-realm session object. The native
  // open below verifies the token before accepting this author.
  setTrustedReservedSession(config, tokenSession);
}

function runtimeAuthorFromConfig(config: DbConfig) {
  const session = sessionFromConfig(config);
  if (session) return session;

  // A sessionless default runtime may be an explicitly trusted admin open. Its
  // raw config still needs a syntactically valid, untrusted author because the
  // native constructor validates that input before its separate admin ABI
  // derives SYSTEM. This placeholder never becomes the runtime's author.
  if (config.adminSecret) {
    return { issuer: "https://jazz.invalid", user_id: "backend-open" };
  }
  throw new Error("Default runtime requires a verified session or admin credential");
}

function isBackendRuntime(config: DbConfig): boolean {
  return !sessionFromConfig(config) && Boolean(config.adminSecret);
}

export function selfSignedClientProofFromConfig(
  config: DbConfig,
  session: ReturnType<typeof sessionFromConfig>,
): NativeSelfSignedClientProof | undefined {
  if (
    !session ||
    !config.jwtToken ||
    (session.issuer !== LOCAL_FIRST_JWT_ISSUER && session.issuer !== ANONYMOUS_JWT_ISSUER)
  ) {
    return undefined;
  }
  return {
    token: config.jwtToken,
    appId: config.appId,
    claimedAuthor: canonicalAuthorSubject(session.issuer, session.user_id),
  };
}

function initialSyncFlushEvery(config: DbConfig): number {
  const value = config.initialSyncFlushEvery ?? 512;
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error("initialSyncFlushEvery must be a positive integer");
  }
  return value;
}

function browserWorkerRuntimeSources(config: DbConfig): DbConfig["runtimeSources"] {
  const sources = config.runtimeSources;
  if (sources?.wasmModule || sources?.wasmSource || sources?.wasmUrl || sources?.baseUrl) {
    return sources;
  }
  // The bundled SharedWorker contains wasm-bindgen glue and ships its matching
  // binary beside that glue. Let wasm-bindgen resolve that worker-local pair.
  // Passing the page bundle's `jazz-wasm` URL here can cross versions during a
  // rebuild or cache transition, leaving a new binary to instantiate against
  // old worker glue.
  return sources;
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

  override admitConfig(config: DbConfig): void {
    trustAttachedBrowserWorkerSession(config);
  }

  override createClient({
    config,
    schema,
    onAuthFailure,
    foregroundNodeLease,
  }: RuntimeClientContext<DbConfig>): JazzClient {
    setGlobalWasmLogLevel(config.logLevel);

    const runtimeOptions: ConnectRuntimeOptions = {
      onAuthFailure,
    };

    const session = sessionFromConfig(config);
    const selfSignedClientProof = selfSignedClientProofFromConfig(config, session);
    const backendMode = isBackendRuntime(config);
    // The persistent worker owns durable recovery. A foreground runtime owns
    // only its live optimistic writes, so every new runtime needs a fresh node
    // identity. Reusing a deterministic node across independently opened tabs
    // would let their fresh HLC registers mint the same TxId before either has
    // observed the other's first commit.
    const node = foregroundNodeLease?.node.slice() ?? randomBytes();
    const author = authorBytesForSession(runtimeAuthorFromConfig(config));
    const flushEvery = initialSyncFlushEvery(config);
    const browserMode = isPersistentBrowserConfig(config);
    const mainThreadPeerRuntime = this.nativeSchemaView(
      schema,
      node,
      author,
      flushEvery,
      !browserMode,
      selfSignedClientProof,
      backendMode,
    );
    if (foregroundNodeLease) {
      mainThreadPeerRuntime.seedForegroundTxTimeHighWater(foregroundNodeLease.confirmedTxTime);
    }
    if (browserMode) {
      mainThreadPeerRuntime.setNonDurableClient();
      if (!foregroundNodeLease) {
        throw new Error("Persistent browser runtime requires a foreground node lease");
      }
    }

    const context: AppContext = {
      appId: config.appId,
      schema,
      driver: config.driver,
      serverUrl: config.serverUrl,
      env: config.env,
      jwtToken: config.jwtToken,
      cookieSession: config.cookieSession,
      adminSecret: config.adminSecret,
      tier: "local",
    };
    setTrustedReservedSession(context, getTrustedReservedSession(config));
    return JazzClient.connectWithRuntime(mainThreadPeerRuntime, context, runtimeOptions);
  }

  override async acquireBrowserForegroundNodeLease(config: DbConfig) {
    if (!isPersistentBrowserConfig(config)) {
      throw new Error("Browser foreground node leases require persistent browser storage");
    }
    const dbName = resolveDefaultPersistentDbName(config);
    return await SharedBrowserForegroundNodeLease.acquire({
      runtimeSources: browserWorkerRuntimeSources(config),
      dbName,
      storageOwner: createBrowserStorageOwner(config),
    });
  }

  override async acquireForegroundNodeLease(config: DbConfig) {
    if (!isNodeRuntime() || (config.driver?.type ?? "persistent") !== "persistent") {
      return undefined;
    }
    // This guarded dynamic import keeps Node filesystem code out of the path
    // executed by browser/RN bundles while remaining visible to Node test and
    // package tooling.
    const { acquireNodeForegroundNodeLease } =
      await import("./native-runtime/node-foreground-node-lease.js");
    return await acquireNodeForegroundNodeLease({
      appId: config.appId,
      env: config.env ?? "dev",
      authScope: createBrowserAuthSessionKey(config),
    });
  }

  override createBrowserWorkerConnection({
    config,
    schema,
    client,
    onAuthFailure,
    onAuthRestored,
    onExplicitOfflineChange,
    onFailure,
    onStorageReset,
    onStorageInvalidated,
  }: BrowserWorkerConnectionContext<DbConfig>): BrowserWorkerConnection {
    const runtime = client.getRuntime();
    if (!(runtime instanceof NativeRuntimeAdapter)) {
      throw new Error("Browser worker connections require the native runtime adapter");
    }
    const session = sessionFromConfig(config);
    const selfSignedClientProof = selfSignedClientProofFromConfig(config, session);
    const backendMode = isBackendRuntime(config);
    if (backendMode) {
      throw new Error(
        "Persistent browser workers require a verified client session, not backend credentials",
      );
    }
    const dbName = resolveDefaultPersistentDbName(config);
    const author = authorBytesForSession(runtimeAuthorFromConfig(config));
    if (config.runtimeSources?.browserWorkerPort) {
      return new AttachedBrowserWorkerConnection(
        runtime,
        config.runtimeSources.browserWorkerPort,
        sessionFromConfig(config)?.claims ?? {},
        dbName,
        {
          onAuthFailure,
          onAuthRestored,
          onExplicitOfflineChange,
          onFailure,
          onStorageReset,
          onStorageInvalidated,
        },
      );
    }
    return new SharedBrowserWorkerConnection(
      runtime,
      {
        runtimeSources: browserWorkerRuntimeSources(config),
        schema,
        dbName,
        author,
        selfSignedClientProof,
        initialSyncFlushEvery: initialSyncFlushEvery(config),
        appId: config.appId,
        storageOwner: createBrowserStorageOwner(config),
        authSessionKey: createBrowserAuthSessionKey(config),
        serverUrl: config.serverUrl ? httpUrlToWs(config.serverUrl, config.appId) : undefined,
        authJson: JSON.stringify(browserWorkerTransportAuth(config)),
        sessionClaims: sessionFromConfig(config)?.claims ?? {},
        logLevel: config.logLevel,
        telemetryCollectorUrl: config.telemetryCollectorUrl,
      },
      createBrowserWorkerFingerprint(config, dbName, getRuntimeSchemaCacheKey(schema)),
      {
        onAuthFailure,
        onAuthRestored,
        onExplicitOfflineChange,
        onFailure,
        onStorageReset,
        onStorageInvalidated,
      },
    );
  }

  override createBrowserFollowerConnection({
    config,
    client,
    port,
    onAuthFailure,
    onAuthRestored,
    onExplicitOfflineChange,
    onFailure,
  }: BrowserFollowerConnectionContext<DbConfig>): BrowserFollowerConnection {
    const runtime = client.getRuntime();
    if (!(runtime instanceof NativeRuntimeAdapter)) {
      throw new Error("Browser follower connections require the native runtime adapter");
    }
    const sessionClaims = sessionFromConfig(config)?.claims ?? {};
    const connection = new MessagePortBrowserFollowerConnection(
      runtime,
      port,
      sessionClaims,
      null,
      {
        onAuthFailure,
        onAuthRestored,
        onExplicitOfflineChange,
        onFailure,
      },
    );
    connection.updateAuth(JSON.stringify(browserWorkerTransportAuth(config)), sessionClaims);
    return connection;
  }

  private nativeSchemaView(
    schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
    flushEvery: number,
    historyComplete: boolean,
    selfSignedClientProof?: NativeSelfSignedClientProof,
    backendMode = false,
  ): NativeRuntimeAdapter {
    if (!this.ownerRuntime || this.ownerRuntime.isClosed()) {
      this.ownerRuntime = new NativeRuntimeAdapter(
        this.wasmModule.WasmDb,
        schema,
        node,
        author,
        1,
        historyComplete,
        { initialSyncFlushEvery: flushEvery, selfSignedClientProof, backendMode },
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

function isNodeRuntime(): boolean {
  return typeof process !== "undefined" && Boolean(process.versions?.node);
}

function isPersistentBrowserConfig(config: DbConfig): boolean {
  return isBrowserRuntime() && (config.driver?.type ?? "persistent") === "persistent";
}

/** @internal A client relay never acquires the application's admin capability. */
export function browserWorkerTransportAuth(config: DbConfig): Record<string, unknown> {
  return {
    jwt_token: config.jwtToken ?? null,
    // Dev/deployment credentials may coexist with a user session in DbConfig.
    // Sending them here would override JWT admission at the server and remove
    // this connection's scope-isolated client-relay capability.
    ...(config.cookieSession ? { backend_session: config.cookieSession } : {}),
  };
}
