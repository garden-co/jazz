import { clientScope } from "./client-scope.js";
import type { ClientMigrations } from "./client-migrations.js";
import { JazzClient, type ConnectRuntimeOptions } from "./client.js";
import { loadWasmModule, type WasmModule } from "./wasm-loader.js";
import { textAnchorsFrom, type TextAnchors } from "./text-anchors.js";
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
import {
  NativeRuntimeAdapter,
  type SlowQueryLogOptions,
} from "./native-runtime/native-runtime-adapter.js";
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
import { randomNodeUuidBytes } from "./runtime-entropy.js";
import { buildBrowserEncryption } from "../encryption/browser-host.js";
import { compiledBindings } from "../encryption/build.js";
import { resolveSchemaSource } from "../schema-source.js";
import { declaredEntries, declaresEncryption } from "../encryption/plain-schema.js";
import { Encryption } from "../encryption/index.js";
import { accountConfigBearer } from "../accounts/config-capability.js";
import { bearerWebSocket } from "./credentialed-transport.js";
import type { WebSocketConstructor } from "./native-runtime/websocket.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

function setGlobalWasmLogLevel(level?: DbConfig["logLevel"]): void {
  (globalThis as any).__JAZZ_WASM_LOG_LEVEL = level ?? DEFAULT_WASM_LOG_LEVEL;
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
      ? "device-account"
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
    appId: clientScope(config),
    claimedAuthor: canonicalAuthorSubject(session.issuer, session.user_id, session.account_id),
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

  /** The end-to-end encryption runtime built for this tab's account, for an app that declares encryption. */
  private encryption:
    | {
        runtime: unknown;
        store: import("../encryption/private-store.js").PrivateStore;
        accountSecret?: () => string | undefined;
        app: import("../schema-source.js").SchemaSourceInput;
      }
    | undefined;

  override async load(config: DbConfig): Promise<void> {
    this.module ??= await loadWasmModule(config.runtimeSources);
    if (config.encryption && !this.encryption) {
      if (!config.accountId) {
        throw new Error("End-to-end encryption needs an account: sign in before opening the app");
      }
      const host = (this.module as unknown as { EncryptionRuntime?: never }).EncryptionRuntime;
      if (!declaresEncryption(config.encryption.app)) return;
      if (!host) {
        throw new Error(
          "The WASM runtime does not support encrypted apps; rebuild the matching Jazz WASM artifact",
        );
      }
      const secret = config.encryption.accountSecret?.();
      const built = await buildBrowserEncryption({
        host,
        config: config.encryption,
        appId: clientScope(config),
        environment: config.env ?? "dev",
        accountId: config.accountId,
        deviceAccountSecret: secret,
      });
      if (built) {
        this.encryption = {
          ...built,
          accountSecret: config.encryption.accountSecret,
          app: config.encryption.app,
        };
      }
    }
  }

  override encryptionHandle(): Encryption | undefined {
    return this.ownerRuntime?.encryptionHandle();
  }

  override admitConfig(config: DbConfig): void {
    trustAttachedBrowserWorkerSession(config);
  }

  override createClient({
    config,
    schema,
    migrations,
    onAuthFailure,
    onAuthRestored,
    foregroundNodeLease,
  }: RuntimeClientContext<DbConfig>): JazzClient {
    setGlobalWasmLogLevel(config.logLevel);

    const runtimeOptions: ConnectRuntimeOptions = {
      onAuthFailure,
      onAuthRestored,
    };

    const session = sessionFromConfig(config);
    const selfSignedClientProof = selfSignedClientProofFromConfig(config, session);
    const backendMode = isBackendRuntime(config);
    // The persistent worker owns durable recovery. A foreground runtime owns
    // only its live optimistic writes, so every new runtime needs a fresh node
    // identity. Reusing a deterministic node across independently opened tabs
    // would let their fresh HLC registers mint the same TxId before either has
    // observed the other's first commit.
    const node = foregroundNodeLease?.node.slice() ?? randomNodeUuidBytes();
    const author = authorBytesForSession(runtimeAuthorFromConfig(config));
    const flushEvery = initialSyncFlushEvery(config);
    const browserMode = isPersistentBrowserConfig(config);
    // A user the app's backend signed in with its bearer (a host without
    // cookies): the sync socket carries it, as a browser's carries the cookie.
    const bearer =
      config.cookieSession && config.accountId ? accountConfigBearer(config) : undefined;
    const mainThreadPeerRuntime = this.nativeSchemaView(
      schema,
      node,
      author,
      flushEvery,
      !browserMode,
      selfSignedClientProof,
      backendMode,
      {
        appId: clientScope(config),
        thresholdMs: config.slowQueryThresholdMs,
        // End users see a browser's console: plan and SQL go to debug.
        ...(isBrowserRuntime() ? { consoleDetail: "summary" as const } : {}),
      },
      migrations,
      bearer && config.serverUrl ? bearerWebSocket(config.serverUrl, bearer) : undefined,
      config.subscriptionBacklogRows,
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
      appId: clientScope(config),
      accountId: config.accountId,
      schema,
      driver: config.driver,
      serverUrl: config.serverUrl,
      env: config.env,
      jwtToken: config.jwtToken,
      cookieSession: config.cookieSession,
      adminSecret: config.adminSecret,
    };
    setTrustedReservedSession(context, getTrustedReservedSession(config));
    return JazzClient.connectWithRuntime(
      this.wrapClientRuntime(mainThreadPeerRuntime, config, schema),
      context,
      runtimeOptions,
    );
  }

  /** Private runtime sources may attach diagnostic reads to their admitted owner. */
  protected wrapClientRuntime(
    runtime: NativeRuntimeAdapter,
    _config: DbConfig,
    _schema: WasmSchema,
  ): NativeRuntimeAdapter {
    return runtime;
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
      appId: clientScope(config),
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
    onRemoteLinkChange,
    onFailure,
    onStorageReset,
    onStorageInvalidated,
    onNewerBuild,
  }: BrowserWorkerConnectionContext<DbConfig>): BrowserWorkerConnection {
    const runtime = client.getRuntime();
    if (!(runtime instanceof NativeRuntimeAdapter)) {
      throw new Error("Browser worker connections require the native runtime adapter");
    }
    ({ onRemoteLinkChange, onFailure } = relayWorkerLink(runtime, onRemoteLinkChange, onFailure));
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
          onRemoteLinkChange,
          onFailure,
          onStorageReset,
          onStorageInvalidated,
        },
        config.runtimeSources.inspectorBinding,
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
        migrations: runtime.migrations,
        appId: clientScope(config),
        slowQueryThresholdMs: config.slowQueryThresholdMs,
        ...(config.subscriptionBacklogRows === undefined
          ? {}
          : { subscriptionBacklogRows: config.subscriptionBacklogRows }),
        storageOwner: createBrowserStorageOwner(config),
        authSessionKey: createBrowserAuthSessionKey(config),
        serverUrl: config.serverUrl
          ? httpUrlToWs(config.serverUrl, clientScope(config))
          : undefined,
        authJson: JSON.stringify(browserWorkerTransportAuth(config)),
        sessionClaims: sessionFromConfig(config)?.claims ?? {},
        logLevel: config.logLevel,
        telemetryCollectorUrl: config.telemetryCollectorUrl,
        // The worker holds and relays sealed rows and no keys: the tabs hold them.
        ...(this.encryption
          ? {
              keyless: {
                bindings: compiledBindings(this.encryption.app),
                environment: config.env ?? "dev",
              },
            }
          : {}),
      },
      createBrowserWorkerFingerprint(config, dbName, getRuntimeSchemaCacheKey(schema)),
      {
        onAuthFailure,
        onAuthRestored,
        onExplicitOfflineChange,
        onRemoteLinkChange,
        onFailure,
        onStorageReset,
        onStorageInvalidated,
        onNewerBuild,
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
    onRemoteLinkChange,
    onFailure,
  }: BrowserFollowerConnectionContext<DbConfig>): BrowserFollowerConnection {
    const runtime = client.getRuntime();
    if (!(runtime instanceof NativeRuntimeAdapter)) {
      throw new Error("Browser follower connections require the native runtime adapter");
    }
    ({ onRemoteLinkChange, onFailure } = relayWorkerLink(runtime, onRemoteLinkChange, onFailure));
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
        onRemoteLinkChange,
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
    slowQueryLog?: SlowQueryLogOptions,
    migrations?: ClientMigrations,
    webSocket?: WebSocketConstructor,
    subscriptionBacklogRows?: number,
  ): NativeRuntimeAdapter {
    if (!this.ownerRuntime || this.ownerRuntime.isClosed()) {
      const encryption = this.encryption;
      // The encryption runtime is built in `load` (it waits on the private
      // store) and given to the first owner runtime, which owns it from then
      // on. A runtime opened here after that one closed would have none, and
      // building another cannot happen in this synchronous path: say so,
      // rather than fail later with the wasm runtime's "not ready". Each
      // `createDb` has its own source, so a new client is the way back.
      if (this.ownerRuntime && encryption) {
        throw new Error(
          "This database's runtime closed, and its encryption runtime with it: open a new client to use an encrypted app again",
        );
      }
      this.ownerRuntime = new NativeRuntimeAdapter(
        this.wasmModule.WasmDb,
        schema,
        node,
        author,
        1,
        historyComplete,
        {
          initialSyncFlushEvery: flushEvery,
          selfSignedClientProof,
          backendMode,
          slowQueryLog,
          subscriptionBacklogRows,
          migrations,
          webSocket,
          ...(encryption
            ? {
                encryption: {
                  runtime: encryption.runtime,
                  store: encryption.store,
                  createHandle: (native) =>
                    new Encryption(native, { accountSecret: encryption.accountSecret }),
                },
                plainEntries: declaredEntries(resolveSchemaSource(encryption.app)),
              }
            : {}),
        },
      );
      return this.ownerRuntime;
    }
    return Object.keys(schema).length === 0
      ? this.ownerRuntime
      : this.ownerRuntime.registerSchemaView(schema, migrations);
  }

  override installTelemetry({
    config,
    collectorUrl,
    runtimeThread,
  }: RuntimeTelemetryContext<DbConfig>): (() => void) | null {
    return installWasmTelemetry({
      wasmModule: this.wasmModule,
      collectorUrl,
      runtimeThread,
    });
  }

  override textAnchors(): TextAnchors {
    return textAnchorsFrom(this.wasmModule);
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

/**
 * The callbacks of one connection to the shared worker, also telling the
 * tab's runtime the worker's server link: a tab has no link of its own, and
 * its encryption is online only while the worker is connected. A connection
 * that fails takes its link with it, as does one that replaced it.
 *
 * @internal Exported for tests.
 */
export function relayWorkerLink(
  runtime: NativeRuntimeAdapter,
  onRemoteLinkChange: BrowserWorkerConnectionContext["onRemoteLinkChange"],
  onFailure: BrowserWorkerConnectionContext["onFailure"],
): Pick<BrowserWorkerConnectionContext, "onRemoteLinkChange" | "onFailure"> {
  const connection = {};
  runtime.noteWorkerConnection(connection);
  return {
    onRemoteLinkChange: (state, link) => {
      if (link) runtime.noteWorkerLink(connection, link);
      onRemoteLinkChange?.(state, link);
    },
    onFailure: (error) => {
      runtime.forgetWorkerLink(connection);
      onFailure(error);
    },
  };
}
