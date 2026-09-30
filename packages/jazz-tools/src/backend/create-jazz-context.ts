import { accountRegistryUrl } from "../accounts/context.js";
import { NapiDb } from "jazz-napi";
import type { JWK } from "jose";
import type { WasmSchema } from "../drivers/types.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { CompiledPermissions } from "../permissions/index.js";
import {
  JazzClient,
  type MutationErrorEvent,
  type RequestLike,
  type Runtime,
} from "../runtime/client.js";
import type { AppContext, Session } from "../runtime/context.js";
import { RuntimeSource, type RuntimeClientContext } from "../runtime/runtime-source.js";
import { Db, type DbConfig } from "../runtime/db.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { SYSTEM_READ_SESSION } from "../runtime/system-identity.js";
import { canonicalAuthorSubject, withCanonicalUser } from "../runtime/author-id.js";
import { authorBytesForSession } from "../runtime/author-id.js";
import type { AuthState } from "../runtime/auth-state.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import {
  resolveSchemaSource,
  type QuerySchemaSource,
  type SchemaSourceInput,
  type WasmSchemaSource,
} from "../schema-source.js";
import {
  resolveRequestSession,
  verifiedLocalFirstRequestProof,
  type BackendRequestOptions,
} from "./request-auth.js";

export type BackendSchemaSource = WasmSchemaSource;
export type BackendQuerySchemaSource = QuerySchemaSource;
export type BackendSchemaInput = SchemaSourceInput;
export type BackendJwtPublicKey = JWK | string;

export type BackendDriver =
  | {
      type: "persistent";
      /** Path to the Fjall file used by the server runtime. */
      dataPath: string;
    }
  | {
      type: "memory";
    };

type BackendContextSchemaConfig =
  | {
      /** Default app/schema source for the context. */
      app: BackendSchemaSource;
      /** Compiled row-level permissions paired with the app schema. */
      permissions: CompiledPermissions;
    }
  | {
      app?: undefined;
      permissions?: undefined;
    };

export type BackendContextConfig = Omit<AppContext, "schema" | "driver" | "clientId" | "tier"> & {
  /** Server runtime driver mode and storage location. */
  driver: BackendDriver;
  /** Optional node durability tier identity. */
  tier?: "local" | "global";
  /**
   * Direct JWKS endpoint used to verify external bearer JWTs in `forRequest()`.
   * Requires HTTPS, except development HTTP whose WHATWG-canonical hostname is
   * localhost, [::1], or an IPv4 address in 127/8.
   * HTTP with the trailing-dot spelling localhost., other schemes, and remote
   * HTTP are rejected before fetching. Redirects are
   * rejected, including redirects to HTTPS; configure the final URL directly.
   */
  jwksUrl?: string;
  /** Single JWK object or PEM/JWK string used to verify external bearer JWTs in `forRequest()`. */
  jwtPublicKey?: BackendJwtPublicKey;
  /** Required issuer for external bearer JWTs accepted by `forRequest()`. */
  jwtIssuer?: string;
  /** Required audience, or audiences, for external bearer JWTs accepted by `forRequest()`. */
  jwtAudience?: string | readonly string[];
  /** Whether local-first bearer JWTs are accepted in `forRequest()`. Defaults to `true`. */
  allowLocalFirstAuth?: boolean;
} & BackendContextSchemaConfig;

type ResolvedBackendContextConfig = BackendContextConfig & {
  allowLocalFirstAuth: boolean;
};

/** @internal A memory handle retains its node clock across failed-transition reopen. */
export interface BackendNodeClock {
  initialHighWater?: bigint;
  closed(highWater: bigint): void;
}

type FlushableRuntime = Runtime & { flush?: () => void };

/** Pre-listener mutation errors a backend context retains for its first listener. */
export const MAX_PENDING_MUTATION_ERROR_EVENTS = 256;

interface BackendMutationErrorSink {
  hasMutationErrorListeners(): boolean;
  deliverMutationError(event: MutationErrorEvent): void;
}

class BackendRuntimeSource extends RuntimeSource<DbConfig> {
  private initializedSchemaJson?: string;
  private runtime?: FlushableRuntime;
  private client?: JazzClient;
  private backendSyncEnabled = false;
  private isDisconnected = false;
  private reconnectWaiters = new Set<{ resolve: () => void; reject: (error: Error) => void }>();
  private explicitOfflineListeners = new Map<
    (offline: boolean) => void,
    { signal: AbortSignal; onAbort: () => void }
  >();
  private transportTransition: Promise<void> = Promise.resolve();
  private shutdownState: "open" | "closing" | "closed" = "open";
  private shutdownPromise?: Promise<void>;
  private gracefulWait?: object;
  /**
   * Every backend `Db` facade shares this source's single runtime, so its
   * mutation errors belong to the source rather than to whichever facade
   * happened to be created last. The source holds facades only weakly: a
   * per-request `Db` that is dropped takes its listeners (and whatever they
   * capture) with it, instead of living as long as the context.
   */
  private readonly mutationErrorSinks = new Set<WeakRef<BackendMutationErrorSink>>();
  /**
   * Rejections that arrive while no facade has a listener (for example,
   * persisted in-flight writes settled during startup replay). They drain to
   * whichever facade registers a listener first. Bounded: beyond the cap, the
   * events are still logged as unhandled but only counted, not retained.
   */
  private readonly pendingMutationErrorEvents: MutationErrorEvent[] = [];
  private droppedMutationErrorEvents = 0;

  constructor(
    private readonly config: ResolvedBackendContextConfig,
    private readonly nodeIdentityScope: string,
    private readonly nodeIdentity?: Uint8Array,
    private readonly nodeClock?: BackendNodeClock,
  ) {
    super();
    this.nativeConnection = {
      configured: () => !!this.config.serverUrl,
      disconnect: () => this.disconnectTransport(),
      reconnect: () => this.reconnectTransport(),
      isExplicitlyOffline: () => this.isDisconnected,
      waitForTransportTransition: () => this.waitForTransportTransition(),
      waitForReconnect: (signal) => this.waitForReconnect(signal),
      onExplicitOfflineChange: (listener, signal) => {
        if (signal.aborted) return;
        const onAbort = () => this.explicitOfflineListeners.delete(listener);
        this.explicitOfflineListeners.set(listener, { signal, onAbort });
        signal.addEventListener("abort", onAbort, { once: true });
      },
    };
  }

  get currentRuntime(): FlushableRuntime | undefined {
    return this.runtime;
  }

  admitSession(session: Session): void {
    this.assertOpen();
    const proof = verifiedLocalFirstRequestProof(session);
    if (!proof) return;
    if (proof.appId !== this.config.appId || !(this.runtime instanceof NativeRuntimeAdapter)) {
      throw new Error("Local-first request proof does not match the backend runtime");
    }
    this.runtime.admitLocalFirstSession(session, proof.token, this.config.appId);
  }

  override createClient({
    config,
    schema,
    onAuthFailure,
  }: RuntimeClientContext<DbConfig>): JazzClient {
    this.assertOpen();
    const hasSeparatePermissionsBundle = this.config.permissions !== undefined;
    const schemaJson = serializeRuntimeSchema(schema, {
      loadedPolicyBundle: hasSeparatePermissionsBundle,
    });

    if (this.client) {
      if (this.initializedSchemaJson !== schemaJson) {
        throw new Error(
          "JazzContext is already initialized with a different schema. Create a separate context for each schema/app.",
        );
      }
      return this.client;
    }

    this.initializedSchemaJson = schemaJson;
    const nodeTier = this.config.tier ?? "global";
    const env = this.config.env ?? "dev";
    this.runtime = new NativeRuntimeAdapter(
      NapiDb,
      schema,
      this.nodeIdentity ??
        deterministicBytes(`${this.config.appId}:${env}:${this.nodeIdentityScope}:node`),
      authorBytesForSession({ issuer: "https://jazz.invalid", user_id: "backend-open" }),
      1,
      true,
      this.config.driver.type === "persistent"
        ? {
            persistentPath: this.config.driver.dataPath,
            readAuthorizationHost: "trusted-serving",
            backendMode: true,
          }
        : {
            readAuthorizationHost: "trusted-serving",
            backendMode: true,
          },
    );

    if (this.nodeClock?.initialHighWater !== undefined) {
      if (!(this.runtime instanceof NativeRuntimeAdapter))
        throw new Error("Backend node clock requires the native runtime");
      this.runtime.seedForegroundTxTimeHighWater(this.nodeClock.initialHighWater);
    }
    this.client = JazzClient.connectWithRuntime(
      this.runtime,
      {
        appId: config.appId,
        schema,
        serverUrl: config.serverUrl,
        env: config.env,
        jwtToken: config.jwtToken,
        backendSecret: this.config.backendSecret,
        adminSecret: config.adminSecret,
        cookieSession: config.cookieSession,
        tier: nodeTier,
        defaultDurabilityTier:
          this.config.defaultDurabilityTier ?? (config.serverUrl ? nodeTier : undefined),
      },
      { onAuthFailure },
    );
    this.client.onMutationError((event) => this.handleMutationError(event));
    return this.client;
  }

  /**
   * @internal Register a facade that now has mutation-error listeners and
   * hand it any rejections that arrived while nobody was listening.
   */
  attachMutationErrorSink(sink: WeakRef<BackendMutationErrorSink>): void {
    this.mutationErrorSinks.add(sink);
    const target = sink.deref();
    if (!target) return;
    const dropped = this.droppedMutationErrorEvents;
    this.droppedMutationErrorEvents = 0;
    const pending = this.pendingMutationErrorEvents.splice(0);
    if (dropped > 0) {
      console.error(
        `Jazz dropped ${dropped} unhandled mutation error(s) that arrived before a listener was attached; each was logged when it arrived.`,
      );
    }
    for (const event of pending) target.deliverMutationError(event);
  }

  /** @internal A facade whose last listener was removed, or that shut down. */
  detachMutationErrorSink(sink: WeakRef<BackendMutationErrorSink>): void {
    this.mutationErrorSinks.delete(sink);
  }

  private handleMutationError(event: MutationErrorEvent): void {
    const targets: BackendMutationErrorSink[] = [];
    for (const sink of this.mutationErrorSinks) {
      const target = sink.deref();
      if (target?.hasMutationErrorListeners()) targets.push(target);
      else this.mutationErrorSinks.delete(sink);
    }
    if (targets.length === 0) {
      console.error("Unhandled Jazz mutation error", event);
      if (this.pendingMutationErrorEvents.length < MAX_PENDING_MUTATION_ERROR_EVENTS) {
        this.pendingMutationErrorEvents.push(event);
      } else {
        this.droppedMutationErrorEvents += 1;
      }
      return;
    }
    for (const target of targets) target.deliverMutationError(event);
  }

  override async waitForPendingWrites(signal?: AbortSignal): Promise<void> {
    this.assertOpen();
    // Fence every facade while the shared owner settles its existing writes.
    const attempt = {};
    this.gracefulWait = attempt;
    const release = () => {
      if (this.gracefulWait === attempt) this.gracefulWait = undefined;
    };
    signal?.addEventListener("abort", release, { once: true });
    try {
      if (signal?.aborted) throw new Error("Graceful shutdown cancelled");
      if (this.runtime instanceof NativeRuntimeAdapter) {
        await this.runtime.waitForPendingWrites("global");
      }
      // Success keeps the fence until shutdown; failure restores the live source.
    } catch (error) {
      release();
      throw error;
    } finally {
      signal?.removeEventListener("abort", release);
    }
  }

  async shutdown(): Promise<void> {
    if (this.shutdownState === "closed") return;
    if (this.shutdownPromise) return this.shutdownPromise;

    this.shutdownState = "closing";
    this.rejectReconnectWaiters(this.shutdownError());
    const client = this.client;
    const shutdown = this.enqueueTransportTransition(async () => {
      const highWater =
        this.nodeClock && this.runtime instanceof NativeRuntimeAdapter
          ? await this.runtime.quiesceForegroundTxTimeHighWater()
          : (this.nodeClock?.initialHighWater ?? 0n);
      await client?.shutdown();
      this.nodeClock?.closed(highWater);
      this.client = undefined;
      this.runtime = undefined;
      this.initializedSchemaJson = undefined;
      this.backendSyncEnabled = false;
      this.isDisconnected = false;
      this.shutdownState = "closed";
      for (const { signal, onAbort } of this.explicitOfflineListeners.values()) {
        signal.removeEventListener("abort", onAbort);
      }
      this.explicitOfflineListeners.clear();
      this.mutationErrorSinks.clear();
      this.pendingMutationErrorEvents.length = 0;
      this.droppedMutationErrorEvents = 0;
    });
    this.shutdownPromise = shutdown;
    // A failed native close may have partially torn down the runtime. Keep
    // the source terminal and retain the failure rather than admitting a new
    // facade or pretending the old client can be used again.
    await shutdown;
  }

  enableBackendSync(client: JazzClient): void {
    this.assertOpen();
    if (!this.config.serverUrl) return;
    if (!this.config.backendSecret) {
      throw new Error(
        "backendSecret required for request/session-scoped sync when serverUrl is configured.",
      );
    }
    client.asBackend();
    if (this.backendSyncEnabled) return;
    this.backendSyncEnabled = true;
    if (!this.isDisconnected) this.connectBackendTransport(client);
  }

  private connectBackendTransport(client: JazzClient): void {
    if (!this.config.serverUrl) return;
    client.connectTransport(this.config.serverUrl, {
      jwt_token: undefined,
      backend_secret: this.config.backendSecret,
      backend_session: this.config.cookieSession,
    });
  }

  private async disconnectTransport(): Promise<void> {
    this.assertOpen();
    await this.enqueueTransportTransition(async () => {
      await this.client?.disconnectTransport();
      this.isDisconnected = true;
      this.publishExplicitOfflineState();
    });
  }

  private async reconnectTransport(): Promise<void> {
    this.assertOpen(true);
    await this.enqueueTransportTransition(() => {
      if (this.client && this.backendSyncEnabled) this.connectBackendTransport(this.client);
      this.isDisconnected = false;
      this.publishExplicitOfflineState();
    });
    this.resolveReconnectWaiters();
  }

  private async waitForReconnect(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return;
    this.assertOpen();
    await this.transportTransition;
    if (signal?.aborted) return;
    this.assertOpen();
    if (!this.isDisconnected) return;
    await new Promise<void>((resolve, reject) => {
      const finish = () => {
        this.reconnectWaiters.delete(waiter);
        signal?.removeEventListener("abort", onAbort);
        resolve();
      };
      const fail = (error: Error) => {
        this.reconnectWaiters.delete(waiter);
        signal?.removeEventListener("abort", onAbort);
        reject(error);
      };
      const onAbort = () => finish();
      signal?.addEventListener("abort", onAbort, { once: true });
      const waiter = { resolve: finish, reject: fail };
      this.reconnectWaiters.add(waiter);
    });
  }

  private async waitForTransportTransition(): Promise<void> {
    this.assertOpen();
    await this.transportTransition;
    this.assertOpen();
  }

  private enqueueTransportTransition(run: () => void | Promise<void>): Promise<void> {
    const transition = this.transportTransition.then(run, run);
    this.transportTransition = transition.catch(() => undefined);
    return transition;
  }

  private publishExplicitOfflineState(): void {
    for (const listener of this.explicitOfflineListeners.keys()) {
      try {
        listener(this.isDisconnected);
      } catch {
        // Observers must not turn a completed transport state change into a
        // failed transition for every scoped Db sharing this source.
      }
    }
  }

  private resolveReconnectWaiters(): void {
    const waiters = [...this.reconnectWaiters];
    this.reconnectWaiters.clear();
    for (const { resolve } of waiters) resolve();
  }

  private rejectReconnectWaiters(error: Error): void {
    const waiters = [...this.reconnectWaiters];
    this.reconnectWaiters.clear();
    for (const { reject } of waiters) reject(error);
  }

  assertOpen(allowSyncRecovery = false): void {
    if (this.shutdownState !== "open" || (this.gracefulWait && !allowSyncRecovery))
      throw this.shutdownError();
  }

  private shutdownError(): Error {
    return new Error("JazzContext is shutting down or has already shut down.");
  }
}

class BackendDb extends Db implements BackendMutationErrorSink {
  constructor(
    config: DbConfig,
    private readonly coreSource: BackendRuntimeSource,
    private readonly client: JazzClient,
    private readonly runtimeSchema: WasmSchema,
    private readonly operationContext: {
      session?: Session;
      attribution?: string;
      readSession?: Session;
    } | null,
    scopedAuthState?: AuthState,
  ) {
    super(
      config,
      coreSource,
      scopedAuthState
        ? {
            initialState: scopedAuthState,
            lockAuthenticatedState: true,
          }
        : undefined,
    );
  }

  protected override getRuntimeOperationContext(): {
    session?: Session;
    attribution?: string;
    readSession?: Session;
  } | null {
    return this.operationContext;
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    this.coreSource.assertOpen();
    this.assertOpen();
    return this.client;
  }

  protected override getCurrentClient(): JazzClient {
    this.coreSource.assertOpen();
    this.assertOpen();
    return this.client;
  }

  private readonly backendMutationErrorListeners = new Set<(event: MutationErrorEvent) => void>();
  private mutationErrorSinkRef: WeakRef<BackendMutationErrorSink> | undefined;

  /**
   * Attach a fallback listener for write rejections that are not handled by an
   * active {@link WriteHandle.wait} call.
   *
   * On a backend context this is **context-wide**: every `Db` from the same
   * context (`db()`, `asBackend()`, `forRequest()`, `forSession()`, ...)
   * shares one runtime, so a listener sees rejections of writes made through
   * any of them, for any session. A per-request handler must match events
   * against its own transaction ids. Rejections that arrived before any
   * listener existed (such as in-flight writes settled during startup replay)
   * drain to whichever `Db` registers a listener first.
   *
   * The context holds this `Db` only weakly, so its listeners are released
   * when the `Db` is dropped, when it shuts down, or via the returned callback.
   * `context.db()` returns a new `Db` on each call, so keep a reference to the
   * `Db` you register on (for example a long-lived `const db = context.db()`);
   * a listener on a `Db` you don't keep stops firing once it is collected.
   *
   * @returns an unsubscribe callback
   */
  override onMutationError(listener: (event: MutationErrorEvent) => void): () => void {
    this.assertOpen();
    const first = this.backendMutationErrorListeners.size === 0;
    this.backendMutationErrorListeners.add(listener);
    if (first) {
      if (!this.mutationErrorSinkRef) {
        this.mutationErrorSinkRef = new WeakRef<BackendMutationErrorSink>(this);
        this.onShutdown(() => this.releaseMutationErrorListeners());
      }
      this.coreSource.attachMutationErrorSink(this.mutationErrorSinkRef);
    }
    return () => {
      this.backendMutationErrorListeners.delete(listener);
      if (this.backendMutationErrorListeners.size === 0) this.releaseMutationErrorListeners();
    };
  }

  /** @internal */
  hasMutationErrorListeners(): boolean {
    return this.backendMutationErrorListeners.size > 0;
  }

  /** @internal */
  deliverMutationError(event: MutationErrorEvent): void {
    for (const listener of this.backendMutationErrorListeners) listener(event);
  }

  private releaseMutationErrorListeners(): void {
    this.backendMutationErrorListeners.clear();
    if (this.mutationErrorSinkRef)
      this.coreSource.detachMutationErrorSink(this.mutationErrorSinkRef);
  }
}

export function deterministicBytes(seed: string): Uint8Array {
  let hash = 0x811c9dc5;
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  for (let round = 0; round < 4; round += 1) {
    for (let i = 0; i < seed.length; i += 1) {
      hash ^= seed.charCodeAt(i) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    view.setUint32(round * 4, hash >>> 0, true);
  }
  return bytes;
}

function assertValidBackendConfig(config: BackendContextConfig): void {
  if (config.driver.type === "memory" && !config.serverUrl) {
    throw new Error("driver.type='memory' requires serverUrl.");
  }

  if (config.jwksUrl !== undefined && config.jwtPublicKey !== undefined) {
    throw new Error(
      "Backend auth config cannot set both jwksUrl and jwtPublicKey. Pick one external JWT verification mode.",
    );
  }
}

/**
 * Server-side Jazz context with lazy runtime setup.
 *
 * The first call to `db()`, `asBackend()`, `forRequest()`, or `forSession()`
 * initializes a NAPI runtime and backing client using the provided app/schema
 * source plus any compiled permissions.
 * Later calls reuse the same initialized runtime.
 */
export class JazzContext {
  private readonly config: ResolvedBackendContextConfig;
  private readonly defaultSchemaInput?: BackendSchemaInput;
  private readonly nodeIdentityScope: string;
  private readonly coreSource: BackendRuntimeSource;

  constructor(
    config: BackendContextConfig,
    nodeIdentity?: Uint8Array,
    nodeClock?: BackendNodeClock,
  ) {
    assertValidBackendConfig(config);
    this.config = {
      ...config,
      allowLocalFirstAuth: config.allowLocalFirstAuth ?? true,
    };
    this.defaultSchemaInput = config.app;
    this.nodeIdentityScope =
      config.driver.type === "persistent"
        ? config.driver.dataPath
        : `memory:${Date.now()}:${Math.random()}`;
    this.coreSource = new BackendRuntimeSource(
      this.config,
      this.nodeIdentityScope,
      nodeIdentity,
      nodeClock,
    );
  }

  private resolveSchema(source?: BackendSchemaInput): WasmSchema {
    const selected = source ?? this.defaultSchemaInput;
    if (!selected) {
      throw new Error(
        "No schema source provided. Pass `app` to createJazzContext or provide a schema source when calling db()/asBackend()/forRequest()/forSession().",
      );
    }
    const schema = resolveSchemaSource(selected);
    return this.config.permissions
      ? mergePermissionsIntoWasmSchema(schema, this.config.permissions)
      : schema;
  }

  private buildDbConfig(): DbConfig {
    return {
      appId: this.config.appId,
      driver: this.config.driver.type === "memory" ? { type: "memory" } : { type: "persistent" },
      serverUrl: this.config.serverUrl,
      env: this.config.env,
      jwtToken: this.config.jwtToken,
      adminSecret: this.config.adminSecret,
    };
  }

  private wrapDb(
    client: JazzClient,
    schema: WasmSchema,
    session?: Session,
    attribution?: string,
    backendScoped = false,
    backendReads = false,
  ): Db {
    if (session) this.coreSource.admitSession(session);
    return new BackendDb(
      this.buildDbConfig(),
      this.coreSource,
      client,
      schema,
      session || attribution || backendReads
        ? {
            session,
            attribution,
            readSession: backendReads ? SYSTEM_READ_SESSION : undefined,
          }
        : null,
      backendScoped
        ? {
            authMode: session?.authMode ?? "external",
            session: session ? withCanonicalUser(session) : null,
          }
        : undefined,
    );
  }

  /**
   * Get the shared Jazz client, lazily creating it on first access.
   */
  private getClient(source?: BackendSchemaInput): JazzClient {
    const schema = this.resolveSchema(source);
    return this.coreSource.createClient({
      config: this.buildDbConfig(),
      schema,
      onAuthFailure: () => {},
    });
  }

  private getClientAndSchema(source?: BackendSchemaInput): {
    client: JazzClient;
    schema: WasmSchema;
  } {
    const schema = this.resolveSchema(source);
    const client = this.coreSource.createClient({
      config: this.buildDbConfig(),
      schema,
      onAuthFailure: () => {},
    });
    return { client, schema };
  }

  /**
   * Get the shared high-level `Db` for this context with no per-request session attached.
   */
  db(source?: BackendSchemaInput): Db {
    const { client, schema } = this.getClientAndSchema(source);
    return this.wrapDb(client, schema);
  }

  /** @internal Display admitted SYSTEM provenance without passing it as policy authority. */
  openBackendAccount(session: Session): Db {
    const { client, schema } = this.getClientAndSchema();
    this.enableBackendSyncIfConfigured(client);
    return new BackendDb(this.buildDbConfig(), this.coreSource, client, schema, null, {
      authMode: "external",
      session: withCanonicalUser(session),
    });
  }

  /**
   * Get a backend-scoped `Db` authenticated with `backendSecret`.
   */
  asBackend(source?: BackendSchemaInput): Db {
    const { client, schema } = this.getClientAndSchema(source);
    this.enableBackendSyncIfConfigured(client);
    return this.wrapDb(client, schema, undefined, undefined, true, false);
  }

  /**
   * Build a backend-scoped `Db` that stamps write provenance as one issuer/subject pair
   * without evaluating permissions as that user.
   */
  withAttribution(issuer: string, subject: string, source?: BackendSchemaInput): Db {
    const { client, schema } = this.getClientAndSchema(source);
    this.enableBackendSyncIfConfigured(client);
    return this.wrapDb(
      client,
      schema,
      undefined,
      canonicalAuthorSubject(issuer, subject),
      true,
      true,
    );
  }

  /**
   * Enable backend-authenticated sync for a scoped `Db` when this context is connected
   * to a sync server. Local-only runtimes can scope sessions without backend auth.
   */
  private enableBackendSyncIfConfigured(client: JazzClient): void {
    this.coreSource.enableBackendSync(client);
  }

  private async resolveRequestSession(
    request: RequestLike,
    options?: BackendRequestOptions,
  ): Promise<Session> {
    if (!this.config.serverUrl) {
      throw new Error("forRequest requires a configured core serverUrl for account admission");
    }
    return await resolveRequestSession(
      request,
      {
        appId: this.config.appId,
        accountRegistry: accountRegistryUrl(this.config.serverUrl, this.config.appId),
        jwksUrl: this.config.jwksUrl,
        jwtPublicKey: this.config.jwtPublicKey,
        jwtIssuer: this.config.jwtIssuer,
        jwtAudience: this.config.jwtAudience,
        allowLocalFirstAuth: this.config.allowLocalFirstAuth,
      },
      options,
    );
  }

  /**
   * Verify the original bearer and resolve its active core account before
   * building a requester-scoped `Db`. Registration requires an explicit request option.
   */
  async forRequest(
    request: RequestLike,
    source?: BackendSchemaInput,
    options?: BackendRequestOptions,
  ): Promise<Db> {
    const session = await this.resolveRequestSession(request, options);
    const { client, schema } = this.getClientAndSchema(source);
    this.enableBackendSyncIfConfigured(client);
    return this.wrapDb(client, schema, session, undefined, true);
  }

  /**
   * Build a backend-scoped `Db` that stamps write provenance using the
   * principal in `session` without switching permission evaluation to it.
   */
  withAttributionForSession(session: Session, source?: BackendSchemaInput): Db {
    const { client, schema } = this.getClientAndSchema(source);
    this.enableBackendSyncIfConfigured(client);
    return this.wrapDb(
      client,
      schema,
      session,
      canonicalAuthorSubject(session.issuer, session.user_id, session.account_id),
      true,
      true,
    );
  }

  /**
   * Build a backend-scoped `Db` that stamps write provenance using the
   * authenticated principal from `request` without switching permissions.
   */
  async withAttributionForRequest(request: RequestLike, source?: BackendSchemaInput): Promise<Db> {
    return this.withAttributionForSession(await this.resolveRequestSession(request), source);
  }

  /**
   * Build a session-scoped `Db` for explicitly trusted server-side impersonation.
   * This bypasses public account admission; the backend owns every supplied claim.
   */
  forSession(session: Session, source?: BackendSchemaInput): Db {
    const { client, schema } = this.getClientAndSchema(source);
    this.enableBackendSyncIfConfigured(client);
    return this.wrapDb(client, schema, session, undefined, true);
  }

  /**
   * Flush the underlying runtime if initialized.
   */
  flush(): void {
    this.coreSource.currentRuntime?.flush?.();
  }

  /**
   * Shutdown the context and release runtime resources.
   */
  async shutdown(): Promise<void> {
    await this.coreSource.shutdown();
  }
}

export function createJazzContext(config: BackendContextConfig): JazzContext {
  return new JazzContext(config);
}
