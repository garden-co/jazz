import { getRuntimeSchemaCacheKey } from "../../drivers/schema-wire.js";
import type { WasmSchema } from "../../drivers/types.js";
import type { DurabilityTier, JazzClient, MutationErrorEvent } from "../client.js";
import type { Session } from "../context.js";
import type { DbConfig } from "../db.js";
import type { ForegroundNodeLease, RuntimeSource } from "../runtime-source.js";
import { resolveTelemetryCollectorUrlFromEnv } from "../sync-telemetry.js";
import type { AuthFailureReason } from "../auth-state.js";
import { getTrustedReservedSession, setTrustedReservedSession } from "../db-internal-session.js";

function shouldBypassLocalPolicies(config: DbConfig): boolean {
  return !!config.adminSecret;
}

function stripSchemaPolicies(schema: WasmSchema): WasmSchema {
  return Object.fromEntries(
    Object.entries(schema).map(([tableName, tableSchema]) => [
      tableName,
      {
        ...tableSchema,
        policies: undefined,
      },
    ]),
  ) as WasmSchema;
}

const policyStrippedSchemaCache = new WeakMap<WasmSchema, WasmSchema>();

function getPolicyStrippedSchema(schema: WasmSchema): WasmSchema {
  const cached = policyStrippedSchemaCache.get(schema);
  if (cached) return cached;

  const strippedSchema = stripSchemaPolicies(schema);
  policyStrippedSchemaCache.set(schema, strippedSchema);
  return strippedSchema;
}

export interface ConnectionManagerClientInput {
  schemaKey: string;
  schema: WasmSchema;
  client: JazzClient;
}

/** The narrow part of Db needed by connection managers. */
export interface DbForConnection {
  readonly config: DbConfig;
  readonly runtimeSource: RuntimeSource<any>;
  readonly isShuttingDown: boolean;
  markUnauthenticated(reason: AuthFailureReason): void;
  clearAuthError(): void;
  onMutationError(event: MutationErrorEvent): void;
  /** Enables Inspector-local reads after the worker's authenticated receipt. */
  enableAuthenticatedInspectorLocalReads(physicalDbName: string): void;
  /**
   * Revokes an Inspector attachment receipt when its worker connection is no
   * longer current. A new generation must authenticate itself again.
   */
  clearAuthenticatedInspectorLocalReads(): void;
}

/**
 * Owns the single runtime client and schema associated with a Db.
 *
 * Platform-specific managers extend this boundary with their connection and
 * durability topology; Db remains responsible for the public typed API.
 */
export abstract class ConnectionManager {
  private client: JazzClient | null = null;
  private clientSchema: WasmSchema | null = null;
  private disposeRuntimeTelemetry: (() => void) | null = null;

  /** Browser managers set this during their asynchronous bootstrap. */
  protected foregroundNodeLease: ForegroundNodeLease | undefined;

  protected constructor(protected readonly host: DbForConnection) {}

  abstract start(): Promise<void>;

  getClient(schema: WasmSchema): JazzClient {
    const { config, runtimeSource } = this.host;
    const runtimeSchema =
      runtimeSource.supportsPolicyBypass && shouldBypassLocalPolicies(config)
        ? getPolicyStrippedSchema(schema)
        : schema;
    const schemaKey = getRuntimeSchemaCacheKey(runtimeSchema);

    if (this.client) {
      if (!this.clientSchema || getRuntimeSchemaCacheKey(this.clientSchema) !== schemaKey) {
        throw new Error(
          "Db is already initialized with a different schema. Create a separate Db for each schema/app.",
        );
      }
      return this.client;
    }

    this.installRuntimeTelemetry();
    const runtimeConfig = { ...config };
    // Reserved local-first/anonymous sessions are carried by a package-private
    // capability sidecar, not an enumerable config property. Preserve that
    // capability when isolating the runtime's config object so native opens
    // retain the verified session author used by persistence and transport.
    setTrustedReservedSession(runtimeConfig, getTrustedReservedSession(config));
    const client = runtimeSource.createClient({
      config: runtimeConfig,
      schema: runtimeSchema,
      onAuthFailure: (reason) => this.host.markUnauthenticated(reason),
      foregroundNodeLease: this.foregroundNodeLease,
    });
    client.onMutationError((event) => this.host.onMutationError(event));

    this.client = client;
    this.clientSchema = runtimeSchema;
    this.onClientCreated({ schemaKey, schema: runtimeSchema, client });
    return client;
  }

  getCurrentClient(): JazzClient | null {
    return this.client;
  }

  getRuntimeSchema(): WasmSchema | null {
    return this.client?.getSchema() ?? null;
  }

  protected get clientEntry(): ConnectionManagerClientInput | null {
    if (!this.client || !this.clientSchema) return null;
    return {
      schemaKey: getRuntimeSchemaCacheKey(this.clientSchema),
      schema: this.clientSchema,
      client: this.client,
    };
  }

  protected onClientCreated(_input: ConnectionManagerClientInput): void {}

  abstract ensureReady(tier?: DurabilityTier, signal?: AbortSignal): Promise<void>;

  abstract shouldDeferSubscriptionStart(tier?: DurabilityTier): boolean;

  /** True only after the application explicitly called Db.disconnect(). */
  abstract isExplicitlyOffline(): boolean;
  /** Resolves when that explicit offline state is cleared. */
  abstract waitForReconnect(signal?: AbortSignal): Promise<void>;

  /**
   * Browser worker followers learn the namespace-wide explicit-offline state
   * during their initial handshake. Other runtimes already have a synchronous
   * state snapshot, so they deliberately return `null`: tier choice must not
   * gain an asynchronous gap after an operation has started.
   */
  initialExplicitOfflineState(): Promise<void> | null {
    return null;
  }

  openInspectorControlPort(): Promise<MessagePort> {
    return Promise.reject(new Error("This runtime has no shared browser worker"));
  }

  abstract disconnect(): Promise<void>;

  abstract reconnect(): Promise<void>;

  updateAuth(auth: {
    jwtToken?: string;
    cookieSession?: Session;
    trustedReservedSession?: Session;
  }): void {
    if ("jwtToken" in auth) {
      if (auth.jwtToken && auth.trustedReservedSession) {
        this.client?.updateTrustedAuthToken(auth.jwtToken, auth.trustedReservedSession);
      } else {
        this.client?.updateAuthToken(auth.jwtToken);
      }
    }
    if ("cookieSession" in auth) this.client?.updateCookieSession(auth.cookieSession);
  }

  abstract deleteClientStorage(): Promise<void>;

  protected async shutdownClient(): Promise<void> {
    try {
      await this.client?.shutdown();
    } finally {
      this.clearClient();
    }
  }

  protected clearClient(): void {
    this.client = null;
    this.clientSchema = null;
  }

  protected detachClient(): JazzClient | null {
    const client = this.client;
    this.clearClient();
    return client;
  }

  protected telemetryCollectorUrl(): string | undefined {
    return resolveTelemetryCollectorUrlFromEnv() ?? this.host.config.telemetryCollectorUrl;
  }

  private installRuntimeTelemetry(): void {
    const collectorUrl = this.telemetryCollectorUrl();
    if (!collectorUrl || this.disposeRuntimeTelemetry) return;

    this.disposeRuntimeTelemetry =
      this.host.runtimeSource.installTelemetry?.({
        config: this.host.config,
        collectorUrl,
        runtimeThread: "main",
      }) ?? null;
  }

  async shutdown(): Promise<void> {
    this.disposeRuntimeTelemetry?.();
    this.disposeRuntimeTelemetry = null;
    await this.shutdownClient();
  }
}
