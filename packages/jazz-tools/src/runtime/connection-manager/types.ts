import { getRuntimeSchemaCacheKey } from "../../drivers/schema-wire.js";
import type { WasmSchema } from "../../drivers/types.js";
import type { JazzClient, DurabilityTier, MutationErrorEvent } from "../client.js";
import type { Session } from "../context.js";
import type { DbRuntimeModule } from "../db-runtime-module.js";
import type { DbConfig } from "../db.js";
import { resolveTelemetryCollectorUrlFromEnv } from "../sync-telemetry.js";
import type { AuthFailureReason } from "../sync-transport.js";
import type { WorkerLifecycleEvent } from "../worker-bridge.js";

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
  if (cached) {
    return cached;
  }

  const strippedSchema = stripSchemaPolicies(schema);
  policyStrippedSchemaCache.set(schema, strippedSchema);
  return strippedSchema;
}

export interface ConnectionManagerClientInput {
  schemaKey: string;
  schema: WasmSchema;
  client: JazzClient;
}

/**
 * Part of the {@link Db} API used by the {@link ConnectionManager}
 */
export interface DbForConnection {
  readonly config: DbConfig;
  readonly runtimeModule: DbRuntimeModule<any> | null;
  readonly isShuttingDown: boolean;
  markUnauthenticated(reason: AuthFailureReason): void;
  onMutationError(event: MutationErrorEvent): void;
}

export abstract class ConnectionManager {
  private client: JazzClient | null = null;
  private disposeWasmTelemetry: (() => void) | null = null;
  /**
   * Db schema, cached for performance.
   */
  private clientSchema: WasmSchema | null = null;
  protected abstract readonly hasDurablePeer: boolean;

  protected constructor(protected readonly host: DbForConnection) {}

  abstract start(): Promise<void>;

  getClient(schema: WasmSchema): JazzClient {
    const { runtimeModule } = this.host;
    if (!runtimeModule) {
      throw new Error("Db runtime module is not initialized for this Db implementation");
    }

    const runtimeSchema =
      runtimeModule.supportsPolicyBypass && shouldBypassLocalPolicies(this.host.config)
        ? getPolicyStrippedSchema(schema)
        : schema;

    const key = getRuntimeSchemaCacheKey(runtimeSchema);

    if (this.client) {
      if (!this.clientSchema || getRuntimeSchemaCacheKey(this.clientSchema) !== key) {
        throw new Error(
          "Db is already initialized with a different schema. Create a separate Db for each schema/app.",
        );
      }
      return this.client;
    }

    this.installMainThreadWasmTelemetry(runtimeModule);

    const client = runtimeModule.createClient({
      config: { ...this.host.config },
      schema: runtimeSchema,
      hasWorker: this.hasDurablePeer,
      useBinaryEncoding: this.hasDurablePeer,
      bufferOutboxWithoutSyncSender: this.hasDurablePeer,
      onAuthFailure: (reason) => {
        this.host.markUnauthenticated(reason);
      },
    });

    client.onMutationError((event) => {
      this.host.onMutationError(event);
    });

    this.client = client;
    this.clientSchema = runtimeSchema;
    this.onClientCreated({
      schemaKey: key,
      schema: runtimeSchema,
      client,
    });

    return client;
  }

  /**
   * The current runtime client's schema, or null if no client has been created
   * yet (no query/subscription run). Used by `Db.getRuntimeSchema()` so the
   * inspector overlay can render columns and build queries without reaching into
   * private fields.
   */
  getRuntimeSchema(): WasmSchema | null {
    return this.client ? this.client.getSchema() : null;
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

  abstract ensureReady(tier?: DurabilityTier): Promise<void>;

  updateAuth(auth: { jwtToken?: string; cookieSession?: Session }): void {
    if ("jwtToken" in auth) {
      this.client?.updateAuthToken(auth.jwtToken);
    }
    if ("cookieSession" in auth) {
      this.client?.updateCookieSession(auth.cookieSession);
    }
  }

  abstract sendLifecycleHint(event: WorkerLifecycleEvent): void;

  abstract shouldDeferSubscriptionStart(): boolean;

  abstract deleteClientStorage(): Promise<void>;

  protected async shutdownClient(): Promise<void> {
    await this.client?.shutdown();
    this.client = null;
    this.clientSchema = null;
  }

  protected telemetryCollectorUrl(): string | undefined {
    return resolveTelemetryCollectorUrlFromEnv() ?? this.host.config.telemetryCollectorUrl;
  }

  private installMainThreadWasmTelemetry(runtimeModule: DbRuntimeModule<any>): void {
    const collectorUrl = this.telemetryCollectorUrl();
    if (!collectorUrl || this.disposeWasmTelemetry) {
      return;
    }

    this.disposeWasmTelemetry =
      runtimeModule.installTelemetry?.({
        config: this.host.config,
        collectorUrl,
        runtimeThread: "main",
      }) ?? null;
  }

  private disposeMainThreadWasmTelemetry(): void {
    this.disposeWasmTelemetry?.();
    this.disposeWasmTelemetry = null;
  }

  async shutdown(): Promise<void> {
    this.disposeMainThreadWasmTelemetry();
    await this.shutdownClient();
  }
}
