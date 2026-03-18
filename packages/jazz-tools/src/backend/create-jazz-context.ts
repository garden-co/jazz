import { NapiRuntime } from "jazz-napi";
import type { WasmSchema } from "../drivers/types.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import { JazzClient, sessionFromRequest, type RequestLike } from "../runtime/client.js";
import type { AppContext, Session } from "../runtime/context.js";
import { createDbFromClient, type Db, type DbConfig } from "../runtime/db.js";
import { resolveLocalAuthDefaults } from "../runtime/local-auth.js";

export interface BackendSchemaSource {
  wasmSchema: WasmSchema;
}

export interface BackendQuerySchemaSource {
  _schema: WasmSchema;
}

export type BackendSchemaInput = WasmSchema | BackendSchemaSource | BackendQuerySchemaSource;

export type BackendDriver =
  | {
      type: "persistent";
      /** Path to the Fjall file used by the server runtime. */
      dataPath: string;
    }
  | {
      type: "memory";
    };

export interface BackendContextConfig extends Omit<
  AppContext,
  "schema" | "driver" | "clientId" | "tier"
> {
  /** Server runtime driver mode and storage location. */
  driver: BackendDriver;
  /** Optional default schema source (typically generated `app` export). */
  app?: BackendSchemaSource;
  /** Optional node durability tier identity. */
  tier?: "worker" | "edge" | "global";
}

interface ResolvedBackendContextConfig extends BackendContextConfig {
  localAuthMode?: "anonymous" | "demo";
  localAuthToken?: string;
}

function assertValidBackendConfig(config: BackendContextConfig): void {
  if (config.driver.type === "memory" && !config.serverUrl) {
    throw new Error("driver.type='memory' requires serverUrl.");
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isTableSchema(value: unknown): boolean {
  return isRecord(value) && Array.isArray(value.columns);
}

function isWasmSchema(value: unknown): value is WasmSchema {
  return (
    isRecord(value) &&
    !("_schema" in value) &&
    !("wasmSchema" in value) &&
    Object.values(value).every((table) => isTableSchema(table))
  );
}

function resolveSchema(input: BackendSchemaInput): WasmSchema {
  if (isWasmSchema(input)) {
    return input;
  }
  if (isRecord(input) && "_schema" in input && isWasmSchema(input._schema)) {
    return input._schema;
  }
  if (isRecord(input) && "wasmSchema" in input && isWasmSchema(input.wasmSchema)) {
    return input.wasmSchema;
  }

  throw new Error(
    "Invalid schema source. Pass a WasmSchema, a generated app object, or a generated query/table object.",
  );
}

/**
 * Server-side Jazz context with lazy runtime setup.
 *
 * The first call to `db()`, `asBackend()`, `forRequest()`, or `forSession()`
 * initializes a NAPI runtime and backing client using the provided app/schema source.
 * Later calls reuse the same initialized runtime.
 */
export class JazzContext {
  private readonly config: ResolvedBackendContextConfig;
  private readonly defaultSchemaInput?: BackendSchemaInput;
  private initializedSchemaJson?: string;
  private runtime?: NapiRuntime;
  private clientInstance?: JazzClient;

  constructor(config: BackendContextConfig) {
    assertValidBackendConfig(config);
    this.config = resolveLocalAuthDefaults(config);
    this.defaultSchemaInput = config.app;
  }

  private resolveSchema(source?: BackendSchemaInput): WasmSchema {
    const selected = source ?? this.defaultSchemaInput;
    if (!selected) {
      throw new Error(
        "No schema source provided. Pass `app` to createJazzContext or provide a schema source when calling db()/asBackend()/forRequest()/forSession().",
      );
    }
    return resolveSchema(selected);
  }

  private createClient(schema: WasmSchema): JazzClient {
    const schemaJson = serializeRuntimeSchema(schema);
    this.initializedSchemaJson = schemaJson;
    const nodeTier = this.config.tier ?? "edge";

    if (this.config.driver.type === "persistent") {
      this.runtime = new NapiRuntime(
        schemaJson,
        this.config.appId,
        this.config.env ?? "dev",
        this.config.userBranch ?? "main",
        this.config.driver.dataPath,
        nodeTier,
      );
    } else {
      this.runtime = NapiRuntime.inMemory(
        schemaJson,
        this.config.appId,
        this.config.env ?? "dev",
        this.config.userBranch ?? "main",
        nodeTier,
      );
    }

    const context: AppContext = {
      appId: this.config.appId,
      schema,
      serverUrl: this.config.serverUrl,
      serverPathPrefix: this.config.serverPathPrefix,
      env: this.config.env,
      userBranch: this.config.userBranch,
      jwtToken: this.config.jwtToken,
      localAuthMode: this.config.localAuthMode,
      localAuthToken: this.config.localAuthToken,
      backendSecret: this.config.backendSecret,
      adminSecret: this.config.adminSecret,
      tier: nodeTier,
      defaultDurabilityTier: "edge",
    };

    this.clientInstance = JazzClient.connectWithRuntime(this.runtime, context);
    return this.clientInstance;
  }

  private buildDbConfig(): DbConfig {
    return {
      appId: this.config.appId,
      driver: this.config.driver.type === "memory" ? { type: "memory" } : { type: "persistent" },
      serverUrl: this.config.serverUrl,
      serverPathPrefix: this.config.serverPathPrefix,
      env: this.config.env,
      userBranch: this.config.userBranch,
      jwtToken: this.config.jwtToken,
      localAuthMode: this.config.localAuthMode,
      localAuthToken: this.config.localAuthToken,
      adminSecret: this.config.adminSecret,
    };
  }

  private wrapDb(client: JazzClient, session?: Session): Db {
    return createDbFromClient(this.buildDbConfig(), client, session);
  }

  /**
   * Get the shared Jazz client, lazily creating it on first access.
   */
  private getClient(source?: BackendSchemaInput): JazzClient {
    const schema = this.resolveSchema(source);
    const schemaJson = serializeRuntimeSchema(schema);

    if (!this.clientInstance) {
      return this.createClient(schema);
    }

    if (this.initializedSchemaJson !== schemaJson) {
      throw new Error(
        "JazzContext is already initialized with a different schema. Create a separate context for each schema/app.",
      );
    }

    return this.clientInstance;
  }

  /**
   * Get a high-level `Db` using the context's configured auth/runtime identity.
   */
  db(source?: BackendSchemaInput): Db {
    return this.wrapDb(this.getClient(source));
  }

  /**
   * Get a backend-scoped `Db` authenticated with `backendSecret`.
   */
  asBackend(source?: BackendSchemaInput): Db {
    return this.wrapDb(this.getClient(source).asBackend());
  }

  /**
   * Enable backend-authenticated sync for a scoped `Db` when this context is connected
   * to a sync server. Local-only runtimes can scope sessions without backend auth.
   */
  private enableBackendSyncIfConfigured(client: JazzClient): void {
    if (!this.config.serverUrl) {
      return;
    }
    if (!this.config.backendSecret) {
      throw new Error(
        "backendSecret required for request/session-scoped sync when serverUrl is configured.",
      );
    }
    client.asBackend();
  }

  /**
   * Build a requester-scoped `Db` from an authenticated request.
   */
  forRequest(request: RequestLike, source?: BackendSchemaInput): Db {
    const client = this.getClient(source);
    const session = sessionFromRequest(request);
    this.enableBackendSyncIfConfigured(client);
    return this.wrapDb(client, session);
  }

  /**
   * Build a session-scoped `Db` for server-side impersonation flows.
   */
  forSession(session: Session, source?: BackendSchemaInput): Db {
    const client = this.getClient(source);
    this.enableBackendSyncIfConfigured(client);
    return this.wrapDb(client, session);
  }

  /**
   * Flush the underlying runtime if initialized.
   */
  flush(): void {
    this.runtime?.flush();
  }

  /**
   * Shutdown the context and release runtime resources.
   */
  async shutdown(): Promise<void> {
    const client = this.clientInstance;

    this.clientInstance = undefined;
    this.runtime = undefined;
    this.initializedSchemaJson = undefined;

    if (client) {
      await client.shutdown();
    }
  }
}

export function createJazzContext(config: BackendContextConfig): JazzContext {
  return new JazzContext(config);
}
