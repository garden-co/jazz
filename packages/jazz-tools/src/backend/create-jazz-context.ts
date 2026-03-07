import { NapiRuntime } from "jazz-napi";
import type { WasmSchema } from "../drivers/types.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import { JazzClient, type RequestLike, type SessionClient } from "../runtime/client.js";
import type { AppContext, Session } from "../runtime/context.js";
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
      /** Path to the SurrealKV file used by the server runtime. */
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
 * The first call to `client()`, `asBackend()`, `forRequest()`, or `forSession()` initializes
 * a NAPI runtime and JazzClient using the provided app/schema source. Later
 * calls reuse the same initialized runtime.
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
        "No schema source provided. Pass `app` to createJazzContext or provide a schema source when calling client()/asBackend()/forRequest()/forSession().",
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

  /**
   * Get the shared Jazz client, lazily creating it on first access.
   */
  client(source?: BackendSchemaInput): JazzClient {
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
   * Get a backend-scoped client authenticated with `backendSecret`.
   */
  asBackend(source?: BackendSchemaInput): JazzClient {
    return this.client(source).asBackend();
  }

  /**
   * Build a requester-scoped client from an authenticated request.
   */
  forRequest(request: RequestLike, source?: BackendSchemaInput): SessionClient {
    return this.client(source).forRequest(request);
  }

  /**
   * Build a session-scoped client for server-side impersonation flows.
   */
  forSession(session: Session, source?: BackendSchemaInput): SessionClient {
    return this.client(source).forSession(session);
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
