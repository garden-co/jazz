import { NapiDb } from "jazz-napi";
import type { JWK } from "jose";
import type { WasmSchema } from "../drivers/types.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { CompiledPermissions } from "../permissions/index.js";
import { JazzClient, type RequestLike, type Runtime } from "../runtime/client.js";
import type { AppContext, Session } from "../runtime/context.js";
import { RuntimeSource, type RuntimeClientContext } from "../runtime/runtime-source.js";
import { Db, type DbConfig } from "../runtime/db.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { SYSTEM_AUTHOR_ID, SYSTEM_READ_SESSION } from "../runtime/system-identity.js";
import type { AuthState } from "../runtime/auth-state.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import {
  resolveSchemaSource,
  type QuerySchemaSource,
  type SchemaSourceInput,
  type WasmSchemaSource,
} from "../schema-source.js";
import { resolveRequestSession } from "./request-auth.js";

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
  tier?: "local" | "edge" | "global";
  /** JWKS endpoint used to verify external bearer JWTs in `forRequest()`. */
  jwksUrl?: string;
  /** Single JWK object or PEM/JWK string used to verify external bearer JWTs in `forRequest()`. */
  jwtPublicKey?: BackendJwtPublicKey;
  /** Whether local-first bearer JWTs are accepted in `forRequest()`. Defaults to `true`. */
  allowLocalFirstAuth?: boolean;
} & BackendContextSchemaConfig;

type ResolvedBackendContextConfig = BackendContextConfig & {
  allowLocalFirstAuth: boolean;
};

type FlushableRuntime = Runtime & { flush?: () => void };

function schemaHasNativePolicies(schema: WasmSchema): boolean {
  return Object.values(schema).some((table) => table.policies !== undefined);
}

class BackendRuntimeSource extends RuntimeSource<DbConfig> {
  private initializedSchemaJson?: string;
  private runtime?: FlushableRuntime;
  private client?: JazzClient;

  constructor(
    private readonly config: ResolvedBackendContextConfig,
    private readonly nodeIdentityScope: string,
  ) {
    super();
  }

  get currentRuntime(): FlushableRuntime | undefined {
    return this.runtime;
  }

  override createClient({
    config,
    schema,
    onAuthFailure,
  }: RuntimeClientContext<DbConfig>): JazzClient {
    const hasSeparatePermissionsBundle =
      this.config.permissions !== undefined && !schemaHasNativePolicies(schema);
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
    const nodeTier = this.config.tier ?? "edge";
    const env = this.config.env ?? "dev";
    const userBranch = this.config.userBranch ?? "main";
    this.runtime = new NativeRuntimeAdapter(
      NapiDb,
      schema,
      deterministicBytes(
        `${this.config.appId}:${env}:${userBranch}:${this.nodeIdentityScope}:node`,
      ),
      deterministicBytes(`${this.config.appId}:${env}:${userBranch}:author`),
      1,
      true,
      this.config.driver.type === "persistent"
        ? { persistentPath: this.config.driver.dataPath }
        : undefined,
    );

    this.client = JazzClient.connectWithRuntime(
      this.runtime,
      {
        appId: config.appId,
        schema,
        serverUrl: config.serverUrl,
        env: config.env,
        userBranch: config.userBranch,
        jwtToken: config.jwtToken,
        backendSecret: config.backendSecret,
        adminSecret: config.adminSecret,
        cookieSession: config.cookieSession,
        tier: nodeTier,
        defaultDurabilityTier: config.serverUrl ? nodeTier : undefined,
      },
      { onAuthFailure },
    );
    return this.client;
  }

  async shutdown(): Promise<void> {
    const client = this.client;
    this.client = undefined;
    this.runtime = undefined;
    this.initializedSchemaJson = undefined;
    if (client) {
      await client.shutdown();
    }
  }
}

class BackendDb extends Db {
  constructor(
    config: DbConfig,
    coreSource: RuntimeSource<DbConfig>,
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
    return this.client;
  }
}

function deterministicBytes(seed: string): Uint8Array {
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
  private backendSyncEnabled = false;

  constructor(config: BackendContextConfig) {
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
    this.coreSource = new BackendRuntimeSource(this.config, this.nodeIdentityScope);
  }

  private resolveSchema(source?: BackendSchemaInput): WasmSchema {
    const selected = source ?? this.defaultSchemaInput;
    if (!selected) {
      throw new Error(
        "No schema source provided. Pass `app` to createJazzContext or provide a schema source when calling db()/asBackend()/forRequest()/forSession().",
      );
    }
    const schema = resolveSchemaSource(selected);
    return this.config.permissions && !schemaHasNativePolicies(schema)
      ? mergePermissionsIntoWasmSchema(schema, this.config.permissions)
      : schema;
  }

  private buildDbConfig(): DbConfig {
    return {
      appId: this.config.appId,
      driver: this.config.driver.type === "memory" ? { type: "memory" } : { type: "persistent" },
      serverUrl: this.config.serverUrl,
      env: this.config.env,
      userBranch: this.config.userBranch,
      jwtToken: this.config.jwtToken,
      adminSecret: this.config.adminSecret,
      backendSecret: this.config.backendSecret,
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
            session: session ?? null,
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

  /**
   * Get a backend-scoped `Db` authenticated with `backendSecret`.
   */
  asBackend(source?: BackendSchemaInput): Db {
    const { client, schema } = this.getClientAndSchema(source);
    this.enableBackendSyncIfConfigured(client);
    return this.wrapDb(client, schema, undefined, SYSTEM_AUTHOR_ID, true, true);
  }

  /**
   * Build a backend-scoped `Db` that stamps write provenance as `principalId`
   * without evaluating permissions as that user.
   */
  withAttribution(principalId: string, source?: BackendSchemaInput): Db {
    const { client, schema } = this.getClientAndSchema(source);
    this.enableBackendSyncIfConfigured(client);
    return this.wrapDb(client, schema, undefined, principalId, true, true);
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
    if (this.backendSyncEnabled) {
      return;
    }
    client.connectTransport(this.config.serverUrl, {
      jwt_token: this.config.jwtToken,
      admin_secret: this.config.adminSecret,
      backend_secret: this.config.backendSecret,
      backend_session: this.config.cookieSession,
    });
    this.backendSyncEnabled = true;
  }

  private async resolveRequestSession(request: RequestLike): Promise<Session> {
    return await resolveRequestSession(request, {
      appId: this.config.appId,
      jwksUrl: this.config.jwksUrl,
      jwtPublicKey: this.config.jwtPublicKey,
      allowLocalFirstAuth: this.config.allowLocalFirstAuth,
    });
  }

  /**
   * Build a requester-scoped `Db` from an authenticated request.
   */
  async forRequest(request: RequestLike, source?: BackendSchemaInput): Promise<Db> {
    const { client, schema } = this.getClientAndSchema(source);
    const session = await this.resolveRequestSession(request);
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
    return this.wrapDb(client, schema, undefined, session.user_id, true);
  }

  /**
   * Build a backend-scoped `Db` that stamps write provenance using the
   * authenticated principal from `request` without switching permissions.
   */
  async withAttributionForRequest(request: RequestLike, source?: BackendSchemaInput): Promise<Db> {
    return this.withAttributionForSession(await this.resolveRequestSession(request), source);
  }

  /**
   * Build a session-scoped `Db` for server-side impersonation flows.
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
    this.backendSyncEnabled = false;
    await this.coreSource.shutdown();
  }
}

export function createJazzContext(config: BackendContextConfig): JazzContext {
  return new JazzContext(config);
}
