import { createHash, randomUUID } from "node:crypto";
import { admitBackendSecret } from "./admission.js";
import type { BackendRequestOptions } from "./request-auth.js";
import { NapiDb, mintLocalFirstToken } from "jazz-napi";
import { accountRegistryUrl, createAccountDbWithRuntimeSource } from "../accounts/context.js";
import {
  accountRegistry,
  accountToken,
  getBackendAuth,
  type BackendAuth,
} from "../accounts/enrollment.js";
import { prepareAccountManager, type AccountStore } from "../accounts/persistence.js";
import type { AccountHandle } from "../accounts/state.js";
import { authSecretSeedForMinting } from "../runtime/auth-secret-codec.js";
import { resolveSchemaSource } from "../schema-source.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import { authorBytesForSession } from "../runtime/author-id.js";
import { JazzClient as RuntimeClient, type RequestLike } from "../runtime/client.js";
import { resolveClientInternalSessionSync } from "../runtime/client-session.js";
import type { AppContext, PublicSession, Session } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import {
  getTrustedReservedSession,
  setTrustedReservedSession,
} from "../runtime/db-internal-session.js";
import { selfSignedClientProofFromConfig } from "../runtime/default-runtime-source.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { RuntimeSource, type RuntimeClientContext } from "../runtime/runtime-source.js";
import { attachSubscriptionStore, getSubscriptionStore } from "../subscription-store-internal.js";
import { createJazzSessionOwner, type JazzSession } from "../session/state.js";
import {
  createJazzClientFromDb,
  type JazzClient as SharedJazzClient,
} from "../web/create-jazz-client.js";
import {
  JazzContext,
  type BackendContextConfig,
  type BackendNodeClock,
  type BackendSchemaInput,
} from "./create-jazz-context.js";
import { randomNodeUuidBytes } from "../runtime/runtime-entropy.js";
import { deterministicBytes } from "../runtime/deterministic-bytes.js";
import { formatUuidAt } from "../runtime/hex.js";

export type JazzSessionConfig = Omit<
  BackendContextConfig,
  | "app"
  | "permissions"
  | "backendSecret"
  | "jwtToken"
  | "cookieSession"
  | "adminSecret"
  | "serverUrl"
> & {
  app: BackendSchemaInput;
  permissions: NonNullable<BackendContextConfig["permissions"]>;
  serverUrl: string;
  store?: AccountStore;
  initial?: "local-first" | BackendAuth;
};

export interface JazzClient extends SharedJazzClient {
  flush(): void;
  /** Verify a request and retain its immutable user policy context. Backend clients only. */
  forRequest(request: RequestLike, options?: BackendRequestOptions): Promise<Db>;
  /** Use an admitted account without switching the shared session. Backend clients only. */
  forAccount(account: AccountHandle): Promise<Db>;
  /** Keep backend permissions while recording verified user provenance. */
  withAttribution(account: AccountHandle): Promise<Db>;
  withAttributionForRequest(request: RequestLike): Promise<Db>;
}

function uuidBytes(uuid: string): Uint8Array {
  return Uint8Array.from(Buffer.from(uuid.replaceAll("-", ""), "hex"));
}

function backendNodeId(config: JazzSessionConfig): string {
  if (config.driver.type === "memory") return randomUUID();
  // Preserve the existing persistent backend node identity across this API migration.
  return formatUuidAt(
    deterministicBytes(`${config.appId}:${config.env ?? "dev"}:${config.driver.dataPath}:node`),
    0,
  );
}

/** Each selected user opens an ordinary native client, never a backend facade. */
class NodeUserRuntimeSource extends RuntimeSource {
  override readonly supportsPolicyBypass = false;
  flush(): void {
    // Native writes already cross the local durability boundary when committed.
  }
  constructor(
    private readonly host: JazzSessionConfig,
    private readonly account: AccountHandle,
  ) {
    super();
  }
  override createClient({
    config,
    schema,
    migrations,
    onAuthFailure,
  }: RuntimeClientContext): RuntimeClient {
    const declaredSchema = resolveSchemaSource(this.host.app);
    if (
      serializeRuntimeSchema(mergePermissionsIntoWasmSchema(declaredSchema, {})) !==
      serializeRuntimeSchema(mergePermissionsIntoWasmSchema(schema, {}))
    ) {
      throw new Error("Node session query schema does not match its configured app");
    }
    const runtimeSchema = mergePermissionsIntoWasmSchema(schema, this.host.permissions);
    const trustedReservedSession = getTrustedReservedSession(config);
    const session = resolveClientInternalSessionSync({ ...config, trustedReservedSession });
    if (!session) throw new Error("Node user runtime requires an admitted account");
    const scope = createHash("sha256")
      .update(
        JSON.stringify([
          accountRegistry(this.account),
          this.host.env ?? "dev",
          this.account.id,
          this.account.identity.issuer,
          this.account.identity.subject,
        ]),
      )
      .digest("hex");
    const runtime = new NativeRuntimeAdapter(
      NapiDb,
      runtimeSchema,
      this.host.driver.type === "persistent"
        ? deterministicBytes(
            `${this.host.appId}:${this.host.env ?? "dev"}:${this.host.driver.dataPath}.accounts/${scope}:node`,
          )
        : uuidBytes(randomUUID()),
      authorBytesForSession(session),
      1,
      false,
      {
        ...(this.host.driver.type === "persistent"
          ? { persistentPath: `${this.host.driver.dataPath}.accounts/${scope}` }
          : {}),
        selfSignedClientProof: selfSignedClientProofFromConfig(config, session),
        readAuthorizationHost: "client-local",
        slowQueryLog: { appId: this.host.appId, thresholdMs: this.host.slowQueryThresholdMs },
        migrations,
      },
    );
    const context: AppContext = { ...config, schema: runtimeSchema };
    setTrustedReservedSession(context, trustedReservedSession);
    return RuntimeClient.connectWithRuntime(runtime, context, { onAuthFailure });
  }
}

function memoryAccountStore(): AccountStore {
  let value: string | null = null;
  return {
    async read() {
      return value;
    },
    async update(transform) {
      value = transform(value);
    },
  };
}

/**
 * Own one Node client through the shared session lifecycle. Backend admission
 * requires the configured core URL. Supply an AccountStore to retain local roots
 * across process restarts; service secrets are never persisted there.
 */
export async function createJazzSession(
  config: JazzSessionConfig,
): Promise<JazzSession<JazzClient>> {
  for (const key of ["backendSecret", "jwtToken", "cookieSession", "adminSecret"]) {
    if (Object.hasOwn(config, key))
      throw new Error(`Use session account actions instead of ${key}`);
  }
  if (!config.app) throw new Error("Node createJazzSession requires app");
  if (config.permissions == null) {
    throw new Error(
      "createJazzSession requires an explicit permissions bundle. Pass {} to deny all access.",
    );
  }
  const registry = accountRegistryUrl(config.serverUrl, config.appId);
  // A memory backend handle's pooled node and, after a clean handoff, its clock.
  const memoryClocks = new WeakMap<AccountHandle, { node: Uint8Array; highWater?: bigint }>();
  const accounts = await prepareAccountManager({
    appId: config.appId,
    registry,
    store: config.store ?? memoryAccountStore(),
    mintToken: (secret, audience) =>
      mintLocalFirstToken(authSecretSeedForMinting(secret), audience, 3600),
    backend: {
      async admitBackend({ backendSecret }) {
        await admitBackendSecret(
          `${registry.slice(0, -"/accounts".length)}/backend/admit`,
          backendSecret,
        );
        return { nodeId: backendNodeId(config) };
      },
    },
  });
  return createJazzSessionOwner({
    accounts,
    initial: config.initial,
    openClient: async (account) => {
      const backend = getBackendAuth(account, registry);
      if (!backend) {
        const source = new NodeUserRuntimeSource(config, account);
        const db = await createAccountDbWithRuntimeSource(
          {
            appId: config.appId,
            serverUrl: config.serverUrl,
            env: config.env,
            driver: { type: config.driver.type },
            account,
          },
          source,
        );
        let client: SharedJazzClient;
        try {
          client = await createJazzClientFromDb(db);
        } catch (error) {
          await db.shutdown();
          throw error;
        }
        return Object.assign(client, {
          flush() {
            source.flush();
          },
          async forRequest(): Promise<Db> {
            throw new Error("Request scopes require a backend account");
          },
          async forAccount(): Promise<Db> {
            throw new Error("Account scopes require a backend account");
          },
          async withAttribution(): Promise<Db> {
            throw new Error("Attribution requires a backend account");
          },
          async withAttributionForRequest(): Promise<Db> {
            throw new Error("Attribution requires a backend account");
          },
        });
      }
      let memoryClock: BackendNodeClock | undefined;
      if (config.driver.type === "memory") {
        const pooled = memoryClocks.get(account);
        if (pooled && pooled.highWater === undefined) {
          throw new Error("Backend memory node cannot reopen without its completed clock handoff");
        }
        const node = pooled?.node ?? randomNodeUuidBytes();
        memoryClock = {
          node,
          initialHighWater: pooled?.highWater,
          closed(highWater: bigint) {
            memoryClocks.set(account, { node, highWater });
          },
        };
        // A failed/partial close must never reuse a stale earlier high-water.
        memoryClocks.set(account, { node });
      }
      // The class remains internal; only a validated opaque account can reach it.
      // Its SYSTEM provenance names the backend's node.
      const context = new JazzContext(
        { ...config, backendSecret: backend.backendSecret } as BackendContextConfig,
        uuidBytes(backend.nodeId),
        memoryClock,
      );
      let db: Db;
      let client: SharedJazzClient;
      try {
        db = context.openBackendAccount({
          issuer: account.identity.issuer,
          user_id: account.identity.subject,
          account_id: account.id,
          claims: {},
          authMode: "external",
        });
        client = await createJazzClientFromDb(db);
      } catch (error) {
        await context.shutdown();
        throw error;
      }
      let closed = false;
      const assertActive = () => {
        if (closed) throw new Error("Backend client is closed");
        getBackendAuth(account, registry);
      };
      const backendClient = attachSubscriptionStore(
        {
          db,
          flush() {
            assertActive();
            context.flush();
          },
          get session(): PublicSession | null {
            return client.session;
          },
          async forRequest(request: RequestLike, options?: BackendRequestOptions) {
            assertActive();
            const scoped = await context.forRequest(request, undefined, options);
            assertActive();
            return scoped;
          },
          async forAccount(requestAccount: AccountHandle) {
            assertActive();
            if (accountRegistry(requestAccount) !== registry)
              throw new Error("Account application mismatch");
            if (getBackendAuth(requestAccount, registry))
              throw new Error("Request scopes require a user account");
            const token = await accountToken(requestAccount, registry);
            // Re-verify bearer and core liveness through the same request admission path.
            const scoped = await context.forRequest({
              headers: { authorization: `Bearer ${token}` },
            });
            assertActive();
            return scoped;
          },
          async withAttribution(requestAccount: AccountHandle) {
            assertActive();
            if (accountRegistry(requestAccount) !== registry)
              throw new Error("Account application mismatch");
            if (getBackendAuth(requestAccount, registry))
              throw new Error("Attribution requires a user account");
            const token = await accountToken(requestAccount, registry);
            const scoped = await context.withAttributionForRequest({
              headers: { authorization: `Bearer ${token}` },
            });
            assertActive();
            return scoped;
          },
          async withAttributionForRequest(request: RequestLike) {
            assertActive();
            const scoped = await context.withAttributionForRequest(request);
            assertActive();
            return scoped;
          },
          async shutdown(options?: { waitForSync?: boolean }) {
            await client.shutdown(options);
            closed = true;
            await context.shutdown();
          },
        },
        getSubscriptionStore(client),
      );
      sessionScopes.set(backendClient, (session) => {
        assertActive();
        return context.forSession(session);
      });
      return backendClient;
    },
  });
}

const sessionScopes = new WeakMap<object, (session: Session) => Db>();

/**
 * A `Db` acting as `session`, on a backend client: for an embedder that has
 * already decided who the user is (createJazz). Not part of the public API.
 */
export function backendDbForSession(client: JazzClient, session: Session): Db {
  const scope = sessionScopes.get(client);
  if (!scope) throw new Error("Session scopes require a backend account");
  return scope(session);
}
