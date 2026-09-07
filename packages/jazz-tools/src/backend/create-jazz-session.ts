import { createHash, randomUUID } from "node:crypto";
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
import { authorBytesForSession } from "../runtime/author-id.js";
import { JazzClient as RuntimeClient, type RequestLike } from "../runtime/client.js";
import { resolveClientInternalSessionSync } from "../runtime/client-session.js";
import type { AppContext, PublicSession } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import {
  getTrustedReservedSession,
  setTrustedReservedSession,
} from "../runtime/db-internal-session.js";
import { selfSignedClientProofFromConfig } from "../runtime/default-runtime-source.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { RuntimeSource, type RuntimeClientContext } from "../runtime/runtime-source.js";
import { createJazzSessionOwner, type JazzSession } from "../session/state.js";
import {
  createJazzClientFromDb,
  type JazzClient as SharedJazzClient,
} from "../web/create-jazz-client.js";
import {
  JazzContext,
  type BackendContextConfig,
  type BackendSchemaInput,
} from "./create-jazz-context.js";

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
  permissions?: BackendContextConfig["permissions"];
  serverUrl: string;
  store?: AccountStore;
  initial?: "local-first" | BackendAuth;
};

export interface JazzClient extends SharedJazzClient {
  flush(): void;
  /** Verify a request and retain its immutable user policy context. Backend clients only. */
  forRequest(request: RequestLike): Promise<Db>;
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
  const hex = createHash("sha256")
    .update(JSON.stringify([config.appId, config.env ?? "dev", config.driver.dataPath]))
    .digest("hex")
    .slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
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
  override createClient({ config, schema, onAuthFailure }: RuntimeClientContext): RuntimeClient {
    const trustedReservedSession = getTrustedReservedSession(config);
    const session = resolveClientInternalSessionSync({ ...config, trustedReservedSession });
    if (!session) throw new Error("Node user runtime requires an admitted account");
    const scope = createHash("sha256")
      .update(
        JSON.stringify([
          this.account.id,
          this.account.identity.issuer,
          this.account.identity.subject,
        ]),
      )
      .digest("hex");
    const runtime = new NativeRuntimeAdapter(
      NapiDb,
      schema,
      uuidBytes(randomUUID()),
      authorBytesForSession(session),
      1,
      false,
      {
        ...(this.host.driver.type === "persistent"
          ? { persistentPath: `${this.host.driver.dataPath}.accounts/${scope}` }
          : {}),
        selfSignedClientProof: selfSignedClientProofFromConfig(config, session),
        readAuthorizationHost: "client-local",
      },
    );
    const context: AppContext = { ...config, schema, tier: "local" };
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
  const registry = accountRegistryUrl(config.serverUrl, config.appId);
  const nodeId = backendNodeId(config);
  const accounts = await prepareAccountManager({
    appId: config.appId,
    registry,
    store: config.store ?? memoryAccountStore(),
    mintToken: (secret, audience) =>
      mintLocalFirstToken(authSecretSeedForMinting(secret), audience, 3600),
    backend: {
      async admitBackend({ backendSecret }) {
        const response = await fetch(`${registry.slice(0, -"/accounts".length)}/backend/admit`, {
          method: "POST",
          headers: { "X-Jazz-Backend-Secret": backendSecret },
          redirect: "error",
          signal: AbortSignal.timeout(10_000),
        });
        if (response.status !== 204)
          throw new Error(`Backend admission failed (${response.status})`);
        return { nodeId };
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
        const client = await createJazzClientFromDb(db);
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
      // The class remains internal; only a validated opaque account can reach it.
      const context = new JazzContext(
        { ...config, backendSecret: backend.backendSecret } as BackendContextConfig,
        uuidBytes(backend.nodeId),
      );
      const db = context.openBackendAccount({
        issuer: account.identity.issuer,
        user_id: account.identity.subject,
        account_id: account.id,
        claims: {},
        authMode: "external",
      });
      const client = await createJazzClientFromDb(db);
      let closed = false;
      const assertActive = () => {
        if (closed) throw new Error("Backend client is closed");
        getBackendAuth(account, registry);
      };
      return {
        db,
        flush() {
          assertActive();
          context.flush();
        },
        get session(): PublicSession | null {
          return client.session;
        },
        async forRequest(request: RequestLike) {
          assertActive();
          const scoped = await context.forRequest(request);
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
      };
    },
  });
}
