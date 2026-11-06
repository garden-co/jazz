import { textAnchorsFrom, type TextAnchors } from "../runtime/text-anchors.js";
import { createHash, randomUUID } from "node:crypto";
import { clientScope } from "../runtime/client-scope.js";
import { admitBackendSecret } from "./admission.js";
import {
  credentialedTransport,
  type BackendClientCredential,
} from "../runtime/credentialed-transport.js";
import type { WebSocketConstructor } from "../runtime/native-runtime/websocket.js";
import {
  NapiDb,
  captureTextAnchor,
  captureTextRange,
  resolveTextAnchor,
  resolveTextRange,
} from "jazz-napi";
import { mintLocalFirstTokenFromSeed } from "../runtime/local-first-token.js";
import { accountRegistryUrl, createAccountDbWithRuntimeSource } from "../accounts/context.js";
import { accountRegistry, getBackendAuth, type BackendAuth } from "../accounts/enrollment.js";
import { prepareAccountManager, type AccountStore } from "../accounts/persistence.js";
import type { AccountHandle } from "../accounts/state.js";
import { authSecretSeedForMinting } from "../runtime/auth-secret-codec.js";
import { resolveSchemaSource } from "../schema-source.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import { authorBytesForSession } from "../runtime/author-id.js";
import { JazzClient as RuntimeClient } from "../runtime/client.js";
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

export type JazzClientConfig = Omit<
  BackendContextConfig,
  | "app"
  | "permissions"
  | "backendSecret"
  | "jwtToken"
  | "cookieSession"
  | "adminSecret"
  | "serverUrl"
  | "webSocket"
  | "appId"
> & {
  /** Leave out to act as a browser client, scoped by `serverUrl`. */
  appId?: string;
  /** The app id an existing local-first app's users were created under. */
  scope?: string;
  app: BackendSchemaInput;
  permissions: NonNullable<BackendContextConfig["permissions"]>;
  serverUrl: string;
  store?: AccountStore;
  /** Start as a local-first guest. */
  initial?: "local-first";
  /**
   * For a backend client (a machine that syncs through an app's main backend,
   * not a browser): its workload's OIDC token, or a function that returns a
   * fresh one. It is sent as `Authorization: Bearer` on every request and
   * socket to `serverUrl`, and the main backend trusts it with
   * `trustWorkload` (see `createJazz`'s `backendClients`). The session opens
   * as whatever the main backend trusts the token as: a trusted machine with
   * backend authority, a scoped machine as its user. It fails when the main
   * backend trusts the token as neither. Not with `initial`.
   */
  credential?: BackendClientCredential;
};

/** @internal `createJazz`'s own session config: it may admit itself with the core's backend secret. */
type OpenJazzSessionConfig = Omit<JazzClientConfig, "initial"> & {
  initial?: "local-first" | BackendAuth;
};

export interface JazzClient extends SharedJazzClient {
  flush(): void;
}

function uuidBytes(uuid: string): Uint8Array {
  return Uint8Array.from(Buffer.from(uuid.replaceAll("-", ""), "hex"));
}

function backendNodeId(config: OpenJazzSessionConfig): string {
  if (config.driver.type === "memory") return randomUUID();
  // Preserve the existing persistent backend node identity across this API migration.
  return formatUuidAt(
    deterministicBytes(
      `${clientScope(config)}:${config.env ?? "dev"}:${config.driver.dataPath}:node`,
    ),
    0,
  );
}

/** Each selected user opens an ordinary native client, never a backend facade. */
class NodeUserRuntimeSource extends RuntimeSource {
  override textAnchors(): TextAnchors {
    return textAnchorsFrom({
      captureTextAnchor,
      captureTextRange,
      resolveTextAnchor,
      resolveTextRange,
    });
  }

  override readonly supportsPolicyBypass = false;
  flush(): void {
    // Native writes already cross the local durability boundary when committed.
  }
  constructor(
    private readonly host: OpenJazzSessionConfig & { webSocket?: WebSocketConstructor },
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
            `${clientScope(this.host)}:${this.host.env ?? "dev"}:${this.host.driver.dataPath}.accounts/${scope}:node`,
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
        webSocket: this.host.webSocket,
        slowQueryLog: { appId: this.host.appId, thresholdMs: this.host.slowQueryThresholdMs },
        migrations,
      },
    );
    const context: AppContext = { ...config, appId: clientScope(config), schema: runtimeSchema };
    setTrustedReservedSession(context, trustedReservedSession);
    return RuntimeClient.connectWithRuntime(runtime, context, { onAuthFailure });
  }
}

/** What a trusted backend client states as its secret: its token, not this, states its access. */
const BACKEND_CLIENT_STAND_IN_SECRET = "stated-by-backend-credential";

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

/** How a backend client's session closes (see {@link BackendClientSession}). */
export interface BackendClientCloseOptions {
  /**
   * How long to wait for the writes this client holds to reach the server
   * first. Default `CLOSE_WAITS_FOR_UPLOADS_MS` (10 s); `0` closes at once.
   */
  timeoutMs?: number;
}

/** A backend client's session: its `close()` uploads the writes it holds first. */
export interface BackendClientSession extends JazzSession<JazzClient> {
  /**
   * Upload the writes this client holds, for up to `timeoutMs`, then close.
   * With the memory driver, writes that didn't reach the server are lost:
   * then it rejects, saying how many, once closed (unless `timeoutMs` is 0,
   * which accepts that). A persistent driver keeps them for the next open,
   * and it says so on the console instead.
   */
  close(options?: BackendClientCloseOptions): Promise<void>;
}

/**
 * A Node client that syncs through an app's main backend (`createJazz`) as
 * a backend client, with `credential`, or as a local-first guest. Supply an
 * AccountStore to retain local roots across process restarts.
 */
export async function createJazzClient(input: JazzClientConfig): Promise<BackendClientSession> {
  if (input.initial !== undefined && input.initial !== "local-first")
    throw new Error('createJazzClient: initial is "local-first" or left out');
  const session = await openJazzSession(input);
  const keeps = input.driver.type !== "memory";
  return Object.freeze({
    ...session,
    close: (options?: BackendClientCloseOptions) => closeAfterUploads(session, keeps, options),
  });
}

/** How long a backend client's `close()` waits for its writes to reach the server. */
export const CLOSE_WAITS_FOR_UPLOADS_MS = 10_000;

/**
 * A backend client's `close()`: its writes live only in this process (or
 * its driver) until the server accepts them, and a script's last act is
 * usually to close, so it uploads them first, the way a local-first client
 * shuts down. Writes the server rejects are reported as they always are
 * (`db.onMutationError`, their `wait()`). It stops waiting at the deadline,
 * or at once when the server refused this client or the app disconnected
 * it, and closes either way.
 */
async function closeAfterUploads(
  session: JazzSession<JazzClient>,
  keeps: boolean,
  options: BackendClientCloseOptions = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? CLOSE_WAITS_FOR_UPLOADS_MS;
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw new Error("close(): timeoutMs is a number of milliseconds, 0 or more");
  }
  const db = session.getSnapshot().client?.db;
  let left: { pending: number; why: unknown } | undefined;
  if (db && timeoutMs > 0) {
    const uploaded = db.shutdown({ waitForSync: true }).then(
      () => undefined,
      (error: unknown) => error,
    );
    let timer: ReturnType<typeof setInterval> | undefined;
    const deadline = Date.now() + timeoutMs;
    const gaveUp = new Promise<string>((resolve) => {
      timer = setInterval(() => {
        const refused = authError(db);
        const link = syncLink(db);
        if (refused) resolve(`the server refused this client (${refused})`);
        else if (link?.state === "offline" && !link.retrying) {
          resolve(`this client is offline (${link.reason})`);
        } else if (Date.now() >= deadline) {
          resolve(`the server didn't accept them within ${timeoutMs / 1000}s`);
        }
      }, 50);
    });
    const outcome = await Promise.race([uploaded.then((error) => ({ error })), gaveUp]);
    clearInterval(timer);
    if (typeof outcome === "string") {
      left = { pending: pendingUploads(db), why: outcome };
      db.abortGracefulShutdown();
      await uploaded;
    } else if (outcome.error !== undefined) {
      left = { pending: pendingUploads(db), why: outcome.error };
    }
  } else if (db) {
    left = { pending: pendingUploads(db), why: "close() was asked not to wait" };
  }
  const notUploaded = left && left.pending > 0 ? left : undefined;
  const lost =
    notUploaded && !keeps ? lostOnClose(notUploaded.pending, notUploaded.why) : undefined;
  try {
    await session.close();
  } catch (error) {
    if (lost)
      throw new Error(`createJazzClient: close() failed after: ${lost.message}`, { cause: error });
    throw error;
  }
  if (!notUploaded) return;
  if (keeps) {
    console.warn(
      `createJazzClient: closed with ${notUploaded.pending} write${notUploaded.pending === 1 ? "" : "s"} not uploaded yet (${reasonOf(notUploaded.why)}); this client's driver keeps them and uploads them when it opens again.`,
    );
  } else if (timeoutMs > 0) {
    throw lost;
  } else {
    console.warn(lost!.message);
  }
}

function authError(db: Db): string | undefined {
  try {
    return db.getAuthState().error;
  } catch {
    return undefined;
  }
}

function syncLink(db: Db) {
  try {
    return db.getSyncStatus().link;
  } catch {
    return undefined;
  }
}

function reasonOf(why: unknown): string {
  return why instanceof Error ? why.message : String(why);
}

function pendingUploads(db: Db | undefined): number {
  try {
    return db?.getSyncStatus().pendingUploads ?? 0;
  } catch {
    // A db already shutting down has nothing left to upload.
    return 0;
  }
}

function lostOnClose(pending: number, why: unknown): Error {
  return new Error(
    `createJazzClient: closed with ${pending} write${pending === 1 ? "" : "s"} the server never accepted, which are lost: ${reasonOf(why)}. Await \`db.waitForSync()\` before close() to handle this yourself.`,
    why instanceof Error ? { cause: why } : undefined,
  );
}

/**
 * @internal The session `createJazz` opens on its own core, admitted with the
 * core's backend secret (`initial: { backendSecret }`).
 */
export async function openJazzSession(
  input: OpenJazzSessionConfig,
): Promise<JazzSession<JazzClient>> {
  if (Object.hasOwn(input, "webSocket")) throw new Error("Use credential instead of webSocket");
  if (input.credential !== undefined && input.initial !== undefined) {
    throw new Error("A backend client's credential decides how it opens; leave out initial");
  }
  const transport =
    input.credential === undefined
      ? undefined
      : credentialedTransport(input.serverUrl, input.credential);
  const config: OpenJazzSessionConfig & { webSocket?: WebSocketConstructor } = {
    ...input,
    webSocket: transport?.WebSocket,
  };
  for (const key of ["backendSecret", "jwtToken", "cookieSession", "adminSecret"]) {
    if (Object.hasOwn(config, key)) throw new Error(`${key} is not a createJazzClient option`);
  }
  if (!config.app) throw new Error("Node createJazzClient requires app");
  if (config.permissions == null) {
    throw new Error(
      "createJazzClient requires an explicit permissions bundle. Pass {} to deny all access.",
    );
  }
  const registry = accountRegistryUrl(config.serverUrl, clientScope(config));
  // A memory backend handle's pooled node and, after a clean handoff, its clock.
  const memoryClocks = new WeakMap<AccountHandle, { node: Uint8Array; highWater?: bigint }>();
  const accounts = await prepareAccountManager({
    appId: clientScope(config),
    registry,
    store: config.store ?? memoryAccountStore(),
    fetch: transport?.fetch,
    mintToken: (secret, audience) =>
      mintLocalFirstTokenFromSeed(authSecretSeedForMinting(secret), audience, 3600),
    backend: {
      async admitBackend({ backendSecret }) {
        await admitBackendSecret(
          `${registry.slice(0, -"/accounts".length)}/backend/admit`,
          backendSecret,
          { fetch: transport?.fetch },
        );
        return { nodeId: backendNodeId(config) };
      },
    },
  });
  // A backend client opens as what its credential is trusted as: a trusted
  // machine is admitted as a backend, a scoped one signs in as its user.
  let initial = config.initial;
  let signInAsUser = false;
  if (transport) {
    try {
      await admitBackendSecret(
        `${registry.slice(0, -"/accounts".length)}/backend/admit`,
        BACKEND_CLIENT_STAND_IN_SECRET,
        { fetch: transport.fetch },
      );
      initial = { backendSecret: BACKEND_CLIENT_STAND_IN_SECRET };
    } catch (error) {
      const status = (error as { status?: number }).status;
      if (status !== 401 && status !== 404) throw error;
      signInAsUser = true;
    }
  }
  const session = await createJazzSessionOwner({
    accounts,
    initial: typeof initial === "object" ? undefined : initial,
    backend: typeof initial === "object" ? initial : undefined,
    openClient: async (account) => {
      const backend = getBackendAuth(account, registry);
      if (!backend) {
        const source = new NodeUserRuntimeSource(config, account);
        const db = await createAccountDbWithRuntimeSource(
          {
            appId: clientScope(config),
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
          async shutdown(options?: { waitForSync?: boolean }) {
            await client.shutdown(options);
            closed = true;
            await context.shutdown();
          },
        },
        getSubscriptionStore(client),
      );
      sessionScopes.set(backendClient, (session, attribution) => {
        assertActive();
        return attribution
          ? context.withAttributionForSession(session)
          : context.forSession(session);
      });
      return backendClient;
    },
  });
  // A backend session syncs with its core, which admits only the app's
  // backend: a guest there would be refused. A guest syncs with the app's own
  // endpoint, from a session opened there.
  const refuseGuest =
    <Args extends unknown[]>(operation: string, switchTo: (...args: Args) => Promise<void>) =>
    (...args: Args): Promise<void> =>
      owned.getSnapshot().account?.identity.issuer === "urn:jazz:system"
        ? Promise.reject(
            new Error(
              `${operation}() on a backend session is not supported yet: open a session at the app's /jazz endpoint for a local-first guest`,
            ),
          )
        : switchTo(...args);
  const owned = session;
  const guarded: JazzSession<JazzClient> = Object.freeze({
    ...owned,
    createLocalFirst: refuseGuest("createLocalFirst", owned.createLocalFirst),
    restoreLocalFirst: refuseGuest("restoreLocalFirst", owned.restoreLocalFirst),
  });
  if (signInAsUser) {
    try {
      await guarded.loginWithBackend();
    } catch (error) {
      await guarded.close();
      // Only a refusal means the credential isn't trusted; an outage stays itself.
      if ((error as { status?: number }).status !== 401) throw error;
      throw new Error(
        "createJazzClient: the main backend trusts this backend client's credential neither as a trusted machine nor as a user (see trustWorkload in its backendClients)",
        { cause: error },
      );
    }
  }
  return guarded;
}

const sessionScopes = new WeakMap<object, (session: Session, attribution: boolean) => Db>();

/**
 * A `Db` acting as `session`, on a backend client: for an embedder that has
 * already decided who the user is (createJazz). Not part of the public API.
 */
export function backendDbForSession(client: JazzClient, session: Session): Db {
  const scope = sessionScopes.get(client);
  if (!scope) throw new Error("Session scopes require a backend account");
  return scope(session, false);
}

/**
 * A `Db` with backend authority whose writes are stamped as `session`'s user,
 * on a backend client (createJazz). Not part of the public API.
 */
export function backendDbAttributedTo(client: JazzClient, session: Session): Db {
  const scope = sessionScopes.get(client);
  if (!scope) throw new Error("Session scopes require a backend account");
  return scope(session, true);
}
