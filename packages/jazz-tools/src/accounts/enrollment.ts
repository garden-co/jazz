import { parseAuthSecret } from "../runtime/auth-secret-codec.js";
import {
  AccountAuthError,
  requestAccountRegistry,
  requestBackendSignIn,
  readAccountAssignment,
} from "./registry-client.js";
export { AccountAuthError } from "./registry-client.js";
import { isReservedJazzIssuer, parseJwtPayload } from "../runtime/client-session.js";
import { isPortableAuthorComponent } from "../runtime/author-id.js";
import {
  AccountManager,
  type AccountHandle,
  type AccountIdentity,
  type AccountSelectionDurability,
} from "./state.js";

/** @internal A local-first account's self-minted token, or its refresh callback. */
export type JWTAuth = string | { getToken(): Promise<string> };

export interface BackendAuth {
  readonly backendSecret: string;
}
/** @internal Only supported hosts may validate and admit backend credentials. */
export interface BackendAccountHost {
  admitBackend(auth: BackendAuth): Promise<{ nodeId: string }>;
}

interface HandleCredentials {
  registry: string;
  auth?: JWTAuth;
  backend?: Readonly<BackendAuth & { nodeId: string }>;
  /**
   * Signed in through the app's own backend (`createJazz`): the backend states
   * the identity and its claims; the client holds no credential.
   */
  delegated?: Readonly<{ claims: Record<string, unknown> }>;
  localFirstSecret?: string;
  invalidated: Set<() => void>;
}
const credentials = new WeakMap<AccountHandle, HandleCredentials>();

const ACCOUNT_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const NIL_ACCOUNT_ID = "00000000-0000-0000-0000-000000000000";

/** @internal Native key derivation stays in Rust; hosts prepare it before use. */
export interface LocalFirstAccountFactory {
  create(): { accountId: string; identity: AccountIdentity; auth: JWTAuth; secret?: string };
  restore?(secret: string): {
    accountId: string;
    identity: AccountIdentity;
    auth: JWTAuth;
    secret?: string;
  };
}

function identityFromToken(token: string): AccountIdentity {
  // This extracts the intended target only. The core verifies the signature,
  // issuer and audience independently on both authenticated requests.
  const payload = parseJwtPayload(token);
  if (
    typeof payload?.iss !== "string" ||
    typeof payload.sub !== "string" ||
    !isPortableAuthorComponent(payload.iss) ||
    !isPortableAuthorComponent(payload.sub)
  ) {
    throw new AccountAuthError("invalid_identity_token");
  }
  return { issuer: payload.iss, subject: payload.sub };
}
function sameIdentity(a: AccountIdentity, b: AccountIdentity): boolean {
  return a.issuer === b.issuer && a.subject === b.subject;
}
async function tokenFor(auth: JWTAuth): Promise<string> {
  if (typeof auth === "string") return auth;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      Promise.resolve().then(() => auth.getToken()),
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(
          () => reject(new AccountAuthError("credential_refresh_timeout")),
          30_000,
        );
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}
class EnrolledAccount {
  readonly identity: AccountIdentity;
  constructor(
    readonly id: string,
    identity: AccountIdentity,
  ) {
    this.identity = Object.freeze({ ...identity });
    Object.freeze(this);
  }
}

function mintHandle(
  registry: string,
  id: string,
  identity: AccountIdentity,
  auth: JWTAuth,
  localFirstSecret?: string,
): AccountHandle {
  const handle = new EnrolledAccount(id, identity) as AccountHandle;
  credentials.set(handle, { registry, auth, localFirstSecret, invalidated: new Set() });
  return handle;
}

/** Export a local signing root for passphrase/passkey backup. Never store it in UI snapshots. */
export function exportLocalFirstSecret(handle: AccountHandle): string {
  const material = credentials.get(handle);
  if (!material || handle.identity.issuer !== "urn:jazz:local-first" || !material.localFirstSecret)
    throw new AccountAuthError("local_first_recovery_unavailable");
  parseAuthSecret(material.localFirstSecret);
  return material.localFirstSecret;
}

/** @internal Context creation validates the opaque handle and refresh identity. */
export async function accountToken(handle: AccountHandle, registry: string): Promise<string> {
  const material = credentials.get(handle);
  if (!material || material.registry !== registry)
    throw new AccountAuthError("invalid_account_handle");
  if (material.delegated) throw new AccountAuthError("backend_signed_in_account_has_no_token");
  if (material.backend || !material.auth)
    throw new AccountAuthError("backend_account_requires_backend_host");
  const token = await tokenFor(material.auth);
  if (credentials.get(handle) !== material) throw new AccountAuthError("account_logged_out");
  if (!sameIdentity(identityFromToken(token), handle.identity))
    throw new AccountAuthError("credential_identity_changed");
  return token;
}

/**
 * @internal The claims of an account signed in through the app's backend, or
 * undefined for any other account. The backend re-states them on every
 * connection; these only shape local permission checks.
 */
export function delegatedAccountClaims(
  handle: AccountHandle,
  registry: string,
): Readonly<Record<string, unknown>> | undefined {
  const material = credentials.get(handle);
  if (!material || material.registry !== registry)
    throw new AccountAuthError("invalid_account_handle");
  return material.delegated?.claims;
}

/** @internal Opaque backend material, accessible only to a supported host adapter. */
export function getBackendAuth(
  handle: AccountHandle,
  registry: string,
): Readonly<BackendAuth & { nodeId: string }> | undefined {
  const material = credentials.get(handle);
  if (!material || material.registry !== registry)
    throw new AccountAuthError("invalid_account_handle");
  return material.backend;
}

/** @internal Stop contexts when their owning manager logs out. */
export function onAccountInvalidated(handle: AccountHandle, listener: () => void): () => void {
  const material = credentials.get(handle);
  if (!material) {
    listener();
    return () => {};
  }
  material.invalidated.add(listener);
  return () => material.invalidated.delete(listener);
}

/** @internal Validate application scope before opening a local runtime. */
export function accountRegistry(handle: AccountHandle): string {
  const material = credentials.get(handle);
  if (!material) throw new AccountAuthError("invalid_account_handle");
  return material.registry;
}

/** @internal Public host factories prepare native crypto and persistence first. */
export function createAccountManagerWithRuntime(options: {
  /** Exact application-scoped HTTP URL ending in /accounts. */
  registry: string;
  localFirst: LocalFirstAccountFactory;
  backend?: BackendAccountHost;
  restoredLocalFirstSecret?: string;
  selectionDurability?: AccountSelectionDurability;
  fetch?: typeof fetch;
}): AccountManager<JWTAuth> {
  const registry = options.registry.replace(/\/$/, "");
  const issued = new Set<AccountHandle>();
  let epoch = 0;
  const assertCurrent = (started: number) => {
    if (started !== epoch) throw new AccountAuthError("account_logged_out");
  };
  const retain = (handle: AccountHandle) => {
    issued.add(handle);
    return handle;
  };
  // Test fixtures only: enroll with a JWT the test's core trusts. Apps sign
  // in through their backend (`loginWithBackend`) or stay local-first.
  const enrollJWT = async (operation: string, auth: JWTAuth): Promise<AccountHandle> => {
    const started = epoch;
    const token = await tokenFor(auth);
    assertCurrent(started);
    const identity = identityFromToken(token);
    if (isReservedJazzIssuer(identity.issuer))
      throw new AccountAuthError("external_identity_required");
    const response = await requestAccountRegistry(
      registry,
      operation,
      token,
      undefined,
      options.fetch,
    );
    assertCurrent(started);
    return retain(mintHandle(registry, readAccountAssignment(response, identity), identity, auth));
  };
  if (options.restoredLocalFirstSecret !== undefined)
    parseAuthSecret(options.restoredLocalFirstSecret);
  const restored =
    options.restoredLocalFirstSecret === undefined
      ? undefined
      : options.localFirst.restore?.(options.restoredLocalFirstSecret);
  if (options.restoredLocalFirstSecret !== undefined && !restored)
    throw new AccountAuthError("local_first_restore_unavailable");
  return new AccountManager(
    {
      logout() {
        epoch++;
        const listeners: (() => void)[] = [];
        for (const handle of issued) {
          const material = credentials.get(handle);
          credentials.delete(handle);
          listeners.push(...(material?.invalidated ?? []));
        }
        issued.clear();
        // Revoke every credential before invoking application/runtime callbacks.
        for (const listener of listeners) {
          try {
            listener();
          } catch (error) {
            console.error("Account cleanup failed", error);
          }
        }
      },
      createLocalFirst() {
        const local = options.localFirst.create();
        return retain(
          mintHandle(registry, local.accountId, local.identity, local.auth, local.secret),
        );
      },
      restoreLocalFirst(secret) {
        parseAuthSecret(secret);
        const local = options.localFirst.restore?.(secret);
        if (!local) throw new AccountAuthError("local_first_restore_unavailable");
        return retain(mintHandle(registry, local.accountId, local.identity, local.auth, secret));
      },
      async becomeBackend(auth) {
        const started = epoch;
        if (!options.backend) throw new AccountAuthError("backend_host_unavailable");
        if (typeof auth?.backendSecret !== "string" || !auth.backendSecret)
          throw new AccountAuthError("invalid_backend_secret");
        const backendSecret = auth.backendSecret;
        const { nodeId } = await options.backend.admitBackend({ backendSecret });
        assertCurrent(started);
        if (!ACCOUNT_ID_PATTERN.test(nodeId) || nodeId === NIL_ACCOUNT_ID)
          throw new AccountAuthError("invalid_backend_node");
        const handle = new EnrolledAccount(NIL_ACCOUNT_ID, {
          issuer: "urn:jazz:system",
          subject: nodeId,
        }) as AccountHandle;
        credentials.set(handle, {
          registry,
          backend: Object.freeze({ backendSecret, nodeId }),
          invalidated: new Set(),
        });
        return retain(handle);
      },
      registerJWT: (auth) => enrollJWT("register", auth),
      loginJWT: (auth) => enrollJWT("login", auth),
      loginOrRegisterJWT: (auth) => enrollJWT("login-or-register", auth),
      loginWithBackend: () => backendAccount(() => requestBackendSignIn(registry, options.fetch)),
      async linkWithBackend(account) {
        // The guest proves itself with its own local-first token; the backend
        // names the signed-in user and links the two.
        if (account.identity.issuer !== "urn:jazz:local-first")
          throw new AccountAuthError("local_first_account_required");
        const guestToken = await accountToken(account, registry);
        const linked = await backendAccount(() =>
          requestBackendSignIn(registry, options.fetch, { link: guestToken }),
        );
        if (linked.id !== account.id) throw new AccountAuthError("linked_account_mismatch");
        return linked;
      },
    },
    restored
      ? retain(
          mintHandle(
            registry,
            restored.accountId,
            restored.identity,
            restored.auth,
            options.restoredLocalFirstSecret,
          ),
        )
      : undefined,
    options.selectionDurability,
  );

  async function backendAccount(signIn: () => Promise<unknown>): Promise<AccountHandle> {
    const started = epoch;
    const value = await signIn();
    assertCurrent(started);
    const { identity, claims } = (value ?? {}) as {
      identity?: AccountIdentity;
      claims?: unknown;
    };
    if (
      !identity ||
      typeof identity.issuer !== "string" ||
      typeof identity.subject !== "string" ||
      !isPortableAuthorComponent(identity.issuer) ||
      !isPortableAuthorComponent(identity.subject) ||
      isReservedJazzIssuer(identity.issuer)
    )
      throw new AccountAuthError("invalid_account_response");
    const id = readAccountAssignment(value, identity);
    const handle = new EnrolledAccount(id, identity) as AccountHandle;
    credentials.set(handle, {
      registry,
      delegated: Object.freeze({
        claims:
          claims && typeof claims === "object" && !Array.isArray(claims)
            ? Object.freeze({ ...(claims as Record<string, unknown>) })
            : Object.freeze({}),
      }),
      invalidated: new Set(),
    });
    return retain(handle);
  }
}
