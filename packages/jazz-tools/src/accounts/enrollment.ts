import { signChallenge } from "../local-first-auth/index.js";
import { parseAuthSecret } from "../runtime/auth-secret-codec.js";
import {
  AccountAuthError,
  isWhitespaceOnlySecret,
  LINK_GUEST_KEY_MISSING_MESSAGE,
  LINK_NEEDS_GUEST_MESSAGE,
  requestBackendLinkChallenge,
  requestBackendSignIn,
  readAccountAssignment,
  WHITESPACE_BACKEND_SECRET,
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

/**
 * The backend refuses a link for good when the signed-in user already has an
 * account of their own (most often: they signed up on another device), or
 * when this guest's key was retired or is another app's.
 */
const LINK_REFUSED_MESSAGE =
  "link_refused: the backend won't link this device account to the signed-in user, most likely because they already have an account (from another device). Call refresh() to open their account instead. The device account stays signed in on this device until then, and linking never moves its data into an existing account.";

/**
 * The app's core admits no device accounts at all: nothing about this user.
 * Linking can pass once the core does (a backend fix, then a retry).
 */
const LINK_GUESTS_OFF_MESSAGE =
  "local_first_auth_disabled: the backend's core admits no device accounts, so this device account can't be linked (or sync) yet. It is a backend setup problem, not this user's: the device account stays signed in on this device, with its data, and linking can be retried once the core admits device accounts (allowLocalFirstAuth).";

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
  delegated?: Readonly<{ claims: Record<string, unknown>; bearer?: AppBearer }>;
  localFirstSecret?: string;
  invalidated: Set<() => void>;
}
const credentials = new WeakMap<AccountHandle, HandleCredentials>();

/**
 * The app's own credential for its backend, for a client that sends no
 * cookie (a native app): its current bearer token, or null when signed out.
 */
export type AppBearer = () => string | null | Promise<string | null>;

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
  readonly claims?: Readonly<Record<string, unknown>>;
  constructor(
    readonly id: string,
    identity: AccountIdentity,
    claims?: Readonly<Record<string, unknown>>,
  ) {
    this.identity = Object.freeze({ ...identity });
    if (claims) this.claims = claims;
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

/**
 * @internal The app's bearer an account signed in through the app's backend
 * was signed in with, for a host whose sync connection sends no cookie.
 */
export function delegatedAccountBearer(
  handle: AccountHandle,
  registry: string,
): AppBearer | undefined {
  const material = credentials.get(handle);
  if (!material || material.registry !== registry)
    throw new AccountAuthError("invalid_account_handle");
  return material.delegated?.bearer;
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

/**
 * @internal What a device remembers of the user its backend signed in, so a
 * reload reopens their account (offline too) instead of starting over. The
 * shape of the backend's sign-in answer: the account, identity and claims,
 * never a credential. The backend still states the identity on every
 * connection.
 */
export interface RememberedBackendUser {
  readonly account: string;
  readonly identity: AccountIdentity;
  readonly claims: Readonly<Record<string, unknown>>;
}

/** @internal The remembered form of a user the backend signed in, or undefined for any other account. */
export function rememberBackendUser(handle: AccountHandle): RememberedBackendUser | undefined {
  const material = credentials.get(handle);
  if (!material?.delegated) return undefined;
  return {
    account: handle.id,
    identity: { issuer: handle.identity.issuer, subject: handle.identity.subject },
    claims: { ...material.delegated.claims },
  };
}

/** @internal Public host factories prepare native crypto and persistence first. */
export function createAccountManagerWithRuntime(options: {
  /** Exact application-scoped HTTP URL ending in /accounts. */
  registry: string;
  localFirst: LocalFirstAccountFactory;
  backend?: BackendAccountHost;
  restoredLocalFirstSecret?: string;
  /** The backend user this device last had selected (see `RememberedBackendUser`). */
  restoredBackendUser?: RememberedBackendUser;
  selectionDurability?: AccountSelectionDurability;
  fetch?: typeof fetch;
  /** Sent as `Authorization: Bearer` on sign-in requests to the app's backend. */
  bearer?: AppBearer;
}): AccountManager<JWTAuth> {
  const registry = options.registry.replace(/\/$/, "");
  const bearer = options.bearer;
  const backendFetch: typeof fetch | undefined = bearer
    ? async (input, init) => {
        const token = await bearer();
        const headers = new Headers(init?.headers);
        if (token) headers.set("Authorization", `Bearer ${token}`);
        return (options.fetch ?? fetch)(input, { ...init, headers });
      }
    : options.fetch;
  const issued = new Set<AccountHandle>();
  let epoch = 0;
  const assertCurrent = (started: number) => {
    if (started !== epoch) throw new AccountAuthError("account_logged_out");
  };
  const retain = (handle: AccountHandle) => {
    issued.add(handle);
    return handle;
  };
  if (options.restoredLocalFirstSecret !== undefined)
    parseAuthSecret(options.restoredLocalFirstSecret);
  const restored =
    options.restoredLocalFirstSecret === undefined
      ? undefined
      : options.localFirst.restore?.(options.restoredLocalFirstSecret);
  if (options.restoredLocalFirstSecret !== undefined && !restored)
    throw new AccountAuthError("local_first_restore_unavailable");
  if (options.restoredLocalFirstSecret !== undefined && options.restoredBackendUser !== undefined)
    throw new AccountAuthError("ambiguous_restored_account");
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
      async admitBackend(auth) {
        const started = epoch;
        if (!options.backend) throw new AccountAuthError("backend_host_unavailable");
        if (typeof auth?.backendSecret !== "string" || !auth.backendSecret)
          throw new AccountAuthError("invalid_backend_secret");
        if (isWhitespaceOnlySecret(auth.backendSecret))
          throw new AccountAuthError(
            "invalid_backend_secret",
            undefined,
            WHITESPACE_BACKEND_SECRET,
          );
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
      loginWithBackend: () => backendAccount(() => requestBackendSignIn(registry, backendFetch)),
      async linkWithBackend(account) {
        // The guest proves itself by signing a challenge the backend issued
        // for the signed-in user, never with its sync token: a proof links
        // once, and only to that user.
        if (account.identity.issuer !== "urn:jazz:local-first")
          throw new AccountAuthError(
            "local_first_account_required",
            undefined,
            LINK_NEEDS_GUEST_MESSAGE,
          );
        const material = credentials.get(account);
        if (!material || material.registry !== registry || !material.localFirstSecret)
          throw new AccountAuthError(
            "local_first_account_required",
            undefined,
            LINK_GUEST_KEY_MISSING_MESSAGE,
          );
        const secret = material.localFirstSecret;
        // The namespace the guest's account was founded in (its token's
        // audience: the app id, or the endpoint for an app-less client), so
        // the backend founds the same account.
        const audience = material.auth
          ? parseJwtPayload(await tokenFor(material.auth))?.aud
          : undefined;
        const linked = await backendAccount(async () => {
          const challenge = await requestBackendLinkChallenge(registry, backendFetch);
          if (credentials.get(account) !== material)
            throw new AccountAuthError("account_logged_out");
          return requestBackendSignIn(registry, backendFetch, {
            link: {
              challenge,
              proof: signChallenge(secret, challenge),
              ...(typeof audience === "string" ? { audience } : {}),
            },
          }).catch((error: unknown) => {
            if (error instanceof AccountAuthError && error.code === "link_refused")
              throw new AccountAuthError(error.code, error.status, LINK_REFUSED_MESSAGE);
            if (error instanceof AccountAuthError && error.code === "local_first_auth_disabled")
              throw new AccountAuthError(error.code, error.status, LINK_GUESTS_OFF_MESSAGE);
            throw error;
          });
        });
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
      : options.restoredBackendUser
        ? delegatedHandle(options.restoredBackendUser)
        : undefined,
    options.selectionDurability,
  );

  async function backendAccount(signIn: () => Promise<unknown>): Promise<AccountHandle> {
    const started = epoch;
    const value = await signIn();
    assertCurrent(started);
    return delegatedHandle(value);
  }

  /** A handle for a user the backend signed in, from its answer (or a remembered one). */
  function delegatedHandle(value: unknown): AccountHandle {
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
    const delegatedClaims =
      claims && typeof claims === "object" && !Array.isArray(claims)
        ? Object.freeze({ ...(claims as Record<string, unknown>) })
        : Object.freeze({});
    const handle = new EnrolledAccount(id, identity, delegatedClaims) as AccountHandle;
    credentials.set(handle, {
      registry,
      delegated: Object.freeze({
        claims: delegatedClaims,
        ...(bearer ? { bearer } : {}),
      }),
      invalidated: new Set(),
    });
    return retain(handle);
  }
}
