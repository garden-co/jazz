import { parseAuthSecret } from "../runtime/auth-secret-codec.js";
import {
  AccountAuthError,
  requestAccountRegistry,
  readAccountAssignment,
} from "./registry-client.js";
export { AccountAuthError } from "./registry-client.js";
import { isReservedJazzIssuer, parseJwtPayload } from "../runtime/client-session.js";
import { isPortableAuthorComponent } from "../runtime/author-id.js";
import {
  AccountManager,
  AccountOperationSuperseded,
  type AccountHandle,
  type AccountIdentity,
} from "./state.js";

/** A refresh callback must continue to authenticate the same exact identity. */
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
  localFirstSecret?: string;
  /** A retained external assignment awaiting its first provider credential. */
  provisional?: boolean;
  /** The JWT the registry just accepted, offered once to the next context token request. */
  primed?: string;
  invalidated: Set<() => void>;
  bound: Set<() => void>;
}
const credentials = new WeakMap<AccountHandle, HandleCredentials>();

/** Non-secret assignment a manager may persist to reopen an external account locally. */
export interface RetainedAccountAssignment {
  readonly account: string;
  readonly issuer: string;
  readonly subject: string;
}

const ACCOUNT_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** @internal Validate a stored assignment; never trust persisted text as an identity proof. */
export function isRetainedAccountAssignment(value: unknown): value is RetainedAccountAssignment {
  if (!value || typeof value !== "object") return false;
  const { account, issuer, subject } = value as Record<string, unknown>;
  return (
    typeof account === "string" &&
    ACCOUNT_ID_PATTERN.test(account) &&
    account !== "00000000-0000-0000-0000-000000000000" &&
    typeof issuer === "string" &&
    typeof subject === "string" &&
    isPortableAuthorComponent(issuer) &&
    isPortableAuthorComponent(subject) &&
    !isReservedJazzIssuer(issuer)
  );
}

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
  auth: JWTAuth | undefined,
  localFirstSecret?: string,
): AccountHandle {
  const handle = new EnrolledAccount(id, identity) as AccountHandle;
  credentials.set(handle, {
    registry,
    auth,
    localFirstSecret,
    provisional: auth === undefined,
    invalidated: new Set(),
    bound: new Set(),
  });
  return handle;
}

function tokenStillFresh(token: string): boolean {
  const expires = parseJwtPayload(token)?.exp;
  // Leave headroom for transport admission; an expiring token is refetched.
  return typeof expires === "number" && expires * 1000 > Date.now() + 60_000;
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
  if (material.provisional) throw new AccountAuthError("account_credential_pending");
  if (material.backend || !material.auth)
    throw new AccountAuthError("backend_account_requires_backend_host");
  const primed = material.primed;
  material.primed = undefined;
  // Reuse the JWT the registry accepted moments ago instead of asking the
  // provider again; its identity was checked against this handle then.
  const token = primed && tokenStillFresh(primed) ? primed : await tokenFor(material.auth);
  if (credentials.get(handle) !== material) throw new AccountAuthError("account_logged_out");
  if (!sameIdentity(identityFromToken(token), handle.identity))
    throw new AccountAuthError("credential_identity_changed");
  return token;
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

/**
 * @internal True while a retained external handle has no provider credential.
 * Its context is open locally and presents no credential upstream until
 * revalidation binds one.
 */
export function isProvisionalAccount(handle: AccountHandle): boolean {
  return credentials.get(handle)?.provisional === true;
}

/** @internal The non-secret assignment to persist for a JWT-enrolled external handle. */
export function retainedAccountAssignment(
  handle: AccountHandle,
): RetainedAccountAssignment | undefined {
  const material = credentials.get(handle);
  if (
    !material ||
    material.backend ||
    material.localFirstSecret !== undefined ||
    isReservedJazzIssuer(handle.identity.issuer)
  )
    return undefined;
  const assignment = {
    account: handle.id,
    issuer: handle.identity.issuer,
    subject: handle.identity.subject,
  };
  return isRetainedAccountAssignment(assignment) ? assignment : undefined;
}

/** @internal Notify a context when revalidation binds a fresh credential to its handle. */
export function onAccountCredentialBound(handle: AccountHandle, listener: () => void): () => void {
  const material = credentials.get(handle);
  if (!material) return () => {};
  material.bound.add(listener);
  return () => material.bound.delete(listener);
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
  /** A retained external assignment, reopened without credentials until revalidated. */
  restoredAccount?: RetainedAccountAssignment;
  /**
   * Re-admit the selected external account in place on a same-identity
   * login. Only hosts that retain assignments across reloads opt in; others
   * keep the ordinary teardown-and-reopen transition.
   */
  revalidateInPlace?: boolean;
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
  const request = (path: string, token: string, body?: unknown) =>
    requestAccountRegistry(registry, path, token, body, options.fetch);
  const readHandle = (value: unknown, auth: JWTAuth, expected: AccountIdentity): AccountHandle =>
    retain(mintHandle(registry, readAccountAssignment(value, expected), expected, auth));
  const prime = (handle: AccountHandle, token: string) => {
    const material = credentials.get(handle);
    if (material) material.primed = token;
    return handle;
  };
  // The provider token a declined revalidation already fetched. Only the
  // enrollment that immediately follows it, for the same auth, in the same
  // logout epoch and within a few seconds, may reuse it; any other account
  // operation discards it first, so a stale identity is never re-enrolled.
  let pinned: { auth: JWTAuth; token: string; epoch: number; at: number } | undefined;
  const pin = (auth: JWTAuth, token: string) => {
    pinned = { auth, token, epoch, at: Date.now() };
  };
  const unpin = () => {
    const taken = pinned;
    pinned = undefined;
    return taken;
  };
  const enrollmentToken = (auth: JWTAuth) => {
    const taken = unpin();
    const reuse =
      taken &&
      taken.auth === auth &&
      taken.epoch === epoch &&
      Date.now() - taken.at < 10_000 &&
      tokenStillFresh(taken.token)
        ? taken.token
        : undefined;
    return reuse !== undefined ? Promise.resolve(reuse) : tokenFor(auth);
  };
  const enroll = async (operation: string, auth: JWTAuth): Promise<AccountHandle> => {
    const started = epoch;
    const token = await enrollmentToken(auth);
    assertCurrent(started);
    const identity = identityFromToken(token);
    if (isReservedJazzIssuer(identity.issuer))
      throw new AccountAuthError("external_identity_required");
    const response = await request(operation, token);
    assertCurrent(started);
    return prime(readHandle(response, auth, identity), token);
  };
  if (options.restoredLocalFirstSecret !== undefined)
    parseAuthSecret(options.restoredLocalFirstSecret);
  const restored =
    options.restoredLocalFirstSecret === undefined
      ? undefined
      : options.localFirst.restore?.(options.restoredLocalFirstSecret);
  if (options.restoredLocalFirstSecret !== undefined && !restored)
    throw new AccountAuthError("local_first_restore_unavailable");
  if (
    options.restoredAccount !== undefined &&
    (restored || !isRetainedAccountAssignment(options.restoredAccount))
  )
    throw new AccountAuthError("invalid_retained_account");
  const retainedAccount = options.restoredAccount;
  return new AccountManager(
    {
      logout() {
        unpin();
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
        unpin();
        const local = options.localFirst.create();
        return retain(
          mintHandle(registry, local.accountId, local.identity, local.auth, local.secret),
        );
      },
      restoreLocalFirst(secret) {
        unpin();
        parseAuthSecret(secret);
        const local = options.localFirst.restore?.(secret);
        if (!local) throw new AccountAuthError("local_first_restore_unavailable");
        return retain(mintHandle(registry, local.accountId, local.identity, local.auth, secret));
      },
      async becomeBackend(auth) {
        unpin();
        const started = epoch;
        if (!options.backend) throw new AccountAuthError("backend_host_unavailable");
        if (typeof auth?.backendSecret !== "string" || !auth.backendSecret)
          throw new AccountAuthError("invalid_backend_secret");
        const backendSecret = auth.backendSecret;
        const { nodeId } = await options.backend.admitBackend({ backendSecret });
        assertCurrent(started);
        if (
          !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(nodeId) ||
          nodeId === "00000000-0000-0000-0000-000000000000"
        )
          throw new AccountAuthError("invalid_backend_node");
        const handle = new EnrolledAccount("00000000-0000-0000-0000-000000000000", {
          issuer: "urn:jazz:system",
          subject: nodeId,
        }) as AccountHandle;
        credentials.set(handle, {
          registry,
          backend: Object.freeze({ backendSecret, nodeId }),
          invalidated: new Set(),
          bound: new Set(),
        });
        return retain(handle);
      },
      registerJWT: (auth) => enroll("register", auth),
      loginJWT: (auth) => enroll("login", auth),
      loginOrRegisterJWT: (auth) => enroll("login-or-register", auth),
      revalidateJWT: !options.revalidateInPlace
        ? undefined
        : async (account, operation, auth, isCurrent = () => true) => {
            unpin();
            // In-place revalidation of the selected external account. Anything
            // other than "the registry still assigns this exact identity to this
            // exact account" returns undefined so the caller performs a full,
            // ordinary transition (shutdown, enrollment, reopen).
            const material = credentials.get(account);
            if (
              !material ||
              material.registry !== registry ||
              material.backend ||
              material.localFirstSecret !== undefined ||
              isReservedJazzIssuer(account.identity.issuer)
            )
              return undefined;
            const started = epoch;
            const token = await tokenFor(auth);
            assertCurrent(started);
            const identity = identityFromToken(token);
            // A different provider subject is decided before any registry call;
            // the ordinary enrollment then reuses this token instead of asking
            // the provider again.
            if (!sameIdentity(identity, account.identity)) {
              pin(auth, token);
              return undefined;
            }
            const response = await request(
              operation === "loginJWT" ? "login" : "login-or-register",
              token,
            );
            assertCurrent(started);
            if (readAccountAssignment(response, identity) !== account.id) {
              pin(auth, token);
              return undefined;
            }
            if (credentials.get(account) !== material)
              throw new AccountAuthError("account_logged_out");
            // A logout or newer operation that started meanwhile wins: the
            // account stays unconfirmed rather than syncing as it is torn down.
            if (!isCurrent()) throw new AccountOperationSuperseded();
            material.auth = auth;
            material.provisional = false;
            material.primed = token;
            for (const listener of [...material.bound]) {
              try {
                listener();
              } catch (error) {
                console.error("Account credential listener failed", error);
              }
            }
            return account;
          },
      async linkJWT(account, auth) {
        unpin();
        const started = epoch;
        const approvingToken = await accountToken(account, registry);
        const token = await tokenFor(auth);
        assertCurrent(started);
        const identity = identityFromToken(token);
        if (isReservedJazzIssuer(identity.issuer))
          throw new AccountAuthError("external_identity_required");
        if (account.identity.issuer === "urn:jazz:local-first") {
          const response = await request("found-local-first", approvingToken);
          assertCurrent(started);
          const registered = readHandle(response, approvingToken, account.identity);
          if (registered.id !== account.id) throw new AccountAuthError("founding_account_mismatch");
        }
        const intent = (await request("links/request", approvingToken, { identity })) as {
          nonce?: unknown;
        };
        assertCurrent(started);
        if (typeof intent?.nonce !== "string") throw new AccountAuthError("invalid_link_response");
        const response = await request("links/accept", token, { nonce: intent.nonce });
        assertCurrent(started);
        const linked = readHandle(response, auth, identity);
        if (linked.id !== account.id) throw new AccountAuthError("linked_account_mismatch");
        return prime(linked, token);
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
      : retainedAccount
        ? retain(
            mintHandle(
              registry,
              retainedAccount.account,
              { issuer: retainedAccount.issuer, subject: retainedAccount.subject },
              undefined,
            ),
          )
        : undefined,
  );
}
