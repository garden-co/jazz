import { parseAuthSecret } from "../runtime/auth-secret-codec.js";
import {
  AccountAuthError,
  requestAccountRegistry,
  readAccountAssignment,
} from "./registry-client.js";
export { AccountAuthError } from "./registry-client.js";
import { isReservedJazzIssuer, parseJwtPayload } from "../runtime/client-session.js";
import { isPortableAuthorComponent } from "../runtime/author-id.js";
import { AccountManager, type AccountHandle, type AccountIdentity } from "./state.js";

/** A refresh callback must continue to authenticate the same exact identity. */
export type JWTAuth = string | { getToken(): Promise<string> };

interface HandleCredentials {
  registry: string;
  auth: JWTAuth;
  localFirstSecret?: string;
  invalidated: Set<() => void>;
}
const credentials = new WeakMap<AccountHandle, HandleCredentials>();

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
  const token = await tokenFor(material.auth);
  if (credentials.get(handle) !== material) throw new AccountAuthError("account_logged_out");
  if (!sameIdentity(identityFromToken(token), handle.identity))
    throw new AccountAuthError("credential_identity_changed");
  return token;
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
  restoredLocalFirstSecret?: string;
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
  const enroll = async (operation: string, auth: JWTAuth): Promise<AccountHandle> => {
    const started = epoch;
    const token = await tokenFor(auth);
    assertCurrent(started);
    const identity = identityFromToken(token);
    if (isReservedJazzIssuer(identity.issuer))
      throw new AccountAuthError("external_identity_required");
    const response = await request(operation, token);
    assertCurrent(started);
    return readHandle(response, auth, identity);
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
      registerJWT: (auth) => enroll("register", auth),
      loginJWT: (auth) => enroll("login", auth),
      async linkJWT(account, auth) {
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
  );
}
