import { isReservedJazzIssuer, parseJwtPayload } from "../runtime/client-session.js";
import { isPortableAuthorComponent } from "../runtime/author-id.js";
import { AccountManager, type AccountHandle, type AccountIdentity } from "./state.js";

/** A refresh callback must continue to authenticate the same exact identity. */
export type JWTAuth = string | { getToken(): Promise<string> };

interface HandleCredentials {
  registry: string;
  auth: JWTAuth;
  invalidated: Set<() => void>;
}
const credentials = new WeakMap<AccountHandle, HandleCredentials>();

export class AccountAuthError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "AccountAuthError";
  }
}

/** @internal Native key derivation stays in Rust; hosts prepare it before use. */
export interface LocalFirstAccountFactory {
  create(): { accountId: string; identity: AccountIdentity; auth: JWTAuth };
  restore?(secret: string): { accountId: string; identity: AccountIdentity; auth: JWTAuth };
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
  return typeof auth === "string" ? auth : auth.getToken();
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
): AccountHandle {
  const handle = new EnrolledAccount(id, identity) as AccountHandle;
  credentials.set(handle, { registry, auth, invalidated: new Set() });
  return handle;
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
  const request = async (path: string, token: string, body?: unknown): Promise<unknown> => {
    const response = await (options.fetch ?? globalThis.fetch)(`${registry}/${path}`, {
      method: "POST",
      credentials: "omit",
      redirect: "error",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      const code = await response.text();
      throw new AccountAuthError(/^[a-z_]{1,80}$/.test(code) ? code : "account_request_failed");
    }
    return response.json();
  };
  const readHandle = (value: unknown, auth: JWTAuth, expected: AccountIdentity): AccountHandle => {
    if (!value || typeof value !== "object") throw new AccountAuthError("invalid_account_response");
    const result = value as { account?: unknown; identity?: AccountIdentity };
    if (
      typeof result.account !== "string" ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(result.account) ||
      !result.identity ||
      !sameIdentity(result.identity, expected)
    ) {
      throw new AccountAuthError("invalid_account_response");
    }
    return retain(mintHandle(registry, result.account, expected, auth));
  };
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
        return retain(mintHandle(registry, local.accountId, local.identity, local.auth));
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
      ? retain(mintHandle(registry, restored.accountId, restored.identity, restored.auth))
      : undefined,
  );
}
