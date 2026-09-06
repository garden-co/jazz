import { parseJwtPayload } from "../runtime/client-session.js";
import { isPortableAuthorComponent } from "../runtime/author-id.js";
import { AccountManager, type AccountHandle, type AccountIdentity } from "./state.js";

/** A refresh callback must continue to authenticate the same exact identity. */
export type JWTAuth = string | { getToken(): Promise<string> };

interface HandleCredentials {
  registry: string;
  auth: JWTAuth;
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
function mintHandle(
  registry: string,
  id: string,
  identity: AccountIdentity,
  auth: JWTAuth,
): AccountHandle {
  const handle = Object.freeze({ id, identity: Object.freeze({ ...identity }) }) as AccountHandle;
  credentials.set(handle, { registry, auth });
  return handle;
}

/** @internal Context creation validates the opaque handle and refresh identity. */
export async function accountToken(handle: AccountHandle, registry: string): Promise<string> {
  const material = credentials.get(handle);
  if (!material || material.registry !== registry)
    throw new AccountAuthError("invalid_account_handle");
  const token = await tokenFor(material.auth);
  if (!sameIdentity(identityFromToken(token), handle.identity))
    throw new AccountAuthError("credential_identity_changed");
  return token;
}

/** @internal Public host factories prepare native crypto and persistence first. */
export function createAccountManagerWithRuntime(options: {
  /** Exact application-scoped HTTP URL ending in /accounts. */
  registry: string;
  localFirst: LocalFirstAccountFactory;
  fetch?: typeof fetch;
}): AccountManager<JWTAuth> {
  const registry = options.registry.replace(/\/$/, "");
  const issued = new Set<AccountHandle>();
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
      !result.identity ||
      !sameIdentity(result.identity, expected)
    ) {
      throw new AccountAuthError("invalid_account_response");
    }
    return retain(mintHandle(registry, result.account, expected, auth));
  };
  const enroll = async (operation: string, auth: JWTAuth): Promise<AccountHandle> => {
    const token = await tokenFor(auth);
    const identity = identityFromToken(token);
    if (identity.issuer.startsWith("urn:jazz:"))
      throw new AccountAuthError("external_identity_required");
    return readHandle(await request(operation, token), auth, identity);
  };
  return new AccountManager({
    logout() {
      for (const handle of issued) credentials.delete(handle);
      issued.clear();
    },
    createLocalFirst() {
      const local = options.localFirst.create();
      return retain(mintHandle(registry, local.accountId, local.identity, local.auth));
    },
    registerJWT: (auth) => enroll("register", auth),
    loginJWT: (auth) => enroll("login", auth),
    async linkJWT(account, auth) {
      const approvingToken = await accountToken(account, registry);
      const token = await tokenFor(auth);
      const identity = identityFromToken(token);
      if (identity.issuer.startsWith("urn:jazz:"))
        throw new AccountAuthError("external_identity_required");
      if (account.identity.issuer === "urn:jazz:local-first") {
        const registered = readHandle(
          await request("found-local-first", approvingToken),
          approvingToken,
          account.identity,
        );
        if (registered.id !== account.id) throw new AccountAuthError("founding_account_mismatch");
      }
      const intent = (await request("links/request", approvingToken, { identity })) as {
        nonce?: unknown;
      };
      if (typeof intent?.nonce !== "string") throw new AccountAuthError("invalid_link_response");
      const linked = readHandle(
        await request("links/accept", token, { nonce: intent.nonce }),
        auth,
        identity,
      );
      if (linked.id !== account.id) throw new AccountAuthError("linked_account_mismatch");
      return linked;
    },
  });
}
