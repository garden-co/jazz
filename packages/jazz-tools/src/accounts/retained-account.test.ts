import { describe, expect, it, vi } from "vitest";
import { prepareAccountManager, type AccountStore } from "./persistence.js";
import {
  accountToken,
  isProvisionalAccount,
  onAccountCredentialBound,
  retainedAccountAssignment,
} from "./enrollment.js";
import type { AccountHandle } from "./state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { connectAuthProvider } from "../session/auth-provider.js";

const registry = "https://core.example/apps/test/accounts";
const alice = { issuer: "https://issuer.example", subject: "alice" };
const bob = { issuer: "https://issuer.example", subject: "bob" };
const aliceAccount = "00000000-0000-4000-8000-00000000000a";
const bobAccount = "00000000-0000-4000-8000-00000000000b";
const mintToken = () =>
  `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.sig`;

function jwt(identity: { issuer: string; subject: string }, extra: Record<string, unknown> = {}) {
  const exp = Math.floor(Date.now() / 1000) + 3600;
  return `e30.${btoa(JSON.stringify({ iss: identity.issuer, sub: identity.subject, exp, ...extra }))}.sig`;
}

function memoryStore(): AccountStore & { value: string | null } {
  const store = {
    value: null as string | null,
    async read() {
      return store.value;
    },
    async update(transform: (current: string | null) => string) {
      store.value = transform(store.value);
    },
  };
  return store;
}

/** A registry that assigns alice and bob to fixed accounts, unless revoked. */
function registryFetch(revoked = new Set<string>()) {
  return vi.fn(async (input: unknown, init?: RequestInit) => {
    const bearer = String((init?.headers as Record<string, string>).Authorization).slice(7);
    const payload = JSON.parse(atob(bearer.split(".")[1]!)) as { iss: string; sub: string };
    if (revoked.has(payload.sub)) return new Response("identity_not_authorized", { status: 403 });
    const account = payload.sub === "alice" ? aliceAccount : bobAccount;
    expect(String(input)).toMatch(/\/(login|login-or-register|register)$/);
    return new Response(
      JSON.stringify({ account, identity: { issuer: payload.iss, subject: payload.sub } }),
    );
  });
}

async function manager(store: AccountStore, fetch: typeof globalThis.fetch) {
  return prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken,
    fetch,
    retainAccountAssignment: true,
  });
}

const tick = async () => {
  for (let i = 0; i < 60; i++) await Promise.resolve();
};

describe("retained external accounts", () => {
  it("persists only the non-secret assignment and reopens it without credentials", async () => {
    const store = memoryStore();
    const first = await manager(store, registryFetch());
    const token = jwt(alice);
    const handle = await first.loginJWT({ getToken: async () => token });
    await vi.waitFor(() =>
      expect(JSON.parse(store.value!).assignment).toEqual({
        account: aliceAccount,
        issuer: alice.issuer,
        subject: alice.subject,
      }),
    );
    expect(store.value).not.toContain(token);
    expect(retainedAccountAssignment(handle)).toEqual({
      account: aliceAccount,
      ...alice,
    });

    const restarted = await manager(store, registryFetch());
    const retained = restarted.getLoggedIn()!;
    expect(retained).toMatchObject({ id: aliceAccount, identity: alice });
    expect(isProvisionalAccount(retained)).toBe(true);
    await expect(accountToken(retained, registry)).rejects.toMatchObject({
      code: "account_credential_pending",
    });

    restarted.logout();
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).toBeNull());
    expect((await manager(store, registryFetch())).getLoggedIn()).toBeUndefined();
  });

  it("hosts that do not opt in neither persist nor restore an assignment", async () => {
    const store = memoryStore();
    const plain = () =>
      prepareAccountManager({ appId: "test", registry, store, mintToken, fetch: registryFetch() });
    await (await plain()).loginJWT(jwt(alice));
    await vi.waitFor(() => expect(store.value).not.toBeNull());
    expect(JSON.parse(store.value!).assignment).toBeNull();
    store.value = JSON.stringify({
      format: "jazz-account-selection-v1",
      roots: [],
      selected: null,
      assignment: { account: aliceAccount, ...alice },
    });
    expect((await plain()).getLoggedIn()).toBeUndefined();
  });

  it("ignores a malformed or reserved stored assignment", async () => {
    for (const assignment of [
      { account: "not-a-uuid", ...alice },
      { account: aliceAccount, issuer: "urn:jazz:local-first", subject: "alice" },
      { account: aliceAccount, issuer: alice.issuer },
    ]) {
      const store = memoryStore();
      store.value = JSON.stringify({
        format: "jazz-account-selection-v1",
        roots: [],
        selected: null,
        assignment,
      });
      expect((await manager(store, registryFetch())).getLoggedIn()).toBeUndefined();
    }
  });

  it("selecting a local-first account clears the retained assignment", async () => {
    const store = memoryStore();
    const first = await manager(store, registryFetch());
    await first.loginJWT(jwt(alice));
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
    first.createLocalFirst();
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).toBeNull());
    const restarted = await manager(store, registryFetch());
    expect(restarted.getLoggedIn()?.identity.issuer).toBe("urn:jazz:local-first");
  });

  it("revalidation binds a credential to the same handle and reuses the accepted JWT once", async () => {
    const store = memoryStore();
    await (await manager(store, registryFetch())).loginJWT(jwt(alice));
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
    const restarted = await manager(store, registryFetch());
    const retained = restarted.getLoggedIn()!;
    const bound = vi.fn();
    onAccountCredentialBound(retained, bound);
    const token = jwt(alice, { role: "editor" });
    const getToken = vi.fn(async () => token);
    await expect(restarted.revalidateJWT("loginJWT", { getToken })).resolves.toBe(retained);
    expect(restarted.getLoggedIn()).toBe(retained);
    expect(isProvisionalAccount(retained)).toBe(false);
    expect(bound).toHaveBeenCalledOnce();
    await expect(accountToken(retained, registry)).resolves.toBe(token);
    expect(getToken).toHaveBeenCalledOnce();
    await expect(accountToken(retained, registry)).resolves.toBe(token);
    expect(getToken).toHaveBeenCalledTimes(2);
  });

  it("revalidation declines a changed identity or reassigned account", async () => {
    const store = memoryStore();
    await (await manager(store, registryFetch())).loginJWT(jwt(alice));
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
    const restarted = await manager(store, registryFetch());
    const retained = restarted.getLoggedIn()!;
    await expect(restarted.revalidateJWT("loginJWT", jwt(bob))).resolves.toBeUndefined();
    expect(isProvisionalAccount(retained)).toBe(true);

    const reassigned = vi.fn(
      async () =>
        new Response(JSON.stringify({ account: bobAccount, identity: alice }), { status: 200 }),
    );
    const other = await manager(store, reassigned);
    await expect(other.revalidateJWT("loginJWT", jwt(alice))).resolves.toBeUndefined();
    expect(isProvisionalAccount(other.getLoggedIn()!)).toBe(true);
  });

  it("a manager login as the retained identity binds its credential to the same handle", async () => {
    // Hosts that drive the AccountManager themselves (no JazzSession) have
    // only the public login to give a retained account its credential.
    const store = memoryStore();
    await (await manager(store, registryFetch())).loginJWT(jwt(alice));
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
    for (const operation of ["loginJWT", "loginOrRegisterJWT"] as const) {
      const fetch = registryFetch();
      const restarted = await manager(store, fetch);
      const retained = restarted.getLoggedIn()!;
      const bound = vi.fn();
      onAccountCredentialBound(retained, bound);
      await expect(restarted[operation](jwt(alice))).resolves.toBe(retained);
      expect(restarted.getLoggedIn()).toBe(retained);
      expect(isProvisionalAccount(retained)).toBe(false);
      expect(bound).toHaveBeenCalledOnce();
      expect(fetch).toHaveBeenCalledOnce();
    }
  });

  it("a manager login as a different identity enrolls it, asking the provider once", async () => {
    const store = memoryStore();
    await (await manager(store, registryFetch())).loginJWT(jwt(alice));
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
    const fetch = registryFetch();
    const restarted = await manager(store, fetch);
    const retained = restarted.getLoggedIn()!;
    const getToken = vi.fn(async () => jwt(bob));
    const switched = await restarted.loginJWT({ getToken });
    expect(switched).not.toBe(retained);
    expect(switched).toMatchObject({ id: bobAccount, identity: bob });
    expect(isProvisionalAccount(retained)).toBe(true);
    expect(getToken).toHaveBeenCalledOnce();
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("a logout racing a manager login of the retained account wins", async () => {
    const store = memoryStore();
    await (await manager(store, registryFetch())).loginJWT(jwt(alice));
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
    const restarted = await manager(store, registryFetch());
    const bound = vi.fn();
    onAccountCredentialBound(restarted.getLoggedIn()!, bound);
    const login = restarted.loginJWT(jwt(alice));
    restarted.logout();
    await expect(login).rejects.toThrow();
    expect(restarted.getLoggedIn()).toBeUndefined();
    expect(bound).not.toHaveBeenCalled();
  });

  it("decides a changed subject before calling the registry", async () => {
    const store = memoryStore();
    await (await manager(store, registryFetch())).loginJWT(jwt(alice));
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
    const fetch = registryFetch();
    const restarted = await manager(store, fetch);
    await restarted.revalidateJWT("loginJWT", jwt(bob));
    expect(fetch).not.toHaveBeenCalled();
  });
});

describe("retained accounts in a Jazz session", () => {
  async function retainedSession(fetch = registryFetch()) {
    const store = memoryStore();
    await (await manager(store, registryFetch())).loginJWT(jwt(alice));
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
    const accounts = await manager(store, fetch);
    const events: string[] = [];
    const session = await createJazzSessionOwner({
      accounts,
      async openClient(account: AccountHandle) {
        events.push(`open:${account.identity.subject}:${isProvisionalAccount(account)}`);
        return {
          account,
          async shutdown(options?: { waitForSync?: boolean }) {
            events.push(`shutdown:${account.identity.subject}:${!!options?.waitForSync}`);
          },
        };
      },
    });
    return { session, accounts, events, store };
  }

  it("opens the retained account at once and does not tear it down for the same subject", async () => {
    const { session, events } = await retainedSession();
    const opened = session.getSnapshot();
    expect(opened).toMatchObject({ status: "ready", account: { id: aliceAccount } });
    expect(events).toEqual(["open:alice:true"]);

    await session.loginJWT(jwt(alice));
    expect(session.getSnapshot().client).toBe(opened.client);
    expect(session.getSnapshot().account).toBe(opened.account);
    expect(isProvisionalAccount(opened.account!)).toBe(false);
    // Logging in again keeps the same client as well.
    await session.loginOrRegisterJWT(jwt(alice));
    expect(session.getSnapshot().client).toBe(opened.client);
    expect(events).toEqual(["open:alice:true"]);
    await session.close();
  });

  it("switches accounts when the provider identity changed", async () => {
    const { session, events } = await retainedSession();
    await session.loginJWT(jwt(bob));
    expect(session.getSnapshot()).toMatchObject({ status: "ready", account: { id: bobAccount } });
    // The never-admitted retained client cannot reach the core; do not wait on sync.
    expect(events).toEqual(["open:alice:true", "shutdown:alice:false", "open:bob:false"]);
    await session.close();
  });

  it("keeps the retained client and reports the failure when the registry revokes it", async () => {
    const { session, events } = await retainedSession(registryFetch(new Set(["alice"])));
    const before = session.getSnapshot();
    await expect(session.loginJWT(jwt(alice))).rejects.toThrow("identity_not_authorized");
    expect(session.getSnapshot()).toMatchObject({
      status: "ready",
      client: before.client,
      error: expect.objectContaining({ message: "identity_not_authorized" }),
    });
    expect(isProvisionalAccount(before.account!)).toBe(true);
    await session.logout();
    expect(session.getSnapshot().status).toBe("signed-out");
    expect(events).toEqual(["open:alice:true", "shutdown:alice:false"]);
    await session.close();
  });

  it("an auth provider keeps the retained account ready while hydrating and revalidating", async () => {
    const { session, events } = await retainedSession();
    const token = deferred<string>();
    const auth = connectAuthProvider(session, { getToken: () => token.promise });
    auth.update({ key: null, isPending: true });
    await tick();
    expect(auth.getSnapshot()).toMatchObject({ ready: true, isPending: true });
    auth.update({ key: "alice:session-2" });
    await tick();
    expect(auth.getSnapshot()).toMatchObject({ ready: true, isPending: true });
    token.resolve(jwt(alice));
    await tick();
    await vi.waitFor(() =>
      expect(auth.getSnapshot()).toMatchObject({ ready: true, isPending: false }),
    );
    expect(events).toEqual(["open:alice:true"]);
    auth.dispose();
    await session.close();
  });

  it("asks the provider once and the registry once when the identity changed", async () => {
    const fetch = registryFetch();
    const { session, events } = await retainedSession(fetch);
    const getToken = vi.fn(async () => jwt(bob));
    await session.loginJWT({ getToken });
    expect(session.getSnapshot()).toMatchObject({ status: "ready", account: { id: bobAccount } });
    expect(getToken).toHaveBeenCalledOnce();
    expect(fetch).toHaveBeenCalledOnce();
    expect(events).toEqual(["open:alice:true", "shutdown:alice:false", "open:bob:false"]);
    await session.close();
  });

  it("stops showing the old account as ready once a different subject is known", async () => {
    const { session } = await retainedSession();
    const statuses: string[] = [];
    session.subscribe(() => statuses.push(session.getSnapshot().status));
    await session.loginJWT(jwt(bob));
    expect(statuses[0]).toBe("transitioning");
    await session.close();
  });

  it("retry after a registry rejection revalidates again instead of reopening", async () => {
    const revoked = new Set(["alice"]);
    const fetch = registryFetch(revoked);
    const { session, events } = await retainedSession(fetch);
    await expect(session.loginJWT(jwt(alice))).rejects.toThrow("identity_not_authorized");
    const failed = session.getSnapshot();

    // Still rejected: retry fails the same way and never publishes a clean "ready".
    await expect(session.retry()).rejects.toThrow("identity_not_authorized");
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(session.getSnapshot()).toMatchObject({
      status: "ready",
      client: failed.client,
      error: expect.objectContaining({ message: "identity_not_authorized" }),
    });
    expect(isProvisionalAccount(failed.account!)).toBe(true);

    revoked.delete("alice");
    await session.retry();
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(session.getSnapshot()).toMatchObject({ status: "ready", client: failed.client });
    expect(session.getSnapshot().error).toBeUndefined();
    expect(isProvisionalAccount(failed.account!)).toBe(false);
    expect(events).toEqual(["open:alice:true"]);
    await session.close();
  });

  it("retry without a failed revalidation still reopens the selected account", async () => {
    const { session, events } = await retainedSession();
    await session.retry();
    expect(session.getSnapshot()).toMatchObject({ status: "ready", account: { id: aliceAccount } });
    expect(events).toEqual(["open:alice:true", "shutdown:alice:false", "open:alice:true"]);
    await session.close();
  });

  it("a logout racing an in-place revalidation wins and never binds the account", async () => {
    const gate = deferred<void>();
    const inner = registryFetch();
    const fetch = vi.fn(async (input: unknown, init?: RequestInit) => {
      await gate.promise;
      return inner(input, init);
    });
    const { session, events, store } = await retainedSession(fetch);
    const retained = session.getSnapshot().account!;
    const login = session.loginJWT(jwt(alice));
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledOnce());
    const logout = session.logout();
    gate.resolve();
    await expect(login).rejects.toThrow();
    await logout;
    expect(session.getSnapshot().status).toBe("signed-out");
    expect(isProvisionalAccount(retained)).toBe(false);
    await expect(accountToken(retained, registry)).rejects.toThrow();
    expect(events).toEqual(["open:alice:true", "shutdown:alice:false"]);
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).toBeNull());
    await session.close();
  });

  it("never re-enrolls a token fetched by a revalidation that a logout superseded", async () => {
    const store = memoryStore();
    await (await manager(store, registryFetch())).loginJWT(jwt(alice));
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
    const accounts = await manager(store, registryFetch());
    const teardown = deferred<void>();
    const session = await createJazzSessionOwner({
      accounts,
      async openClient(account: AccountHandle) {
        return {
          account,
          async shutdown() {
            if (account.identity.subject === "alice") await teardown.promise;
          },
        };
      },
    });
    const carol = { issuer: alice.issuer, subject: "carol" };
    let provider = bob;
    const auth = { getToken: vi.fn(async () => jwt(provider)) };

    // The provider now yields bob: the in-place revalidation declines, and the
    // switch is torn down when a logout supersedes it.
    const login = session.loginJWT(auth);
    await vi.waitFor(() => expect(auth.getToken).toHaveBeenCalledOnce());
    await tick();
    const logout = session.logout();
    teardown.resolve();
    await expect(login).rejects.toThrow();
    await logout;
    expect(session.getSnapshot().status).toBe("signed-out");

    // Later the same provider signs in as carol; bob's earlier token is gone.
    provider = carol;
    await session.loginJWT(auth);
    expect(session.getSnapshot().account?.identity).toEqual(carol);
    expect(auth.getToken).toHaveBeenCalledTimes(2);
    await session.close();
  });

  it("reuses a declined revalidation's token only for the enrollment that follows", async () => {
    const store = memoryStore();
    await (await manager(store, registryFetch())).loginJWT(jwt(alice));
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
    const accounts = await manager(store, registryFetch());
    const auth = { getToken: vi.fn(async () => jwt(bob)) };
    await accounts.revalidateJWT("loginJWT", auth);
    // Any other account operation in between discards the fetched token.
    accounts.createLocalFirst();
    await accounts.loginJWT(auth);
    expect(auth.getToken).toHaveBeenCalledTimes(2);
  });

  it("an auth provider that hydrates signed out closes the retained account", async () => {
    const { session, events, store } = await retainedSession();
    const auth = connectAuthProvider(session, { getToken: async () => jwt(alice) });
    auth.update({ key: null, isPending: true });
    await tick();
    expect(auth.getSnapshot().ready).toBe(true);
    auth.update({ key: null });
    await vi.waitFor(() => expect(session.getSnapshot().status).toBe("signed-out"));
    expect(auth.getSnapshot().ready).toBe(true);
    expect(events).toEqual(["open:alice:true", "shutdown:alice:false"]);
    await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).toBeNull());
    auth.dispose();
    await session.close();
  });
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
