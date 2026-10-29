import { afterEach, describe, expect, it, vi } from "vitest";
import { prepareAccountManager, type AccountStore } from "../accounts/persistence.js";
import { isProvisionalAccount } from "../accounts/enrollment.js";
import type { AccountHandle } from "../accounts/state.js";
import { notifyLocalWrite } from "../runtime/local-write-observer.js";
import { createJazzAppOwner, jwtAuth } from "./app.js";
import { connectAuthProvider } from "./auth-provider.js";
import { NEEDS_LOGIN_WARNING_DELAY_MS, needsLoginWarningMessage } from "./needs-login-warning.js";
import { createJazzSessionOwner, type JazzSession } from "./state.js";

const registry = "https://core.example/apps/test/accounts";
const alice = { issuer: "https://issuer.example", subject: "alice" };
const bob = { issuer: "https://issuer.example", subject: "bob" };
const aliceAccount = "00000000-0000-4000-8000-00000000000a";
const bobAccount = "00000000-0000-4000-8000-00000000000b";
const mintToken = () =>
  `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.sig`;

function jwt(identity: { issuer: string; subject: string }) {
  const exp = Math.floor(Date.now() / 1000) + 3600;
  return `e30.${btoa(JSON.stringify({ iss: identity.issuer, sub: identity.subject, exp }))}.sig`;
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
  return vi.fn(async (_input: unknown, init?: RequestInit) => {
    const bearer = String((init!.headers as Record<string, string>).Authorization).slice(7);
    const payload = JSON.parse(atob(bearer.split(".")[1]!)) as { iss: string; sub: string };
    if (revoked.has(payload.sub)) return new Response("identity_not_authorized", { status: 403 });
    const account = payload.sub === "alice" ? aliceAccount : bobAccount;
    return new Response(
      JSON.stringify({ account, identity: { issuer: payload.iss, subject: payload.sub } }),
    );
  });
}

const manager = (store: AccountStore, fetch: typeof globalThis.fetch) =>
  prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken,
    fetch,
    retainAccountAssignment: true,
  });

interface FakeClient {
  account: AccountHandle;
  /** Stands in for the client's Db: writes are reported with notifyLocalWrite(db). */
  db: object;
  shutdown(): Promise<void>;
}

/** A browser that signed in as alice earlier, reloaded, and reopened her account. */
async function reloadedBrowser(fetch: typeof globalThis.fetch = registryFetch()) {
  const store = memoryStore();
  await (await manager(store, registryFetch())).loginJWT(jwt(alice));
  await vi.waitFor(() => expect(JSON.parse(store.value!).assignment).not.toBeNull());
  return manager(store, fetch);
}

async function retainedSession(fetch: typeof globalThis.fetch = registryFetch()) {
  const accounts = await reloadedBrowser(fetch);
  return createJazzSessionOwner<FakeClient>({
    accounts,
    async openClient(account) {
      return { account, db: {}, async shutdown() {} };
    },
  });
}

/** Simulates application code writing through the open client's Db. */
function write(session: JazzSession<FakeClient>) {
  notifyLocalWrite(session.getSnapshot().client!.db);
}

const tick = async () => {
  for (let i = 0; i < 60; i++) await Promise.resolve();
};
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("needsLogin", () => {
  for (const operation of ["loginJWT", "loginOrRegisterJWT"] as const) {
    it(`is true after a reload reopens a retained account and false after ${operation}`, async () => {
      const session = await retainedSession();
      const reopened = session.getSnapshot();
      expect(reopened).toMatchObject({
        status: "ready",
        account: { id: aliceAccount, identity: alice },
        needsLogin: true,
      });

      const seen: boolean[] = [];
      session.subscribe(() => seen.push(session.getSnapshot().needsLogin!));
      await session[operation](jwt(alice));

      // Same account and client, re-admitted in place: only the flag changes.
      expect(session.getSnapshot()).toMatchObject({
        status: "ready",
        account: reopened.account,
        client: reopened.client,
        needsLogin: false,
      });
      expect(seen.at(-1)).toBe(false);
      await session.close();
    });
  }

  it("is false for an account that was just logged in or created", async () => {
    const store = memoryStore();
    const accounts = await manager(store, registryFetch());
    const session = await createJazzSessionOwner({
      accounts,
      async openClient(account: AccountHandle) {
        return { account, async shutdown() {} };
      },
    });
    expect(session.getSnapshot()).toMatchObject({ status: "signed-out", needsLogin: false });
    await session.loginJWT(jwt(alice));
    expect(session.getSnapshot()).toMatchObject({ status: "ready", needsLogin: false });
    await session.createLocalFirst();
    expect(session.getSnapshot()).toMatchObject({ status: "ready", needsLogin: false });
    await session.close();
  });

  it("is false once a login as a different identity switches accounts", async () => {
    const session = await retainedSession();
    await session.loginJWT(jwt(bob));
    expect(session.getSnapshot()).toMatchObject({
      status: "ready",
      account: { id: bobAccount },
      needsLogin: false,
    });
    await session.close();
  });

  it("stays true while the registry rejects the login, and clears on a successful retry", async () => {
    const revoked = new Set(["alice"]);
    const session = await retainedSession(registryFetch(revoked));
    await expect(session.loginJWT(jwt(alice))).rejects.toThrow("identity_not_authorized");
    expect(session.getSnapshot()).toMatchObject({ status: "ready", needsLogin: true });
    revoked.delete("alice");
    await session.retry();
    expect(session.getSnapshot()).toMatchObject({ status: "ready", needsLogin: false });
    expect(session.getSnapshot().error).toBeUndefined();
    await session.close();
  });

  it("is false after logout", async () => {
    const session = await retainedSession();
    await session.logout();
    expect(session.getSnapshot()).toMatchObject({ status: "signed-out", needsLogin: false });
    await session.close();
  });

  for (const ending of ["logout", "close"] as const) {
    it(`is false, and never flips back, when ${ending} interrupts a login in flight`, async () => {
      const gate = deferred<void>();
      const inner = registryFetch();
      const fetch = vi.fn(async (input: unknown, init?: RequestInit) => {
        await gate.promise;
        return inner(input, init);
      });
      const session = await retainedSession(fetch as unknown as typeof globalThis.fetch);
      const retained = session.getSnapshot().account!;
      const seen: (boolean | undefined)[] = [];
      session.subscribe(() => seen.push(session.getSnapshot().needsLogin));
      const login = session.loginJWT(jwt(alice));
      await vi.waitFor(() => expect(fetch).toHaveBeenCalledOnce());
      const ended = session[ending]();
      gate.resolve();
      await expect(login).rejects.toThrow();
      await ended;
      expect(session.getSnapshot()).toMatchObject({
        status: ending === "logout" ? "signed-out" : "closed",
        needsLogin: false,
      });
      expect(session.getSnapshot().account).toBeUndefined();
      // The interrupted login never bound a credential to the reopened account.
      expect(isProvisionalAccount(retained)).toBe(ending === "close");
      await tick();
      expect(seen.at(-1)).toBe(false);
      if (ending === "logout") await session.close();
    });
  }

  it("does not republish when logging in again changes nothing", async () => {
    const session = await retainedSession();
    await session.loginJWT(jwt(alice));
    const settled = session.getSnapshot();
    const notified = vi.fn();
    session.subscribe(notified);
    await session.loginOrRegisterJWT(jwt(alice));
    expect(session.getSnapshot()).toBe(settled);
    expect(notified).not.toHaveBeenCalled();
    await session.close();
  });

  it("managed auth clears it when its revalidation re-admits the account", async () => {
    const session = await retainedSession();
    const token = deferred<string>();
    const auth = connectAuthProvider(session, { getToken: () => token.promise });
    auth.update({ key: null, isPending: true });
    await tick();
    expect(session.getSnapshot().needsLogin).toBe(true);
    auth.update({ key: "alice:session-2" });
    await tick();
    expect(session.getSnapshot().needsLogin).toBe(true);
    token.resolve(jwt(alice));
    await vi.waitFor(() =>
      expect(auth.getSnapshot()).toMatchObject({ ready: true, isPending: false }),
    );
    expect(session.getSnapshot()).toMatchObject({ status: "ready", needsLogin: false });
    auth.dispose();
    await session.close();
  });
});

describe("needsLogin on the app snapshot (useJazzAuth)", () => {
  it("manual mode reports it until the app logs in through sessionActions", async () => {
    const accounts = await reloadedBrowser();
    const app = createJazzAppOwner({}, () =>
      createJazzSessionOwner<FakeClient>({
        accounts,
        async openClient(account) {
          return { account, db: {}, async shutdown() {} };
        },
      }),
    );
    await vi.waitFor(() => expect(app.getSnapshot().status).toBe("ready"));
    expect(app.getSnapshot()).toMatchObject({
      account: { id: aliceAccount },
      needsLogin: true,
    });
    const before = app.getSnapshot();
    const notified = vi.fn();
    app.subscribe(notified);

    await app.sessionActions.loginOrRegisterJWT(jwt(alice));

    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      account: before.account,
      client: before.client,
      needsLogin: false,
    });
    expect(notified).toHaveBeenCalled();
    await app.dispose();
  });

  it("managed auth clears it on the app snapshot", async () => {
    const accounts = await reloadedBrowser();
    const getToken = vi.fn(async () => jwt(alice));
    const app = createJazzAppOwner(
      { auth: jwtAuth({ key: "alice:session", getToken, logout() {} }) },
      () =>
        createJazzSessionOwner<FakeClient>({
          accounts,
          async openClient(account) {
            return { account, db: {}, async shutdown() {} };
          },
        }),
    );
    await vi.waitFor(() =>
      expect(app.getSnapshot()).toMatchObject({ status: "ready", needsLogin: false }),
    );
    expect(getToken).toHaveBeenCalled();
    await app.dispose();
  });

  it("starts false before any session exists", () => {
    const app = createJazzAppOwner({}, () => new Promise<never>(() => {}), { start: false });
    expect(app.getSnapshot()).toEqual({ status: "starting", needsLogin: false });
  });
});

describe("needsLogin development warning", () => {
  async function warnedSession(fetch?: typeof globalThis.fetch) {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const session = await retainedSession(fetch);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    return { session, warn };
  }

  it("warns once when the app writes and never starts a login", async () => {
    const { session, warn } = await warnedSession();
    const account = session.getSnapshot().account!;
    write(session);
    write(session);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS - 1);
    expect(warn).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(warn).toHaveBeenCalledOnce();
    expect(warn).toHaveBeenCalledWith(needsLoginWarningMessage(account));
    expect(warn.mock.calls[0]![0]).toMatch(/loginJWT.*loginOrRegisterJWT/);

    // Once per account: later writes stay quiet.
    write(session);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS * 2);
    expect(warn).toHaveBeenCalledOnce();
    await session.close();
  });

  it("stays silent without writes", async () => {
    const { session, warn } = await warnedSession();
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS * 4);
    expect(warn).not.toHaveBeenCalled();
    await session.close();
  });

  it("stays silent when the app logs in shortly after an early write", async () => {
    const { session, warn } = await warnedSession();
    write(session);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS / 2);
    await session.loginJWT(jwt(alice));
    expect(session.getSnapshot().needsLogin).toBe(false);
    write(session);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS * 4);
    expect(warn).not.toHaveBeenCalled();
    await session.close();
  });

  it("stays silent while a login is in flight", async () => {
    const gate = deferred<void>();
    const inner = registryFetch();
    const { session, warn } = await warnedSession(
      vi.fn(async (input: unknown, init?: RequestInit) => {
        await gate.promise;
        return inner(input, init);
      }) as unknown as typeof globalThis.fetch,
    );
    const login = session.loginJWT(jwt(alice));
    write(session);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS * 2);
    expect(warn).not.toHaveBeenCalled();
    gate.resolve();
    await login;
    expect(session.getSnapshot().needsLogin).toBe(false);
    expect(warn).not.toHaveBeenCalled();
    await session.close();
  });

  it("still warns if a login attempt fails and the app keeps writing", async () => {
    const { session, warn } = await warnedSession(registryFetch(new Set(["alice"])));
    await expect(session.loginJWT(jwt(alice))).rejects.toThrow("identity_not_authorized");
    write(session);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS);
    expect(warn).toHaveBeenCalledOnce();
    await session.close();
  });

  it("stays silent under managed auth, even while the provider has not answered", async () => {
    const { session, warn } = await warnedSession();
    const auth = connectAuthProvider(session, { getToken: () => new Promise<string>(() => {}) });
    auth.update({ key: null, isPending: true });
    write(session);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS * 4);
    expect(warn).not.toHaveBeenCalled();
    auth.dispose();
    await session.close();
  });

  it("stays silent through the managed app owner (JazzProvider auth=...)", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const accounts = await reloadedBrowser();
    const token = deferred<string>();
    const app = createJazzAppOwner(
      { auth: jwtAuth({ key: "alice:session", getToken: () => token.promise, logout() {} }) },
      () =>
        createJazzSessionOwner<FakeClient>({
          accounts,
          async openClient(account) {
            return { account, db: {}, async shutdown() {} };
          },
        }),
    );
    await vi.waitFor(() => expect(app.getSnapshot().client).toBeDefined());
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    notifyLocalWrite(app.getSnapshot().client!.db);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS * 4);
    expect(warn).not.toHaveBeenCalled();
    vi.useRealTimers();
    token.resolve(jwt(alice));
    await vi.waitFor(() => expect(app.getSnapshot().needsLogin).toBe(false));
    await app.dispose();
  });

  it("warns for a manual app owner that never logs in", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const accounts = await reloadedBrowser();
    const app = createJazzAppOwner({}, () =>
      createJazzSessionOwner<FakeClient>({
        accounts,
        async openClient(account) {
          return { account, db: {}, async shutdown() {} };
        },
      }),
    );
    await vi.waitFor(() => expect(app.getSnapshot().status).toBe("ready"));
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    notifyLocalWrite(app.getSnapshot().client!.db);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS);
    expect(warn).toHaveBeenCalledOnce();
    await app.dispose();
  });

  it("close() clears a pending warning timer", async () => {
    const { session, warn } = await warnedSession();
    write(session);
    expect(vi.getTimerCount()).toBe(1);
    await session.close();
    expect(session.getSnapshot()).toMatchObject({ status: "closed", needsLogin: false });
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS * 4);
    expect(warn).not.toHaveBeenCalled();
  });

  it("logout clears a pending warning timer", async () => {
    const { session, warn } = await warnedSession();
    write(session);
    expect(vi.getTimerCount()).toBe(1);
    await session.logout();
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS * 4);
    expect(warn).not.toHaveBeenCalled();
    await session.close();
  });

  it("stays silent once the account no longer needs a login", async () => {
    const { session, warn } = await warnedSession();
    await session.loginJWT(jwt(alice));
    write(session);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS * 4);
    expect(warn).not.toHaveBeenCalled();
    await session.close();
  });

  it("stays silent in production builds", async () => {
    vi.stubEnv("NODE_ENV", "production");
    const { session, warn } = await warnedSession();
    expect(session.getSnapshot().needsLogin).toBe(true);
    write(session);
    await vi.advanceTimersByTimeAsync(NEEDS_LOGIN_WARNING_DELAY_MS * 4);
    expect(warn).not.toHaveBeenCalled();
    await session.close();
  });
});
