import { afterEach, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle, type AccountIdentity } from "../accounts/state.js";
import { AccountAuthError } from "../accounts/registry-client.js";
import { connectAuthProvider, rememberProviderKeys } from "./auth-provider.js";
import { watchBackendUser } from "./backend-watch.js";
import { followUser } from "./follow-user.js";
import { createJazzSessionOwner } from "./state.js";

const handle = (issuer: string, subject: string) =>
  Object.freeze({ id: `${issuer}:${subject}`, identity: { issuer, subject } }) as AccountHandle;
const guest = handle("urn:jazz:local-first", "guest");
const user = (subject: string) => handle("jazz:app:test", subject);
const settle = async (ms = 0) => {
  await new Promise((resolve) => setTimeout(resolve, ms));
  for (let i = 0; i < 20; i++) await Promise.resolve();
};
afterEach(() => vi.useRealTimers());

async function setup(
  initial: AccountHandle | undefined,
  options: { noBackend?: boolean; clearOnSignOut?: boolean; visible?: boolean } = {},
) {
  let backendUser: AccountHandle | null = null;
  const events: string[] = [];
  const loginWithBackend = vi.fn(async () => {
    if (!backendUser) throw new Error("signed_out");
    return backendUser;
  });
  const accounts = new AccountManager(
    {
      createLocalFirst: () => guest,
      restoreLocalFirst: () => guest,
      loginWithBackend,
      logout() {},
    },
    initial,
  );
  const session = await createJazzSessionOwner({
    accounts,
    clearOnSignOut: options.clearOnSignOut,
    openClient: async (account) => ({
      db: {
        deleteClientStorage: async () => void events.push(`wipe:${account.identity.subject}`),
      },
      shutdown: async () => void events.push(`close:${account.identity.subject}`),
    }),
  });
  let focus: (() => void) | undefined;
  const posts = vi.fn();
  let otherTab: (() => void) | undefined;
  const probe = vi.fn(async (): Promise<AccountIdentity | null> => {
    if (options.noBackend) throw new AccountAuthError("not_found", 404);
    return backendUser?.identity ?? null;
  });
  const reconnect = vi.fn();
  const watch = watchBackendUser(
    session,
    probe,
    {
      onFocus(check) {
        focus = check;
        return () => (focus = undefined);
      },
      crossTab: {
        post: posts,
        listen(check) {
          otherTab = check;
          return () => (otherTab = undefined);
        },
      },
      isVisible: () => options.visible ?? false,
    },
    { confirmMs: 10, reconnect, reconnectBackoffMs: 20, refusedConfirmMs: 30 },
  );
  return {
    session,
    probe,
    posts,
    events,
    reconnect,
    loginWithBackend,
    watch,
    signIn: (next: AccountHandle | null) => (backendUser = next),
    focus: () => focus?.(),
    otherTab: () => otherTab?.(),
    hasListeners: () => !!focus || !!otherTab,
  };
}

it("signs in whoever the backend names on start, then follows focus and other tabs", async () => {
  const t = await setup(undefined);
  await settle();
  expect(t.probe).toHaveBeenCalledOnce();
  expect(t.session.getSnapshot().status).toBe("signed-out");

  t.signIn(user("alice"));
  t.focus();
  await settle();
  expect(t.session.getSnapshot().account?.identity.subject).toBe("alice");
  expect(t.posts).toHaveBeenCalledOnce();

  // Same user on the next check: nothing to do.
  t.focus();
  await settle();
  expect(t.loginWithBackend).toHaveBeenCalledOnce();

  // Another user: the first one is signed out on the way.
  t.signIn(user("bob"));
  t.otherTab();
  await settle();
  expect(t.session.getSnapshot().account?.identity.subject).toBe("bob");

  // Signed out, confirmed by a second answer.
  t.signIn(null);
  t.focus();
  await settle(30);
  expect(t.session.getSnapshot().status).toBe("signed-out");
  expect(t.events).toEqual(["close:alice", "close:bob"]);
  t.watch.stop();
  expect(t.hasListeners()).toBe(false);
  await t.session.close();
});

it("keeps a local-first guest when the backend says no one is signed in", async () => {
  const t = await setup(guest);
  await settle();
  expect(t.probe).toHaveBeenCalledOnce();
  expect(t.session.getSnapshot()).toMatchObject({ status: "ready", account: guest });
  t.signIn(user("alice"));
  t.focus();
  await settle();
  expect(t.session.getSnapshot().account?.identity.subject).toBe("alice");
  // The guest's storage is kept; only a backend user is signed out on a switch.
  expect(t.events).toEqual(["close:guest"]);
  t.watch.stop();
  await t.session.close();
});

it("doesn't sign out, or wipe with clearOnSignOut, on one signed-out answer", async () => {
  const t = await setup(user("alice"), { clearOnSignOut: true });
  t.signIn(user("alice"));
  await settle();
  // One refresh failure in `authenticate`, then it recovers.
  t.probe.mockResolvedValueOnce(null);
  t.focus();
  await settle(30);
  expect(t.session.getSnapshot()).toMatchObject({ status: "ready" });
  expect(t.events).toEqual([]);
  // A real sign-out is confirmed and wipes.
  t.signIn(null);
  t.focus();
  await settle(30);
  expect(t.session.getSnapshot().status).toBe("signed-out");
  expect(t.events).toEqual(["wipe:alice", "close:alice"]);
  t.watch.stop();
  await t.session.close();
});

it("stays quiet while a `user` prop or setUser drives the session, and only reconnects on a refusal", async () => {
  const t = await setup(user("alice"));
  t.signIn(user("alice"));
  const connection = connectAuthProvider(t.session);
  await settle();
  // The provider attached before the first check.
  expect(t.probe).not.toHaveBeenCalled();
  t.signIn(user("bob"));
  t.focus();
  t.otherTab();
  await settle();
  expect(t.probe).not.toHaveBeenCalled();
  expect(t.loginWithBackend).not.toHaveBeenCalled();

  // The sync server refused the sign-in and the backend still names her: reconnect.
  t.signIn(user("alice"));
  t.watch.authRefused();
  await settle();
  expect(t.reconnect).toHaveBeenCalledOnce();
  // Refused, and the backend says no one is signed in: the provider decides.
  t.signIn(null);
  t.watch.authRefused();
  await settle(30);
  expect(t.session.getSnapshot().status).toBe("ready");
  connection.dispose();
  t.watch.stop();
  await t.session.close();
});

it("stops when there is no backend to ask", async () => {
  const t = await setup(undefined, { noBackend: true });
  await settle();
  expect(t.hasListeners()).toBe(false);
  await t.session.close();
});

it("setUser signs users in and out, and clearOnSignOut wipes on a direct switch", async () => {
  let backendUser: AccountHandle | null = null;
  const events: string[] = [];
  const fetch = vi.fn(async () => new Response("signed_out", { status: 401 }));
  const following = await createJazzSessionOwner({
    accounts: new AccountManager({
      createLocalFirst: () => guest,
      logout() {},
      loginWithBackend: async () => backendUser!,
    }),
    clearOnSignOut: true,
    decorate: followUser({
      registry: "https://app.example.test/jazz/apps/x/accounts",
      events: { onFocus: () => () => {} },
      fetch: fetch as unknown as typeof globalThis.fetch,
    }),
    openClient: async (account) => ({
      db: {
        deleteClientStorage: async () => void events.push(`wipe:${account.identity.subject}`),
      },
      shutdown: async () => void events.push(`close:${account.identity.subject}`),
    }),
  });
  following.setUser(undefined);
  await settle();
  // setUser took over before the first check.
  expect(fetch).not.toHaveBeenCalled();
  expect(following.getSnapshot().status).toBe("signed-out");

  backendUser = user("alice");
  following.setUser("alice");
  await settle();
  expect(following.getSnapshot().account?.identity.subject).toBe("alice");

  backendUser = user("bob");
  following.setUser("bob");
  await settle();
  expect(following.getSnapshot().account?.identity.subject).toBe("bob");
  expect(events).toEqual(["wipe:alice", "close:alice"]);

  following.setUser(null);
  await settle();
  expect(following.getSnapshot().status).toBe("signed-out");
  expect(events).toEqual(["wipe:alice", "close:alice", "wipe:bob", "close:bob"]);
  await following.close();
});

it("signs out after a refusal only once a check after focus agrees, and backs off reconnects", async () => {
  const t = await setup(user("alice"), { clearOnSignOut: true });
  t.signIn(user("alice"));
  await settle();
  // A background tab's stale cookie: refused, but the backend still names her.
  t.watch.authRefused();
  await settle();
  expect(t.reconnect).toHaveBeenCalledTimes(1);
  // Refused again right away: the next reconnect waits.
  t.watch.authRefused();
  await settle();
  expect(t.reconnect).toHaveBeenCalledTimes(1);
  await settle(40);
  expect(t.reconnect).toHaveBeenCalledTimes(2);

  // Refused while the backend says no one: nothing yet, not even a wipe.
  t.signIn(null);
  t.watch.authRefused();
  await settle(30);
  expect(t.session.getSnapshot().status).toBe("ready");
  expect(t.events).toEqual([]);
  // Focus returns and the backend still says no one: signed out.
  t.focus();
  await settle(30);
  expect(t.session.getSnapshot().status).toBe("signed-out");
  expect(t.events).toEqual(["wipe:alice", "close:alice"]);
  t.watch.stop();
  await t.session.close();
});

it("doesn't sign a reloaded user out when the provider's id isn't the backend's", async () => {
  const restored = user("internal-42");
  let backendUser: AccountHandle = restored;
  const events: string[] = [];
  const session = await createJazzSessionOwner({
    accounts: new AccountManager(
      { createLocalFirst: () => guest, logout() {}, loginWithBackend: async () => backendUser },
      restored,
    ),
    clearOnSignOut: true,
    openClient: async (account) => ({
      db: { deleteClientStorage: async () => void events.push(`wipe:${account.identity.subject}`) },
      shutdown: async () => void events.push(`close:${account.identity.subject}`),
    }),
  });
  const connection = connectAuthProvider(session);
  connection.update({ key: "alice@example.com" });
  await settle();
  expect(session.getSnapshot().account?.identity.subject).toBe("internal-42");
  expect(events).toEqual(["close:internal-42"]);
  // A real switch between provider keys does sign out first.
  backendUser = user("internal-43");
  connection.update({ key: "bob@example.com" });
  await settle();
  expect(events).toEqual(["close:internal-42", "wipe:internal-42", "close:internal-42"]);
  connection.dispose();
  await session.close();
});

it("signs out a revoked user in a visible tab once a later check agrees", async () => {
  const t = await setup(user("alice"), { visible: true });
  t.signIn(user("alice"));
  await settle();
  t.signIn(null);
  t.watch.authRefused();
  await settle();
  // A refusal alone never signs out.
  expect(t.session.getSnapshot().status).toBe("ready");
  // 30ms later (30s by default) the page is still visible: a plain check signs out.
  await settle(80);
  expect(t.session.getSnapshot().status).toBe("signed-out");
  t.watch.stop();
  await t.session.close();
});

it("runs a focus check that arrives during a refused check as its own check", async () => {
  const t = await setup(user("alice"));
  t.signIn(user("alice"));
  await settle();
  let answer!: (identity: AccountIdentity | null) => void;
  t.probe.mockImplementationOnce(() => new Promise((resolve) => (answer = resolve)));
  t.watch.authRefused();
  await settle();
  t.signIn(null);
  t.focus();
  answer(null);
  await settle(40);
  expect(t.session.getSnapshot().status).toBe("signed-out");
  t.watch.stop();
  await t.session.close();
});

it("treats a reload whose first provider key differs from the remembered one as a switch", async () => {
  const restored = user("internal-42");
  let backendUser: AccountHandle = restored;
  const events: string[] = [];
  const session = await createJazzSessionOwner({
    accounts: new AccountManager(
      { createLocalFirst: () => guest, logout() {}, loginWithBackend: async () => backendUser },
      restored,
    ),
    clearOnSignOut: true,
    openClient: async (account) => ({
      db: { deleteClientStorage: async () => void events.push(`wipe:${account.identity.subject}`) },
      shutdown: async () => void events.push(`close:${account.identity.subject}`),
    }),
  });
  let stored: string | null | undefined = "alice@example.com";
  rememberProviderKeys(session, { get: () => stored, set: (key) => (stored = key) });
  backendUser = user("internal-43");
  const connection = connectAuthProvider(session);
  connection.update({ key: "bob@example.com" });
  await settle();
  expect(session.getSnapshot().account?.identity.subject).toBe("internal-43");
  expect(events).toEqual(["wipe:internal-42", "close:internal-42"]);
  expect(stored).toBe("bob@example.com");
  connection.dispose();
  await session.close();
});
