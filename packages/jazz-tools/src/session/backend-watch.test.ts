import { afterEach, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle, type AccountIdentity } from "../accounts/state.js";
import { AccountAuthError } from "../accounts/registry-client.js";
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
  let openFails = false;
  let openGate: Promise<void> | undefined;
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
    openClient: async (account) => {
      await openGate;
      if (openFails) throw new Error("open failed");
      return {
        db: {
          deleteClientStorage: async () => void events.push(`wipe:${account.identity.subject}`),
        },
        shutdown: async () => void events.push(`close:${account.identity.subject}`),
      };
    },
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
    failOpen: (fails: boolean) => (openFails = fails),
    /** Hold opening clients until the returned release is called. */
    holdOpen() {
      let release!: () => void;
      openGate = new Promise((resolve) => (release = resolve));
      return () => {
        openGate = undefined;
        release();
      };
    },
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

it("follows the backend at once on reauthenticate, and only reconnects on a refusal", async () => {
  const t = await setup(user("alice"));
  t.signIn(user("alice"));
  await settle();
  expect(t.probe).toHaveBeenCalledOnce();
  expect(t.loginWithBackend).not.toHaveBeenCalled();

  // The sync server refused the sign-in and the backend still names her: reconnect.
  t.watch.authRefused();
  await settle();
  expect(t.reconnect).toHaveBeenCalledOnce();
  // Refused, and the backend says no one is signed in: a refusal alone signs no one out.
  t.signIn(null);
  t.watch.authRefused();
  await settle(30);
  expect(t.session.getSnapshot().status).toBe("ready");

  // The app signed out and says so: no second answer is needed.
  await t.watch.reauthenticate();
  expect(t.session.getSnapshot().status).toBe("signed-out");
  expect(t.events).toEqual(["close:alice"]);
  // The app signed someone in.
  t.signIn(user("bob"));
  await t.watch.reauthenticate();
  expect(t.session.getSnapshot().account?.identity.subject).toBe("bob");
  t.watch.stop();
  // A stopped watch answers at once.
  await t.watch.reauthenticate();
  await t.session.close();
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

it("answers a reauthenticate with a probe made after it, not a running check's stale one", async () => {
  const t = await setup(undefined);
  await settle();
  // A focus check asks before the app signs in, and its answer arrives late.
  const stale = deferred<AccountIdentity | null>();
  t.probe.mockReturnValueOnce(stale.promise);
  t.focus();
  t.signIn(user("alice"));
  let answered = false;
  const asked = t.watch.reauthenticate().then(() => (answered = true));
  await settle();
  expect(answered).toBe(false);
  stale.resolve(null);
  await asked;
  expect(t.probe).toHaveBeenCalledTimes(3);
  expect(t.session.getSnapshot().account?.identity.subject).toBe("alice");
  expect(t.loginWithBackend).toHaveBeenCalledOnce();
  t.watch.stop();
  await t.session.close();
});

it("follows a reauthenticate asked mid-transition once the session settles", async () => {
  const t = await setup(undefined);
  await settle();
  t.signIn(user("alice"));
  const release = t.holdOpen();
  const signingIn = t.session.loginWithBackend();
  await settle();
  expect(t.session.getSnapshot().status).toBe("transitioning");
  // A focus check finishing meanwhile doesn't answer the waiting reauthenticate.
  t.focus();
  t.signIn(user("bob"));
  let answered = false;
  const asked = t.watch.reauthenticate().then(() => (answered = true));
  await settle();
  expect(answered).toBe(false);
  release();
  await signingIn;
  await asked;
  expect(t.session.getSnapshot()).toMatchObject({ status: "ready" });
  expect(t.session.getSnapshot().account?.identity.subject).toBe("bob");
  t.watch.stop();
  await t.session.close();
});

it("recovers a session left in error for the same user on reauthenticate", async () => {
  const t = await setup(undefined);
  await settle();
  t.signIn(user("alice"));
  t.failOpen(true);
  await expect(t.watch.reauthenticate()).rejects.toThrow("open failed");
  expect(t.session.getSnapshot()).toMatchObject({ status: "error" });
  expect(t.session.getSnapshot().account?.identity.subject).toBe("alice");
  t.failOpen(false);
  await t.watch.reauthenticate();
  expect(t.session.getSnapshot()).toMatchObject({ status: "ready" });
  expect(t.session.getSnapshot().account?.identity.subject).toBe("alice");
  t.watch.stop();
  await t.session.close();
});

it("rejects a reauthenticate when the backend can't be asked", async () => {
  const t = await setup(undefined);
  await settle();
  t.signIn(user("alice"));
  t.probe.mockRejectedValueOnce(new AccountAuthError("unavailable", 503));
  const failed = t.watch.reauthenticate();
  await expect(failed).rejects.toThrow("Jazz could not ask your backend who is signed in");
  await expect(failed).rejects.toMatchObject({ cause: expect.any(AccountAuthError) });
  expect(t.session.getSnapshot().status).toBe("signed-out");
  // The next one asks again.
  await t.watch.reauthenticate();
  expect(t.session.getSnapshot().account?.identity.subject).toBe("alice");
  t.watch.stop();
  await t.session.close();
});

it("answers a reauthenticate waiting on a transition when the watch stops", async () => {
  const t = await setup(undefined);
  await settle();
  t.signIn(user("alice"));
  const release = t.holdOpen();
  const signingIn = t.session.loginWithBackend();
  await settle();
  const probes = t.probe.mock.calls.length;
  const asked = t.watch.reauthenticate();
  t.watch.stop();
  await asked;
  expect(t.probe).toHaveBeenCalledTimes(probes);
  release();
  await signingIn;
  await t.session.close();
});

it("stops when there is no backend to ask", async () => {
  const t = await setup(undefined, { noBackend: true });
  await settle();
  expect(t.hasListeners()).toBe(false);
  await t.session.close();
});

it("fails running and queued reauthenticates alike when there is no backend", async () => {
  const t = await setup(undefined, { noBackend: true });
  const running = t.watch.reauthenticate();
  const queued = t.watch.reauthenticate();
  await expect(running).rejects.toThrow("Jazz could not ask your backend who is signed in");
  await expect(queued).rejects.toThrow("Jazz found no backend to ask who is signed in");
  // A stopped watch answers later ones at once.
  await t.watch.reauthenticate();
  await t.session.close();
});

it("reauthenticate signs users in and out, and clearOnSignOut wipes on a direct switch", async () => {
  let backendUser: AccountHandle | null = null;
  const events: string[] = [];
  const fetch = vi.fn(async () =>
    backendUser
      ? Response.json({ account: backendUser.id, identity: backendUser.identity })
      : new Response("signed_out", { status: 401 }),
  );
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
  await settle();
  expect(fetch).toHaveBeenCalledOnce();
  expect(following.getSnapshot().status).toBe("signed-out");

  backendUser = user("alice");
  await following.reauthenticate();
  expect(following.getSnapshot().account?.identity.subject).toBe("alice");

  backendUser = user("bob");
  await following.reauthenticate();
  expect(following.getSnapshot().account?.identity.subject).toBe("bob");
  expect(events).toEqual(["wipe:alice", "close:alice"]);

  backendUser = null;
  await following.reauthenticate();
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

it("links a guest to a user who has no account yet, however the watch finds out", async () => {
  for (const trigger of ["start", "focus", "reauthenticate"] as const) {
    let backendUser: AccountHandle | null = trigger === "start" ? user("alice") : null;
    const linkWithBackend = vi.fn(async () => user("alice"));
    const loginWithBackend = vi.fn(async () => backendUser!);
    let focus: (() => void) | undefined;
    const accounts = new AccountManager(
      { createLocalFirst: () => guest, loginWithBackend, linkWithBackend, logout() {} },
      guest,
    );
    const session = await createJazzSessionOwner({
      accounts,
      openClient: async () => ({ shutdown: async () => {} }),
    });
    const watch = watchBackendUser(
      session,
      async () => (backendUser ? { ...backendUser.identity, hasAccount: false } : null),
      {
        onFocus(check) {
          focus = check;
          return () => (focus = undefined);
        },
      },
    );
    await settle();
    backendUser = user("alice");
    if (trigger === "focus") focus?.();
    if (trigger === "reauthenticate") await watch.reauthenticate();
    await settle();
    expect(linkWithBackend, trigger).toHaveBeenCalledOnce();
    expect(loginWithBackend, trigger).not.toHaveBeenCalled();
    expect(session.getSnapshot().account?.identity.subject).toBe("alice");
    watch.stop();
    await session.close();
  }
});

it("signs a guest in as a user who already has an account, without linking", async () => {
  const linkWithBackend = vi.fn(async () => user("alice"));
  const loginWithBackend = vi.fn(async () => user("alice"));
  const session = await createJazzSessionOwner({
    accounts: new AccountManager(
      { createLocalFirst: () => guest, loginWithBackend, linkWithBackend, logout() {} },
      guest,
    ),
    openClient: async () => ({ shutdown: async () => {} }),
  });
  const watch = watchBackendUser(
    session,
    async () => ({ ...user("alice").identity, hasAccount: true }),
    { onFocus: () => () => {} },
  );
  await settle();
  expect(loginWithBackend).toHaveBeenCalledOnce();
  expect(linkWithBackend).not.toHaveBeenCalled();
  watch.stop();
  await session.close();
});

it("an app's own linkWithBackend after sign-up joins the link the watch started", async () => {
  let signedUp = false;
  let releaseLink!: () => void;
  const linkWithBackend = vi.fn(
    () => new Promise<AccountHandle>((resolve) => (releaseLink = () => resolve(user("alice")))),
  );
  const fetch = vi.fn(async (url: string) => {
    expect(url).toMatch(/\/backend-user$/);
    return signedUp
      ? Response.json({ account: null, identity: user("alice").identity })
      : new Response("signed_out", { status: 401 });
  });
  let focus: (() => void) | undefined;
  const following = await createJazzSessionOwner({
    accounts: new AccountManager(
      {
        createLocalFirst: () => guest,
        logout() {},
        loginWithBackend: async () => user("alice"),
        linkWithBackend,
      },
      guest,
    ),
    decorate: followUser({
      registry: "https://app.example.test/jazz/apps/x/accounts",
      events: {
        onFocus(check) {
          focus = check;
          return () => (focus = undefined);
        },
      },
      fetch: fetch as unknown as typeof globalThis.fetch,
    }),
    openClient: async () => ({ shutdown: async () => {} }),
  });
  await settle();
  // The sign-up's popup closes: focus returns and the watch starts the link.
  signedUp = true;
  focus?.();
  await settle();
  expect(linkWithBackend).toHaveBeenCalledOnce();
  // The app's own call after its sign-up joins it instead of failing as busy.
  const joined = following.linkWithBackend();
  releaseLink();
  await joined;
  expect(following.getSnapshot().account?.identity.subject).toBe("alice");
  // And once it is done, a late call is a no-op too.
  await following.linkWithBackend();
  expect(linkWithBackend).toHaveBeenCalledOnce();
  await following.close();
});
