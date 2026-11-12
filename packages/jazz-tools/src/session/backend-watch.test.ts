import { afterEach, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle, type AccountIdentity } from "../accounts/state.js";
import { AccountAuthError } from "../accounts/registry-client.js";
import { browserSingleFlight, watchBackendUser, type BackendUser } from "./backend-watch.js";
import { followUser } from "./follow-user.js";
import { createJazzSessionOwner } from "./state.js";

const handle = (issuer: string, subject: string) =>
  Object.freeze({ id: `${issuer}:${subject}`, identity: { issuer, subject } }) as AccountHandle;
const guest = handle("urn:jazz:local-first", "guest");
const user = (subject: string) => handle("jazz:backend", subject);
const settle = async (ms = 0) => {
  await new Promise((resolve) => setTimeout(resolve, ms));
  for (let i = 0; i < 20; i++) await Promise.resolve();
};
afterEach(() => vi.useRealTimers());

async function setup(
  initial: AccountHandle | undefined,
  options: {
    noBackend?: boolean;
    clearOnSignOut?: boolean;
    visible?: boolean;
    forgotten?: (account: AccountHandle) => Promise<boolean>;
    onSignedOut?: () => "sign-out" | "keep" | void | Promise<"sign-out" | "keep" | void>;
  } = {},
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
  // Every state the app could render, from the first on (repeats collapse).
  const statuses: string[] = [];
  const record = () => {
    const snapshot = session.getSnapshot();
    const next = `${snapshot.status}:${snapshot.account?.identity.subject ?? "-"}`;
    if (statuses.at(-1) !== next) statuses.push(next);
  };
  record();
  session.subscribe(record);
  let focus: (() => void) | undefined;
  const posts = vi.fn();
  let otherTab: (() => void) | undefined;
  const probe = vi.fn(async (): Promise<BackendUser | null> => {
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
    {
      confirmMs: 10,
      reconnect,
      reconnectBackoffMs: 20,
      refusedConfirmMs: 30,
      notFoundRetryMs: 10,
      forgotten: options.forgotten,
      onSignedOut: options.onSignedOut,
    },
  );
  return {
    session,
    probe,
    posts,
    events,
    statuses,
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

it("signs out, and wipes with clearOnSignOut, a guest another tab linked once no one is signed in", async () => {
  // Another tab linked this guest to Kim (her sign-up) and forgot it: the
  // account is hers, so it goes when she signs out, as hers would.
  let forgotten = false;
  const t = await setup(guest, { clearOnSignOut: true, forgotten: async () => forgotten });
  await settle();
  expect(t.session.getSnapshot()).toMatchObject({ status: "ready", account: guest });

  forgotten = true;
  // One signed-out answer is confirmed first, as for a backend user.
  t.probe.mockResolvedValueOnce(null).mockResolvedValueOnce(user("kim").identity);
  t.otherTab();
  await settle(30);
  expect(t.session.getSnapshot()).toMatchObject({ status: "ready", account: guest });
  // A refused sync link alone never signs it out.
  t.watch.authRefused();
  await settle(5);
  expect(t.session.getSnapshot()).toMatchObject({ status: "ready", account: guest });

  t.otherTab();
  await settle(30);
  expect(t.session.getSnapshot()).toMatchObject({ status: "signed-out", account: undefined });
  expect(t.events).toEqual(["wipe:guest", "close:guest"]);
  t.watch.stop();
  await t.session.close();
});

it("signs out a guest another tab linked at once on reauthenticate", async () => {
  const t = await setup(guest, { forgotten: async (account) => account === guest });
  await settle();
  expect(t.session.getSnapshot()).toMatchObject({ status: "ready", account: guest });
  await t.watch.reauthenticate();
  expect(t.session.getSnapshot().status).toBe("signed-out");
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

it("asks the app's onSignedOut before a confirmed sign-out, and keeps the user when it says so", async () => {
  let decision: "sign-out" | "keep" = "keep";
  const onSignedOut = vi.fn(() => decision);
  const t = await setup(user("alice"), { clearOnSignOut: true, onSignedOut });
  t.signIn(user("alice"));
  await settle();
  // The backend says no one (a session the app's auth lost): confirmed, and
  // the app keeps the user, say because it refreshes its session itself.
  t.signIn(null);
  t.focus();
  await settle(30);
  expect(onSignedOut).toHaveBeenCalledOnce();
  expect(t.session.getSnapshot()).toMatchObject({ status: "ready" });
  expect(t.events).toEqual([]);
  // One signed-out answer that the second one doesn't confirm never asks.
  t.signIn(user("alice"));
  t.probe.mockResolvedValueOnce(null);
  t.focus();
  await settle(30);
  expect(onSignedOut).toHaveBeenCalledOnce();
  // The next confirmed answer asks again; this time the app lets go.
  t.signIn(null);
  decision = "sign-out";
  t.focus();
  await settle(30);
  expect(onSignedOut).toHaveBeenCalledTimes(2);
  expect(t.session.getSnapshot().status).toBe("signed-out");
  expect(t.events).toEqual(["wipe:alice", "close:alice"]);
  t.watch.stop();
  await t.session.close();
});

it("signs out when the app's onSignedOut throws or answers nothing", async () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => {});
  try {
    for (const onSignedOut of [
      () => {
        throw new Error("hook failed");
      },
      async () => undefined,
    ]) {
      const t = await setup(user("alice"), { onSignedOut });
      t.signIn(user("alice"));
      await settle();
      t.signIn(null);
      t.focus();
      await settle(30);
      expect(t.session.getSnapshot().status).toBe("signed-out");
      t.watch.stop();
      await t.session.close();
    }
    expect(error).toHaveBeenCalledWith(
      "Jazz: onSignedOut threw, so the session signs out.",
      expect.any(Error),
    );
  } finally {
    error.mockRestore();
  }
});

it(
  "lets a browser's windows ask the backend one at a time, sharing a fresh answer",
  { timeout: 20_000 },
  async () => {
    // Two windows of one browser: one lock manager, one broadcast channel name.
    const queue: Array<() => Promise<unknown>> = [];
    let running: Promise<unknown> | undefined;
    const locks = {
      request: (_name: string, run: () => Promise<unknown>) =>
        new Promise((resolve, reject) => {
          queue.push(() => run().then(resolve, reject));
          const pump = (): void => {
            if (running || !queue.length) return;
            running = queue.shift()!().finally(() => {
              running = undefined;
              pump();
            });
          };
          pump();
        }),
    };
    const stored = new Map<string, string>();
    vi.stubGlobal("navigator", { locks });
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => stored.get(key) ?? null,
      setItem: (key: string, value: string) => void stored.set(key, value),
      removeItem: (key: string) => void stored.delete(key),
    });
    try {
      const windowA = browserSingleFlight("jazz-account:test:probe")!;
      const windowB = browserSingleFlight("jazz-account:test:probe")!;
      let inFlight = 0;
      let most = 0;
      let probes = 0;
      const probe = async () => {
        probes += 1;
        inFlight += 1;
        most = Math.max(most, inFlight);
        await settle(20);
        inFlight -= 1;
        return { subject: "alice", n: probes };
      };
      // Both wake at once: one probe, the other window takes its answer.
      const [a, b] = await Promise.all([windowA(probe), windowB(probe)]);
      expect(most).toBe(1);
      expect(probes).toBe(1);
      expect(a).toEqual({ subject: "alice", n: 1 });
      expect(b).toEqual(a);
      // A later ask is a fresh probe, not the old answer.
      await settle(5);
      expect(await windowB(probe)).toEqual({ subject: "alice", n: 2 });
      // Who is signed in doesn't stay on the device: the answer is gone soon.
      expect(stored.size).toBe(1);
      await new Promise((resolve) => setTimeout(resolve, 10_050));
      expect(stored.size).toBe(0);
    } finally {
      vi.unstubAllGlobals();
    }
  },
);

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
  // A stopped watch follows nothing: it says so rather than answer as if it had.
  await expect(t.watch.reauthenticate()).rejects.toThrow("Jazz stopped following your backend");
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

/** Whether `promise` has settled yet. */
function tracked(promise: Promise<unknown>) {
  let done = false;
  void promise.then(
    () => (done = true),
    () => (done = true),
  );
  return () => done;
}

it("started settles once the session follows the backend's first answer", async () => {
  const t = await setup(undefined);
  t.signIn(user("alice"));
  const release = t.holdOpen();
  const started = tracked(t.watch.started);
  await settle();
  // Asked, and signing alice in: not started until the session follows.
  expect(t.probe).toHaveBeenCalledOnce();
  expect(started()).toBe(false);
  release();
  await t.watch.started;
  expect(t.session.getSnapshot().status).toBe("ready");
  expect(t.session.getSnapshot().account?.identity.subject).toBe("alice");
  t.watch.stop();
  await t.session.close();
});

it("started settles signed out when the backend names no one or can't be asked", async () => {
  const nobody = await setup(undefined);
  await nobody.watch.started;
  expect(nobody.probe).toHaveBeenCalledOnce();
  expect(nobody.session.getSnapshot().status).toBe("signed-out");
  nobody.watch.stop();
  await nobody.session.close();

  // A failed first check settles it too: it never rejects.
  const offline = await setup(undefined);
  offline.signIn(user("alice"));
  offline.probe.mockRejectedValueOnce(new TypeError("Failed to fetch"));
  await expect(offline.watch.started).resolves.toBeUndefined();
  expect(offline.session.getSnapshot().status).toBe("signed-out");
  offline.watch.stop();
  await offline.session.close();
});

it("started settles when the watch stops or finds no backend", async () => {
  const stopped = await setup(undefined);
  stopped.watch.stop();
  await stopped.watch.started;
  await settle();
  expect(stopped.probe).not.toHaveBeenCalled();
  await stopped.session.close();

  // A 404 on start settles it (the check failed), though the watch keeps
  // asking until 404s persist.
  const none = await setup(undefined, { noBackend: true });
  await none.watch.started;
  expect(none.session.getSnapshot().status).toBe("signed-out");
  expect(none.hasListeners()).toBe(true);
  await settle(60);
  expect(none.hasListeners()).toBe(false);
  await none.session.close();
});

it("started waits past a refused check for one that may sign someone in", async () => {
  const t = await setup(undefined);
  t.signIn(user("alice"));
  // A refusal still asking when the check on start comes: it only
  // reconnects, never signs in, so it doesn't answer who is signed in.
  t.probe.mockImplementationOnce(
    () => new Promise((resolve) => setTimeout(() => resolve(user("alice").identity), 5)),
  );
  t.watch.authRefused();
  await t.watch.started;
  expect(t.session.getSnapshot().status).toBe("ready");
  expect(t.session.getSnapshot().account?.identity.subject).toBe("alice");
  t.watch.stop();
  await t.session.close();
});

it("a refused check leaves a remembered user's claims refresh to the next check", async () => {
  // Reopened from this device's memory, with the claims of an earlier sign-in.
  const remembered = Object.freeze({
    ...user("alice"),
    claims: { role: "viewer" },
  }) as AccountHandle;
  const t = await setup(remembered);
  t.signIn(user("alice"));
  const answer = { ...user("alice").identity, claims: { role: "editor" } };
  // The check on start can't reach the backend.
  t.probe.mockRejectedValueOnce(new Error("offline"));
  await settle();
  expect(t.loginWithBackend).not.toHaveBeenCalled();
  // A refused check only reconnects, so it doesn't refresh the claims...
  t.probe.mockResolvedValue(answer);
  t.watch.authRefused();
  await settle();
  expect(t.reconnect).toHaveBeenCalledOnce();
  expect(t.loginWithBackend).not.toHaveBeenCalled();
  // ...and the next check still does.
  t.focus();
  await settle();
  expect(t.loginWithBackend).toHaveBeenCalledOnce();
  t.watch.stop();
  await t.session.close();
});

it("signs a remembered user in again, once, when the core lost their account", async () => {
  // Reopened from this device's memory, but the core no longer holds the
  // account (a reset dev store, a new core): sync admission refuses it.
  const t = await setup(user("alice"));
  t.signIn(user("alice"));
  t.probe.mockResolvedValue({ ...user("alice").identity, hasAccount: false });
  t.watch.authRefused();
  await settle();
  // Signing in again gives her a new account; reconnecting would only be refused.
  expect(t.loginWithBackend).toHaveBeenCalledOnce();
  expect(t.reconnect).not.toHaveBeenCalled();
  // A backend that keeps saying no account doesn't make it sign in again and again.
  t.watch.authRefused();
  await settle();
  expect(t.loginWithBackend).toHaveBeenCalledOnce();
  // Once the backend says she has one, a later loss signs her in again.
  t.watch.connected();
  t.probe.mockResolvedValue({ ...user("alice").identity, hasAccount: true });
  t.focus();
  await settle();
  t.probe.mockResolvedValue({ ...user("alice").identity, hasAccount: false });
  t.watch.authRefused();
  await settle();
  expect(t.loginWithBackend).toHaveBeenCalledTimes(2);
  expect(t.session.getSnapshot().status).toBe("ready");
  t.watch.stop();
  await t.session.close();
});

it("tries the sign-in for a lost account again when it failed", async () => {
  const t = await setup(user("alice"));
  t.signIn(user("alice"));
  t.probe.mockResolvedValue({ ...user("alice").identity, hasAccount: false });
  // The first sign-in can't reach the backend.
  t.loginWithBackend.mockRejectedValueOnce(new AccountAuthError("account_request_failed", 503));
  t.watch.authRefused();
  await settle();
  // A later check signs in again, and that one works.
  t.watch.authRefused();
  await settle();
  expect(t.loginWithBackend).toHaveBeenCalledTimes(2);
  await expect(t.loginWithBackend.mock.results[0]!.value).rejects.toThrow();
  expect(t.session.getSnapshot().status).toBe("ready");
  t.watch.stop();
  await t.session.close();
});

it("stops only once the backend keeps answering 404, asking again with backoff", async () => {
  const t = await setup(undefined, { noBackend: true });
  await settle();
  // One 404 may be a deploy or a proxy page: the watch keeps following.
  expect(t.hasListeners()).toBe(true);
  expect(t.probe).toHaveBeenCalledOnce();
  await settle(15);
  expect(t.probe).toHaveBeenCalledTimes(2);
  expect(t.hasListeners()).toBe(true);
  // Three 404s over three times the first wait: no backend to ask.
  await settle(40);
  expect(t.probe).toHaveBeenCalledTimes(3);
  expect(t.hasListeners()).toBe(false);
  await settle(50);
  expect(t.probe).toHaveBeenCalledTimes(3);
  await t.session.close();
});

it("rejects reauthenticates on a 404, and every later one once there is no backend", async () => {
  const t = await setup(undefined, { noBackend: true });
  const running = t.watch.reauthenticate();
  const queued = t.watch.reauthenticate();
  await expect(running).rejects.toThrow("Jazz could not ask your backend who is signed in");
  await expect(queued).rejects.toThrow("Jazz could not ask your backend who is signed in");
  await settle(60);
  expect(t.hasListeners()).toBe(false);
  // A watch stopped for no backend never answers as if it followed one.
  await expect(t.watch.reauthenticate()).rejects.toThrow(
    "Jazz found no backend to ask who is signed in",
  );
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

// A network blip as the watch signs a user in (a guest's first visit after
// sign-in): the watch tries again once it passes, with no focus needed.
it("tries a sign-in that failed on the network again, without waiting for focus", async () => {
  let blip = true;
  const loginWithBackend = vi.fn(async () => {
    if (blip) throw new TypeError("Failed to fetch");
    return user("alice");
  });
  const session = await createJazzSessionOwner({
    accounts: new AccountManager(
      { createLocalFirst: () => guest, loginWithBackend, logout() {} },
      guest,
    ),
    openClient: async () => ({ shutdown: async () => {} }),
  });
  let probeFails = true;
  const watch = watchBackendUser(
    session,
    async () => {
      if (probeFails) throw new TypeError("Failed to fetch");
      return { ...user("alice").identity, hasAccount: true };
    },
    { onFocus: () => () => {} },
    { retryMs: 10 },
  );
  // The backend can't be asked at first, then the sign-in itself fails.
  await settle(5);
  probeFails = false;
  await expect.poll(() => loginWithBackend.mock.calls.length).toBeGreaterThan(0);
  expect(session.getSnapshot().account).toBe(guest);
  blip = false;
  await expect.poll(() => session.getSnapshot().account?.identity.subject).toBe("alice");
  // Once it followed, nothing more is scheduled.
  const calls = loginWithBackend.mock.calls.length;
  await settle(100);
  expect(loginWithBackend).toHaveBeenCalledTimes(calls);
  watch.stop();
  await session.close();
});

it("doesn't retry a sign-in the backend refused", async () => {
  const loginWithBackend = vi.fn(async () => {
    throw new AccountAuthError("account_registry_refused", 403);
  });
  const session = await createJazzSessionOwner({
    accounts: new AccountManager(
      { createLocalFirst: () => guest, loginWithBackend, logout() {} },
      guest,
    ),
    openClient: async () => ({ shutdown: async () => {} }),
  });
  const watch = watchBackendUser(
    session,
    async () => ({ ...user("alice").identity, hasAccount: true }),
    { onFocus: () => () => {} },
    { retryMs: 10 },
  );
  await settle(100);
  expect(loginWithBackend).toHaveBeenCalledOnce();
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

/** A guest's session whose backend names `backend` (and whether they have an account). */
async function guestSetup(options: {
  linkWithBackend: () => Promise<AccountHandle>;
  loginWithBackend?: () => Promise<AccountHandle>;
  linkGuest?: boolean;
  initial?: AccountHandle;
}) {
  let backend: { user: AccountHandle; hasAccount: boolean } | null = null;
  let focus: (() => void) | undefined;
  let otherTab: (() => void) | undefined;
  const opened: string[] = [];
  const linkWithBackend = vi.fn(options.linkWithBackend);
  const loginWithBackend = vi.fn(
    options.loginWithBackend ??
      (async () => {
        if (!backend) throw new AccountAuthError("signed_out", 401);
        return backend.user;
      }),
  );
  const session = await createJazzSessionOwner({
    accounts: new AccountManager(
      { createLocalFirst: () => guest, loginWithBackend, linkWithBackend, logout() {} },
      options.initial ?? guest,
    ),
    openClient: async (account) => {
      opened.push(account.identity.subject);
      return { shutdown: async () => {} };
    },
  });
  const probe = vi.fn(async () =>
    backend ? { ...backend.user.identity, hasAccount: backend.hasAccount } : null,
  );
  const watch = watchBackendUser(
    session,
    probe,
    {
      onFocus(check) {
        focus = check;
        return () => (focus = undefined);
      },
      crossTab: {
        post() {},
        listen(check) {
          otherTab = check;
          return () => (otherTab = undefined);
        },
      },
    },
    { linkGuest: options.linkGuest },
  );
  await settle();
  return {
    session,
    watch,
    probe,
    opened,
    linkWithBackend,
    loginWithBackend,
    backendNames: (user: AccountHandle | null, hasAccount = false) =>
      (backend = user ? { user, hasAccount } : null),
    focus: () => focus?.(),
    otherTab: () => otherTab?.(),
    subject: () => session.getSnapshot().account?.identity.subject,
    async close() {
      watch.stop();
      await session.close();
    },
  };
}

it("a link that fails to reach the backend clears, rejects reauthenticate, and a retry links", async () => {
  let reachable = false;
  const t = await guestSetup({
    linkWithBackend: async () => {
      if (!reachable) throw new AccountAuthError("account_request_failed", 503);
      return user("alice");
    },
  });
  t.backendNames(user("alice"));
  await expect(t.watch.reauthenticate()).rejects.toThrow("account_request_failed");
  expect(t.watch.linking).toBeUndefined();
  expect(t.subject()).toBe("guest");
  // Not a refusal: the next check links again, and doesn't sign alice in instead.
  reachable = true;
  await t.watch.reauthenticate();
  expect(t.linkWithBackend).toHaveBeenCalledTimes(2);
  expect(t.loginWithBackend).not.toHaveBeenCalled();
  expect(t.subject()).toBe("alice");
  await t.close();
});

it("signs the user in instead, once, when the backend refuses the guest's link", async () => {
  for (const refusal of [
    new AccountAuthError("link_refused", 409),
    new AccountAuthError("invalid_local_first_proof", 401),
  ]) {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    let logins = 0;
    const t = await guestSetup({
      linkWithBackend: async () => {
        throw refusal;
      },
      // The first sign-in fails too, so the guest stays and focus keeps checking.
      loginWithBackend: async () => {
        if (logins++ === 0) throw new AccountAuthError("account_request_failed", 503);
        return user("alice");
      },
    });
    t.backendNames(user("alice"));
    t.focus();
    await settle();
    expect(t.subject(), refusal.code).toBe("guest");
    for (let i = 0; i < 3; i++) {
      t.focus();
      await settle();
    }
    // One link, never repeated for the same guest and user; the sign-in that
    // replaced it retried until it worked.
    expect(t.linkWithBackend, refusal.code).toHaveBeenCalledOnce();
    expect(t.loginWithBackend, refusal.code).toHaveBeenCalledTimes(2);
    expect(t.subject(), refusal.code).toBe("alice");
    expect(warn).toHaveBeenCalledOnce();
    warn.mockRestore();
    await t.close();
  }
});

it("reauthenticate resolves once a refused link falls back to signing in", async () => {
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const t = await guestSetup({
    linkWithBackend: async () => {
      throw new AccountAuthError("link_refused", 409);
    },
  });
  t.backendNames(user("alice"));
  await t.watch.reauthenticate();
  expect(t.subject()).toBe("alice");
  expect(t.loginWithBackend).toHaveBeenCalledOnce();
  warn.mockRestore();
  await t.close();
});

it("signs in to the account that appeared while the link ran (409)", async () => {
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  // Alice signed in elsewhere between the probe and the link: the core has
  // her account by now and refuses to give her the guest's.
  const t = await guestSetup({
    linkWithBackend: async () => {
      t.backendNames(user("alice"), true);
      throw new AccountAuthError("link_refused", 409);
    },
  });
  t.backendNames(user("alice"), false);
  await t.watch.reauthenticate();
  expect(t.subject()).toBe("alice");
  expect(t.linkWithBackend).toHaveBeenCalledOnce();
  expect(t.loginWithBackend).toHaveBeenCalledOnce();
  warn.mockRestore();
  await t.close();
});

it("keeps the guest when the user signs out while the link runs", async () => {
  const t = await guestSetup({
    linkWithBackend: async () => {
      t.backendNames(null);
      throw new AccountAuthError("signed_out", 401);
    },
  });
  t.backendNames(user("alice"));
  // The session follows the backend's newer answer: no one signed in.
  await t.watch.reauthenticate();
  expect(t.subject()).toBe("guest");
  expect(t.session.getSnapshot().status).toBe("ready");
  expect(t.loginWithBackend).not.toHaveBeenCalled();
  // Not a refusal: once alice signs in again, the guest links to her.
  t.linkWithBackend.mockImplementation(async () => user("alice"));
  t.backendNames(user("alice"));
  t.focus();
  await settle();
  expect(t.subject()).toBe("alice");
  expect(t.linkWithBackend).toHaveBeenCalledTimes(2);
  await t.close();
});

it("with linkGuest off, leaves a guest whose user has no account to the app", async () => {
  const t = await guestSetup({ linkWithBackend: async () => user("alice"), linkGuest: false });
  t.backendNames(user("alice"));
  await t.watch.reauthenticate();
  t.focus();
  await settle();
  expect(t.linkWithBackend).not.toHaveBeenCalled();
  expect(t.loginWithBackend).not.toHaveBeenCalled();
  expect(t.subject()).toBe("guest");
  // So the app's own link after its sign-up still goes through.
  await t.session.linkWithBackend();
  expect(t.subject()).toBe("alice");
  // A user who already has an account is still signed in.
  const u = await guestSetup({ linkWithBackend: async () => user("bob"), linkGuest: false });
  u.backendNames(user("bob"), true);
  await u.watch.reauthenticate();
  expect(u.loginWithBackend).toHaveBeenCalledOnce();
  expect(u.subject()).toBe("bob");
  await t.close();
  await u.close();
});

it("keeps linking, never signs in instead, when the link fails for a reason that may pass", async () => {
  for (const failure of [
    new AccountAuthError("account_registry_unavailable", 503),
    new AccountAuthError("invalid_link_challenge", 400),
    new AccountAuthError("account_request_timeout"),
  ]) {
    let fails = true;
    const t = await guestSetup({
      linkWithBackend: async () => {
        if (fails) throw failure;
        return user("alice");
      },
    });
    t.backendNames(user("alice"));
    await expect(t.watch.reauthenticate(), failure.code).rejects.toThrow(failure.code);
    t.focus();
    await settle();
    expect(t.loginWithBackend, failure.code).not.toHaveBeenCalled();
    expect(t.subject(), failure.code).toBe("guest");
    fails = false;
    t.focus();
    await settle();
    expect(t.subject(), failure.code).toBe("alice");
    await t.close();
  }
});

it("re-checks once when the link says signed out, and stops if the backend keeps disagreeing", async () => {
  // The probe keeps naming alice while every link answers signed out.
  const t = await guestSetup({
    linkWithBackend: async () => {
      throw new AccountAuthError("signed_out", 401);
    },
  });
  t.backendNames(user("alice"));
  await expect(t.watch.reauthenticate()).rejects.toThrow("signed_out");
  expect(t.linkWithBackend).toHaveBeenCalledTimes(2);
  expect(t.loginWithBackend).not.toHaveBeenCalled();
  expect(t.subject()).toBe("guest");
  await t.close();
});

it("two tabs of one guest linking at once both end on the guest's account", async () => {
  // The backend's link is idempotent for one guest and one user: the second
  // tab's link answers with the account the first one linked.
  let linked: AccountHandle | undefined;
  const guestAccount = handle("urn:jazz:local-first", "guest");
  const linkedAccount = Object.freeze({
    id: guestAccount.id,
    identity: user("alice").identity,
  }) as AccountHandle;
  const link = async () => {
    await settle();
    linked ??= linkedAccount;
    return linked;
  };
  const tabs = [
    await guestSetup({ linkWithBackend: link }),
    await guestSetup({ linkWithBackend: link }),
  ];
  for (const tab of tabs) tab.backendNames(user("alice"));
  await Promise.all(tabs.map((tab) => tab.watch.reauthenticate()));
  for (const tab of tabs) {
    expect(tab.linkWithBackend).toHaveBeenCalledOnce();
    expect(tab.loginWithBackend).not.toHaveBeenCalled();
    expect(tab.session.getSnapshot().account?.id).toBe(guestAccount.id);
  }
  // A tab that checks after the link sees alice with an account: it signs in, no second link.
  const late = await guestSetup({ linkWithBackend: link, loginWithBackend: async () => linked! });
  late.backendNames(user("alice"), true);
  late.otherTab();
  await settle();
  expect(late.linkWithBackend).not.toHaveBeenCalled();
  expect(late.session.getSnapshot().account?.id).toBe(guestAccount.id);
  for (const tab of [...tabs, late]) await tab.close();
});

it("an app's linkWithBackend a moment before the watch's own doesn't fail the watch's check", async () => {
  let release!: () => void;
  const t = await guestSetup({
    linkWithBackend: () =>
      new Promise<AccountHandle>((resolve) => (release = () => resolve(user("alice")))),
  });
  t.backendNames(user("alice"));
  // The app links first; its operation is pending before the watch's check
  // gets its answer, so the check waits instead of failing as busy.
  const own = t.session.linkWithBackend();
  expect(t.session.getSnapshot().status).toBe("transitioning");
  const asked = t.watch.reauthenticate();
  await settle();
  release();
  await own;
  await asked;
  expect(t.subject()).toBe("alice");
  expect(t.linkWithBackend).toHaveBeenCalledOnce();
  await t.close();
});

it("gives each user their own re-check when links keep answering signed out", async () => {
  const t = await guestSetup({
    linkWithBackend: async () => {
      throw new AccountAuthError("signed_out", 401);
    },
  });
  t.backendNames(user("alice"));
  await expect(t.watch.reauthenticate()).rejects.toThrow("signed_out");
  expect(t.linkWithBackend).toHaveBeenCalledTimes(2);
  // Bob signs in next, with no signed-out answer between: he gets his own re-check.
  t.backendNames(user("bob"));
  await expect(t.watch.reauthenticate()).rejects.toThrow("signed_out");
  expect(t.linkWithBackend).toHaveBeenCalledTimes(4);
  await t.close();
});

it("switches from one backend user to another in one transition, never signed-out in between", async () => {
  const t = await setup(user("alice"), { clearOnSignOut: true });
  t.signIn(user("alice"));
  await settle();
  t.signIn(user("bob"));
  t.focus();
  await settle(20);
  expect(t.statuses).toEqual(["ready:alice", "transitioning:alice", "ready:bob"]);
  // Alice is torn down as on sign-out, wipe included.
  expect(t.events).toEqual(["wipe:alice", "close:alice"]);
  // Other tabs hear of the switch once, when it is done.
  expect(t.posts).toHaveBeenCalledOnce();
  // The same through reauthenticate.
  t.signIn(user("carol"));
  await t.watch.reauthenticate();
  expect(t.statuses.slice(3)).toEqual(["transitioning:bob", "ready:carol"]);
  expect(t.posts).toHaveBeenCalledTimes(2);
  t.watch.stop();
  await t.session.close();
});

it("a switch whose sign-in fails ends signed out, never back on the first user", async () => {
  const t = await setup(user("alice"));
  t.signIn(user("alice"));
  await settle();
  t.signIn(user("bob"));
  t.loginWithBackend.mockRejectedValueOnce(new Error("backend down"));
  await expect(t.watch.reauthenticate()).rejects.toThrow("backend down");
  expect(t.statuses).toEqual(["ready:alice", "transitioning:alice", "signed-out:-"]);
  expect(t.session.getSnapshot().error?.message).toBe("backend down");
  // Asking again signs bob in.
  await t.watch.reauthenticate();
  expect(t.statuses.at(-1)).toBe("ready:bob");
  t.watch.stop();
  await t.session.close();
});

it("a switch waits for an operation still finishing, then follows the backend", async () => {
  const t = await setup(user("alice"));
  t.signIn(user("alice"));
  await settle();
  // The app retries its session while the backend now names bob.
  const release = t.holdOpen();
  const retrying = t.session.retry();
  t.signIn(user("bob"));
  const asked = t.watch.reauthenticate();
  release();
  await retrying;
  await asked;
  expect(t.session.getSnapshot().account?.identity.subject).toBe("bob");
  expect(t.statuses).not.toContain("signed-out:-");
  t.watch.stop();
  await t.session.close();
});

it("a stale sign-out confirmation does not log out a guest created meanwhile", async () => {
  const t = await setup(user("alice"), { clearOnSignOut: true });
  t.signIn(user("alice"));
  await settle();
  // The user signs out of the app's auth; a focus check sees it and waits to confirm.
  t.signIn(null);
  t.focus();
  await settle(2);
  // Meanwhile the app signs out of Jazz itself and continues as a guest.
  await t.session.logout();
  await t.session.createLocalFirst();
  await settle(30);
  expect(t.statuses).toEqual([
    "ready:alice",
    "transitioning:alice",
    "signed-out:-",
    "transitioning:-",
    "ready:guest",
  ]);
  expect(t.events).toEqual(["wipe:alice", "close:alice"]);
  t.watch.stop();
  await t.session.close();
});

it("a stale sign-out confirmation does not log out a user the app signed in meanwhile", async () => {
  const t = await setup(user("alice"));
  t.signIn(user("alice"));
  await settle();
  t.signIn(null);
  t.focus();
  await settle(2);
  // The app signs bob in itself, but its backend still says no one (its cookie lags).
  t.loginWithBackend.mockResolvedValueOnce(user("bob"));
  await t.session.loginWithBackend();
  await settle(30);
  expect(t.session.getSnapshot().account?.identity.subject).toBe("bob");
  expect(t.statuses).not.toContain("signed-out:-");
  t.watch.stop();
  await t.session.close();
});

it("a transient 404 neither stops the watch nor lets reauthenticate resolve without following", async () => {
  const t = await setup(user("alice"));
  t.signIn(user("alice"));
  t.probe.mockRejectedValueOnce(new AccountAuthError("not_found", 404));
  await settle();
  expect(t.hasListeners()).toBe(true);
  // The backend is back and names bob; the app signed bob in and asks Jazz to follow.
  t.signIn(user("bob"));
  await t.watch.reauthenticate();
  expect(t.session.getSnapshot().account?.identity.subject).toBe("bob");
  expect(t.statuses).toEqual(["ready:alice", "transitioning:alice", "ready:bob"]);
  // Focus is still followed.
  t.signIn(user("carol"));
  t.focus();
  await settle(20);
  expect(t.session.getSnapshot().account?.identity.subject).toBe("carol");
  t.watch.stop();
  await t.session.close();
});

it("a 404 asks again after a wait, and an answer in between resets the count", async () => {
  const t = await setup(user("alice"));
  t.signIn(user("alice"));
  const notFound = () => t.probe.mockRejectedValueOnce(new AccountAuthError("not_found", 404));
  notFound();
  await settle();
  expect(t.probe).toHaveBeenCalledTimes(1);
  // The retry the 404 scheduled asks again on its own.
  await settle(15);
  expect(t.probe).toHaveBeenCalledTimes(2);
  // Two 404s, an answer, then two more: never three in a row, so it keeps watching.
  notFound();
  notFound();
  t.focus();
  await settle(15);
  t.focus();
  await settle(60);
  expect(t.hasListeners()).toBe(true);
  t.signIn(user("bob"));
  await t.watch.reauthenticate();
  expect(t.session.getSnapshot().account?.identity.subject).toBe("bob");
  t.watch.stop();
  await t.session.close();
});
