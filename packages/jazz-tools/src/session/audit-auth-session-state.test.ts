// State-space audit repros for the client session / backend-watch machine.
// Each test asserts the behaviour the docs and abstractions promise; on main
// they fail where noted in /mnt/project-files/state-space-audit/auth-session.md.
import { expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle, type AccountIdentity } from "../accounts/state.js";
import { prepareAccountManager, type AccountStore } from "../accounts/persistence.js";
import { mintLocalFirstTokenInJs } from "../thin/index.js";
import { watchBackendUser } from "./backend-watch.js";
import { followUser } from "./follow-user.js";
import { createJazzSessionOwner } from "./state.js";

const handle = (issuer: string, subject: string) =>
  Object.freeze({ id: `${issuer}:${subject}`, identity: { issuer, subject } }) as AccountHandle;
const user = (subject: string) => handle("jazz:backend", subject);
const guestHandle = handle("urn:jazz:local-first", "guest");
const settle = async (ms = 0) => {
  await new Promise((resolve) => setTimeout(resolve, ms));
  for (let i = 0; i < 20; i++) await Promise.resolve();
};

async function harness(initial: AccountHandle | undefined, clearOnSignOut = false) {
  let backendUser: AccountHandle | null = null;
  const events: string[] = [];
  const accounts = new AccountManager(
    {
      createLocalFirst: () => guestHandle,
      restoreLocalFirst: () => guestHandle,
      loginWithBackend: async () => {
        if (!backendUser) throw new Error("signed_out");
        return backendUser;
      },
      logout() {},
    },
    initial,
  );
  const session = await createJazzSessionOwner({
    accounts,
    clearOnSignOut,
    openClient: async (account) => ({
      db: {
        deleteClientStorage: async () => void events.push(`wipe:${account.identity.subject}`),
      },
      shutdown: async () => void events.push(`close:${account.identity.subject}`),
    }),
  });
  let focus: (() => void) | undefined;
  const probe = vi.fn(async (): Promise<AccountIdentity | null> => backendUser?.identity ?? null);
  const watch = watchBackendUser(
    session,
    probe,
    {
      onFocus(check) {
        focus = check;
        return () => (focus = undefined);
      },
    },
    { confirmMs: 50 },
  );
  const statuses: string[] = [];
  session.subscribe(() => {
    const s = session.getSnapshot();
    statuses.push(`${s.status}:${s.account?.identity.subject ?? "-"}`);
  });
  return {
    session,
    watch,
    events,
    statuses,
    probe,
    signIn: (next: AccountHandle | null) => (backendUser = next),
    focus: () => focus?.(),
  };
}

// Finding: a backend user switch A -> B is observable as "signed-out" in between,
// so an app's signedOut screen (or a route guard redirecting to /login) flashes
// on every account switch, the same class of bug as #4768.
it("switching backend users never publishes signed-out in between", async () => {
  const t = await harness(user("alice"));
  t.signIn(user("alice"));
  await settle();
  t.statuses.length = 0;
  t.signIn(user("bob"));
  t.focus();
  await settle(20);
  expect(t.session.getSnapshot().account?.identity.subject).toBe("bob");
  expect(t.statuses).not.toContain("signed-out:-");
  t.watch.stop();
  await t.session.close();
});

// Finding: a plain check that saw "no one signed in" waits confirmMs, asks again
// and then logs out WHATEVER account the session holds by then. A guest the app
// created in that window (after its own sign-out) is logged out, and with
// clearOnSignOut its local storage is wiped.
it("a stale sign-out confirmation does not log out a guest created meanwhile", async () => {
  const t = await harness(user("alice"), true);
  t.signIn(user("alice"));
  await settle();
  // The user signs out of the app's auth; a focus check sees it and waits to confirm.
  t.signIn(null);
  t.focus();
  await settle(5);
  // Meanwhile the app signs out of Jazz itself and continues as a guest.
  await t.session.logout();
  await t.session.createLocalFirst();
  expect(t.session.getSnapshot().account?.identity.issuer).toBe("urn:jazz:local-first");
  await settle(80);
  expect(t.events).not.toContain("wipe:guest");
  expect(t.session.getSnapshot().status).toBe("ready");
  expect(t.session.getSnapshot().account?.identity.issuer).toBe("urn:jazz:local-first");
  t.watch.stop();
  await t.session.close();
});

// ---- reload with initial: "local-first" and a backend user -------------------

const ORIGIN = "https://app.example";
const SERVER = `${ORIGIN}/jazz`;
const ALICE_ACCOUNT = "11111111-1111-4111-8111-111111111111";

function memoryStore(): AccountStore & { value: () => string | null } {
  let value: string | null = null;
  return {
    value: () => value,
    read: async () => value,
    update: async (transform) => {
      value = transform(value);
    },
  };
}

function backendFetch(state: { online: boolean; user: string | null }): typeof fetch {
  return (async (input: RequestInfo | URL) => {
    if (!state.online) throw new TypeError("Failed to fetch");
    const url = String(input);
    const identity = state.user ? { issuer: "jazz:backend", subject: state.user } : undefined;
    const json = (body: unknown, status = 200) =>
      new Response(typeof body === "string" ? body : JSON.stringify(body), { status });
    if (url.endsWith("/backend-user"))
      return identity ? json({ identity, account: ALICE_ACCOUNT }) : json("signed_out", 401);
    if (url.endsWith("/backend-sign-in"))
      return identity ? json({ identity, account: ALICE_ACCOUNT }) : json("signed_out", 401);
    return json("not_found", 404);
  }) as typeof fetch;
}

/** One page load of a createJazzClient-shaped session over a persistent store. */
async function pageLoad(store: AccountStore, backend: { online: boolean; user: string | null }) {
  const fetch = backendFetch(backend);
  const accounts = await prepareAccountManager({
    appId: SERVER,
    registry: `${SERVER}/accounts`,
    store,
    mintToken: mintLocalFirstTokenInJs,
    fetch,
  });
  const opened: string[] = [];
  const session = await createJazzSessionOwner({
    accounts,
    initial: "local-first",
    openClient: async (account) => {
      opened.push(`${account.identity.issuer}|${account.identity.subject}`);
      return { shutdown: async () => {} };
    },
    decorate: followUser({
      registry: `${SERVER}/accounts`,
      events: { onFocus: () => () => {} },
      fetch,
    }),
  });
  const first = session.getSnapshot();
  return { session, first, opened };
}

// Finding: a backend user's selection is never persisted (selectionChanged sets
// selected = null), so with initial: "local-first" every reload of a signed-in
// user mints a brand-new guest root, opens a client for that empty guest
// ("ready", before the backend has answered), then switches to the user.
it("reloading a signed-in backend user with initial local-first reopens the user, not a fresh guest", async () => {
  const store = memoryStore();
  const backend = { online: true, user: "alice" as string | null };

  const load1 = await pageLoad(store, backend);
  await settle(20);
  expect(load1.session.getSnapshot().account?.identity.subject).toBe("alice");
  await load1.session.close();
  const rootsAfter1 = JSON.parse(store.value()!).roots.length;

  const load2 = await pageLoad(store, backend);
  // What the app renders before the backend answers.
  const firstIssuer = load2.first.account?.identity.issuer;
  await settle(20);
  expect(load2.session.getSnapshot().account?.identity.subject).toBe("alice");
  await load2.session.close();
  const rootsAfter2 = JSON.parse(store.value()!).roots.length;

  expect({ firstIssuer, opened: load2.opened, rootsAfter1, rootsAfter2 }).toEqual({
    firstIssuer: "jazz:backend",
    opened: [`jazz:backend|alice`],
    rootsAfter1,
    rootsAfter2: rootsAfter1,
  });
});

// Finding (same root cause): offline at load, a signed-in backend user is not
// reopened at all; the session settles "ready" as a fresh, empty guest, and
// whatever the user writes offline goes to that orphan guest account.
it("offline reload of a signed-in backend user does not land on a fresh guest", async () => {
  const store = memoryStore();
  const backend = { online: true, user: "alice" as string | null };
  const load1 = await pageLoad(store, backend);
  await settle(20);
  expect(load1.session.getSnapshot().account?.identity.subject).toBe("alice");
  await load1.session.close();

  backend.online = false;
  const load2 = await pageLoad(store, backend);
  await settle(20);
  const snapshot = load2.session.getSnapshot();
  await load2.session.close();
  // Acceptable outcomes: alice's account (offline start), or signed-out. Not a new guest.
  expect(`${snapshot.status}:${snapshot.account?.identity.issuer ?? "-"}`).not.toBe(
    "ready:urn:jazz:local-first",
  );
});

// Finding: one 404 from /backend-user (a deploy swapping routes, a CDN or
// preview-protection page) stops the watch for the page's lifetime. After that
// reauthenticate() RESOLVES without asking anyone, so the app believes the
// session followed its sign-in while it still shows the old user.
it("reauthenticate after a transient 404 does not silently resolve without following the backend", async () => {
  const t = await harness(user("alice"));
  t.signIn(user("alice"));
  let fail404 = true;
  t.probe.mockImplementation(async () => {
    if (fail404) {
      fail404 = false;
      const { AccountAuthError } = await import("../accounts/registry-client.js");
      throw new AccountAuthError("not_found", 404);
    }
    return user("bob").identity;
  });
  await settle();
  // The backend is back and now names bob; the app signed bob in and asks Jazz to follow.
  t.signIn(user("bob"));
  const outcome = await t.watch.reauthenticate().then(
    () => "resolved",
    () => "rejected",
  );
  const subject = t.session.getSnapshot().account?.identity.subject;
  t.watch.stop();
  await t.session.close();
  // Either follow bob, or tell the app it could not (reject). Not "resolved" while still alice.
  expect({ outcome, subject }).not.toEqual({ outcome: "resolved", subject: "alice" });
});
