import { it, expect, vi } from "vitest";
import { prepareAccountManager, type AccountStore } from "../accounts/persistence.js";
import { accountToken } from "../accounts/enrollment.js";
import type { AccountHandle } from "../accounts/state.js";
import { mintLocalFirstTokenInJs } from "../thin/index.js";
import { followUser } from "./follow-user.js";
import { createJazzSessionOwner } from "./state.js";

const settle = async (ms = 0) => {
  await new Promise((resolve) => setTimeout(resolve, ms));
  for (let i = 0; i < 20; i++) await Promise.resolve();
};

it("logout waits for durable selection and retry clears it without restoring an invalid handle", async () => {
  let value: string | null = null;
  let fail = false;
  const failure = new Error("storage full");
  const registry = "https://core.example/apps/test/accounts";
  const store = {
    read: async () => value,
    update: async (transform: (current: string | null) => string) => {
      if (fail) throw failure;
      value = transform(value);
    },
  };
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken: () =>
      `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.sig`,
  });
  const old = accounts.createLocalFirst();
  await accountToken(old, registry);
  const openClient = vi.fn(async () => ({ shutdown: async () => {} }));
  const session = await createJazzSessionOwner({ accounts, openClient });
  fail = true;
  await expect(session.logout()).rejects.toBe(failure);
  expect(session.getSnapshot()).toMatchObject({ status: "error", error: failure });
  expect(session.getSnapshot().account).toBeUndefined();
  expect(session.getSnapshot().client).toBeUndefined();
  expect(JSON.parse(value!).selected).toBe(0);
  expect(openClient).toHaveBeenCalledTimes(1);
  fail = false;
  await session.retry();
  expect(session.getSnapshot().status).toBe("signed-out");
  expect(JSON.parse(value!).selected).toBeNull();
  expect(openClient).toHaveBeenCalledTimes(1);
  await session.close();
});

it("retry durably retains the exact newly selected root before opening its client", async () => {
  let value: string | null = null;
  let fail = true;
  const failure = new Error("storage unavailable");
  const registry = "https://core.example/apps/test/accounts";
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    mintToken: () =>
      `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.sig`,
    store: {
      read: async () => value,
      update: async (transform) => {
        if (fail) throw failure;
        value = transform(value);
      },
    },
  });
  const openClient = vi.fn(async (account) => {
    await accountToken(account, registry);
    return { shutdown: async () => {} };
  });
  const session = await createJazzSessionOwner({ accounts, openClient });
  await expect(session.createLocalFirst()).rejects.toBe(failure);
  const selected = session.getSnapshot().account;
  expect(selected).toBeDefined();
  expect(openClient).not.toHaveBeenCalled();
  expect(value).toBeNull();
  fail = false;
  await session.retry();
  expect(session.getSnapshot().status).toBe("ready");
  expect(session.getSnapshot().account).toBe(selected);
  expect(openClient).toHaveBeenCalledExactlyOnceWith(selected);
  expect(JSON.parse(value!).roots).toHaveLength(1);
  await session.close();
});

it("backend initialization retains roots but never persists backend selection or credentials", async () => {
  let value: string | null = null;
  const registry = "https://core.example/apps/test/accounts";
  const nodeId = "00000000-0000-4000-8000-000000000001";
  const admitBackend = vi.fn(async () => ({ nodeId }));
  const config = {
    appId: "test",
    registry,
    backend: { admitBackend },
    mintToken: () =>
      `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: nodeId }))}.sig`,
    store: {
      read: async () => value,
      update: async (transform: (current: string | null) => string) => {
        value = transform(value);
      },
    },
  };
  const accounts = await prepareAccountManager(config);
  const local = accounts.createLocalFirst();
  await accountToken(local, registry);
  const shutdown = vi.fn(async () => {});
  const openClient = vi.fn(async () => ({ shutdown }));
  const session = await createJazzSessionOwner({
    accounts,
    openClient,
    backend: { backendSecret: "ephemeral-secret" },
  });
  expect(session.getSnapshot().account?.identity).toEqual({
    issuer: "urn:jazz:system",
    subject: nodeId,
  });
  expect(JSON.parse(value!).roots).toHaveLength(1);
  expect(JSON.parse(value!).selected).toBeNull();
  expect(value).not.toContain("ephemeral-secret");
  expect((await prepareAccountManager(config)).getLoggedIn()).toBeUndefined();
  // A session opens as the backend only at start; it can't switch to it later.
  expect("becomeBackend" in session).toBe(false);
  await session.close();
});

// ---- reloading a device whose selected account is a user the backend signed in ----

const ORIGIN = "https://app.example";
const SERVER = `${ORIGIN}/jazz`;
const ALICE_ACCOUNT = "11111111-1111-4111-8111-111111111111";
const BOB_ACCOUNT = "22222222-2222-4222-8222-222222222222";
type Backend = {
  online: boolean;
  user: "alice" | "bob" | null;
  claims?: Record<string, unknown>;
};

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
const rootsIn = (store: { value: () => string | null }) => JSON.parse(store.value()!).roots.length;

/** The app's backend (`createJazz` mounted at SERVER), for the routes a session asks. */
function backendFetch(backend: Backend): typeof fetch {
  return (async (input: RequestInfo | URL) => {
    if (!backend.online) throw new TypeError("Failed to fetch");
    const url = String(input);
    const json = (body: unknown, status = 200) =>
      new Response(typeof body === "string" ? body : JSON.stringify(body), { status });
    if (url.endsWith("/backend-user") || url.endsWith("/backend-sign-in")) {
      if (!backend.user) return json("signed_out", 401);
      return json({
        identity: { issuer: "jazz:backend", subject: backend.user },
        account: backend.user === "alice" ? ALICE_ACCOUNT : BOB_ACCOUNT,
        claims: backend.claims ?? {},
      });
    }
    return json("not_found", 404);
  }) as typeof fetch;
}

const label = (snapshot: { status: string; account?: AccountHandle }) =>
  `${snapshot.status}:${snapshot.account ? `${snapshot.account.identity.issuer}|${snapshot.account.identity.subject}` : "-"}`;
const ALICE = "jazz:backend|alice";
const BOB = "jazz:backend|bob";

/**
 * One page load of a `createJazzClient`-shaped session over a store that
 * outlives it. `statuses` holds every snapshot the app could render, from
 * the first one on.
 */
async function pageLoad(
  store: AccountStore,
  backend: Backend,
  initial: "local-first" | "signed-out" = "local-first",
) {
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
    initial,
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
  // Repeats of the same state collapse (logout publishes "transitioning" twice).
  const statuses = [label(session.getSnapshot())];
  session.subscribe(() => {
    const next = label(session.getSnapshot());
    if (statuses.at(-1) !== next) statuses.push(next);
  });
  return { session, statuses, opened };
}

/** Sign alice in on a fresh device (as the first load after her sign-in does). */
async function signedInDevice(backend: Backend) {
  const store = memoryStore();
  const first = await pageLoad(store, backend);
  await settle(20);
  expect(label(first.session.getSnapshot())).toBe(`ready:${ALICE}`);
  await first.session.close();
  return store;
}

it("reloading a signed-in backend user with initial local-first reopens the user, not a fresh guest", async () => {
  const backend: Backend = { online: true, user: "alice" };
  const store = await signedInDevice(backend);
  const roots = rootsIn(store);

  const reload = await pageLoad(store, backend);
  await settle(20);
  await reload.session.close();
  // Ready as alice from the first snapshot, and nothing contradicts it after.
  expect(reload.statuses).toEqual([`ready:${ALICE}`, "closed:-"]);
  expect(reload.opened).toEqual([ALICE]);
  expect(rootsIn(store)).toBe(roots);
});

it("reloading again and again never adds a guest root", async () => {
  const backend: Backend = { online: true, user: "alice" };
  const store = await signedInDevice(backend);
  const roots = rootsIn(store);
  for (let i = 0; i < 3; i++) {
    const reload = await pageLoad(store, backend);
    await settle(10);
    await reload.session.close();
    expect(reload.opened).toEqual([ALICE]);
  }
  expect(rootsIn(store)).toBe(roots);
});

it("offline reload of a signed-in backend user reopens their account, not a fresh guest", async () => {
  const backend: Backend = { online: true, user: "alice" };
  const store = await signedInDevice(backend);
  const roots = rootsIn(store);

  backend.online = false;
  const reload = await pageLoad(store, backend);
  await settle(20);
  await reload.session.close();
  expect(reload.statuses).toEqual([`ready:${ALICE}`, "closed:-"]);
  expect(reload.opened).toEqual([ALICE]);
  expect(rootsIn(store)).toBe(roots);
});

it("reopens a remembered backend user with initial signed-out too", async () => {
  const backend: Backend = { online: true, user: "alice" };
  const store = await signedInDevice(backend);
  backend.online = false;
  const reload = await pageLoad(store, backend, "signed-out");
  await settle(20);
  await reload.session.close();
  expect(reload.statuses).toEqual([`ready:${ALICE}`, "closed:-"]);
});

it("a remembered user the backend replaced switches to the new one in one transition", async () => {
  const backend: Backend = { online: true, user: "alice" };
  const store = await signedInDevice(backend);
  backend.user = "bob";
  const reload = await pageLoad(store, backend);
  await settle(20);
  await reload.session.close();
  expect(reload.statuses).toEqual([
    `ready:${ALICE}`,
    `transitioning:${ALICE}`,
    `ready:${BOB}`,
    "closed:-",
  ]);
  // Bob is remembered now.
  expect(JSON.parse(store.value()!).backendUser.identity.subject).toBe("bob");
});

it("a remembered user's claims refresh on the backend's first answer when they changed", async () => {
  const backend: Backend = { online: true, user: "alice", claims: { role: "viewer" } };
  const store = await signedInDevice(backend);

  // Unchanged claims (in any key order): nothing to refresh.
  const same = await pageLoad(store, backend);
  await settle(20);
  expect(same.statuses).toEqual([`ready:${ALICE}`]);
  expect(same.session.getSnapshot().account?.claims).toEqual({ role: "viewer" });
  await same.session.close();

  backend.claims = { role: "editor" };
  const reload = await pageLoad(store, backend);
  await settle(20);
  // Signed in again as the same user, never signed out in between.
  expect(reload.statuses).toEqual([`ready:${ALICE}`, `transitioning:${ALICE}`, `ready:${ALICE}`]);
  expect(reload.session.getSnapshot().account?.claims).toEqual({ role: "editor" });
  await reload.session.close();
  expect(JSON.parse(store.value()!).backendUser.claims).toEqual({ role: "editor" });
});

it("a remembered user the backend signed out is signed out once confirmed, and forgotten", async () => {
  const backend: Backend = { online: true, user: "alice" };
  const store = await signedInDevice(backend);
  backend.user = null;
  const reload = await pageLoad(store, backend, "signed-out");
  // followUser confirms a sign-out 2s after the first signed-out answer.
  await settle(2_200);
  await reload.session.close();
  expect(reload.statuses).toEqual([
    `ready:${ALICE}`,
    `transitioning:${ALICE}`,
    "signed-out:-",
    "closed:-",
  ]);
  expect(JSON.parse(store.value()!).backendUser).toBeUndefined();
  const next = await pageLoad(store, { online: false, user: null }, "signed-out");
  expect(next.statuses).toEqual(["signed-out:-"]);
  await next.session.close();
}, 10_000);

it("logout forgets the remembered user", async () => {
  const backend: Backend = { online: true, user: "alice" };
  const store = await signedInDevice(backend);
  const load = await pageLoad(store, backend, "signed-out");
  await load.session.logout();
  await load.session.close();
  expect(JSON.parse(store.value()!).backendUser).toBeUndefined();
  const next = await pageLoad(store, { online: false, user: null }, "signed-out");
  expect(next.statuses).toEqual(["signed-out:-"]);
  await next.session.close();
});
