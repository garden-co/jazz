import { expect, it, vi } from "vitest";
import {
  accountForgotten,
  ensureAutomaticLocalFirst,
  prepareAccountManager,
} from "./persistence.js";
import { createJazzSessionOwner } from "../session/state.js";
import { formatAuthSecret } from "../runtime/auth-secret-codec.js";
import { generateAuthSecret } from "../runtime/auth-secret-store.js";
import { accountToken, exportLocalFirstSecret } from "./enrollment.js";
import type { AccountHandle } from "./state.js";
import { localFirstAccountId } from "./local-first.js";

const registry = "https://core.example/apps/test/accounts";
// Controlled crypto boundary: these tests prove storage ordering and recovery;
// native signature/subject derivation is covered by the Rust identity corpus.
const mintToken = () =>
  `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.sig`;
const mintRootToken = (secret: string) =>
  `e30.${btoa(
    JSON.stringify({
      iss: "urn:jazz:local-first",
      sub: secret.slice("jazz-auth-v1:".length),
    }),
  )}.sig`;

it("restores local selection and retains its root after logout", async () => {
  let value: string | null = null;
  const store = {
    async read() {
      return value;
    },
    async update(transform: (current: string | null) => string) {
      value = transform(value);
    },
  };
  const options = { appId: "test", registry, store, mintToken };
  const first = await prepareAccountManager(options);
  const handle = first.createLocalFirst();
  await accountToken(handle, registry);
  const restarted = await prepareAccountManager(options);
  expect(restarted.getLoggedIn()?.id).toBe(handle.id);
  const root = JSON.parse(value!).roots[0];
  restarted.logout();
  await vi.waitFor(() => expect(JSON.parse(value!).selected).toBeNull());
  const loggedOut = await prepareAccountManager(options);
  expect(loggedOut.getLoggedIn()).toBeUndefined();
  expect(JSON.parse(value!).roots).toEqual([root]);
});

it("does not release a local credential before asynchronous root persistence finishes", async () => {
  let finish!: () => void;
  const write = vi.fn(
    () =>
      new Promise<void>((resolve) => {
        finish = resolve;
      }),
  );
  const manager = await prepareAccountManager({
    appId: "test",
    registry,
    mintToken,
    store: {
      async read() {
        return null;
      },
      update: write,
    },
  });
  const handle = manager.createLocalFirst();
  const released = vi.fn();
  const pending = accountToken(handle, registry).then(released);
  await vi.waitFor(() => expect(write).toHaveBeenCalledOnce());
  expect(released).not.toHaveBeenCalled();
  finish();
  await pending;
  expect(released).toHaveBeenCalledOnce();
});

it("retains roots created by independent managers with stale inventories", async () => {
  let value: string | null = null;
  const store = {
    async read() {
      return value;
    },
    async update(transform: (current: string | null) => string) {
      value = transform(value);
    },
  };
  const options = { appId: "test", registry, store, mintToken };
  const first = await prepareAccountManager(options);
  const second = await prepareAccountManager(options);
  const a = first.createLocalFirst();
  await accountToken(a, registry);
  const firstRoot = JSON.parse(value!).roots[0];
  const b = second.createLocalFirst();
  await accountToken(b, registry);
  const afterSecond = JSON.parse(value!);
  expect(afterSecond.roots).toHaveLength(2);
  expect(afterSecond.roots[0]).toBe(firstRoot);
  expect(afterSecond.selected).toBe(1);
  first.logout();
  await vi.waitFor(() => expect(JSON.parse(value!).selected).toBeNull());
  expect(JSON.parse(value!).roots).toEqual(afterSecond.roots);
});

it("restores a retained local root without exposing it in account state", async () => {
  let value: string | null = null;
  const manager = await prepareAccountManager({
    appId: "test",
    registry,
    mintToken,
    store: {
      async read() {
        return value;
      },
      async update(transform) {
        value = transform(value);
      },
    },
  });
  const old = manager.createLocalFirst();
  const oldSecret = exportLocalFirstSecret(old);
  const replacement = generateAuthSecret();
  const restored = manager.restoreLocalFirst(replacement);
  expect(restored).not.toBe(old);
  expect(manager.getLoggedIn()).toBe(restored);
  expect(exportLocalFirstSecret(restored)).toBe(replacement);
  expect(exportLocalFirstSecret(old)).toBe(oldSecret);
  expect(JSON.stringify(manager.getSnapshot())).not.toContain(replacement);
  await accountToken(restored, registry);
  expect(JSON.parse(value!).roots).toEqual([oldSecret, replacement]);
  expect(() => manager.restoreLocalFirst("malformed recovery root")).toThrow();
  expect(manager.getLoggedIn()).toBe(restored);
  expect(() => exportLocalFirstSecret({ ...restored } as never)).toThrow(/recovery_unavailable/);
  manager.logout();
  expect(() => exportLocalFirstSecret(old)).toThrow(/recovery_unavailable/);
  expect(() => exportLocalFirstSecret(restored)).toThrow(/recovery_unavailable/);
  const recovered = manager.restoreLocalFirst(replacement);
  await accountToken(recovered, registry);
  expect(exportLocalFirstSecret(recovered)).toBe(replacement);
});

it("converges automatic sessions on one durable root before opening clients", async () => {
  const firstSecret = formatAuthSecret(new Uint8Array(32).fill(1));
  const secondSecret = formatAuthSecret(new Uint8Array(32).fill(2));
  let value: string | null = null;
  let updates = 0;
  let finish!: () => void;
  const durable = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const store = {
    async read() {
      return value;
    },
    async update(transform: (current: string | null) => string) {
      updates++;
      value = transform(value);
      await durable;
    },
  };
  const first = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken: mintRootToken,
    generateSecret: () => firstSecret,
  });
  const second = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken: mintRootToken,
    generateSecret: () => secondSecret,
  });
  const opened: string[] = [];
  const start = (accounts: typeof first) =>
    createJazzSessionOwner({
      accounts,
      initial: "local-first",
      async openClient(account) {
        opened.push(exportLocalFirstSecret(account));
        return { async shutdown() {} };
      },
    });
  const firstSession = start(first);
  const secondSession = start(second);
  await vi.waitFor(() => expect(updates).toBe(2));
  expect(opened).toEqual([]);
  finish();
  const [a, b] = await Promise.all([firstSession, secondSession]);
  expect(a.getSnapshot().account?.identity).toEqual(b.getSnapshot().account?.identity);
  expect(exportLocalFirstSecret(a.getSnapshot().account!)).toBe(
    exportLocalFirstSecret(b.getSnapshot().account!),
  );
  expect(JSON.parse(value!)).toMatchObject({
    selected: 0,
    roots: [firstSecret],
  });
});

it("does not reselect an automatic root after an interleaved selection", async () => {
  const automaticSecret = formatAuthSecret(new Uint8Array(32).fill(3));
  const explicitSecret = formatAuthSecret(new Uint8Array(32).fill(4));
  let generated = 0;
  let value: string | null = null;
  let updates = 0;
  let finish!: () => void;
  const durable = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const store = {
    async read() {
      return value;
    },
    async update(transform: (current: string | null) => string) {
      updates++;
      value = transform(value);
      if (updates === 1) await durable;
    },
  };
  const manager = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken: mintRootToken,
    generateSecret: () => (generated++ === 0 ? automaticSecret : explicitSecret),
  });
  const pending = ensureAutomaticLocalFirst(manager);
  await vi.waitFor(() => expect(updates).toBe(1));
  const explicit = manager.createLocalFirst();
  finish();
  const adopted = await pending;
  expect(adopted).toBe(explicit);
  await vi.waitFor(() => expect(JSON.parse(value!).selected).toBe(1));
  expect(JSON.parse(value!).roots).toEqual([automaticSecret, explicitSecret]);
});
it("durably clears logout racing automatic local-first adoption", async () => {
  const automaticSecret = formatAuthSecret(new Uint8Array(32).fill(5));
  let value: string | null = null;
  let updates = 0;
  let finish!: () => void;
  const durable = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const store = {
    async read() {
      return value;
    },
    async update(transform: (current: string | null) => string) {
      updates++;
      value = transform(value);
      if (updates === 1) await durable;
    },
  };
  const manager = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken: mintRootToken,
    generateSecret: () => automaticSecret,
  });
  const pending = ensureAutomaticLocalFirst(manager);
  await vi.waitFor(() => expect(updates).toBe(1));
  manager.logout();
  finish();
  await expect(pending).rejects.toThrow("superseded");
  await vi.waitFor(() => expect(JSON.parse(value!).selected).toBeNull());
  expect(JSON.parse(value!).roots).toEqual([automaticSecret]);
});

it("returns and persists a re-entrant explicit selection during automatic adoption", async () => {
  const automaticSecret = formatAuthSecret(new Uint8Array(32).fill(6));
  const explicitSecret = formatAuthSecret(new Uint8Array(32).fill(7));
  let generated = 0;
  let value: string | null = null;
  let explicit: AccountHandle | undefined;
  let reentered = false;
  const store = {
    async read() {
      return value;
    },
    async update(transform: (current: string | null) => string) {
      value = transform(value);
    },
  };
  const manager = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken: mintRootToken,
    generateSecret: () => (generated++ === 0 ? automaticSecret : explicitSecret),
  });
  manager.subscribe(() => {
    if (!reentered && manager.getLoggedIn()) {
      reentered = true;
      explicit = manager.createLocalFirst();
    }
  });
  const adopted = await ensureAutomaticLocalFirst(manager);
  expect(explicit).toBeDefined();
  expect(adopted).toBe(explicit);
  await vi.waitFor(() => expect(JSON.parse(value!).selected).toBe(1));
  expect(JSON.parse(value!).roots).toEqual([automaticSecret, explicitSecret]);
});

it("keeps the durable automatic selection when a persistence error is reported during adoption", async () => {
  const automaticSecret = formatAuthSecret(new Uint8Array(32).fill(8));
  let value: string | null = null;
  let updates = 0;
  let finish!: () => void;
  const durable = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const store = {
    async read() {
      return value;
    },
    async update(transform: (current: string | null) => string) {
      updates++;
      value = transform(value);
      if (updates === 1) await durable;
    },
  };
  const manager = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken: mintRootToken,
    generateSecret: () => automaticSecret,
  });
  const pending = ensureAutomaticLocalFirst(manager);
  await vi.waitFor(() => expect(updates).toBe(1));
  // Another tab may already have adopted selected=0 from storage at this point.
  expect(JSON.parse(value!).selected).toBe(0);
  manager.reportPersistenceError(new Error("x"));
  finish();
  const adopted = await pending;
  expect(adopted).toBe(manager.getLoggedIn());
  expect(exportLocalFirstSecret(adopted)).toBe(automaticSecret);
  expect(JSON.parse(value!)).toMatchObject({
    selected: 0,
    roots: [automaticSecret],
  });
});

// ---- the remembered backend user (`backendUser`, jazz-backend-user-v1) ----

const ACCOUNT = "11111111-1111-4111-8111-111111111111";
// Byte fixture: a store whose selection is a user the backend signed in.
const REMEMBERED_FIXTURE =
  '{"format":"jazz-account-selection-v1","roots":[],"selected":null,' +
  '"backendUser":{"format":"jazz-backend-user-v1","account":"11111111-1111-4111-8111-111111111111",' +
  '"identity":{"issuer":"jazz:backend","subject":"alice"},"claims":{"role":"editor"}}}';

function storeWith(initial: string | null) {
  let value = initial;
  return {
    value: () => value,
    read: async () => value,
    update: async (transform: (current: string | null) => string) => {
      value = transform(value);
    },
  };
}
const signInFetch = (async () =>
  Response.json({
    account: ACCOUNT,
    identity: { issuer: "jazz:backend", subject: "alice" },
    claims: { role: "editor" },
  })) as unknown as typeof fetch;

it("remembers a backend-signed-in user as jazz-backend-user-v1, with no credential", async () => {
  const store = storeWith(null);
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken,
    fetch: signInFetch,
    bearer: () => "app-bearer-token",
  });
  await accounts.loginWithBackend();
  await accounts.settleSelection();
  expect(store.value()).toBe(REMEMBERED_FIXTURE);
  expect(store.value()).not.toContain("app-bearer-token");
});

it("reopens the remembered user from the fixture bytes, and forgets them on logout", async () => {
  const store = storeWith(REMEMBERED_FIXTURE);
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken,
  });
  const remembered = accounts.getLoggedIn()!;
  expect(remembered.id).toBe(ACCOUNT);
  expect(remembered.identity).toEqual({
    issuer: "jazz:backend",
    subject: "alice",
  });
  expect(remembered.claims).toEqual({ role: "editor" });
  // Delegated, like a fresh sign-in: no token to hand out.
  await expect(accountToken(remembered, registry)).rejects.toThrow(
    "backend_signed_in_account_has_no_token",
  );
  accounts.logout();
  await accounts.settleSelection();
  // The bytes a store without a remembered user always had.
  expect(store.value()).toBe('{"format":"jazz-account-selection-v1","roots":[],"selected":null}');
});

it("a local-first selection replaces the remembered user", async () => {
  const store = storeWith(REMEMBERED_FIXTURE);
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken,
  });
  const guest = accounts.createLocalFirst();
  await accountToken(guest, registry);
  const stored = JSON.parse(store.value()!);
  expect(stored).toMatchObject({ selected: 0 });
  expect(stored.backendUser).toBeUndefined();
  expect(
    (await prepareAccountManager({ appId: "test", registry, store, mintToken })).getLoggedIn()?.id,
  ).toBe(guest.id);
});

it("an automatic local-first start keeps a remembered user instead of minting a guest", async () => {
  const store = storeWith(REMEMBERED_FIXTURE);
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken,
  });
  const selected = await ensureAutomaticLocalFirst(accounts);
  expect(selected.identity.subject).toBe("alice");
  expect(JSON.parse(store.value()!).roots).toEqual([]);
});

it("reads stores written before backendUser existed, and refuses a malformed one", async () => {
  const old = storeWith('{"format":"jazz-account-selection-v1","roots":[],"selected":null}');
  expect(
    (
      await prepareAccountManager({
        appId: "test",
        registry,
        store: old,
        mintToken,
      })
    ).getLoggedIn(),
  ).toBeUndefined();
  for (const backendUser of [
    {
      format: "jazz-backend-user-v1",
      account: ACCOUNT,
      identity: { issuer: "a" },
      claims: {},
    },
    {
      format: "jazz-backend-user-v1",
      account: "not-an-account",
      identity: { issuer: "a", subject: "b" },
      claims: {},
    },
  ]) {
    const store = storeWith(
      JSON.stringify({
        format: "jazz-account-selection-v1",
        roots: [],
        selected: null,
        backendUser,
      }),
    );
    await expect(
      prepareAccountManager({ appId: "test", registry, store, mintToken }),
    ).rejects.toThrow();
  }
});

it("treats another version's remembered user as absent, and keeps working", async () => {
  // A newer bundle in another tab, or a rollback: the record is a cache.
  const store = storeWith(
    JSON.stringify({
      format: "jazz-account-selection-v1",
      roots: [],
      selected: null,
      backendUser: { format: "jazz-backend-user-v2", user: "alice" },
    }),
  );
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken,
    fetch: signInFetch,
  });
  expect(accounts.getLoggedIn()).toBeUndefined();
  await accounts.loginWithBackend();
  await accounts.settleSelection();
  expect(store.value()).toBe(REMEMBERED_FIXTURE);
});

it("never remembers the backend user with rememberBackendUser: false (clearOnSignOut)", async () => {
  const store = storeWith(REMEMBERED_FIXTURE);
  const options = {
    appId: "test",
    registry,
    store,
    mintToken,
    fetch: signInFetch,
    rememberBackendUser: false,
  };
  const accounts = await prepareAccountManager(options);
  // A record from before the app turned clearOnSignOut on is not reopened.
  expect(accounts.getLoggedIn()).toBeUndefined();
  await accounts.loginWithBackend();
  await accounts.settleSelection();
  expect(JSON.parse(store.value()!).backendUser).toBeUndefined();
  expect((await prepareAccountManager(options)).getLoggedIn()).toBeUndefined();
});

// A shared device (clearOnSignOut): Kim starts as a guest, signs up (the
// backend links her guest), and signs out. Her guest root must not stay on
// the device, or the next person reopens her account with it.
function linkFetch(account: string) {
  return (async (input: unknown) =>
    String(input).endsWith("/backend-link/challenge")
      ? Response.json({ challenge: "issued-for-kim.mac", expiresAt: 0 })
      : Response.json({
          account,
          identity: { issuer: "jazz:backend", subject: "kim" },
          claims: {},
        })) as unknown as typeof fetch;
}

const guestRoot = (n: number) => formatAuthSecret(new Uint8Array(32).fill(n)) as string;
const accountOf = (root: string) => localFirstAccountId("test", root.slice("jazz-auth-v1:".length));
/** A shared-device manager (clearOnSignOut) whose next guest gets `root`. */
function sharedDevice(store: ReturnType<typeof storeWith>, root: string, linksTo = root) {
  return prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken: mintRootToken,
    generateSecret: () => root,
    fetch: linkFetch(accountOf(linksTo)),
    rememberBackendUser: false,
  });
}

it("forgets a guest's root once it is linked to a user, with rememberBackendUser: false (clearOnSignOut)", async () => {
  const store = storeWith(null);
  const kim = guestRoot(1);
  const accounts = await sharedDevice(store, kim);
  accounts.createLocalFirst();
  await accounts.settleSelection();
  expect(JSON.parse(store.value()!).roots).toEqual([kim]);

  await accounts.linkWithBackend();
  await accounts.settleSelection();
  expect(JSON.parse(store.value()!)).toMatchObject({
    roots: [],
    selected: null,
    forgottenAccounts: [accountOf(kim)],
  });
  accounts.logout();
  await accounts.settleSelection();
  // Nothing on the device holds her root: not the roots, not the tombstone.
  expect(store.value()).not.toContain(kim);
  const next = await sharedDevice(store, guestRoot(2));
  expect(next.getLoggedIn()).toBeUndefined();
  expect((await ensureAutomaticLocalFirst(next)).id).toBe(accountOf(guestRoot(2)));
  expect(store.value()).not.toContain(kim);
});

it("a tab that loaded the guest before the link cannot write its root back", async () => {
  const store = storeWith(null);
  const kim = guestRoot(1);
  const tabA = await sharedDevice(store, kim);
  tabA.createLocalFirst();
  await tabA.settleSelection();
  // Tab B opened as the same guest, before Kim signs up in tab A.
  const tabB = await sharedDevice(store, guestRoot(2), guestRoot(3));
  expect(tabB.getLoggedIn()?.id).toBe(accountOf(kim));

  await tabA.linkWithBackend();
  tabA.logout();
  await tabA.settleSelection();
  expect(store.value()).not.toContain(kim);

  // The next person continues as a guest in tab B, which still holds Kim's
  // root in its snapshot. Its write must not bring the root back.
  tabB.createLocalFirst();
  await tabB.settleSelection();
  expect(JSON.parse(store.value()!)).toMatchObject({
    roots: [guestRoot(2)],
    selected: 0,
  });
  expect(store.value()).not.toContain(kim);
  tabB.logout();
  await tabB.settleSelection();
  expect(store.value()).not.toContain(kim);
});

it("tells a tab still open as a guest that another tab linked and forgot it", async () => {
  const store = storeWith(null);
  const kim = guestRoot(1);
  const tabA = await sharedDevice(store, kim);
  tabA.createLocalFirst();
  await tabA.settleSelection();
  const tabB = await sharedDevice(store, guestRoot(2));
  const held = tabB.getLoggedIn()!;
  expect(held.id).toBe(accountOf(kim));
  expect(await accountForgotten(tabB, held.id)).toBe(false);

  await tabA.linkWithBackend();
  await tabA.settleSelection();
  // Read from the store, not tab B's snapshot from before the link.
  expect(await accountForgotten(tabB, held.id)).toBe(true);
  expect(await accountForgotten(tabB, accountOf(guestRoot(2)))).toBe(false);

  // Restored explicitly, the root is no one's to forget any more.
  tabA.logout();
  tabA.restoreLocalFirst(kim);
  await tabA.settleSelection();
  expect(await accountForgotten(tabB, held.id)).toBe(false);
});

it("never reopens a forgotten root written back while the tombstone stayed", async () => {
  const kim = guestRoot(1);
  const store = storeWith(
    JSON.stringify({
      format: "jazz-account-selection-v1",
      roots: [guestRoot(2), kim],
      selected: 1,
      forgottenAccounts: [accountOf(kim)],
    }),
  );
  const accounts = await sharedDevice(store, guestRoot(3));
  expect(accounts.getLoggedIn()).toBeUndefined();
  // The automatic start mints a fresh guest instead of adopting Kim's root,
  // and its write drops that root from the store.
  expect((await ensureAutomaticLocalFirst(accounts)).id).toBe(accountOf(guestRoot(3)));
  await accounts.settleSelection();
  expect(JSON.parse(store.value()!).roots).toEqual([guestRoot(2), guestRoot(3)]);
  expect(store.value()).not.toContain(kim);
});

it("an explicit restore takes a forgotten root back, and a stale tab keeps it", async () => {
  const store = storeWith(null);
  const kim = guestRoot(1);
  const tabA = await sharedDevice(store, kim);
  tabA.createLocalFirst();
  await tabA.settleSelection();
  await tabA.linkWithBackend();
  await tabA.settleSelection();
  // A tab that loaded the tombstone, then Kim's own app restores her root.
  const tabB = await sharedDevice(store, guestRoot(2));
  tabA.logout();
  tabA.restoreLocalFirst(kim);
  await tabA.settleSelection();
  expect(JSON.parse(store.value()!)).toEqual({
    format: "jazz-account-selection-v1",
    roots: [kim],
    selected: 0,
  });
  // Tab B never forgot anything itself, so its writes keep the restored root.
  tabB.createLocalFirst();
  await tabB.settleSelection();
  expect(JSON.parse(store.value()!).roots).toEqual([kim, guestRoot(2)]);
});

it("keeps only the latest forgotten accounts", async () => {
  const forgottenAccounts = Array.from({ length: 1100 }, (_, i) => `account-${i}`);
  const store = storeWith(
    JSON.stringify({
      format: "jazz-account-selection-v1",
      roots: [],
      selected: null,
      forgottenAccounts,
    }),
  );
  const accounts = await sharedDevice(store, guestRoot(1));
  accounts.createLocalFirst();
  await accounts.settleSelection();
  expect(JSON.parse(store.value()!).forgottenAccounts).toEqual(forgottenAccounts.slice(-1024));
});

it("a tab starting on a guest root keeps the tombstone a link writes meanwhile", async () => {
  const kim = guestRoot(1);
  const before = JSON.stringify({ format: "jazz-account-selection-v1", roots: [kim], selected: 0 });
  // Another tab's link lands between this tab's read and its first write.
  const linked = JSON.stringify({
    format: "jazz-account-selection-v1",
    roots: [],
    selected: null,
    forgottenAccounts: [accountOf(kim)],
  });
  let value: string | null = before;
  const store = {
    value: () => value,
    read: async () => {
      const read = value;
      value = linked;
      return read;
    },
    update: async (transform: (current: string | null) => string) => {
      value = transform(value);
    },
  };
  const accounts = await sharedDevice(store, guestRoot(2));
  expect(accounts.getLoggedIn()?.id).toBe(accountOf(kim));
  await accounts.settleSelection();
  expect(JSON.parse(store.value()!)).toMatchObject({
    roots: [],
    selected: null,
    forgottenAccounts: [accountOf(kim)],
  });
  expect(store.value()).not.toContain(kim);
});

it("keeps a linked guest's root when the device remembers its user", async () => {
  const store = storeWith(null);
  const guestAccount = localFirstAccountId("test", "00000000-0000-4000-8000-000000000001");
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken,
    fetch: linkFetch(guestAccount),
  });
  accounts.createLocalFirst();
  await accounts.linkWithBackend();
  await accounts.settleSelection();
  expect(JSON.parse(store.value()!).roots).toHaveLength(1);
});

it("keeps another guest's root when a different user signs in (clearOnSignOut)", async () => {
  const store = storeWith(null);
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken,
    fetch: signInFetch,
    rememberBackendUser: false,
  });
  accounts.createLocalFirst();
  await accounts.settleSelection();
  // Not a link: the guest's data is its own, and its root its only key.
  await accounts.loginWithBackend();
  await accounts.settleSelection();
  expect(JSON.parse(store.value()!).roots).toHaveLength(1);
});
