import { expect, it, vi } from "vitest";
import { ensureAutomaticLocalFirst, prepareAccountManager } from "./persistence.js";
import { createJazzSessionOwner } from "../session/state.js";
import { formatAuthSecret } from "../runtime/auth-secret-codec.js";
import { generateAuthSecret } from "../runtime/auth-secret-store.js";
import { accountToken, accountGeneratedHere, exportLocalFirstSecret } from "./enrollment.js";
import type { AccountHandle } from "./state.js";
import { settleAccountSelection } from "./selection-durability.js";

const registry = "https://core.example/apps/test/accounts";
// Controlled crypto boundary: these tests prove storage ordering and recovery;
// native signature/subject derivation is covered by the Rust identity corpus.
const mintToken = () =>
  `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.sig`;
const mintRootToken = (secret: string) =>
  `e30.${btoa(
    JSON.stringify({ iss: "urn:jazz:local-first", sub: secret.slice("jazz-auth-v1:".length) }),
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
  const root = exportLocalFirstSecret(handle);
  restarted.logout();
  await settleAccountSelection(restarted);
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
  expect(JSON.parse(value!)).toMatchObject({ selected: 0, roots: [firstSecret] });
  expect(JSON.parse(value!).generatedHere).toEqual([firstSecret]);
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
  expect(JSON.parse(value!)).toMatchObject({ selected: 0, roots: [automaticSecret] });
});

it("retains generated-here eligibility across reopen but never grants it to imported roots", async () => {
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
  const manager = await prepareAccountManager(options);
  const generated = manager.createLocalFirst();
  await expect(accountGeneratedHere(generated)).resolves.toBe(true);
  const reopened = await prepareAccountManager(options);
  await expect(accountGeneratedHere(reopened.getLoggedIn()!)).resolves.toBe(true);
  const imported = reopened.restoreLocalFirst(generateAuthSecret());
  await expect(accountGeneratedHere(imported)).resolves.toBe(false);
  const importedReopen = await prepareAccountManager(options);
  await expect(accountGeneratedHere(importedReopen.getLoggedIn()!)).resolves.toBe(false);
  const original = importedReopen.restoreLocalFirst(exportLocalFirstSecret(generated));
  await expect(accountGeneratedHere(original)).resolves.toBe(true);
});

it("does not manufacture founder eligibility from legacy retained roots or failed retention", async () => {
  const secret = generateAuthSecret();
  let value = JSON.stringify({ format: "jazz-account-selection-v1", roots: [secret], selected: 0 });
  let fail = false;
  const options = {
    appId: "test",
    registry,
    mintToken,
    store: {
      async read() {
        return value;
      },
      async update(transform: (current: string | null) => string) {
        if (fail) throw new Error("Private storage unavailable");
        value = transform(value);
      },
    },
  };
  const legacy = await prepareAccountManager(options);
  await expect(accountGeneratedHere(legacy.getLoggedIn()!)).resolves.toBe(false);
  fail = true;
  const failed = legacy.createLocalFirst();
  await expect(accountGeneratedHere(failed)).rejects.toThrow("Private storage unavailable");
  fail = false;
  const restarted = await prepareAccountManager(options);
  await expect(accountGeneratedHere(restarted.getLoggedIn()!)).resolves.toBe(false);
});

it("writes and reopens the canonical v2 account provenance corpus", async () => {
  const secret = "jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
  const selected =
    '{"format":"jazz-account-selection-v2","roots":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],"selected":0,"generatedHere":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"]}';
  const loggedOut =
    '{"format":"jazz-account-selection-v2","roots":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],"selected":null,"generatedHere":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"]}';
  let value: string | null = null;
  const options = {
    appId: "test",
    registry,
    mintToken,
    generateSecret: () => secret,
    store: {
      async read() {
        return value;
      },
      async update(transform: (current: string | null) => string) {
        value = transform(value);
      },
    },
  };
  const created = await prepareAccountManager(options);
  await expect(accountGeneratedHere(created.createLocalFirst())).resolves.toBe(true);
  expect(value).toBe(selected);
  const reopened = await prepareAccountManager(options);
  expect(exportLocalFirstSecret(reopened.getLoggedIn()!)).toBe(secret);
  await expect(accountGeneratedHere(reopened.getLoggedIn()!)).resolves.toBe(true);
  expect(value).toBe(selected);
  reopened.logout();
  await settleAccountSelection(reopened);
  expect(value).toBe(loggedOut);
});

it("migrates every legacy root and its selection without trusting a v1 provenance extension", async () => {
  const roots = [generateAuthSecret(), generateAuthSecret()];
  let value = JSON.stringify({
    format: "jazz-account-selection-v1",
    roots,
    selected: 1,
    generatedHere: roots,
  });
  const options = {
    appId: "test",
    registry,
    mintToken,
    store: {
      async read() {
        return value;
      },
      async update(transform: (current: string | null) => string) {
        value = transform(value);
      },
    },
  };
  const migrated = await prepareAccountManager(options);
  expect(exportLocalFirstSecret(migrated.getLoggedIn()!)).toBe(roots[1]);
  await expect(accountGeneratedHere(migrated.getLoggedIn()!)).resolves.toBe(false);
  expect(JSON.parse(value).roots).toEqual(roots);
  const reopened = await prepareAccountManager(options);
  expect(exportLocalFirstSecret(reopened.getLoggedIn()!)).toBe(roots[1]);
  await expect(accountGeneratedHere(reopened.restoreLocalFirst(roots[0]!))).resolves.toBe(false);
  expect(JSON.parse(value).roots).toEqual(roots);
});

it("refuses malformed or future account inventories without replacing retained secrets", async () => {
  const secret = "jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
  const valid = {
    format: "jazz-account-selection-v2",
    roots: [secret],
    selected: 0,
    generatedHere: [secret],
  };
  for (const invalid of [
    { ...valid, format: "jazz-account-selection-v3" },
    { ...valid, generatedHere: undefined },
    { ...valid, generatedHere: [generateAuthSecret()] },
    { ...valid, generatedHere: [secret, secret] },
    { ...valid, generatedHere: [1] },
    { ...valid, selected: 1 },
    { ...valid, roots: ["invalid-secret"] },
  ]) {
    const value = JSON.stringify(invalid);
    const update = vi.fn();
    await expect(
      prepareAccountManager({
        appId: "test",
        registry,
        mintToken,
        store: { read: async () => value, update },
      }),
    ).rejects.toThrow();
    expect(update).not.toHaveBeenCalled();
  }
});

// Persisted candidate corpus, not output from the current account writer.
const candidateFounderInventory =
  '{"format":"jazz-account-selection-v2","roots":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","jazz-auth-v1:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"],"selected":1,"generatedHere":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],"founders":[{"root":"jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","scope":"test/notes","deviceId":"device-original","epochId":"epoch-original","closed":false},{"root":"jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","scope":"test/tasks","deviceId":"device-reserved","epochId":null,"closed":false},{"root":"jazz-auth-v1:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE","scope":"test/notes","deviceId":"device-retired","epochId":null,"closed":true}]}';

it("preserves candidate-v2 founder ownership through selection, stale-manager logout and reopen", async () => {
  const corpus = JSON.parse(candidateFounderInventory);
  let value = candidateFounderInventory;
  const options = {
    appId: "test",
    registry,
    mintToken: mintRootToken,
    store: {
      async read() {
        return value;
      },
      async update(transform: (current: string | null) => string) {
        value = transform(value);
      },
    },
  };
  const manager = await prepareAccountManager(options);
  const imported = manager.getLoggedIn()!;
  expect(exportLocalFirstSecret(imported)).toBe(corpus.roots[1]);
  await expect(accountGeneratedHere(imported)).resolves.toBe(false);
  await expect(accountToken(imported, registry)).resolves.toBe(mintRootToken(corpus.roots[1]));
  const stale = await prepareAccountManager(options);
  const staleHandle = stale.getLoggedIn()!;

  const generated = manager.restoreLocalFirst(corpus.roots[0]);
  await expect(accountGeneratedHere(generated)).resolves.toBe(true);
  await expect(accountToken(generated, registry)).resolves.toBe(mintRootToken(corpus.roots[0]));
  await settleAccountSelection(manager);
  expect(JSON.parse(value)).toMatchObject({
    roots: corpus.roots,
    selected: 0,
    generatedHere: corpus.generatedHere,
    founders: corpus.founders,
    founderEligibleRoots: corpus.generatedHere,
  });

  stale.logout();
  await settleAccountSelection(stale);
  expect(() => exportLocalFirstSecret(staleHandle)).toThrow(/recovery_unavailable/);
  await expect(accountToken(staleHandle, registry)).rejects.toMatchObject({
    code: "invalid_account_handle",
  });
  expect(JSON.parse(value)).toMatchObject({
    roots: corpus.roots,
    selected: null,
    generatedHere: corpus.generatedHere,
    founders: corpus.founders,
    founderEligibleRoots: corpus.generatedHere,
  });
  // Logging out one manager must not revoke another manager's live handles.
  await expect(accountToken(generated, registry)).resolves.toBe(mintRootToken(corpus.roots[0]));
  manager.logout();
  await settleAccountSelection(manager);
  for (const handle of [imported, generated]) {
    expect(() => exportLocalFirstSecret(handle)).toThrow(/recovery_unavailable/);
    await expect(accountToken(handle, registry)).rejects.toMatchObject({
      code: "invalid_account_handle",
    });
    await expect(accountGeneratedHere(handle)).rejects.toMatchObject({
      code: "invalid_account_handle",
    });
  }

  const loggedOut = await prepareAccountManager(options);
  expect(loggedOut.getLoggedIn()).toBeUndefined();
  const restored = loggedOut.restoreLocalFirst(corpus.roots[1]);
  await expect(accountGeneratedHere(restored)).resolves.toBe(false);
  await accountToken(restored, registry);
  const reopened = await prepareAccountManager(options);
  expect(exportLocalFirstSecret(reopened.getLoggedIn()!)).toBe(corpus.roots[1]);
  await expect(accountToken(reopened.getLoggedIn()!, registry)).resolves.toBe(
    mintRootToken(corpus.roots[1]),
  );
  expect(JSON.parse(value)).toMatchObject({
    roots: corpus.roots,
    selected: 1,
    generatedHere: corpus.generatedHere,
    founders: corpus.founders,
    founderEligibleRoots: corpus.generatedHere,
  });
});

it.each([
  {
    name: "ordinary-v2",
    inventory:
      '{"format":"jazz-account-selection-v2","roots":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],"selected":0,"generatedHere":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"]}',
    eligible: [],
  },
  {
    name: "candidate-v2 with empty founders",
    inventory:
      '{"format":"jazz-account-selection-v2","roots":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],"selected":0,"generatedHere":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],"founders":[]}',
    eligible: ["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],
  },
])("preserves the founder migration distinction for $name", async ({ inventory, eligible }) => {
  const secret = "jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
  let value = inventory;
  const options = {
    appId: "test",
    registry,
    mintToken: mintRootToken,
    store: {
      async read() {
        return value;
      },
      async update(transform: (current: string | null) => string) {
        value = transform(value);
      },
    },
  };
  const manager = await prepareAccountManager(options);
  const selected = manager.getLoggedIn()!;
  expect(exportLocalFirstSecret(selected)).toBe(secret);
  await expect(accountGeneratedHere(selected)).resolves.toBe(true);
  await expect(accountToken(selected, registry)).resolves.toBe(mintRootToken(secret));
  manager.logout();
  await settleAccountSelection(manager);
  expect(JSON.parse(value)).toMatchObject({
    roots: [secret],
    selected: null,
    generatedHere: [secret],
    founders: [],
    founderEligibleRoots: eligible,
  });
  const reopened = await prepareAccountManager(options);
  expect(reopened.getLoggedIn()).toBeUndefined();
  const restored = reopened.restoreLocalFirst(secret);
  await expect(accountGeneratedHere(restored)).resolves.toBe(true);
  await expect(accountToken(restored, registry)).resolves.toBe(mintRootToken(secret));
  expect(JSON.parse(value)).toMatchObject({ selected: 0, founderEligibleRoots: eligible });
});

it("opens valid v3 founder inventory without changing selection or credential provenance", async () => {
  let value =
    '{"format":"jazz-account-selection-v3","roots":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","jazz-auth-v1:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"],"selected":1,"generatedHere":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],"founderEligibleRoots":["jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],"founders":[{"root":"jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","scope":"test/notes","deviceId":"device-original","epochId":"epoch-original","closed":false}]}';
  const corpus = JSON.parse(value);
  const options = {
    appId: "test",
    registry,
    mintToken: mintRootToken,
    store: {
      async read() {
        return value;
      },
      async update(transform: (current: string | null) => string) {
        value = transform(value);
      },
    },
  };
  const manager = await prepareAccountManager(options);
  const imported = manager.getLoggedIn()!;
  expect(exportLocalFirstSecret(imported)).toBe(corpus.roots[1]);
  await expect(accountGeneratedHere(imported)).resolves.toBe(false);
  await expect(accountToken(imported, registry)).resolves.toBe(mintRootToken(corpus.roots[1]));
  const generated = manager.restoreLocalFirst(corpus.roots[0]);
  await expect(accountGeneratedHere(generated)).resolves.toBe(true);
  await accountToken(generated, registry);
  const reopened = await prepareAccountManager(options);
  expect(exportLocalFirstSecret(reopened.getLoggedIn()!)).toBe(corpus.roots[0]);
  await expect(accountToken(reopened.getLoggedIn()!, registry)).resolves.toBe(
    mintRootToken(corpus.roots[0]),
  );
  expect(JSON.parse(value)).toEqual({ ...corpus, selected: 0 });
});

it.each([
  { name: "null ownership list", change: { founders: null } },
  { name: "non-array ownership list", change: { founders: {} } },
  { name: "null claim", claim: null },
  {
    name: "unretained root",
    claim: { root: "jazz-auth-v1:AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI" },
  },
  { name: "empty scope", claim: { scope: "" } },
  { name: "missing device", claim: { deviceId: undefined } },
  { name: "empty device", claim: { deviceId: "" } },
  { name: "missing epoch", claim: { epochId: undefined } },
  { name: "empty bound epoch", claim: { epochId: "" } },
  { name: "non-string epoch", claim: { epochId: 1 } },
  { name: "non-boolean closure", claim: { closed: "false" } },
  { name: "competing claims for one root and scope", duplicate: true },
])(
  "rejects malformed candidate-v2 founders: $name without writing retained secrets",
  async (invalid) => {
    const corpus = JSON.parse(candidateFounderInventory);
    if ("change" in invalid) Object.assign(corpus, invalid.change);
    else if ("duplicate" in invalid)
      corpus.founders.push({ ...corpus.founders[0], deviceId: "device-competing" });
    else
      corpus.founders[0] =
        invalid.claim === null ? null : { ...corpus.founders[0], ...invalid.claim };
    const value = JSON.stringify(corpus);
    let retained = value;
    const update = vi.fn(async (transform: (current: string | null) => string) => {
      retained = transform(retained);
    });
    await expect(
      prepareAccountManager({
        appId: "test",
        registry,
        mintToken: mintRootToken,
        store: { read: async () => retained, update },
      }),
    ).rejects.toThrow();
    expect(update).not.toHaveBeenCalled();
    expect(retained).toBe(value);
  },
);
