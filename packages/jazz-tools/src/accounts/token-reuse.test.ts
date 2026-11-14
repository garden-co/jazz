import { afterEach, expect, it, vi } from "vitest";
import { prepareAccountManager, reusableLocalFirstToken } from "./persistence.js";
import { accountToken } from "./enrollment.js";
import { mintLocalFirstTokenInJs } from "../thin/index.js";
import { parseJwtPayload } from "../runtime/client-session.js";

// A page load's local-first token: minted at most once, reused from the store
// on the next load while at least half of its hour is left.
const registry = "https://core.example/apps/test/accounts";
const appId = "test";

afterEach(() => {
  vi.useRealTimers();
});

function memoryStore() {
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

const countingMinter = () => vi.fn(mintLocalFirstTokenInJs);

/** A load: a manager over `store`, its restored account and one connection's token. */
async function load(store: ReturnType<typeof memoryStore>, mintToken = countingMinter()) {
  const manager = await prepareAccountManager({ appId, registry, store, mintToken });
  const handle = manager.getLoggedIn() ?? manager.createLocalFirst();
  const token = await accountToken(handle, registry);
  // Let the cache write land.
  await vi.waitFor(() => expect(JSON.parse(store.value!).tokens).toBeDefined());
  return { manager, handle, token, mintToken };
}

it("mints once per load, for the restore, the account it founds and the connection", async () => {
  const store = memoryStore();
  const first = await load(store);
  expect(first.mintToken).toHaveBeenCalledOnce();
  // A second connection of the same load takes the same token.
  expect(await accountToken(first.handle, registry)).toBe(first.token);
  expect(first.mintToken).toHaveBeenCalledOnce();
});

it("reuses a still-valid token on the next load, without signing", async () => {
  vi.useFakeTimers({ now: new Date("2026-11-14T12:00:00Z"), toFake: ["Date"] });
  const store = memoryStore();
  const first = await load(store);
  const root = JSON.parse(store.value!).roots[0];
  expect(JSON.parse(store.value!).tokens).toEqual({ [root]: first.token });

  vi.setSystemTime(new Date("2026-11-14T12:29:00Z"));
  const second = await load(store);
  expect(second.mintToken).not.toHaveBeenCalled();
  expect(second.handle.id).toBe(first.handle.id);
  expect(second.token).toBe(first.token);
});

it("mints again once less than half of the token's lifetime is left", async () => {
  vi.useFakeTimers({ now: new Date("2026-11-14T12:00:00Z"), toFake: ["Date"] });
  const store = memoryStore();
  const first = await load(store);

  vi.setSystemTime(new Date("2026-11-14T12:31:00Z"));
  const second = await load(store);
  expect(second.mintToken).toHaveBeenCalledOnce();
  expect(second.token).not.toBe(first.token);
  expect(parseJwtPayload(second.token)?.exp).toBe(Date.parse("2026-11-14T13:31:00Z") / 1000);
  // The store keeps the new one.
  const root = JSON.parse(store.value!).roots[0];
  expect(JSON.parse(store.value!).tokens[root]).toBe(second.token);
});

it("mints again past expiry, and drops expired tokens from the store", async () => {
  vi.useFakeTimers({ now: new Date("2026-11-14T12:00:00Z"), toFake: ["Date"] });
  const store = memoryStore();
  const first = await load(store);

  vi.setSystemTime(new Date("2026-11-14T14:00:00Z"));
  const manager = await prepareAccountManager({
    appId,
    registry,
    store,
    mintToken: countingMinter(),
  });
  // Nothing has written yet: the expired token is only gone from memory.
  const second = await accountToken(manager.getLoggedIn()!, registry);
  expect(second).not.toBe(first.token);
  expect(parseJwtPayload(second)!.exp).toBeGreaterThan(Date.now() / 1000);
});

it("does not reuse a token for another audience, issuer, or with a longer lifetime", () => {
  const now = Math.floor(Date.now() / 1000);
  const token = mintLocalFirstTokenInJs(
    "jazz-auth-v1:" + Buffer.alloc(32, 3).toString("base64url"),
    appId,
  );
  expect(reusableLocalFirstToken(token, appId, now)).toBe(true);
  expect(reusableLocalFirstToken(token, "other-app", now)).toBe(false);
  const forged = (claims: Record<string, unknown>) => {
    const [header, , signature] = token.split(".");
    const body = { ...parseJwtPayload(token)!, ...claims };
    return `${header}.${Buffer.from(JSON.stringify(body)).toString("base64url")}.${signature}`;
  };
  expect(reusableLocalFirstToken(forged({ iss: "urn:jazz:anonymous" }), appId, now)).toBe(false);
  expect(reusableLocalFirstToken(forged({ exp: now + 7200 }), appId, now)).toBe(false);
  // Issued "in the future": the clock was set back since.
  expect(reusableLocalFirstToken(forged({ iat: now + 600, exp: now + 4200 }), appId, now)).toBe(
    false,
  );
});

it("keeps no token for a root the store no longer keeps", async () => {
  const store = memoryStore();
  await load(store);
  const record = JSON.parse(store.value!);
  // Another client dropped the root (and with it, a token it doesn't know about).
  store.value = JSON.stringify({ ...record, roots: [], selected: null });
  const restarted = await prepareAccountManager({
    appId,
    registry,
    store,
    mintToken: countingMinter(),
  });
  expect(restarted.getLoggedIn()).toBeUndefined();
  await restarted.createLocalFirst();
  await vi.waitFor(() => expect(JSON.parse(store.value!).roots).toHaveLength(1));
  const latest = JSON.parse(store.value!);
  expect(Object.keys(latest.tokens ?? {})).toEqual(latest.roots);
});

it("mints a fresh token for a renewal, even while the cached one could be reused", async () => {
  // The server's clock is 40 min ahead: it calls the cached token expired
  // although this device sees most of its hour left.
  vi.useFakeTimers({ now: new Date("2026-11-14T12:00:00Z"), toFake: ["Date"] });
  const store = memoryStore();
  const first = await load(store);
  vi.setSystemTime(new Date("2026-11-14T12:01:00Z"));
  const renewed = await accountToken(first.handle, registry, { fresh: true });
  expect(first.mintToken).toHaveBeenCalledTimes(2);
  expect(renewed).not.toBe(first.token);
  // The renewal is what the cache keeps, for this load and the next.
  expect(await accountToken(first.handle, registry)).toBe(renewed);
  const root = JSON.parse(store.value!).roots[0];
  await vi.waitFor(() => expect(JSON.parse(store.value!).tokens[root]).toBe(renewed));
});

it("replaces a stored token issued in the future, minted before the clock was set back", async () => {
  vi.useFakeTimers({ now: new Date("2026-11-14T13:00:00Z"), toFake: ["Date"] });
  const store = memoryStore();
  const ahead = await load(store);
  const root = JSON.parse(store.value!).roots[0];

  vi.setSystemTime(new Date("2026-11-14T12:00:00Z"));
  const second = await load(store);
  expect(second.mintToken).toHaveBeenCalledOnce();
  expect(second.token).not.toBe(ahead.token);
  // Stored, although the future token claims a later `iat`.
  await vi.waitFor(() => expect(JSON.parse(store.value!).tokens[root]).toBe(second.token));
  const third = await load(store);
  expect(third.mintToken).not.toHaveBeenCalled();
});
