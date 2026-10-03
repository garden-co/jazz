import { afterEach, describe, expect, it, vi } from "vitest";

const host = vi.hoisted(() => ({ browser: false }));
vi.mock("./browser-host.js", () => ({ isBrowserHostRuntime: () => host.browser }));
import { createAccountManager } from "./create-account-manager.js";
import { isProvisionalAccount } from "./enrollment.js";
import type { AccountHandle } from "./state.js";
import { createJazzSessionOwner } from "../session/state.js";

const serverUrl = "https://core.example";
const alice = { issuer: "https://issuer.example", subject: "alice" };
const aliceAccount = "00000000-0000-4000-8000-00000000000a";

function jwt() {
  const exp = Math.floor(Date.now() / 1000) + 3600;
  return `e30.${btoa(JSON.stringify({ iss: alice.issuer, sub: alice.subject, exp }))}.sig`;
}

function stubRegistry() {
  const fetch = vi.fn(
    async () => new Response(JSON.stringify({ account: aliceAccount, identity: alice })),
  );
  vi.stubGlobal("fetch", fetch);
  return fetch;
}

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

async function sessionEvents(accounts: Awaited<ReturnType<typeof createAccountManager>>) {
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
  return { session, events };
}

afterEach(() => {
  host.browser = false;
  vi.unstubAllGlobals();
});

function stubBrowserStore() {
  const storage = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => void storage.set(key, value),
  });
  vi.stubGlobal("navigator", {
    locks: { request: async (_key: string, run: () => unknown) => run() },
  });
  return storage;
}

describe("retained account hosts", () => {
  it("a host-supplied store (Node, SSR) neither retains nor revalidates in place", async () => {
    stubRegistry();
    const store = memoryStore();
    const config = { appId: "hosts-node", serverUrl, store };
    const accounts = await createAccountManager(config);
    expect(accounts.revalidatesInPlace).toBe(false);
    await accounts.loginJWT(jwt());
    await vi.waitFor(() => expect(store.value).not.toBeNull());
    expect(JSON.parse(store.value!).assignment).toBeNull();
    expect((await createAccountManager(config)).getLoggedIn()).toBeUndefined();

    // A same-identity login keeps the ordinary teardown-and-reopen transition.
    const { session, events } = await sessionEvents(accounts);
    await session.loginJWT(jwt());
    expect(events).toEqual(["open:alice:false", "shutdown:alice:true", "open:alice:false"]);
    await session.close();
  });

  it("the browser's default store retains the assignment and revalidates in place", async () => {
    stubRegistry();
    host.browser = true;
    const storage = stubBrowserStore();
    const config = { appId: "hosts-browser", serverUrl };
    const accounts = await createAccountManager(config);
    expect(accounts.revalidatesInPlace).toBe(true);
    await accounts.loginJWT(jwt());
    await vi.waitFor(() =>
      expect(JSON.parse([...storage.values()][0]!).assignment).toMatchObject({
        account: aliceAccount,
      }),
    );

    const restarted = await createAccountManager(config);
    expect(isProvisionalAccount(restarted.getLoggedIn()!)).toBe(true);
    const { session, events } = await sessionEvents(restarted);
    await session.loginJWT(jwt());
    expect(events).toEqual(["open:alice:true"]);
    await session.close();
  });

  it("a non-browser runtime with browser-like storage globals does not retain", async () => {
    stubRegistry();
    host.browser = false;
    const storage = stubBrowserStore();
    const config = { appId: "hosts-node-globals", serverUrl };
    const accounts = await createAccountManager(config);
    expect(accounts.revalidatesInPlace).toBe(false);
    await accounts.loginJWT(jwt());
    await vi.waitFor(() => expect(storage.size).toBe(1));
    expect(JSON.parse([...storage.values()][0]!).assignment).toBeNull();
    expect((await createAccountManager(config)).getLoggedIn()).toBeUndefined();
  });
});
