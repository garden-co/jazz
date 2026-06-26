import assert from "node:assert/strict";
import test from "node:test";
import {
  createAuthSecretStore,
  createUseLocalFirstAuth,
  generateAuthSecret,
  type AuthSecretStorage,
} from "./auth-secret-store.js";

class MemoryStorage implements AuthSecretStorage {
  readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }
}

test("generateAuthSecret returns URL-safe non-empty secrets", () => {
  const first = generateAuthSecret();
  const second = generateAuthSecret();

  assert.match(first, /^[A-Za-z0-9_-]+$/u);
  assert.notEqual(first, second);
});

test("getOrCreateSecret is stable for one appId", () => {
  const storage = new MemoryStorage();
  const store = createAuthSecretStore({ appId: "app-a", storage });

  assert.equal(store.getOrCreateSecret(), store.getOrCreateSecret());
  assert.equal(storage.values.size, 1);
});

test("secrets are scoped by appId", () => {
  const storage = new MemoryStorage();
  const store = createAuthSecretStore({ storage });

  const first = store.getOrCreateSecret("app-a");
  const second = store.getOrCreateSecret("app-b");

  assert.notEqual(first, second);
  assert.equal(store.getOrCreateSecret("app-a"), first);
  assert.equal(store.getOrCreateSecret("app-b"), second);
});

test("clearSecret removes the scoped secret and regenerates on next read", () => {
  const storage = new MemoryStorage();
  const store = createAuthSecretStore({ storage });
  const first = store.getOrCreateSecret("app-a");

  store.clearSecret("app-a");
  const second = store.getOrCreateSecret("app-a");

  assert.notEqual(second, first);
});

test("saveSecret writes only the requested appId scope", () => {
  const storage = new MemoryStorage();
  const store = createAuthSecretStore({ storage });
  const generated = store.getOrCreateSecret("app-b");

  store.saveSecret("known-secret", "app-a");

  assert.equal(store.getOrCreateSecret("app-a"), "known-secret");
  assert.equal(store.getOrCreateSecret("app-b"), generated);
});

test("store works without window or localStorage", () => {
  const store = createAuthSecretStore({ appId: "ssr-app", storage: null });
  const first = store.getOrCreateSecret();

  assert.equal(store.getOrCreateSecret(), first);
  store.clearSecret();
  assert.notEqual(store.getOrCreateSecret(), first);
});

test("createUseLocalFirstAuth returns sync local-first auth state", () => {
  const storage = new MemoryStorage();
  const store = createAuthSecretStore({ appId: "hook-app", storage });
  const useLocalFirstAuth = createUseLocalFirstAuth(store);

  const first = useLocalFirstAuth();
  const second = useLocalFirstAuth();

  assert.equal(first.isLoading, false);
  assert.equal(first.error, undefined);
  assert.equal(first.secret, second.secret);
  assert.equal(useLocalFirstAuth({ appId: "other-app" }).isLoading, false);
  assert.notEqual(useLocalFirstAuth({ appId: "other-app" }).secret, first.secret);
});
