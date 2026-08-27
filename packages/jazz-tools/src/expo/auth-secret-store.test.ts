import { describe, expect, it, vi } from "vitest";

// A valid deterministic root makes corrupt-value tests prove that the store
// rejects rather than silently regenerating and overwriting the existing value.
vi.mock("expo-crypto", () => ({ getRandomBytes: vi.fn(() => new Uint8Array(32)) }));
vi.mock("expo-secure-store", () => ({
  deleteItemAsync: vi.fn(),
  getItemAsync: vi.fn(),
  setItemAsync: vi.fn(),
}));
import { ExpoAuthSecretStore, type ExpoSecureStoreLike } from "./auth-secret-store.js";
import {
  authSecretStorageKey,
  generateAuthSecret,
  parseAuthSecret,
} from "../runtime/auth-secret-store.js";

function recordingStore() {
  const getItemAsync = vi.fn<ExpoSecureStoreLike["getItemAsync"]>(async () => null);
  const setItemAsync = vi.fn<ExpoSecureStoreLike["setItemAsync"]>(async () => {});
  const deleteItemAsync = vi.fn<ExpoSecureStoreLike["deleteItemAsync"]>(async () => {});
  const secureStore: ExpoSecureStoreLike = { getItemAsync, setItemAsync, deleteItemAsync };
  return { secureStore, getItemAsync, setItemAsync, deleteItemAsync };
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function controlledStore() {
  let value: string | null = null;
  const firstRead = deferred<string | null>();
  const events: string[] = [];
  const getItemAsync = vi
    .fn<ExpoSecureStoreLike["getItemAsync"]>()
    .mockImplementationOnce(() => firstRead.promise)
    .mockImplementation(async () => value);
  const setItemAsync = vi.fn<ExpoSecureStoreLike["setItemAsync"]>(async (_key, secret) => {
    events.push(`save:${secret}`);
    value = secret;
  });
  const deleteItemAsync = vi.fn<ExpoSecureStoreLike["deleteItemAsync"]>(async () => {
    events.push("clear");
    value = null;
  });
  const secureStore: ExpoSecureStoreLike = { getItemAsync, setItemAsync, deleteItemAsync };
  return { secureStore, firstRead, events, value: () => value };
}

describe("ExpoAuthSecretStore scoped keys", () => {
  it("uses the same hashed, SecureStore-compatible key as browser storage", async () => {
    const { secureStore, setItemAsync } = recordingStore();
    const store = new ExpoAuthSecretStore({
      secureStore,
      appId: "app:one/%",
      profile: "user@example.com",
    });

    await store.saveSecret(generateAuthSecret());

    const key = setItemAsync.mock.calls[0]?.[0];
    expect(key).toMatch(/^[A-Za-z0-9._-]+$/);
    expect(key).not.toContain(":");
    expect(key).not.toContain("%");
    expect(key).toBe(authSecretStorageKey({ appId: "app:one/%", profile: "user@example.com" }));
  });

  it("keeps app/profile scopes distinct", async () => {
    const { secureStore, getItemAsync } = recordingStore();

    await new ExpoAuthSecretStore({ secureStore, appId: "same" }).loadSecret();
    await new ExpoAuthSecretStore({ secureStore, profile: "same" }).loadSecret();
    await new ExpoAuthSecretStore({ secureStore, appId: "same", profile: "same" }).loadSecret();

    const keys = getItemAsync.mock.calls.map(([key]) => key);
    expect(new Set(keys)).toHaveProperty("size", 3);
  });

  it("uses the common hashed default key", async () => {
    const { secureStore, getItemAsync } = recordingStore();

    await new ExpoAuthSecretStore({ secureStore }).loadSecret();

    expect(getItemAsync).toHaveBeenCalledWith(authSecretStorageKey());
  });

  it("loads the fixed browser/recovery secret representation byte-for-byte", async () => {
    const { secureStore, setItemAsync } = recordingStore();
    const secret = "jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const store = new ExpoAuthSecretStore({ secureStore, appId: "shared-app", profile: "default" });

    await store.saveSecret(secret);
    expect(setItemAsync).toHaveBeenCalledWith(
      authSecretStorageKey({ appId: "shared-app", profile: "default" }),
      secret,
    );
    expect(parseAuthSecret(secret)).toEqual(new Uint8Array(32));
  });

  it("rejects malformed stored secrets before returning them to native auth", async () => {
    const { secureStore } = recordingStore();
    secureStore.getItemAsync = async () =>
      "jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    const store = new ExpoAuthSecretStore({ secureStore });
    await expect(store.loadSecret()).rejects.toThrow(/43 unpadded/);
  });

  it("fails closed for present empty or malformed values without generating over them", async () => {
    for (const corrupt of ["", "jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="]) {
      const { secureStore, setItemAsync } = recordingStore();
      secureStore.getItemAsync = vi.fn(async () => corrupt);
      const store = new ExpoAuthSecretStore({ secureStore });

      await expect(store.getOrCreateSecret()).rejects.toThrow();
      expect(setItemAsync).not.toHaveBeenCalled();
    }
  });
});

describe("ExpoAuthSecretStore operation ordering", () => {
  it("does not let an older get-or-create overwrite a completed save", async () => {
    const { secureStore, firstRead, events, value } = controlledStore();
    const store = new ExpoAuthSecretStore({ secureStore });
    const creating = store.getOrCreateSecret();
    await vi.waitFor(() => expect(secureStore.getItemAsync).toHaveBeenCalledOnce());

    const saving = store.saveSecret("imported-secret");
    firstRead.resolve(null);
    await Promise.all([creating, saving]);

    expect(events.at(-1)).toBe("save:imported-secret");
    expect(value()).toBe("imported-secret");
    expect(await store.getOrCreateSecret()).toBe("imported-secret");
  });

  it("does not let an older get-or-create resurrect a cleared secret", async () => {
    const { secureStore, firstRead, events, value } = controlledStore();
    const store = new ExpoAuthSecretStore({ secureStore });
    const creating = store.getOrCreateSecret();
    await vi.waitFor(() => expect(secureStore.getItemAsync).toHaveBeenCalledOnce());

    const clearing = store.clearSecret();
    firstRead.resolve(null);
    await Promise.all([creating, clearing]);

    expect(events.at(-1)).toBe("clear");
    expect(value()).toBeNull();
  });
});
