import { createHash } from "node:crypto";
import { beforeEach, expect, it, vi } from "vitest";
import { accountToken } from "../accounts/enrollment.js";
import { accountAppId } from "../accounts/local-first.js";
import { accountRegistryUrl } from "../accounts/context.js";
import { createAccountManager } from "./create-account-manager.js";

const mocks = vi.hoisted(() => ({
  install: vi.fn(),
  get: vi.fn(),
  read: vi.fn(),
  set: vi.fn(),
  digest: vi.fn(),
}));
vi.mock("jazz-rn/relay", () => ({ installNativeForegroundRuntime: mocks.install }));
vi.mock("expo-crypto", () => ({
  CryptoDigestAlgorithm: { SHA256: "SHA-256" },
  digestStringAsync: mocks.digest,
}));
vi.mock("expo-secure-store", () => ({
  getItem: mocks.get,
  getItemAsync: mocks.read,
  setItem: mocks.set,
}));

let locked = false;
let sequence = 0;
let values: Map<string, string>;
const native = {
  withAccountStoreLock: vi.fn((callback: () => void) => {
    expect(locked).toBe(false);
    locked = true;
    try {
      callback();
    } finally {
      locked = false;
    }
  }),
  accountSecret: () => new Uint8Array(32).fill(++sequence),
  mintLocalFirstToken: (secret: Uint8Array) =>
    `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: `native-test-${secret[0]}` }))}.signature`,
};
const config = { appId: "expo-accounts", serverUrl: "https://core.example" };
const registry = accountRegistryUrl(config.serverUrl, config.appId);

beforeEach(() => {
  vi.clearAllMocks();
  sequence = 0;
  locked = false;
  values = new Map();
  mocks.install.mockReturnValue(native);
  mocks.digest.mockImplementation(async (_algorithm, text: string) =>
    createHash("sha256").update(text).digest("hex"),
  );
  mocks.read.mockImplementation(async (key: string) => values.get(key) ?? null);
  mocks.get.mockImplementation((key: string) => {
    expect(locked).toBe(true);
    return values.get(key) ?? null;
  });
  mocks.set.mockImplementation((key: string, value: string) => {
    expect(locked).toBe(true);
    values.set(key, value);
  });
});

it("merges independently prepared managers under the native lock and restores selection", async () => {
  const first = await createAccountManager(config);
  const second = await createAccountManager(config);
  const a = first.createLocalFirst();
  const b = second.createLocalFirst();
  await Promise.all([accountToken(a, registry), accountToken(b, registry)]);
  expect(a.id).not.toBe(b.id);
  const saved = JSON.parse([...values.values()][0]!);
  expect(saved.roots).toHaveLength(2);
  const restored = await createAccountManager(config);
  expect(restored.getLoggedIn()?.id).toBe(b.id);
  expect(native.withAccountStoreLock).toHaveBeenCalled();
});

it("isolates registry, application, environment and profile selections", async () => {
  for (const override of [
    {},
    { serverUrl: "https://other.example" },
    { appId: "other-app" },
    { env: "production" },
    { profile: "other" },
  ]) {
    const scoped = { ...config, ...override };
    const manager = await createAccountManager(scoped);
    expect(manager.getLoggedIn()).toBeUndefined();
    const account = manager.createLocalFirst();
    await accountToken(account, accountRegistryUrl(scoped.serverUrl, scoped.appId));
  }
  expect(values.size).toBe(5);
  for (const key of values.keys())
    expect(key).toMatch(/^jazz\.account-selection-v1\.[a-f0-9]{64}$/);
});

it("fails closed without native atomic storage and preserves stored roots on write failure", async () => {
  mocks.install.mockReturnValue({ ...native, withAccountStoreLock: undefined });
  await expect(createAccountManager(config)).rejects.toThrow("atomic account storage");
  expect(mocks.read).not.toHaveBeenCalled();
  mocks.install.mockReturnValue(native);
  const first = await createAccountManager(config);
  await accountToken(first.createLocalFirst(), registry);
  const before = [...values.entries()];
  mocks.set.mockImplementationOnce(() => {
    throw new Error("secure store unavailable");
  });
  await expect(accountToken(first.createLocalFirst(), registry)).rejects.toThrow(
    "secure store unavailable",
  );
  expect([...values.entries()]).toEqual(before);
  expect(locked).toBe(false);
});

it("restores the same selection through canonical application and default environment aliases", async () => {
  const initial = await createAccountManager(config);
  const account = initial.createLocalFirst();
  await accountToken(account, registry);
  const canonical = await createAccountManager({
    ...config,
    appId: accountAppId(config.appId),
    env: "dev",
  });
  expect(canonical.getLoggedIn()?.id).toBe(account.id);
  expect(values.size).toBe(1);
});
