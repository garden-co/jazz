import { afterEach, describe, expect, it, vi } from "vitest";
import { createAccountManager, createDb } from "./index.js";
import { accountToken } from "../accounts/enrollment.js";
import { accountRegistryUrl } from "../accounts/context.js";

const mocks = vi.hoisted(() => ({ install: vi.fn() }));
vi.mock("jazz-rn/relay", () => ({ installNativeForegroundRuntime: mocks.install }));
vi.mock("../runtime/default-runtime-source.js", () => {
  throw new Error("React Native account preparation must not load the WASM runtime");
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});

function store() {
  let value: string | null = null;
  return {
    async read() {
      return value;
    },
    async update(transform: (current: string | null) => string) {
      value = transform(value);
    },
  };
}

describe("React Native account preparation", () => {
  it("creates and restores a synchronous account using native entropy and signing without a context", async () => {
    vi.stubGlobal("crypto", undefined);
    const jwt = `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.signature`;
    const native = {
      abiVersion: 1,
      accountSecret: vi.fn(() => new Uint8Array(32).fill(7)),
      mintLocalFirstToken: vi.fn(() => jwt),
      openAttached: vi.fn(),
    };
    mocks.install.mockReturnValue(native);
    const config = { appId: "native-accounts", serverUrl: "https://core.example", store: store() };
    const manager = await createAccountManager(config);
    const account = manager.createLocalFirst();
    expect(manager.getLoggedIn()).toBe(account);
    await expect(
      accountToken(account, accountRegistryUrl(config.serverUrl, config.appId)),
    ).resolves.toBe(jwt);
    const restored = await createAccountManager(config);
    expect(restored.getLoggedIn()?.id).toBe(account.id);
    expect(native.accountSecret).toHaveBeenCalledOnce();
    expect(native.mintLocalFirstToken).toHaveBeenCalledWith(
      new Uint8Array(32).fill(7),
      config.appId,
      3600,
      expect.any(Number),
    );
    expect(native.openAttached).not.toHaveBeenCalled();
  });

  it("retains and restores native account identity without browser decoder or Node Buffer globals", async () => {
    const subject = "native-café-🧭";
    const jwt = `e30.${Buffer.from(JSON.stringify({ iss: "urn:jazz:local-first", sub: subject })).toString("base64url")}.signature`;
    vi.stubGlobal("TextDecoder", undefined);
    vi.stubGlobal("Buffer", undefined);
    mocks.install.mockReturnValue({
      accountSecret: () => new Uint8Array(32).fill(7),
      mintLocalFirstToken: () => jwt,
    });
    const config = {
      appId: "native-missing-globals",
      serverUrl: "https://core.example",
      store: store(),
    };
    const manager = await createAccountManager(config);
    const account = manager.createLocalFirst();
    expect(account.identity.subject).toBe(subject);
    await expect(
      accountToken(account, accountRegistryUrl(config.serverUrl, config.appId)),
    ).resolves.toBe(jwt);
    const restored = await createAccountManager(config);
    expect(restored.getLoggedIn()?.id).toBe(account.id);
    expect(restored.getLoggedIn()?.identity.subject).toBe(subject);
  });

  it("rejects a native build without account crypto during preparation", async () => {
    mocks.install.mockReturnValue({ abiVersion: 1, openAttached: vi.fn() });
    await expect(
      createAccountManager({
        appId: "native-accounts",
        serverUrl: "https://core.example",
        store: store(),
      }),
    ).rejects.toThrow("native build with account crypto support");
  });

  it("prepares a handle-bound local context and releases an unused setup on normal shutdown", async () => {
    vi.stubGlobal("crypto", undefined);
    const subject = "00000000-0000-4000-8000-000000000001";
    const jwt = `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: subject }))}.signature`;
    const capability = new Uint8Array(32).fill(9);
    const native = {
      abiVersion: 1,
      accountSecret: vi.fn(() => new Uint8Array(32).fill(7)),
      mintLocalFirstToken: vi.fn(() => jwt),
      beginAccountSession: vi.fn((_request: string) => capability),
      attachAccountSchema: vi.fn(),
      releaseAccountSession: vi.fn(),
      refreshAccountSession: vi.fn(),
      openAttached: vi.fn(),
    };
    mocks.install.mockReturnValue(native);
    const config = { appId: "native-context", serverUrl: "https://core.example", store: store() };
    const manager = await createAccountManager(config);
    const account = manager.createLocalFirst();
    const db = await createDb({ appId: config.appId, account });
    expect(JSON.parse(native.beginAccountSession.mock.calls[0]![0])).toEqual({
      registry: accountRegistryUrl(config.serverUrl, config.appId),
      app_id: config.appId,
      env: "dev",
      account_id: account.id,
      issuer: "urn:jazz:local-first",
      subject,
      jwt,
      server_url: null,
      claims: {},
    });
    expect(native.openAttached).not.toHaveBeenCalled();
    await db.refreshAccountAuth(account);
    expect(native.refreshAccountSession).toHaveBeenCalledExactlyOnceWith(
      capability,
      JSON.stringify({ jwt, claims: {} }),
    );
    await db.shutdown();
    await db.shutdown();
    expect(native.releaseAccountSession).toHaveBeenCalledExactlyOnceWith(capability);
    expect(manager.getLoggedIn()).toBe(account);
  });
});
