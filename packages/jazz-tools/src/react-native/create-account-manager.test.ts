import { afterEach, describe, expect, it, vi } from "vitest";
import { createAccountManager } from "./index.js";
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
});
