import { afterEach, describe, expect, it, vi } from "vitest";
import { createAccountManager } from "../react-native/create-account-manager.js";
import { createNativeAccountTestSession } from "./native-account-session.js";

const mocks = vi.hoisted(() => ({ install: vi.fn() }));
vi.mock("jazz-rn/relay", () => ({ installNativeForegroundRuntime: mocks.install }));
vi.mock("../runtime/default-runtime-source.js", () => {
  throw new Error("Native device account admission must not load WASM");
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});

async function setup() {
  vi.stubGlobal("crypto", undefined);
  const jwt = `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.signature`;
  const pending = new Uint8Array(32).fill(4);
  const capability = new Uint8Array(32).fill(5);
  const native = {
    abiVersion: 1,
    accountSecret: vi.fn(() => new Uint8Array(32).fill(7)),
    mintLocalFirstToken: vi.fn(() => jwt),
    beginAccountSession: vi.fn((_request: string) => pending),
    attachAccountSchema: vi.fn(() => capability),
    releaseAccountSession: vi.fn(),
    refreshAccountSession: vi.fn(),
    openAttached: vi.fn(),
  };
  mocks.install.mockReturnValue(native);
  let value: string | null = null;
  const manager = await createAccountManager({
    appId: "native-device-accounts",
    serverUrl: "https://core.example",
    store: {
      async read() {
        return value;
      },
      async update(transform) {
        value = transform(value);
      },
    },
  });
  const account = manager.createLocalFirst();
  return {
    manager,
    account,
    native,
    pending,
    capability,
    config: { appId: "native-device-accounts", account },
  };
}

const schemaSource = JSON.stringify({ tables: {} });

describe("installed-device account lease", () => {
  it("owns production admission until explicit close without logging out the retained account", async () => {
    const { manager, account, native, pending, capability, config } = await setup();
    const lease = await createNativeAccountTestSession(config, schemaSource);
    expect(native.attachAccountSchema).toHaveBeenCalledExactlyOnceWith(pending, schemaSource);
    expect(native.releaseAccountSession).toHaveBeenCalledExactlyOnceWith(pending);
    expect(lease.account).toBe(account);
    expect(lease.capability).toEqual(capability);
    // Returned bytes cannot change the capability used for cleanup.
    lease.capability.fill(0);
    lease.close();
    lease.close();
    expect(native.releaseAccountSession.mock.calls).toEqual([[pending], [capability]]);
    expect(manager.getLoggedIn()).toBe(account);
  });

  it("keeps the original account and scope if caller configuration changes while opening", async () => {
    const { account, native, config } = await setup();
    const opening = createNativeAccountTestSession(config, schemaSource);
    config.account = { ...account };
    config.appId = "different-app";
    const lease = await opening;
    expect(lease.account).toBe(account);
    expect(JSON.parse(native.beginAccountSession.mock.calls[0]![0]!)).toMatchObject({
      app_id: "native-device-accounts",
      account_id: account.id,
    });
    lease.close();
  });

  it("revokes the native lease when its account logs out", async () => {
    const { manager, native, pending, capability, config } = await setup();
    const lease = await createNativeAccountTestSession(config, schemaSource);
    manager.logout();
    lease.close();
    expect(native.releaseAccountSession.mock.calls).toEqual([[pending], [capability]]);
    expect(manager.getLoggedIn()).toBeUndefined();
  });

  it("releases one-shot setup after schema rejection", async () => {
    const { native, pending, config } = await setup();
    native.attachAccountSchema.mockImplementation(() => {
      throw new Error("schema rejected");
    });
    await expect(createNativeAccountTestSession(config, schemaSource)).rejects.toThrow(
      "schema rejected",
    );
    expect(native.releaseAccountSession).toHaveBeenCalledExactlyOnceWith(pending);
  });

  it("rejects copied handles and mismatched authority before native admission", async () => {
    const { native, config } = await setup();
    await expect(
      createNativeAccountTestSession({ ...config, account: { ...config.account } }, schemaSource),
    ).rejects.toThrow();
    await expect(
      createNativeAccountTestSession(
        { ...config, serverUrl: "https://other.example" },
        schemaSource,
      ),
    ).rejects.toThrow("account_application_mismatch");
    expect(native.beginAccountSession).not.toHaveBeenCalled();
  });

  it("does not admit an account logged out during asynchronous preparation", async () => {
    const { manager, native, config } = await setup();
    const opening = createNativeAccountTestSession(config, schemaSource);
    manager.logout();
    await expect(opening).rejects.toThrow();
    expect(native.beginAccountSession).not.toHaveBeenCalled();
  });
});
