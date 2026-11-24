import { afterEach, expect, it, vi } from "vitest";
import { BrowserPasskeyBackup } from "../local-first-auth/passkey.js";
import { resolveAccountRuntimeConfig } from "../accounts/context.js";
import { makeFakeAccount } from "../react-core/test-utils.js";
import {
  beginNativeAccountSession,
  resolveNativeSession,
} from "../react-native/account-session.js";
import { app } from "../../tests/fixtures/encrypted/jazz.generated.js";
import { formatSecret } from "../local-first-auth/secret.js";
import { Encryption, type EncryptionNative } from "./index.js";
import { buildEncryption } from "./build.js";
import { memoryPrivateStore } from "./private-store.js";

const parsed = vi.hoisted(() => ({ last: undefined as Uint8Array | undefined }));
vi.mock("../local-first-auth/secret.js", async (load) => {
  const actual = await load<typeof import("../local-first-auth/secret.js")>();
  return {
    ...actual,
    parseSecret: (secret: string) => (parsed.last = actual.parseSecret(secret)),
  };
});

afterEach(() => vi.unstubAllGlobals());

it("closes a retained encryption handle and discards a late native result", async () => {
  let answer!: (output: string) => void;
  const native = vi.fn(() => new Promise<string>((resolve) => (answer = resolve)));
  const encryption = new Encryption({
    encryptionCall: native,
    encryptionState: () => "{}",
    takeEncryptionReports: () => [],
  });
  const pending = encryption.deviceId();
  encryption.stop();
  answer(JSON.stringify("old-device"));
  await expect(pending).rejects.toMatchObject({ code: "closed" });
  expect(() => encryption.recovery.create()).toThrow(/closed/);
  await expect(encryption.devices.list()).rejects.toMatchObject({ code: "closed" });
  expect(native).toHaveBeenCalledOnce();
});

it.each([false, true])(
  "clears the generated recovery key after passkey backup (registration fails: %s)",
  async (refused) => {
    let generated: Uint8Array | undefined;
    vi.stubGlobal("crypto", {
      getRandomValues(bytes: Uint8Array) {
        if (bytes.length === 32) generated = bytes;
        return bytes.fill(7);
      },
    });
    const backup = vi.fn(async () => ({}));
    vi.stubGlobal("navigator", { credentials: { create: backup } });
    const native: EncryptionNative = {
      encryptionCall: async (_op, args) => {
        expect(args?.secret).toBe("07".repeat(32));
        if (refused) throw new Error("store: registration failed");
        return "null";
      },
      encryptionState: () => "{}",
      takeEncryptionReports: () => [],
    };
    const recovery = new Encryption(native, {}).recovery;
    const result = recovery.enable({ appName: "App", accountName: "alice" });
    if (refused) await expect(result).rejects.toThrow("registration failed");
    else await expect(result).resolves.toEqual({ method: "passkey" });
    expect(backup).toHaveBeenCalledOnce();
    expect(generated).toEqual(new Uint8Array(32));
  },
);

it.each([false, true])(
  "clears the WebAuthn user ID after credentials.create settles (rejected: %s)",
  async (rejected) => {
    const original = new Uint8Array(32).fill(7);
    let userId: Uint8Array | undefined;
    const create = vi.fn(async ({ publicKey }: { publicKey: { user: { id: Uint8Array } } }) => {
      userId = publicKey.user.id;
      expect(userId).toEqual(original);
      if (rejected) throw new Error("credential creation rejected");
      return {};
    });
    vi.stubGlobal("navigator", { credentials: { create } });
    const result = new BrowserPasskeyBackup({ appName: "App" }).backup(
      formatSecret(original),
      "alice",
    );
    if (rejected)
      await expect(result).rejects.toMatchObject({
        code: "create-failed",
        cause: new Error("credential creation rejected"),
      });
    else await result;
    expect(userId).toEqual(new Uint8Array(32));
  },
);

it("clears the decoded secret when the passkey challenge cannot be generated", async () => {
  vi.stubGlobal("crypto", {
    getRandomValues() {
      throw new Error("no randomness");
    },
  });
  const create = vi.fn();
  vi.stubGlobal("navigator", { credentials: { create } });
  const result = new BrowserPasskeyBackup({ appName: "App" }).backup(
    formatSecret(new Uint8Array(32).fill(7)),
    "alice",
  );
  const failure = await result.then(
    () => undefined,
    (error: unknown) => error,
  );
  expect(parsed.last).toEqual(new Uint8Array(32));
  expect(create).not.toHaveBeenCalled();
  expect(failure).toMatchObject({ code: "create-failed" });
});

it("passes the original account secret to the Node and browser runtime factory", async () => {
  const deviceAccountSecret = formatSecret(new Uint8Array(32).fill(7));
  const fromBindings = vi.fn(() => ({
    ready: async () => {},
    takeStoreRequests: () => [],
    replyStore: () => false,
  }));
  parsed.last = undefined;
  await buildEncryption({
    host: { fromBindings },
    app,
    appId: "app-1",
    environment: "test",
    accountId: "alice",
    deviceAccountSecret,
    store: memoryPrivateStore(),
  });
  expect(fromBindings).toHaveBeenCalledWith(
    expect.any(String),
    "app-1",
    "test",
    "alice",
    deviceAccountSecret,
  );
  expect(parsed.last).toBeUndefined();
});

it("passes the original account secret to the React Native host without decoding it", async () => {
  const config = await resolveAccountRuntimeConfig({ scope: "app-1", account: makeFakeAccount() });
  config.encryption = { app, accountSecret: () => formatSecret(new Uint8Array(32).fill(7)) };
  parsed.last = undefined;
  const beginAccountSession = vi.fn((request: string) => {
    expect(JSON.parse(request).device_account_secret).toBe(
      formatSecret(new Uint8Array(32).fill(7)),
    );
    expect(parsed.last).toBeUndefined();
    return new Uint8Array(32);
  });
  beginNativeAccountSession(
    {
      abiVersion: 1,
      beginAccountSession,
      attachAccountSchema: vi.fn(),
      releaseAccountSession: vi.fn(),
      refreshAccountSession: vi.fn(),
      openAttached: vi.fn(),
    },
    config,
    resolveNativeSession(config),
    undefined,
    true,
  );
  expect(beginAccountSession).toHaveBeenCalledOnce();
});
