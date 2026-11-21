import { afterEach, describe, expect, it, vi } from "vitest";
import { Encryption, type EncryptionNative } from "./index.js";
import { formatSecret } from "../local-first-auth/secret.js";

// Recovery of an account the app signs in itself, backed up in a passkey where
// the browser has one, and as the 24-word phrase otherwise. The runtime is a
// recorder here: what matters is which secret reaches it, and when.

afterEach(() => vi.unstubAllGlobals());

function handle(options: { accountSecret?: () => string } = {}) {
  const calls: { op: string; args: Record<string, unknown> }[] = [];
  const native = {
    encryptionCall: async (op: string, args: Record<string, unknown>) => {
      calls.push({ op, args });
      return JSON.stringify(op === "recoveryRestore" ? "active" : null);
    },
    takeEncryptionReports: () => [],
    encryptionState: () => JSON.stringify({ status: "active" }),
  } as unknown as EncryptionNative;
  return { encryption: new Encryption(native, options), calls };
}

const hexOf = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

/** A browser with passkeys: remembers the user handle `create` was given. */
function passkeys() {
  let userHandle: Uint8Array | undefined;
  const events: string[] = [];
  vi.stubGlobal("navigator", {
    credentials: {
      create: async (options: { publicKey: { user: { id: Uint8Array; displayName: string } } }) => {
        events.push(`create:${options.publicKey.user.displayName}`);
        userHandle = new Uint8Array(options.publicKey.user.id);
      },
      get: async () => {
        const authenticatorData = new Uint8Array(37);
        authenticatorData[32] = 0x05;
        return { response: { authenticatorData, userHandle } };
      },
      preventSilentAccess: async () => {},
    },
  });
  return { events, handle: () => userHandle };
}

describe("recovery of an account the app signs in", () => {
  it("backs the secret up in a passkey first, then registers the same secret, and returns no phrase", async () => {
    const browser = passkeys();
    const { encryption, calls } = handle();
    const result = await encryption.recovery.enable({ appName: "App", accountName: "ada" });
    expect(result).toEqual({ method: "passkey" });
    expect(browser.events).toEqual(["create:ada (encryption recovery key)"]);
    expect(calls).toEqual([{ op: "recoveryRegister", args: { secret: hexOf(browser.handle()!) } }]);
  });

  it("restores a new device from the passkey with the secret it holds", async () => {
    const browser = passkeys();
    const { encryption, calls } = handle();
    await encryption.recovery.enable({ appName: "App", accountName: "ada" });
    calls.length = 0;
    expect(await encryption.recovery.restoreFromPasskey({ appName: "App" })).toBe("active");
    expect(calls).toEqual([{ op: "recoveryRestore", args: { secret: hexOf(browser.handle()!) } }]);
  });

  it("returns the 24-word phrase of the registered secret where there are no passkeys", async () => {
    vi.stubGlobal("navigator", {});
    const { encryption, calls } = handle();
    const result = await encryption.recovery.enable({ appName: "App", accountName: "ada" });
    expect(result.method).toBe("phrase");
    const phrase = (result as { phrase: string }).phrase;
    expect(phrase.split(" ")).toHaveLength(24);
    // The phrase restores what was registered.
    const registered = calls[0]!.args.secret;
    await encryption.recovery.restore(phrase);
    expect(calls[1]).toEqual({ op: "recoveryRestore", args: { secret: registered } });
  });

  it("falls back to the phrase when the user declines the passkey, and still registers", async () => {
    vi.stubGlobal("navigator", {
      credentials: {
        create: async () => {
          throw new DOMException("declined", "NotAllowedError");
        },
      },
    });
    const { encryption, calls } = handle();
    const result = await encryption.recovery.enable({ appName: "App", accountName: "ada" });
    expect(result.method).toBe("phrase");
    expect(calls.map((call) => call.op)).toEqual(["recoveryRegister"]);
  });

  it("is refused for a local-first account, whose recovery is its account secret", async () => {
    const { encryption } = handle({
      accountSecret: () => formatSecret(new Uint8Array(32).fill(1)),
    });
    await expect(
      encryption.recovery.enable({ appName: "A", accountName: "b" }),
    ).rejects.toMatchObject({
      code: "device-account",
    });
    await expect(encryption.recovery.restoreFromPasskey({ appName: "A" })).rejects.toMatchObject({
      code: "device-account",
    });
  });
});
