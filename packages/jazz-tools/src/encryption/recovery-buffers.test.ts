import { afterEach, expect, it, vi } from "vitest";
import { BrowserPasskeyBackup } from "../local-first-auth/passkey.js";
import { formatSecret } from "../local-first-auth/secret.js";
import { Encryption, type EncryptionNative } from "./index.js";

const parsed = vi.hoisted(() => ({ last: undefined as Uint8Array | undefined }));
vi.mock("../local-first-auth/secret.js", async (load) => {
  const actual = await load<typeof import("../local-first-auth/secret.js")>();
  return {
    ...actual,
    parseSecret: (secret: string) => (parsed.last = actual.parseSecret(secret)),
  };
});

afterEach(() => vi.unstubAllGlobals());

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
