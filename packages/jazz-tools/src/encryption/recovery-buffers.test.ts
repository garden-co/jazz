import { afterEach, expect, it, vi } from "vitest";
import { Encryption, type EncryptionNative } from "./index.js";

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
