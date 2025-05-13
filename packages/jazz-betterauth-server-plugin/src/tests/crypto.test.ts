import { PureJSCrypto } from "cojson/dist/crypto/PureJSCrypto";
import { describe, expect, it } from "vitest";
import { passwordDecrypt, passwordEncrypt } from "../crypto.js";

describe("crypto", () => {
  it("should encrypt & decrypt", async () => {
    const crypto = new PureJSCrypto();
    const plaintext = "This is a test string.";
    for (let i = 0; i < 10; i++) {
      const randomPassphrase = Array.from(crypto.randomBytes(32))
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");
      const [ciphertext, salt] = await passwordEncrypt(
        plaintext,
        randomPassphrase,
      );
      expect(ciphertext).not.toBe(plaintext);
      const decrypted = await passwordDecrypt(
        ciphertext,
        randomPassphrase,
        salt,
      );
      expect(decrypted).toBe(plaintext);
    }
  });
});
