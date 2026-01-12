import { describe, expect, it } from "vitest";
import {
  base64UrlToUint8Array,
  uint8ArrayToBase64Url,
} from "../auth/passkey-utils";

describe("passkey-utils", () => {
  describe("uint8ArrayToBase64Url", () => {
    it("should encode an empty array", () => {
      const bytes = new Uint8Array([]);
      expect(uint8ArrayToBase64Url(bytes)).toBe("");
    });

    it("should encode a simple byte array", () => {
      // "Hello" in bytes
      const bytes = new Uint8Array([72, 101, 108, 108, 111]);
      expect(uint8ArrayToBase64Url(bytes)).toBe("SGVsbG8");
    });

    it("should use base64url alphabet (- instead of +)", () => {
      // Bytes that produce + in standard base64
      const bytes = new Uint8Array([251, 239]); // produces "++" in base64
      const result = uint8ArrayToBase64Url(bytes);
      expect(result).not.toContain("+");
      expect(result).toContain("-");
    });

    it("should use base64url alphabet (_ instead of /)", () => {
      // Bytes that produce / in standard base64
      const bytes = new Uint8Array([255, 255]); // produces "//" in base64
      const result = uint8ArrayToBase64Url(bytes);
      expect(result).not.toContain("/");
      expect(result).toContain("_");
    });

    it("should omit padding", () => {
      // Single byte produces base64 with padding
      const bytes = new Uint8Array([1]);
      const result = uint8ArrayToBase64Url(bytes);
      expect(result).not.toContain("=");
    });

    it("should handle typical credential payload size (56 bytes)", () => {
      // secretSeedLength (32) + shortHashLength (19) = 51 bytes
      const bytes = new Uint8Array(56);
      for (let i = 0; i < 56; i++) {
        bytes[i] = i;
      }
      const result = uint8ArrayToBase64Url(bytes);
      expect(result.length).toBeGreaterThan(0);
      expect(result).not.toContain("+");
      expect(result).not.toContain("/");
      expect(result).not.toContain("=");
    });
  });

  describe("base64UrlToUint8Array", () => {
    it("should decode an empty string", () => {
      const result = base64UrlToUint8Array("");
      expect(result).toEqual(new Uint8Array([]));
    });

    it("should decode a simple base64url string", () => {
      // "SGVsbG8" is "Hello" in base64url
      const result = base64UrlToUint8Array("SGVsbG8");
      expect(result).toEqual(new Uint8Array([72, 101, 108, 108, 111]));
    });

    it("should handle - character (base64url for +)", () => {
      const result = base64UrlToUint8Array("--8");
      expect(result).toBeInstanceOf(Uint8Array);
    });

    it("should handle _ character (base64url for /)", () => {
      const result = base64UrlToUint8Array("__8");
      expect(result).toBeInstanceOf(Uint8Array);
    });

    it("should add padding automatically", () => {
      // "AQ" needs padding to become "AQ==" for valid base64
      const result = base64UrlToUint8Array("AQ");
      expect(result).toEqual(new Uint8Array([1]));
    });

    it("should handle strings that need 1 padding char", () => {
      // 3 chars needs 1 padding
      const result = base64UrlToUint8Array("ABC");
      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(2);
    });

    it("should handle strings that need 2 padding chars", () => {
      // 2 chars needs 2 padding
      const result = base64UrlToUint8Array("AB");
      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(1);
    });
  });

  describe("roundtrip encoding/decoding", () => {
    it("should roundtrip empty array", () => {
      const original = new Uint8Array([]);
      const encoded = uint8ArrayToBase64Url(original);
      const decoded = base64UrlToUint8Array(encoded);
      expect(decoded).toEqual(original);
    });

    it("should roundtrip simple bytes", () => {
      const original = new Uint8Array([1, 2, 3, 4, 5]);
      const encoded = uint8ArrayToBase64Url(original);
      const decoded = base64UrlToUint8Array(encoded);
      expect(decoded).toEqual(original);
    });

    it("should roundtrip all byte values", () => {
      const original = new Uint8Array(256);
      for (let i = 0; i < 256; i++) {
        original[i] = i;
      }
      const encoded = uint8ArrayToBase64Url(original);
      const decoded = base64UrlToUint8Array(encoded);
      expect(decoded).toEqual(original);
    });

    it("should roundtrip credential-sized payload", () => {
      // Typical passkey credential payload: secretSeed + accountID hash
      const original = new Uint8Array(56);
      crypto.getRandomValues(original);
      const encoded = uint8ArrayToBase64Url(original);
      const decoded = base64UrlToUint8Array(encoded);
      expect(decoded).toEqual(original);
    });

    it("should roundtrip random data of various sizes", () => {
      for (const size of [1, 2, 3, 10, 32, 64, 100, 256]) {
        const original = new Uint8Array(size);
        crypto.getRandomValues(original);
        const encoded = uint8ArrayToBase64Url(original);
        const decoded = base64UrlToUint8Array(encoded);
        expect(decoded).toEqual(original);
      }
    });
  });
});
