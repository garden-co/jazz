import { describe, it, expect } from "vitest";
import { RecoveryPhrase, RecoveryPhraseError } from "./recovery-phrase.js";

const ZERO_SECRET = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const ZERO_PHRASE =
  "abandon abandon abandon abandon abandon abandon abandon abandon " +
  "abandon abandon abandon abandon abandon abandon abandon abandon " +
  "abandon abandon abandon abandon abandon abandon abandon art";

describe("RecoveryPhrase.fromSecret", () => {
  it("encodes a 32-byte base64url secret as 24 BIP39 English words", () => {
    const phrase = RecoveryPhrase.fromSecret(ZERO_SECRET);
    expect(phrase.split(" ")).toHaveLength(24);
    expect(phrase).toBe(ZERO_PHRASE);
  });

  it("produces a lowercase single-space-separated phrase", () => {
    const phrase = RecoveryPhrase.fromSecret(ZERO_SECRET);
    expect(phrase).toBe(phrase.toLowerCase());
    expect(phrase).not.toMatch(/\s{2,}/);
    expect(phrase.trim()).toBe(phrase);
  });
});

describe("RecoveryPhrase.toSecret", () => {
  it("decodes the canonical phrase back to the original secret", () => {
    expect(RecoveryPhrase.toSecret(ZERO_PHRASE)).toBe(ZERO_SECRET);
  });
});

describe("RecoveryPhrase round-trip", () => {
  it("round-trips 10 random 32-byte secrets", () => {
    for (let i = 0; i < 10; i += 1) {
      const bytes = new Uint8Array(32);
      crypto.getRandomValues(bytes);
      let binary = "";
      for (const b of bytes) binary += String.fromCharCode(b);
      const secret = btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

      const phrase = RecoveryPhrase.fromSecret(secret);
      expect(RecoveryPhrase.toSecret(phrase)).toBe(secret);
    }
  });
});

const VALID_PHRASE =
  "abandon abandon abandon abandon abandon abandon abandon abandon " +
  "abandon abandon abandon abandon abandon abandon abandon abandon " +
  "abandon abandon abandon abandon abandon abandon abandon art";

function expectCode(fn: () => unknown, code: RecoveryPhraseError["code"]) {
  try {
    fn();
  } catch (err) {
    expect(err).toBeInstanceOf(RecoveryPhraseError);
    expect((err as RecoveryPhraseError).code).toBe(code);
    return;
  }
  throw new Error(`expected ${code} but function did not throw`);
}

describe("RecoveryPhrase.toSecret — normalization", () => {
  it("accepts UPPER CASE", () => {
    expect(RecoveryPhrase.toSecret(VALID_PHRASE.toUpperCase())).toBeTypeOf("string");
  });

  it("accepts leading and trailing whitespace", () => {
    expect(RecoveryPhrase.toSecret(`   ${VALID_PHRASE}\n\t`)).toBeTypeOf("string");
  });

  it("accepts multiple spaces, tabs, and newlines between words", () => {
    const weird = VALID_PHRASE.split(" ").join("  \n\t");
    expect(RecoveryPhrase.toSecret(weird)).toBeTypeOf("string");
  });
});

describe("RecoveryPhrase.toSecret — errors", () => {
  it("throws invalid-length for 23 words", () => {
    const short = VALID_PHRASE.split(" ").slice(0, 23).join(" ");
    expectCode(() => RecoveryPhrase.toSecret(short), "invalid-length");
  });

  it("throws invalid-length for 25 words", () => {
    const long = VALID_PHRASE + " art";
    expectCode(() => RecoveryPhrase.toSecret(long), "invalid-length");
  });

  it("throws invalid-word when a word is not in the list", () => {
    const bad = VALID_PHRASE.replace(/art$/, "notaword");
    expectCode(() => RecoveryPhrase.toSecret(bad), "invalid-word");
  });

  it("throws invalid-checksum when the last word does not match", () => {
    const badChecksum = VALID_PHRASE.replace(/art$/, "zoo");
    expectCode(() => RecoveryPhrase.toSecret(badChecksum), "invalid-checksum");
  });
});

describe("RecoveryPhrase.fromSecret — errors", () => {
  it("throws invalid-secret for a non-base64url string", () => {
    expectCode(() => RecoveryPhrase.fromSecret("this is not base64url!!!"), "invalid-secret");
  });

  it("throws invalid-secret for a base64url string that decodes to !=32 bytes", () => {
    const tooShort = "AAAAAAAAAAAAAAAAAAAAAA";
    expectCode(() => RecoveryPhrase.fromSecret(tooShort), "invalid-secret");
  });

  it("throws invalid-secret for non-string input", () => {
    expectCode(() => RecoveryPhrase.fromSecret(undefined as unknown as string), "invalid-secret");
  });
});
