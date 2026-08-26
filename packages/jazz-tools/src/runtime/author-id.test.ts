import { describe, expect, it } from "vitest";
import {
  authorBytesForSession,
  canonicalAuthorSubject,
  decodeCanonicalAuthorSubjectBytes,
  isPortableAuthorComponent,
  isUsableSubject,
  parseCanonicalAuthorSubject,
} from "./author-id.js";

const encoder = new TextEncoder();

function storedScalar(value: string): Uint8Array {
  return Uint8Array.from([2, ...encoder.encode(value)]);
}

describe("canonical author subjects", () => {
  it("uses canonical JSON of the exact issuer and subject", () => {
    const canonical = canonicalAuthorSubject("https://issuer.example", "opaque:subject");
    expect(canonical).toBe('["https://issuer.example","opaque:subject"]');
    expect(
      new TextDecoder().decode(
        authorBytesForSession({
          issuer: "https://issuer.example",
          user_id: "opaque:subject",
        }),
      ),
    ).toBe(canonical);
  });

  it("distinguishes the same subject issued by different authorities", () => {
    expect(canonicalAuthorSubject("issuer-a", "user")).not.toBe(
      canonicalAuthorSubject("issuer-b", "user"),
    );
  });

  it("preserves opaque spelling and rejects only ASCII-blank components", () => {
    expect(isUsableSubject(" opaque ")).toBe(true);
    expect(isUsableSubject("\u0085")).toBe(true);
    expect(isUsableSubject("\uFEFF")).toBe(true);
    expect(canonicalAuthorSubject("issuer", " User ")).toBe('["issuer"," User "]');
    expect(canonicalAuthorSubject(" issuer ", "\u0085")).toBe(
      JSON.stringify([" issuer ", "\u0085"]),
    );
    for (const blank of ["", " ", "\t\n\r"])
      expect(() => canonicalAuthorSubject("issuer", blank)).toThrow(/nonempty/);
  });

  it("rejects unpaired UTF-16 surrogates while preserving valid code points", () => {
    expect(isPortableAuthorComponent("\ud83d\ude80")).toBe(true);
    expect(isPortableAuthorComponent("rocket-\ud83d\ude80")).toBe(true);
    expect(isPortableAuthorComponent("\ud800")).toBe(false);
    expect(isPortableAuthorComponent("\udc00")).toBe(false);
    expect(() => canonicalAuthorSubject("issuer", "\ud800")).toThrow(/portable/);
    expect(() => canonicalAuthorSubject("issuer", "\udc00")).toThrow(/portable/);

    const canonical = canonicalAuthorSubject("issuer", "🚀");
    expect(canonical).toBe('["issuer","🚀"]');
    expect(parseCanonicalAuthorSubject(canonical)?.user_id).toBe("🚀");
  });

  it("strictly parses only exact canonical two-string JSON", () => {
    const canonical = '["https://issuer.example","opaque:subject"]';
    expect(parseCanonicalAuthorSubject(canonical)).toEqual({
      issuer: "https://issuer.example",
      user_id: "opaque:subject",
      canonical,
    });

    for (const value of [
      `[ "https://issuer.example", "opaque:subject" ]`,
      '["https://issuer.example","opaque:subject","extra"]',
      '{"issuer":"https://issuer.example","user_id":"opaque:subject"}',
      '["https://issuer.example"," "]',
      '[" ","opaque:subject"]',
      String.raw`["https://issuer.example","\ud800"]`,
      String.raw`["https://issuer.example","\udc00"]`,
      String.raw`["https://issuer.example","\ud83d\ude80"]`,
      "not-json",
    ]) {
      expect(parseCanonicalAuthorSubject(value)).toBeNull();
    }
  });

  it("decodes only the current singly wrapped stored canonical author bytes", () => {
    const canonical = '["urn:jazz:test","author"]';
    expect(decodeCanonicalAuthorSubjectBytes(storedScalar(canonical))).toBe(canonical);
    expect(() =>
      decodeCanonicalAuthorSubjectBytes(Uint8Array.from([0, ...encoder.encode(canonical)])),
    ).toThrow(/canonical author subject/);
    expect(() => decodeCanonicalAuthorSubjectBytes(encoder.encode(canonical))).toThrow(
      /canonical author subject/,
    );
  });

  it("rejects malformed provenance bytes instead of accepting arbitrary text", () => {
    const canonical = '["urn:jazz:test","author"]';
    for (const bytes of [
      encoder.encode("not-json"),
      Uint8Array.from([2, ...storedScalar(canonical)]),
      encoder.encode(`[ "urn:jazz:test", "author" ]`),
      encoder.encode('["urn:jazz:test"," "]'),
      encoder.encode(String.raw`["urn:jazz:test","\ud800"]`),
      encoder.encode(String.raw`["urn:jazz:test","\ud83d\ude80"]`),
      Uint8Array.from([0x5b, 0xff, 0x5d]),
    ]) {
      expect(() => decodeCanonicalAuthorSubjectBytes(bytes)).toThrow(/canonical author subject/);
    }
  });
});
