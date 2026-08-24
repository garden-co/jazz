import { describe, expect, it } from "vitest";
import {
  authorBytesForSession,
  canonicalAuthorSubject,
  decodeCanonicalAuthorSubjectBytes,
  isUsableSubject,
  parseCanonicalAuthorSubject,
} from "./author-id.js";

const encoder = new TextEncoder();

function storedScalar(value: string): Uint8Array {
  return Uint8Array.from([0, ...encoder.encode(value)]);
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
      "not-json",
    ]) {
      expect(parseCanonicalAuthorSubject(value)).toBeNull();
    }
  });

  it("decodes logical and singly wrapped stored canonical author bytes", () => {
    const canonical = '["urn:jazz:test","author"]';
    expect(decodeCanonicalAuthorSubjectBytes(encoder.encode(canonical))).toBe(canonical);
    expect(decodeCanonicalAuthorSubjectBytes(storedScalar(canonical))).toBe(canonical);
  });

  it("rejects malformed provenance bytes instead of accepting arbitrary text", () => {
    const canonical = '["urn:jazz:test","author"]';
    for (const bytes of [
      encoder.encode("not-json"),
      Uint8Array.from([0, ...storedScalar(canonical)]),
      encoder.encode(`[ "urn:jazz:test", "author" ]`),
      encoder.encode('["urn:jazz:test"," "]'),
      Uint8Array.from([0x5b, 0xff, 0x5d]),
    ]) {
      expect(() => decodeCanonicalAuthorSubjectBytes(bytes)).toThrow(/canonical author subject/);
    }
  });
});
