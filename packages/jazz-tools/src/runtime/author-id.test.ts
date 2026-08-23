import { describe, expect, it } from "vitest";
import { authorBytesForSession, canonicalAuthorSubject, isUsableSubject } from "./author-id.js";

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
    expect(canonicalAuthorSubject("issuer", " User ")).toBe('["issuer"," User "]');
    for (const blank of ["", " ", "\t\n\r"])
      expect(() => canonicalAuthorSubject("issuer", blank)).toThrow(/nonempty/);
  });
});
