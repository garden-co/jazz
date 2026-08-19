import { describe, expect, test } from "vitest";
import { authorBytesForSubject, isUsableSubject } from "./author-id.js";

describe("authorBytesForSubject", () => {
  test("preserves UUID subjects", () => {
    expect(Array.from(authorBytesForSubject("123e4567-e89b-12d3-a456-426614174000"))).toEqual([
      0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40,
      0x00,
    ]);
  });

  test("maps non-UUID subjects to UUIDv5 in the URL namespace", () => {
    expect(Array.from(authorBytesForSubject("better-auth-user"))).toEqual([
      0x07, 0x96, 0x0c, 0x5e, 0x28, 0xbb, 0x5e, 0xd4, 0xb4, 0x3a, 0x06, 0xf5, 0x9a, 0x65, 0xe1,
      0x1c,
    ]);
  });

  test("accepts only explicit UUID spellings and maps every other subject exactly", () => {
    const canonical = "123e4567-e89b-12d3-a456-426614174000";
    const direct = Array.from(authorBytesForSubject(canonical));
    for (const subject of [
      canonical,
      "123E4567-E89B-12D3-A456-426614174000",
      "123e4567e89b12d3a456426614174000",
      "123E4567E89B12D3A456426614174000",
    ]) {
      expect(Array.from(authorBytesForSubject(subject))).toEqual(direct);
    }

    // Keep these literal vectors identical to crates/jazz/src/tools/identity.rs.
    for (const [subject, expected] of [
      ["123e4567e89b12d3-a456426614174000", "bf38a3ac-d534-5b16-8d93-14ddea925c47"],
      ["workos_user_01J8Y3K4M5N6P7Q8R9S0T1U2V3", "001ee09d-5506-554f-9581-46bf449082bd"],
      ["\u0085", "8fec6819-1507-5190-a4e6-d61b73fa4091"],
      ["\ufeff", "aef4b8ca-08ab-51a8-adab-bd8c111efbe7"],
    ]) {
      expect(formatUuid(authorBytesForSubject(subject))).toBe(expected);
      expect(Array.from(authorBytesForSubject(subject))).not.toEqual(direct);
    }
    for (const subject of [
      "123e4567-e89b-12d3-a4564-26614174000", // moved hyphen
      "123e4567-e89b-12d3-a456426614174000", // missing hyphen
      "123e4567--e89b-12d3-a456-426614174000", // arbitrary extra hyphen
      " 123e4567-e89b-12d3-a456-426614174000 ",
      "urn:uuid:123e4567-e89b-12d3-a456-426614174000",
      "{123e4567-e89b-12d3-a456-426614174000}",
      "WORKOS_USER_01J8Y3K4M5N6P7Q8R9S0T1U2V3",
    ]) {
      expect(Array.from(authorBytesForSubject(subject))).not.toEqual(direct);
    }
    expect(Array.from(authorBytesForSubject("workos_user_01J8Y3K4M5N6P7Q8R9S0T1U2V3"))).not.toEqual(
      Array.from(authorBytesForSubject("WORKOS_USER_01J8Y3K4M5N6P7Q8R9S0T1U2V3")),
    );
  });

  test("uses ASCII-only blank-subject validation", () => {
    for (const subject of ["", " \t\n\v\f\r "]) {
      expect(isUsableSubject(subject)).toBe(false);
    }
    for (const subject of ["\u0085", "\ufeff", " subject "]) {
      expect(isUsableSubject(subject)).toBe(true);
    }
  });
});

function formatUuid(bytes: Uint8Array): string {
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
