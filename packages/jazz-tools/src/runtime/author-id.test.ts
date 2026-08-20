import { describe, expect, test } from "vitest";
import { authorBytesForSubject } from "./author-id.js";

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
});
