import { describe, expect, it } from "vitest";
import { sessionAuthor } from "../lib/identity";

describe("canonical session authors", () => {
  it("keeps the issuer in the identity when two providers reuse one subject", () => {
    const first = sessionAuthor("https://auth-one.example", "same-subject");
    const second = sessionAuthor("https://auth-two.example", "same-subject");

    expect(first).toBe('["https://auth-one.example","same-subject"]');
    expect(second).toBe('["https://auth-two.example","same-subject"]');
    expect(first).not.toBe(second);
  });
});
