import { describe, expect, it } from "vitest";
import { serverSigningKey } from "./signing-key.js";

describe("serverSigningKey", () => {
  it("derives the same key on every instance from the app's secret", () => {
    const a = serverSigningKey("jazz-link-challenge-v1", "app", "secret");
    expect(serverSigningKey("jazz-link-challenge-v1", "app", "secret").equals(a)).toBe(true);
    expect(serverSigningKey("jazz-link-challenge-v1", "other", "secret").equals(a)).toBe(false);
    expect(serverSigningKey("other-label", "app", "secret").equals(a)).toBe(false);
  });

  it("makes a key of its own without a secret, never one anyone could derive", () => {
    const a = serverSigningKey("jazz-link-challenge-v1", "app", undefined);
    const b = serverSigningKey("jazz-link-challenge-v1", "app", undefined);
    expect(a).toHaveLength(32);
    expect(a.equals(b)).toBe(false);
  });
});
