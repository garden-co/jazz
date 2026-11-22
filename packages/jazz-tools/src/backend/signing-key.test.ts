import { randomBytes } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { backendSigningKeys, serverSigningKey } from "./signing-key.js";

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

describe("backendSigningKeys", () => {
  const label = "jazz-link-challenge-v1";

  it("derives from the app's secret without asking the core", async () => {
    const fetchShared = vi.fn(async () => randomBytes(32));
    const keys = backendSigningKeys({ appId: "app", secret: "secret", fetchShared });
    expect((await keys(label)).equals(serverSigningKey(label, "app", "secret"))).toBe(true);
    expect(fetchShared).not.toHaveBeenCalled();
  });

  it("derives from the core's key, asked for once by concurrent first uses", async () => {
    const shared = randomBytes(32);
    let answer!: (key: Buffer) => void;
    const fetchShared = vi.fn(() => new Promise<Buffer>((resolve) => (answer = resolve)));
    const keys = backendSigningKeys({ appId: "app", secret: undefined, fetchShared });
    const both = Promise.all([keys(label), keys("jazz-session-renewal-v1")]);
    answer(shared);
    const [challenge, renewal] = await both;
    expect(challenge.equals(serverSigningKey(label, "app", shared))).toBe(true);
    expect(renewal.equals(serverSigningKey("jazz-session-renewal-v1", "app", shared))).toBe(true);
    // Another instance with the same core key derives the same keys.
    const other = backendSigningKeys({
      appId: "app",
      secret: undefined,
      fetchShared: async () => shared,
    });
    expect((await other(label)).equals(challenge)).toBe(true);
    expect(fetchShared).toHaveBeenCalledTimes(1);
  });

  it("asks again on the next use after a failed request, never falling back", async () => {
    const shared = randomBytes(32);
    const fetchShared = vi
      .fn<() => Promise<Buffer | null>>()
      .mockRejectedValueOnce(Object.assign(new Error("503"), { status: 503 }))
      .mockResolvedValueOnce(shared);
    const keys = backendSigningKeys({ appId: "app", secret: undefined, fetchShared });
    await expect(keys(label)).rejects.toThrow("503");
    expect((await keys(label)).equals(serverSigningKey(label, "app", shared))).toBe(true);
    expect(fetchShared).toHaveBeenCalledTimes(2);
  });

  it("keeps keys of its own behind a core without the route, and says so once", async () => {
    const warn = vi.fn();
    const fetchShared = vi.fn(async () => null);
    const keys = backendSigningKeys({ appId: "app", secret: undefined, fetchShared, warn });
    const a = await keys(label);
    expect((await keys(label)).equals(a)).toBe(true);
    await keys("jazz-session-renewal-v1");
    expect(a).toHaveLength(32);
    expect(fetchShared).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0]![0]).toMatch(/jazz#5553.*Upgrade the core/);
  });
});
