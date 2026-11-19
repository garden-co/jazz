import { describe, expect, it } from "vitest";
import { retryAfterKey } from "./key-retry.js";

const pending = () => new Error("NotObserved: key-pending: space aaaa-1/bbbb-2 has no held key");

describe("retryAfterKey", () => {
  it("runs a write that needs no key once", () => {
    let calls = 0;
    expect(
      retryAfterKey(
        () => ++calls,
        async () => true,
      ),
    ).toBe(1);
  });

  it("waits for the key, then tries again and succeeds", async () => {
    let calls = 0;
    let prepared = 0;
    const result = await retryAfterKey(
      () => {
        if (++calls < 3) throw pending();
        return "written";
      },
      async (space) => {
        expect(space).toEqual({ scopeId: "aaaa-1", identifier: "bbbb-2" });
        return ++prepared >= 2;
      },
      5_000,
    );
    expect(result).toBe("written");
    expect(prepared).toBeGreaterThanOrEqual(2);
  });

  it("never retries a refusal that is not about a key", () => {
    let prepared = 0;
    expect(() =>
      retryAfterKey(
        () => {
          throw new Error("WriteRejected: authorization_denied: INSERT policy denied");
        },
        async () => {
          prepared += 1;
          return true;
        },
      ),
    ).toThrow(/authorization_denied/);
    expect(prepared).toBe(0);
  });

  it("gives up at the deadline with the key-pending error", async () => {
    const started = Date.now();
    await expect(
      retryAfterKey(
        () => {
          throw pending();
        },
        async () => false,
        400,
      ),
    ).rejects.toThrow(/key-pending/);
    expect(Date.now() - started).toBeLessThan(2_000);
  });

  it("is bounded by the window even when making the space ready never answers", async () => {
    const started = Date.now();
    await expect(
      retryAfterKey(
        () => {
          throw pending();
        },
        () => new Promise<boolean>(() => undefined),
        400,
      ),
    ).rejects.toThrow(/key-pending/);
    expect(Date.now() - started).toBeLessThan(2_000);
  });
});
