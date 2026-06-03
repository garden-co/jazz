import { describe, it, expect } from "vitest";
import { sleep, uniqueAppId, waitForCondition, withTimeout, waitForQuery } from "./support";

describe("support", () => {
  it("uniqueAppId returns distinct, labelled ids", () => {
    const a = uniqueAppId("x");
    const b = uniqueAppId("x");
    expect(a).not.toBe(b);
    expect(a.startsWith("itest-x-")).toBe(true);
  });

  it("sleep resolves", async () => {
    await sleep(10);
  });

  it("waitForCondition resolves when predicate flips true", async () => {
    let flag = false;
    setTimeout(() => {
      flag = true;
    }, 60);
    await waitForCondition(() => flag, 1000, "flag");
  });

  it("waitForCondition rejects with the message on timeout", async () => {
    await expect(waitForCondition(() => false, 120, "never-true")).rejects.toThrow(/never-true/);
  });

  it("withTimeout rejects after the deadline", async () => {
    await expect(withTimeout(new Promise(() => {}), 80, "hang")).rejects.toThrow(/hang/);
  });

  it("withTimeout passes through a resolved value", async () => {
    await expect(withTimeout(Promise.resolve(42), 1000, "ok")).resolves.toBe(42);
  });

  it("waitForQuery polls until the predicate is satisfied", async () => {
    let n = 0;
    const db = {
      all: async <T>() => {
        n += 1;
        return Array.from({ length: n }, (_, i) => ({ id: String(i) })) as T[];
      },
    };
    const rows = await waitForQuery<{ id: string }>(db, null, (r) => r.length >= 3, "three", 2000);
    expect(rows.length).toBeGreaterThanOrEqual(3);
  });

  it("waitForQuery times out with the row count in the message", async () => {
    const db = { all: async <T>() => [] as T[] };
    await expect(waitForQuery(db, null, () => false, "empty", 120)).rejects.toThrow(
      /lastRowsCount=0/,
    );
  });
});
