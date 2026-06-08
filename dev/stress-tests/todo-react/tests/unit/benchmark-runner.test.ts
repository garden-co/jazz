import { describe, expect, it } from "vitest";
import { waitForBenchmarkWriteDurability } from "../../src/benchmark-utils.js";

type WaitTier = "local" | "edge" | "global";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("waitForBenchmarkWriteDurability", () => {
  it("waits for server settlement after local durability when a sync tier is set", async () => {
    const calls: string[] = [];
    const edgeWait = deferred();
    const handles = [
      {
        async wait(options: { tier: WaitTier }) {
          calls.push(`wait:${options.tier}`);
          if (options.tier === "edge") {
            await edgeWait.promise;
          }
        },
      },
    ];

    const promise = waitForBenchmarkWriteDurability(handles, "edge", (status) => {
      calls.push(`status:${status}`);
    });

    await Promise.resolve();
    await Promise.resolve();

    expect(calls).toEqual([
      "status:wait-local-durability",
      "wait:local",
      "status:wait-sync-settlement",
      "wait:edge",
    ]);

    edgeWait.resolve();
    await expect(promise).resolves.toMatchObject({ syncSettlementTier: "edge" });
  });

  it("skips server settlement when no sync tier is set", async () => {
    const calls: string[] = [];
    const handles = [
      {
        async wait(options: { tier: WaitTier }) {
          calls.push(`wait:${options.tier}`);
        },
      },
    ];

    const result = await waitForBenchmarkWriteDurability(handles, undefined, (status) => {
      calls.push(`status:${status}`);
    });

    expect(calls).toEqual(["status:wait-local-durability", "wait:local"]);
    expect(result.syncSettlementMs).toBeUndefined();
  });
});
