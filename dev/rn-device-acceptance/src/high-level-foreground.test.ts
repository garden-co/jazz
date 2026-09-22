import assert from "node:assert/strict";
import test from "node:test";
import { finishSeedClient } from "./seed-teardown.ts";

test("a primary write failure survives unsubscribe failure while shutdown runs once", async () => {
  const primary = new Error("planted write failure");
  let shutdowns = 0;

  await assert.rejects(async () => {
    try {
      await Promise.reject(primary);
    } catch (error) {
      await finishSeedClient(
        () => {
          throw new Error("planted unsubscribe failure");
        },
        async () => {
          shutdowns += 1;
        },
        true,
      );
      throw error;
    }
  }, primary);
  assert.equal(shutdowns, 1);
});

test("a teardown-only failure surfaces after shutdown still runs", async () => {
  let shutdowns = 0;
  await assert.rejects(
    finishSeedClient(
      () => {
        throw new Error("unsubscribe failed");
      },
      async () => {
        shutdowns += 1;
      },
      false,
    ),
    /unsubscribe failed/,
  );
  assert.equal(shutdowns, 1);
});

test("relay readback cleanup preserves its publication failure and still shuts down", async () => {
  const primary = new Error("missing run marker");
  let shutdowns = 0;
  await assert.rejects(async () => {
    try {
      throw primary;
    } catch (error) {
      await finishSeedClient(
        () => {
          throw new Error("unsubscribe failed");
        },
        async () => {
          shutdowns += 1;
        },
        true,
      );
      throw error;
    }
  }, primary);
  assert.equal(shutdowns, 1);
});

test("seed boundaries distinguish unsubscribe from a pending shutdown", async () => {
  const events: string[] = [];
  let release!: () => void;
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  const teardown = finishSeedClient(
    () => {
      assert.equal(events.at(-1), "js-before-unsubscribe");
    },
    () => {
      assert.equal(events.at(-1), "js-before-shutdown");
      return pending;
    },
    false,
    (code) => events.push(code),
  );
  assert.deepEqual(events, ["js-before-unsubscribe", "js-after-unsubscribe", "js-before-shutdown"]);
  release();
  await teardown;
  assert.equal(events.at(-1), "js-after-shutdown");
});
