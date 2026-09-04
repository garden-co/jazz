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
