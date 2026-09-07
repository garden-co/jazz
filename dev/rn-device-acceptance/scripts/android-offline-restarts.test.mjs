import assert from "node:assert/strict";
import test from "node:test";
import { verifyAndroidOfflineRestarts } from "./android-offline-restarts.mjs";

test("three offline verifications each require process death after one upstream shutdown", async () => {
  const events = [];
  let running = true;
  let upstream = true;
  await verifyAndroidOfflineRestarts({
    stopApp: async () => {
      events.push("stop-app");
      running = false;
    },
    stopUpstream: async () => {
      assert.equal(running, false);
      events.push("stop-upstream");
      upstream = false;
    },
    verify: async (iteration) => {
      assert.equal(running, false, "each verify needs a fresh process");
      assert.equal(upstream, false, "no verify may use live upstream");
      events.push(`verify-${iteration}`);
      running = true;
    },
  });
  assert.deepEqual(events, [
    "stop-app",
    "stop-upstream",
    "verify-1",
    "stop-app",
    "verify-2",
    "stop-app",
    "verify-3",
  ]);
});

for (const failingIteration of [1, 2, 3]) {
  test(`failure at verify ${failingIteration} stops immediately without retry`, async () => {
    const attempts = [];
    let stops = 0;
    const failure = new Error("planted receipt failure");
    await assert.rejects(
      verifyAndroidOfflineRestarts({
        stopApp: async () => {
          stops++;
        },
        stopUpstream: async () => {},
        verify: async (iteration) => {
          attempts.push(iteration);
          if (iteration === failingIteration) throw failure;
        },
      }),
      (error) => error === failure,
    );
    assert.deepEqual(
      attempts,
      Array.from({ length: failingIteration }, (_, i) => i + 1),
    );
    assert.equal(stops, failingIteration);
  });
}

for (const boundary of ["stopApp", "stopUpstream"]) {
  test(`${boundary} failure cannot launch an offline proof`, async () => {
    let verified = false;
    const failure = new Error("planted stop failure");
    await assert.rejects(
      verifyAndroidOfflineRestarts({
        stopApp: async () => {},
        stopUpstream: async () => {},
        verify: async () => {
          verified = true;
        },
        [boundary]: async () => {
          throw failure;
        },
      }),
      (error) => error === failure,
    );
    assert.equal(verified, false);
  });
}
