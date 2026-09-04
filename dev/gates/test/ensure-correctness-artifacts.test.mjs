import assert from "node:assert/strict";
import test from "node:test";
import { ensureCorrectnessArtifacts } from "../ensure-correctness-artifacts.mjs";

test("verified exact-source receipt avoids another producer invocation", async () => {
  let builds = 0;
  await ensureCorrectnessArtifacts({
    verify: () => {},
    build: async () => {
      builds += 1;
    },
  });
  assert.equal(builds, 0);
});

test("stale receipt invokes producer and requires a fresh postflight", async () => {
  const events = [];
  await ensureCorrectnessArtifacts({
    verify: () => {
      events.push("verify");
      if (events.length === 1) throw new Error("stale");
    },
    build: async () => {
      events.push("build");
    },
  });
  assert.deepEqual(events, ["verify", "build", "verify"]);
  await assert.rejects(
    ensureCorrectnessArtifacts({
      verify: () => {
        throw new Error("still stale");
      },
      build: async () => {},
    }),
    /still stale/,
  );
});

test("failed producer cannot admit a browser consumer", async () => {
  await assert.rejects(
    ensureCorrectnessArtifacts({
      verify: () => {
        throw new Error("missing");
      },
      build: async () => {
        throw new Error("planted producer failure");
      },
    }),
    /planted producer failure/,
  );
});
