import assert from "node:assert/strict";
import test from "node:test";
import { GUARDED_PACKAGES, checkLatest, wouldMoveLatestBack } from "./latest-guard.mjs";

const version = "2.0.0-alpha.61";

test("publishing to latest moves off Classic and older alphas but never back", () => {
  assert.equal(wouldMoveLatestBack("0.20.19", version), false);
  assert.equal(wouldMoveLatestBack("2.0.0-alpha.60", version), false);
  assert.equal(wouldMoveLatestBack(undefined, version), false);
  assert.equal(wouldMoveLatestBack(version, version), false);
  assert.equal(wouldMoveLatestBack("2.0.0-alpha.62", version), true);
  assert.throws(
    () => wouldMoveLatestBack("2.0.0-alpha.60", "2.0.0-alpha.61-rc.1"),
    /2\.0\.0-alpha\.N/,
  );
});

test("checkLatest refuses before any upload when a package's latest is newer", () => {
  const latest = { "jazz-tools": "2.0.0-alpha.62" };
  const run = (args) => JSON.stringify({ latest: latest[args[1]] ?? "2.0.0-alpha.60" });
  assert.throws(() => checkLatest(version, run), /jazz-tools@2\.0\.0-alpha\.62 is already latest/);
});

test("checkLatest reads every guarded package and allows a newer version", () => {
  const viewed = [];
  checkLatest(version, (args) => {
    viewed.push(args[1]);
    return JSON.stringify({ latest: "2.0.0-alpha.60" });
  });
  assert.deepEqual(viewed, GUARDED_PACKAGES);
});
