import assert from "node:assert/strict";
import test from "node:test";
import { assertPersistedTitleForRun, persistedTitleForRun } from "./run-marker.ts";

test("restart receipt rejects a retained row from a different device run", () => {
  const priorRun = "2e8353a5-ec8d-49ba-8923-8d9ad00f1234";
  const currentRun = "3f1d8bc4-1e02-45d1-a0d7-2f14f0e5b678";
  const retainedTitles = [persistedTitleForRun(priorRun)];

  // Planted stale app data must not satisfy the verify launch just because
  // Android `adb install -r` or an iOS reinstall retained SQLite files.
  assert.throws(
    () => assertPersistedTitleForRun(retainedTitles, currentRun),
    /this run's prior process persisted row/,
  );

  retainedTitles.push(persistedTitleForRun(currentRun));
  assert.doesNotThrow(() => assertPersistedTitleForRun(retainedTitles, currentRun));
});

test("restart receipt refuses a missing run marker", () => {
  assert.throws(() => persistedTitleForRun(""), /host-issued run nonce/);
});
