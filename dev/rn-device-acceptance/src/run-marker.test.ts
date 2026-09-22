import assert from "node:assert/strict";
import test from "node:test";
import {
  assertPersistedTitleForRun,
  isDeviceRunNonce,
  persistedTitleForRun,
  rowIdForRun,
} from "./run-marker.ts";

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

test("each host run supplies a distinct persistent fixture row id", () => {
  assert.deepEqual(
    rowIdForRun("2e8353a5-ec8d-49ba-8923-8d9ad00f1234"),
    Uint8Array.from([
      0x2e, 0x83, 0x53, 0xa5, 0xec, 0x8d, 0x49, 0xba, 0x89, 0x23, 0x8d, 0x9a, 0xd0, 0x0f, 0x12,
      0x34,
    ]),
  );
  assert.notDeepEqual(
    rowIdForRun("2e8353a5-ec8d-49ba-8923-8d9ad00f1234"),
    rowIdForRun("3f1d8bc4-1e02-45d1-a0d7-2f14f0e5b678"),
  );
  for (const malformed of [
    "old-run",
    "2e8353a5ec8d49ba89238d9ad00f1234",
    "2e8353a5-ec8d49ba-8923-8d9ad00f1234",
    "2e8353a5-ec8d-39ba-8923-8d9ad00f1234",
    "2e8353a5-ec8d-49ba-7923-8d9ad00f1234",
    "2E8353A5-EC8D-49BA-8923-8D9AD00F1234",
  ]) {
    assert.equal(isDeviceRunNonce(malformed), false);
    assert.throws(() => rowIdForRun(malformed), /canonical UUIDv4 host-issued run nonce/);
  }
  assert.equal(isDeviceRunNonce("2e8353a5-ec8d-49ba-8923-8d9ad00f1234"), true);
});
