import assert from "node:assert/strict";
import test from "node:test";
import { assertDeviceReceipt, collectResults } from "./device-driver.mjs";

const now = Date.parse("2026-08-28T12:00:00.000Z");
const expected = {
  platform: "android",
  deviceIdentifier: "emulator-5554",
  buildFingerprint: "a".repeat(64),
  runNonce: "run-123",
  startedAt: now - 1_000,
  now,
  scenarios: ["local-write-subscription", "reconnect"],
};
const receipt = (scenario, sequence, overrides = {}) =>
  `JAZZ_DEVICE_RESULT ${JSON.stringify({ protocol: 1, scenario, state: "passed", detail: "observed", receipt: { platform: "android", deviceIdentifier: "emulator-5554", buildFingerprint: "a".repeat(64), runNonce: "run-123", sequence, observedAt: "2026-08-28T12:00:00.000Z", ...overrides } })}`;

test("accepts exactly one fresh canonical receipt per expected scenario", () => {
  assert.equal(
    assertDeviceReceipt(
      [receipt("local-write-subscription", 1), receipt("reconnect", 2)].join("\n"),
      expected,
    ).length,
    2,
  );
});

test("accepts an exact receipt carried in an adb logcat line", () => {
  const line = receipt("local-write-subscription", 1);
  assert.equal(collectResults(`08-28 04:00:00.000  100  101 I ReactNativeJS: ${line}`).length, 1);
});

for (const [name, output] of [
  [
    "missing receipt fields",
    `JAZZ_DEVICE_RESULT ${JSON.stringify({ protocol: 1, scenario: "local-write-subscription", state: "passed", detail: "forged", receipt: { platform: "android" } })}`,
  ],
  [
    "foreign scenario",
    [receipt("local-write-subscription", 1), receipt("other-app", 2)].join("\n"),
  ],
  [
    "duplicate scenario",
    [receipt("local-write-subscription", 1), receipt("local-write-subscription", 2)].join("\n"),
  ],
  [
    "stale receipt",
    [
      receipt("local-write-subscription", 1, { observedAt: "2026-08-28T11:00:00.000Z" }),
      receipt("reconnect", 2),
    ].join("\n"),
  ],
  [
    "wrong nonce",
    [receipt("local-write-subscription", 1, { runNonce: "old-run" }), receipt("reconnect", 2)].join(
      "\n",
    ),
  ],
  [
    "wrong build fingerprint",
    [
      receipt("local-write-subscription", 1, { buildFingerprint: "b".repeat(64) }),
      receipt("reconnect", 2),
    ].join("\n"),
  ],
  [
    "wrong platform",
    [receipt("local-write-subscription", 1, { platform: "ios" }), receipt("reconnect", 2)].join(
      "\n",
    ),
  ],
  [
    "non-monotonic sequence",
    [receipt("local-write-subscription", 2), receipt("reconnect", 1)].join("\n"),
  ],
])
  test(`rejects ${name}`, () => assert.throws(() => assertDeviceReceipt(output, expected)));
