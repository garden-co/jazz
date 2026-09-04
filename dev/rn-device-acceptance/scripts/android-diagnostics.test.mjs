import assert from "node:assert/strict";
import test from "node:test";
import { androidAcceptanceFailure, androidDeviceDiagnostic } from "./android-diagnostics.mjs";

test("Android timeout reports only the latest exact allowlisted stage", () => {
  const output = [
    "08-29 22:52:21.495  4268  4288 E JazzDeviceAcceptance: linked-abi-admission-failed",
    "08-29 22:52:21.496  4268  4288 E JazzDeviceAcceptance: capability=secret-device-token",
    "08-29 22:52:21.497  4268  4288 E JazzDeviceAcceptance: native-admission-failed",
  ].join("\n");
  assert.equal(androidDeviceDiagnostic(output), "native-admission-failed");
  const failure = androidAcceptanceFailure("timeout", "seed", output);
  assert.equal(
    failure,
    "Timed out waiting for phase seed from the launched Android app; device stage: native-admission-failed",
  );
  assert.doesNotMatch(failure, /secret-device-token|linked-abi-admission/);
});

test("invalid Android receipt keeps its safe stage without echoing receipt contents", () => {
  const output = [
    "08-29 22:52:21.495  4268  4288 I ReactNativeJS: JAZZ_DEVICE_RESULT capability=secret-receipt-token",
    "08-29 22:52:21.496  4268  4288 E JazzDeviceAcceptance: relay-command-abi-failed",
  ].join("\n");
  const failure = androidAcceptanceFailure("invalid-receipt", "verify", output);
  assert.equal(
    failure,
    "Android app emitted an invalid verify receipt; device stage: relay-command-abi-failed",
  );
  assert.doesNotMatch(failure, /secret-receipt-token|JAZZ_DEVICE_RESULT/);
});

test("unrecognized or stale Android diagnostics produce a fixed absence marker", () => {
  const output =
    "08-29 22:52:21.495  4268  4288 E JazzDeviceAcceptance: linked-abi-admission-failed";
  assert.equal(androidDeviceDiagnostic(output), undefined);
  assert.equal(
    androidAcceptanceFailure("timeout", "verify", output),
    "Timed out waiting for phase verify from the launched Android app; no device stage",
  );
});

test("ReactNativeJS payload cannot spoof the native diagnostic priority and tag", () => {
  const spoof =
    "08-29 22:52:21.495  4268  4288 I ReactNativeJS: JazzDeviceAcceptance: native-admission-failed";
  assert.equal(androidDeviceDiagnostic(spoof), undefined);
  const plantedSecret = `${spoof} capability=secret-device-token`;
  const failure = androidAcceptanceFailure("timeout", "seed", plantedSecret);
  assert.equal(
    failure,
    "Timed out waiting for phase seed from the launched Android app; no device stage",
  );
  assert.doesNotMatch(failure, /native-admission|secret-device-token/);
});
