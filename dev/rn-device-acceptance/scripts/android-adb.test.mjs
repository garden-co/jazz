import assert from "node:assert/strict";
import test from "node:test";
import { adb } from "./android-adb.mjs";

test("a failing trusted Android launch redacts bearer argv from every error surface", () => {
  const bearerA = "test-bearer-a-secret";
  const bearerB = "test-bearer-b-secret";
  const failingAdb = () => {
    const error = new Error(`Command failed: adb shell am start ${bearerA} ${bearerB}`);
    error.stdout = `stdout ${bearerA}`;
    error.stderr = `stderr ${bearerB}`;
    error.output = `output ${bearerA} ${bearerB}`;
    error.cmd = `adb shell am start ${bearerA} ${bearerB}`;
    error.command = `adb shell am start ${bearerA} ${bearerB}`;
    error.status = 1;
    return error;
  };

  assert.throws(
    () =>
      adb(
        [
          "shell",
          "am",
          "start",
          "--es",
          "jazzDeviceBearerA",
          bearerA,
          "--es",
          "jazzDeviceBearerB",
          bearerB,
        ],
        {
          exec: () => {
            throw failingAdb();
          },
        },
      ),
    (error) => {
      const surfaces = [
        error.message,
        error.stack,
        error.stdout,
        error.stderr,
        error.output,
        error.cmd,
        error.command,
      ].join("\n");
      assert.doesNotMatch(surfaces, new RegExp(`${bearerA}|${bearerB}`));
      assert.match(surfaces, /Command failed: adb shell am start \[redacted\] \[redacted\]/);
      assert.equal(error.status, 1);
      return true;
    },
  );
});
