import assert from "node:assert/strict";
import test from "node:test";
import { adb } from "./android-adb.mjs";

test("adb selects the requested device and preserves command failure status", () => {
  const failure = Object.assign(new Error("device offline"), { status: 1 });
  assert.throws(
    () =>
      adb(["get-state"], {
        serial: "emulator-fixture",
        exec(command, args, options) {
          assert.equal(command, "adb");
          assert.deepEqual(args, ["-s", "emulator-fixture", "get-state"]);
          assert.equal(options.encoding, "utf8");
          throw failure;
        },
      }),
    (error) => error === failure,
  );
});
