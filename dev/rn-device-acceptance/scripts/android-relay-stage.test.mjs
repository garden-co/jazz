import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { androidRelayFiles, verifyAndroidRelayStage } from "./android-relay-stage.mjs";

const revision = "a".repeat(40);
function staged({ omit = false } = {}) {
  const root = mkdtempSync(join(tmpdir(), "jazz-relay-stage-"));
  const libraryRoot = join(root, "android/src/main/jniLibs");
  const files = androidRelayFiles
    .filter((_, index) => !omit || index !== 0)
    .map((file) => {
      const destination = join(libraryRoot, file);
      mkdirSync(join(destination, ".."), { recursive: true });
      writeFileSync(destination, file);
      return { path: file, sha256: createHash("sha256").update(file).digest("hex") };
    });
  mkdirSync(join(root, "android"), { recursive: true });
  writeFileSync(
    join(root, "android/jazz-native-relay.manifest.json"),
    JSON.stringify({
      format: 2,
      nativeRelayAbi: 3,
      sourceRevision: revision,
      files,
    }),
  );
  return root;
}

test("accepts a staged manifest only with the exact four Android ABI libraries", () => {
  assert.equal(
    verifyAndroidRelayStage({ packageRoot: staged(), sourceRevision: revision }).files.length,
    4,
  );
});
test("rejects a manifest that stages only the emulator ABI", () => {
  assert.throws(() =>
    verifyAndroidRelayStage({ packageRoot: staged({ omit: true }), sourceRevision: revision }),
  );
});
test("rejects a staged manifest from another source revision", () => {
  assert.throws(() =>
    verifyAndroidRelayStage({ packageRoot: staged(), sourceRevision: "b".repeat(40) }),
  );
});
