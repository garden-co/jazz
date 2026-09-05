import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { stageAndroidFailureArtifact } from "./stage-android-failure-artifact.mjs";

test(
  "retained symbols match the APK ELF, with exact source and no runtime material",
  { skip: process.platform !== "linux" },
  () => {
    const root = mkdtempSync(join(tmpdir(), "android-artifact-test-"));
    const run = (cmd, args) =>
      execFileSync(cmd, args, { cwd: root, stdio: "pipe" }).toString().trim();
    const put = (path, bytes) => {
      mkdirSync(join(root, path, ".."), { recursive: true });
      writeFileSync(join(root, path), bytes);
    };
    try {
      run("git", ["init"]);
      run("git", [
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "--allow-empty",
        "-m",
        "fixture",
      ]);
      const sourceRevision = run("git", ["rev-parse", "HEAD"]);
      const args = {
        root,
        output: join(root, "out"),
        sourceRevision,
        runId: "123",
        runAttempt: "1",
      };
      assert.deepEqual(stageAndroidFailureArtifact(args), { status: "apk-unavailable" });
      put(
        "crates/jazz-rn/android/jazz-native-relay.manifest.json",
        JSON.stringify({ sourceRevision, nativeRelayAbi: 1 }),
      );
      put("fixture.c", "int synthetic_fixture(void) { return 7; }\n");
      const lib =
        "crates/jazz-rn/android/build/intermediates/cxx/Release/hash/obj/x86_64/libjazzrelay.so";
      mkdirSync(join(root, lib, ".."), { recursive: true });
      run("cc", ["-g", "-shared", "-Wl,--build-id", "fixture.c", "-o", lib]);
      mkdirSync(join(root, "lib/x86_64"), { recursive: true });
      run("strip", ["--strip-unneeded", "-o", "lib/x86_64/libjazzrelay.so", lib]);
      const apk = "dev/rn-device-acceptance/android/app/build/outputs/apk/release/app-release.apk";
      mkdirSync(join(root, apk, ".."), { recursive: true });
      run("python3", [
        "-c",
        "import zipfile,sys; z=zipfile.ZipFile(sys.argv[1], 'w'); z.write('lib/x86_64/libjazzrelay.so'); z.close()",
        apk,
      ]);
      assert.deepEqual(stageAndroidFailureArtifact(args), { status: "staged", symbols: true });
      const receipt = JSON.parse(readFileSync(join(args.output, "provenance.json"), "utf8"));
      assert.equal(receipt.sourceRevision, sourceRevision);
      assert.match(receipt.elfBuildId, /^[a-f0-9]+$/);
      assert.equal(receipt.symbols.sha256.length, 64);
      assert.throws(
        () => stageAndroidFailureArtifact({ ...args, sourceRevision: "0".repeat(40) }),
        /head/,
      );
      // A different build must never be labelled as matching debug symbols.
      put("fixture.c", "int synthetic_fixture(void) { return 42; }\n");
      run("cc", ["-g", "-shared", "-Wl,--build-id", "fixture.c", "-o", lib]);
      const other = { ...args, output: join(root, "mismatch") };
      assert.deepEqual(stageAndroidFailureArtifact(other), { status: "staged", symbols: false });
      assert.equal(existsSync(join(other.output, "libjazzrelay-x86_64-unstripped.so")), false);
      assert.equal(existsSync(join(other.output, "app-release.apk")), true);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  },
);
