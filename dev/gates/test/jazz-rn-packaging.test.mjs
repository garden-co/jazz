import assert from "node:assert/strict";
import test from "node:test";
import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";

const require = createRequire(import.meta.url);
const packageJson = JSON.parse(
  await readFile(new URL("../../../crates/jazz-rn/package.json", import.meta.url), "utf8"),
);
const withJazzRn = require("../../../crates/jazz-rn/app.plugin.js");

test("jazz-rn publishes an Expo config plugin for a New Architecture development build", () => {
  const original = { name: "example", ios: { bundleIdentifier: "dev.jazz.example" } };
  const configured = withJazzRn(original);

  assert.equal(configured.newArchEnabled, true);
  assert.equal(original.newArchEnabled, undefined, "plugin must not mutate Expo's input config");
  assert.equal(configured.ios, original.ios, "unrelated Expo configuration is preserved");
  assert.equal(packageJson.exports["./app.plugin"], "./app.plugin.js");
  assert.equal(packageJson.files.includes("app.plugin.js"), true);
  assert.equal(packageJson.files.includes("scripts"), true);
  assert.equal(packageJson.peerDependencies.expo, ">=54");
});

test("jazz-rn release verification covers every Android ABI and the iOS XCFramework", async () => {
  const script = await readFile(
    new URL("../../../crates/jazz-rn/scripts/verify-native-artifacts.mjs", import.meta.url),
    "utf8",
  );

  for (const artifact of [
    "JazzRnFramework.xcframework/Info.plist",
    "android/src/main/jniLibs/arm64-v8a/libjazz_rn.a",
    "android/src/main/jniLibs/armeabi-v7a/libjazz_rn.a",
    "android/src/main/jniLibs/x86/libjazz_rn.a",
    "android/src/main/jniLibs/x86_64/libjazz_rn.a",
  ]) {
    assert.match(script, new RegExp(artifact.replaceAll("/", "\\/")));
  }
});

test("jazz-rn reserves a thin binary relay TurboModule boundary for matching native builds", async () => {
  const relay = await readFile(
    new URL("../../../crates/jazz-rn/src/relay.ts", import.meta.url),
    "utf8",
  );
  const nativeSpec = await readFile(
    new URL("../../../crates/jazz-rn/src/NativeJazzRelay.ts", import.meta.url),
    "utf8",
  );
  const codegenGate = await readFile(
    new URL("../../../crates/jazz-rn/scripts/test-codegen.sh", import.meta.url),
    "utf8",
  );
  const androidRelay = await readFile(
    new URL(
      "../../../crates/jazz-rn/android/src/main/java/com/jazzrn/JazzRelayModule.kt",
      import.meta.url,
    ),
    "utf8",
  );

  assert.equal(packageJson.exports["./relay"].source, "./src/relay.ts");
  assert.equal(packageJson.scripts["test:codegen"], "bash scripts/test-codegen.sh");
  assert.match(nativeSpec, /TurboModuleRegistry\.get<Spec>\('JazzRelay'\)/);
  assert.match(nativeSpec, /execute\(commandBase64: string\): Promise<string>/);
  assert.match(relay, /getAbiVersion\(\)/);
  assert.match(relay, /matching native development or release build/);
  assert.match(codegenGate, /for platform in android ios/);
  assert.match(codegenGate, /NativeJazzRelay/);
  assert.match(androidRelay, /getAbiVersion\(\): Double = 0\.0/);
  assert.match(androidRelay, /E_JAZZ_RELAY_UNAVAILABLE/);
});
