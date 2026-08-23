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

test("the Expo scaffold retains a relay-only prebuild receipt", async () => {
  const [expoPackage, expoReadme] = await Promise.all([
    readFile(
      new URL("../../../examples/todo-client-localfirst-expo/package.json", import.meta.url),
      "utf8",
    ),
    readFile(
      new URL("../../../examples/todo-client-localfirst-expo/README.md", import.meta.url),
      "utf8",
    ),
  ]);

  assert.match(expoPackage, /"verify:expo:android": "CI=1 expo prebuild/);
  assert.match(expoPackage, /"verify:expo:ios": "CI=1 expo prebuild/);
  assert.match(expoReadme, /JazzRelaySpec/);
  assert.match(expoReadme, /not a runnable persistent Jazz client/);
});

test("jazz-rn autolinks a relay host without requiring obsolete UniFFI artifacts", async () => {
  const [podspec, androidPackage, androidBuild, iosRelay, packageRoot] = await Promise.all([
    readFile(new URL("../../../crates/jazz-rn/JazzRn.podspec", import.meta.url), "utf8"),
    readFile(
      new URL(
        "../../../crates/jazz-rn/android/src/main/java/com/jazzrn/JazzRelayPackage.kt",
        import.meta.url,
      ),
      "utf8",
    ),
    readFile(new URL("../../../crates/jazz-rn/android/build.gradle", import.meta.url), "utf8"),
    readFile(new URL("../../../crates/jazz-rn/ios/JazzRelay.mm", import.meta.url), "utf8"),
    readFile(new URL("../../../crates/jazz-rn/src/index.tsx", import.meta.url), "utf8"),
  ]);

  assert.doesNotMatch(podspec, /vendored_frameworks|uniffi-bindgen-react-native/);
  assert.doesNotMatch(androidBuild, /externalNativeBuild|jniLibs|CMakeLists/);
  assert.match(androidPackage, /class JazzRelayPackage/);
  assert.doesNotMatch(androidPackage, /JazzRnModule/);
  assert.match(iosRelay, /RCT_EXPORT_MODULE\(JazzRelay\)/);
  assert.match(iosRelay, /E_JAZZ_RELAY_UNAVAILABLE/);
  assert.match(iosRelay, /NativeJazzRelaySpecJSI/);
  assert.doesNotMatch(packageRoot, /NativeJazzRn|uniffi/);
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
