import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
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

test("the canonical Expo scaffold really prebuilds both relay-only platforms", () => {
  const root = new URL("../../../", import.meta.url);
  for (const script of ["verify:expo:android", "verify:expo:ios"]) {
    execFileSync("pnpm", ["--filter", "todo-client-localfirst-expo", "run", script], {
      cwd: root,
      env: { ...process.env, CI: "1" },
      stdio: "inherit",
    });
  }

  const autolink = (platform) =>
    JSON.parse(
      execFileSync(
        process.execPath,
        [
          "--no-warnings",
          "--eval",
          "require('expo/bin/autolinking')",
          "expo-modules-autolinking",
          "react-native-config",
          "--json",
          "--platform",
          platform,
        ],
        {
          cwd: new URL("../../../examples/todo-client-localfirst-expo/", import.meta.url),
          encoding: "utf8",
        },
      ),
    );
  const androidAutolink = autolink("android");
  const iosAutolink = autolink("ios");
  assert.equal(
    androidAutolink.dependencies["jazz-rn"].platforms.android.packageInstance,
    "new JazzRelayPackage()",
  );
  assert.match(iosAutolink.dependencies["jazz-rn"].platforms.ios.podspecPath, /JazzRn\.podspec$/);

  const expoRoot = new URL("../../../examples/todo-client-localfirst-expo/", import.meta.url);
  const androidProperties = readFile(new URL("android/gradle.properties", expoRoot), "utf8");
  const androidSettings = readFile(new URL("android/settings.gradle", expoRoot), "utf8");
  const iosProperties = readFile(new URL("ios/Podfile.properties.json", expoRoot), "utf8");
  const iosPodfile = readFile(new URL("ios/Podfile", expoRoot), "utf8");

  return Promise.all([androidProperties, androidSettings, iosProperties, iosPodfile]).then(
    ([androidPropertiesText, androidSettingsText, iosPropertiesText, iosPodfileText]) => {
      assert.match(androidPropertiesText, /^newArchEnabled=true$/m);
      assert.match(androidSettingsText, /expo-autolinking-settings/);
      assert.match(androidSettingsText, /autolinkLibrariesFromCommand/);
      assert.match(iosPropertiesText, /"newArchEnabled": "true"/);
      assert.match(iosPodfileText, /use_native_modules!/);
    },
  );
});

test("jazz-rn autolinks a New-Architecture relay host without legacy artifacts", async () => {
  const [podspec, androidPackage, androidBuild, iosRelay, packageRoot, rootCargo, legacyConfig] =
    await Promise.all([
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
      readFile(new URL("../../../Cargo.toml", import.meta.url), "utf8"),
      readFile(new URL("../../../crates/jazz-rn/ubrn.config.yaml", import.meta.url), "utf8").catch(
        () => null,
      ),
    ]);

  assert.doesNotMatch(podspec, /vendored_frameworks|uniffi-bindgen-react-native/);
  assert.match(podspec, /requires the React Native New Architecture/);
  assert.doesNotMatch(androidBuild, /externalNativeBuild|jniLibs|CMakeLists/);
  assert.doesNotMatch(androidBuild, /AndroidManifestNew/);
  assert.match(androidBuild, /requires the React Native New Architecture/);
  assert.match(androidPackage, /class JazzRelayPackage/);
  assert.doesNotMatch(androidPackage, /JazzRnModule/);
  assert.match(iosRelay, /RCT_EXPORT_MODULE\(JazzRelay\)/);
  assert.match(iosRelay, /E_JAZZ_RELAY_UNAVAILABLE/);
  assert.match(iosRelay, /NativeJazzRelaySpecJSI/);
  assert.doesNotMatch(packageRoot, /NativeJazzRn|uniffi/);
  assert.doesNotMatch(rootCargo, /jazz-rn\/rust/);
  assert.equal(legacyConfig, null);
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
