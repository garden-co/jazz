import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import test from "node:test";
import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

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
  assert.equal(
    packageJson.repository.url,
    "git+https://github.com/garden-co/jazz.git",
    "the published package must point install users at its maintained source repository",
  );
  assert.equal(packageJson.bugs.url, "https://github.com/garden-co/jazz/issues");
});

test("the canonical Expo scaffold really prebuilds both relay-only platforms", () => {
  const root = new URL("../../../", import.meta.url);
  const expoBin = new URL(
    "../../../examples/todo-client-localfirst-expo/node_modules/.bin/expo",
    import.meta.url,
  );
  assert.ok(
    existsSync(expoBin),
    "Expo prebuild requires the example workspace dependencies. Run `pnpm install --frozen-lockfile` from the repository root, then rerun this gate.",
  );
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

  assert.match(podspec, /JazzNativeRelay\.xcframework/);
  assert.match(podspec, /https:\/\/github\.com\/garden-co\/jazz\.git/);
  assert.doesNotMatch(podspec, /https:\/\/https:\/\//);
  assert.doesNotMatch(podspec, /uniffi-bindgen-react-native/);
  assert.match(podspec, /requires the React Native New Architecture/);
  assert.match(androidBuild, /relayNativeArtifactsPresent/);
  assert.match(androidBuild, /externalNativeBuild/);
  assert.doesNotMatch(androidBuild, /AndroidManifestNew/);
  assert.match(androidBuild, /requires the React Native New Architecture/);
  assert.match(androidPackage, /class JazzRelayPackage/);
  assert.doesNotMatch(androidPackage, /JazzRnModule/);
  assert.match(iosRelay, /RCT_EXPORT_MODULE\(JazzRelay\)/);
  assert.match(iosRelay, /JAZZ_RELAY_ARTIFACT_AVAILABLE/);
  assert.match(iosRelay, /jazz_native_relay_host_execute/);
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
  assert.match(androidRelay, /JazzRelayBridge/);
  assert.match(androidRelay, /getAbiVersion\(\): Double = bridge\?\.abiVersion\(\) \?: 0\.0/);
  assert.match(androidRelay, /E_JAZZ_RELAY_UNAVAILABLE/);
});

test("relay artifact staging targets every Android ABI and iOS framework slice", async () => {
  const script = await readFile(
    new URL("../../../crates/jazz-rn/scripts/build-relay-artifacts.sh", import.meta.url),
    "utf8",
  );

  assert.equal(
    packageJson.scripts["build:relay:android"],
    "bash scripts/build-relay-artifacts.sh android",
  );
  assert.equal(packageJson.scripts["build:relay:ios"], "bash scripts/build-relay-artifacts.sh ios");
  assert.match(script, /\[arm64-v8a\]=aarch64-linux-android/);
  assert.match(script, /\[armeabi-v7a\]=armv7-linux-androideabi/);
  assert.match(script, /\[x86\]=i686-linux-android/);
  assert.match(script, /\[x86_64\]=x86_64-linux-android/);
  assert.match(script, /JazzNativeRelay\.xcframework/);
  assert.match(script, /aarch64-apple-ios-sim x86_64-apple-ios/);
  assert.match(script, /nativeRelayAbi/);
});

test("a dry package includes every staged native relay artifact class", async () => {
  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-pack-"));
  try {
    const staged = [
      "native/include/jazz_native_relay.h",
      "android/src/main/jniLibs/arm64-v8a/libjazz_native_relay.a",
      "JazzNativeRelay.xcframework/Info.plist",
    ];
    const manifest = {
      ...packageJson,
      scripts: {},
      main: undefined,
      types: undefined,
      exports: undefined,
    };
    await writeFile(join(directory, "package.json"), `${JSON.stringify(manifest)}\n`);
    for (const path of staged) {
      const destination = join(directory, path);
      await mkdir(dirname(destination), { recursive: true });
      await writeFile(destination, "staged-native-artifact\n");
    }
    const receipt = JSON.parse(
      execFileSync("npm", ["pack", "--dry-run", "--json", "--ignore-scripts"], {
        cwd: directory,
        encoding: "utf8",
      }),
    );
    const packed = new Set(receipt[0].files.map(({ path }) => path));
    for (const path of staged) {
      assert.ok(
        packed.has(path),
        `dry package omitted staged artifact ${path}; packed: ${[...packed].join(", ")}`,
      );
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
