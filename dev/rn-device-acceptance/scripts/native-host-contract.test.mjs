import assert from "node:assert/strict";
import fs from "node:fs";
import { execFileSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "..");
const read = (file) => fs.readFileSync(path.join(root, file), "utf8");

test("Android fixture BuildConfig fields and package registration remain compile-shaped", () => {
  const gradle = read("android/app/build.gradle");
  const fixture = read(
    "android/app/src/main/java/dev/jazz/rndeviceacceptance/JazzDeviceFixtureModule.kt",
  );
  const registration = read(
    "android/app/src/main/java/dev/jazz/rndeviceacceptance/JazzDeviceFixturePackage.kt",
  );
  const host = read("android/app/src/main/java/dev/jazz/rndeviceacceptance/MainApplication.kt");
  for (const field of [
    "APP_NAMESPACE",
    "STORAGE_NAMESPACE",
    "AUTH_SCOPE",
    "SCHEMA_JSON",
    "VERIFIED_IDENTITY_JSON",
    "VERIFIED_CLAIMS_JSON",
  ]) {
    assert.match(gradle, new RegExp(`buildConfigField "String", "JAZZ_DEVICE_${field}"`));
    assert.match(fixture, new RegExp(`BuildConfig\\.JAZZ_DEVICE_${field}`));
  }
  assert.match(registration, /class JazzDeviceFixturePackage : ReactPackage/);
  assert.match(registration, /listOf\(JazzDeviceFixtureModule\(context\)\)/);
  assert.match(host, /add\(JazzDeviceFixturePackage\(\)\)/);
  assert.match(fixture, /Build\.FINGERPRINT/);
  assert.match(fixture, /jazzDeviceRunNonce/);
  assert.match(fixture, /applicationInfo\.sourceDir/);
  assert.match(fixture, /MessageDigest\.getInstance\("SHA-256"\)/);
  assert.match(fixture, /@ReactMethod fun recordReceipt/);
  assert.match(fixture, /jazz-device-receipt\.ndjson/);
  assert.doesNotMatch(fixture, /jazzDeviceBuildFingerprint/);
});

test("iOS fixture imports the public JazzRn pod header, not its private relay framework", () => {
  const podspec = fs.readFileSync(
    path.resolve(root, "../../crates/jazz-rn/JazzRn.podspec"),
    "utf8",
  );
  const fixture = read("native/ios/JazzDeviceFixture.mm");
  assert.match(podspec, /s\.name\s+=\s+"JazzRn"/);
  assert.match(podspec, /s\.source_files\s+=\s+"ios\/\*\*\/\*\.\{h,m,mm,swift\}"/);
  assert.match(fixture, /#import <JazzRn\/JazzRelay\.h>/);
  assert.doesNotMatch(fixture, /JazzNativeRelay\/JazzRelay\.h/);
  assert.equal(
    fixture,
    read("ios/JazzDeviceFixture.mm"),
    "the checked-in iOS host fixture must match the prebuild template",
  );
});

test("Expo config plugin describes the real iOS receipt boundary without claiming TODO scenarios", () => {
  const plugin = read("plugins/with-jazz-device-fixture.cjs");
  assert.match(plugin, /label-gated iOS simulator workflow/);
  assert.match(plugin, /requires its linked/);
  assert.match(plugin, /ABI\/admission receipt/);
  assert.match(plugin, /Multi-peer acceptance remains TODO \(#2291\)/);
  assert.doesNotMatch(plugin, /this workflow is source-only/);
});

test("device fixture does not import internal jazz-tools relay-frame types", () => {
  const fixture = read("src/native-fixture.ts");
  assert.doesNotMatch(fixture, /NativeRelay(?:Capability|Executor)/);
  assert.match(fixture, /execute: typeof executeNativeRelayCommand/);
});

test("Android bootstrap rejects corrupt pinned archives before extraction", () => {
  const bootstrap = read("scripts/bootstrap-android.sh");
  assert.match(bootstrap, /verify-pinned-archive\.sh/);
  assert.match(bootstrap, /refusing corrupt cached Android bootstrap archive/);
  assert.match(bootstrap, /OpenJDK17U-jdk_x64_linux_hotspot_17\.0\.16_8\.tar\.gz/);
  assert.match(bootstrap, /commandlinetools-linux-13114758_latest\.zip/);
  assert.match(bootstrap, /android-ndk-r27b-linux\.zip/);
  assert.match(bootstrap, /33e16af1a6bbabe12cad54b2117085c07eab7e4fa67cdd831805f0e94fd826c1/);
  assert.match(bootstrap, /\.jazz-pinned-sha256/);
  assert.match(bootstrap, /cargo-ndk" --version/);
});

test("dispatch device workflow uses hosted KVM while source jobs remain cheap", () => {
  const workflow = fs.readFileSync(
    path.join(root, "../../.github/workflows/rn-device-acceptance.yml"),
    "utf8",
  );
  assert.match(
    workflow,
    /android-device-acceptance:[\s\S]*runs-on: ubuntu-24\.04[\s\S]*Grant the runner user access to KVM[\s\S]*setfacl -m "u:\$\{USER\}:rw" \/dev\/kvm/,
  );
  assert.match(workflow, /android-source-scaffold:[\s\S]*runs-on: blacksmith-4vcpu-ubuntu-2404/);
  assert.match(workflow, /ios-source-scaffold:[\s\S]*runs-on: blacksmith-6vcpu-macos-15/);
  assert.match(workflow, /\[\[ -r \/dev\/kvm && -w \/dev\/kvm \]\]/);
  assert.match(
    workflow,
    /PATH="\$cache\/cargo\/bin:\$cache\/sdk\/cmdline-tools\/latest\/bin:\$cache\/sdk\/platform-tools:\$cache\/sdk\/emulator:\$PATH"/,
  );
  assert.match(workflow, /scripts\/boot-android-emulator\.sh/);
  assert.doesNotMatch(workflow, /adb wait-for-device/);
  assert.match(workflow, /android-device-acceptance:[\s\S]*timeout-minutes: 45/);
  assert.match(workflow, /scripts\/create-android-avd\.sh[\s\\]+jazz-device-acceptance-api35/);
  assert.doesNotMatch(workflow, /yes no \| avdmanager/);
});

test("Android AVD creation supplies one default-safe answer and fails closed on a second prompt", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-avdmanager-"));
  const transcript = path.join(directory, "transcript");
  const avdHome = path.join(directory, "lane-local-avd");
  const avdmanager = path.join(directory, "avdmanager");
  fs.writeFileSync(
    avdmanager,
    `#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == "create avd --force --name acceptance --package system-images;android-35;google_apis;x86_64" ]]
IFS= read -r first
[[ "$first" == no ]]
if IFS= read -r second; then
  printf 'unexpected extra answer: %s\\n' "$second" >&2
  exit 1
fi
if [[ "\${JAZZ_AVD_REQUIRE_SECOND_PROMPT:-}" == 1 ]]; then
  echo 'second prompt was left unanswered' >&2
  exit 42
fi
if [[ "\${JAZZ_AVD_OMIT_FILES:-}" != 1 ]]; then
  output_home="\${JAZZ_AVD_WRONG_HOME:-$ANDROID_AVD_HOME}"
  mkdir -p "$output_home/acceptance.avd"
  : > "$output_home/acceptance.ini"
  : > "$output_home/acceptance.avd/config.ini"
fi
printf '%s' "$first" > "$JAZZ_AVD_TRANSCRIPT"
`,
    { mode: 0o755 },
  );
  execFileSync(
    "bash",
    [
      path.join(root, "scripts/create-android-avd.sh"),
      "acceptance",
      "system-images;android-35;google_apis;x86_64",
    ],
    {
      env: {
        ...process.env,
        ANDROID_AVD_HOME: avdHome,
        JAZZ_DEVICE_AVDMANAGER: avdmanager,
        JAZZ_AVD_TRANSCRIPT: transcript,
      },
    },
  );
  assert.equal(fs.readFileSync(transcript, "utf8"), "no");
  assert.throws(() =>
    execFileSync(
      "bash",
      [
        path.join(root, "scripts/create-android-avd.sh"),
        "acceptance",
        "system-images;android-35;google_apis;x86_64",
      ],
      {
        env: {
          ...process.env,
          ANDROID_AVD_HOME: avdHome,
          JAZZ_DEVICE_AVDMANAGER: avdmanager,
          JAZZ_AVD_TRANSCRIPT: transcript,
          JAZZ_AVD_REQUIRE_SECOND_PROMPT: "1",
        },
      },
    ),
  );

  const globalAvdHome = path.join(directory, "runner-global-avd");
  assert.throws(
    () =>
      execFileSync(
        "bash",
        [
          path.join(root, "scripts/create-android-avd.sh"),
          "acceptance",
          "system-images;android-35;google_apis;x86_64",
        ],
        {
          env: {
            ...process.env,
            ANDROID_AVD_HOME: path.join(directory, "wrong-placement"),
            JAZZ_DEVICE_AVDMANAGER: avdmanager,
            JAZZ_AVD_TRANSCRIPT: transcript,
            JAZZ_AVD_WRONG_HOME: globalAvdHome,
          },
        },
      ),
    /configured lane-local device/,
    "creation must fail rather than letting boot search a runner-global AVD home",
  );

  assert.throws(
    () =>
      execFileSync(
        "bash",
        [
          path.join(root, "scripts/create-android-avd.sh"),
          "acceptance",
          "system-images;android-35;google_apis;x86_64",
        ],
        {
          env: {
            ...process.env,
            ANDROID_AVD_HOME: path.join(directory, "missing-files"),
            JAZZ_DEVICE_AVDMANAGER: avdmanager,
            JAZZ_AVD_TRANSCRIPT: transcript,
            JAZZ_AVD_OMIT_FILES: "1",
          },
        },
      ),
    /configured lane-local device/,
    "creation must fail when avdmanager reports success without both AVD files",
  );
});

test("checksum pin rejects a planted corrupt cache archive", () => {
  const archive = path.join(
    fs.mkdtempSync(path.join(os.tmpdir(), "jazz-corrupt-cache-")),
    "jdk.tgz",
  );
  fs.writeFileSync(archive, "corrupt");
  assert.throws(() =>
    execFileSync("bash", [
      path.join(root, "scripts/verify-pinned-archive.sh"),
      archive,
      "0".repeat(64),
    ]),
  );
});

test("iOS fixture owns launch-bound metadata and trusted ABI/admission probes", () => {
  const fixture = read("native/ios/JazzDeviceFixture.mm");
  assert.match(fixture, /JazzRelayTrustedAdmission admitScopeJSON/);
  assert.match(fixture, /RCT_REMAP_METHOD\(receiptContext/);
  assert.match(fixture, /@"schema_json": @"\{\\"tables\\":\{\}\}"/);
  assert.doesNotMatch(fixture, /@"schema_json": @"\{\}"/);
  assert.match(fixture, /11111111-1111-4111-8111-111111111111/);
  assert.ok(
    fixture.includes('@"author": @"[\\"https://jazz.device.test\\",\\"fixture-user-a\\"]"'),
  );
  assert.match(fixture, /@"claims": @\{\}/);
  for (const key of [
    "-JazzDeviceRunNonce",
    "-JazzDeviceBuildFingerprint",
    "-JazzDeviceDeviceIdentifier",
  ]) {
    assert.match(fixture, new RegExp(key));
  }
  assert.match(fixture, /RCT_REMAP_METHOD\(recordReceipt/);
  assert.match(fixture, /JAZZ_DEVICE_RESULT/);
  assert.match(fixture, /NSDataWritingAtomic/);
  assert.doesNotMatch(fixture, /recordReceipt[\s\S]*JazzRelayTrustedAdmission/);
});

test("iOS acceptance embeds JavaScript and reports launch diagnostics on receipt timeout", () => {
  const workflow = fs.readFileSync(
    path.resolve(root, "../../.github/workflows/rn-device-acceptance.yml"),
    "utf8",
  );
  const driver = read("scripts/run-ios.mjs");
  assert.match(workflow, /-configuration Release -sdk iphonesimulator/);
  assert.match(workflow, /Release-iphonesimulator\/JazzRNdeviceacceptance\.app/);
  for (const detail of [
    "parseLaunchProcessId",
    "get_app_container",
    "jazz-device-receipt.ndjson",
    "receiptFile",
    "rmSync\\(receiptFilePath\\(\\)",
    "launchctl",
    "recent app logs \\(capped\\)",
    'process == "JazzRNdeviceacceptance"',
  ]) {
    assert.match(driver, new RegExp(detail));
  }
  assert.match(driver, /assertDeviceReceipt\(receiptFile\(\), expected\)/);
  // The sandbox file is the receipt transport. App logs remain diagnostics
  // only: release React Native logging is not reliable evidence transport.
  assert.doesNotMatch(driver, /assertDeviceReceipt\(receiptOutput\(\), expected\)/);
  assert.doesNotMatch(driver, /eventMessage CONTAINS 'JAZZ_DEVICE_RESULT'/);
  const app = read("App.tsx");
  assert.match(app, /await proveAdmittedRelay/);
  assert.match(app, /await recordDeviceReceipt\(result\)/);
  assert.ok(
    app.indexOf("await proveAdmittedRelay") < app.indexOf("await recordDeviceReceipt(result)"),
    "the native receipt sink must run only after the JS relay proof",
  );
});
