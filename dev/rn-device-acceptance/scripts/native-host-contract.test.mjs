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
  assert.match(bootstrap, /java" -version/);
  assert.match(bootstrap, /sdkmanager" --version/);
  assert.match(bootstrap, /cargo-ndk" --version/);
  assert.match(bootstrap, /reset_cache_path/);
});

test("dispatch workflow fails clearly without KVM and bounds emulator boot", () => {
  const workflow = fs.readFileSync(
    path.join(root, "../../.github/workflows/rn-device-acceptance.yml"),
    "utf8",
  );
  assert.match(workflow, /\[\[ -r \/dev\/kvm && -w \/dev\/kvm \]\]/);
  assert.match(workflow, /did not boot within 180s/);
  assert.match(workflow, /tail -200 "\$cache\/emulator\.log"/);
  assert.match(workflow, /android-device-acceptance:[\s\S]*timeout-minutes: 45/);
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
    "simctl launch did not return an app process id",
    "get_app_container",
    "launchctl",
    "recent app logs \\(capped\\)",
    'process == "JazzRNdeviceacceptance"',
  ]) {
    assert.match(driver, new RegExp(detail));
  }
});
