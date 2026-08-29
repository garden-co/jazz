import assert from "node:assert/strict";
import fs from "node:fs";
import { execFileSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "..");
const read = (file) => fs.readFileSync(path.join(root, file), "utf8");

// Documentation is part of the native-package contract. Normalize prose
// formatting so legitimate wrapping/Markdown emphasis edits do not turn this
// into a brittle snapshot, while materially stronger or weaker claims fail.
const prose = (text) => text.replace(/[`*]/g, "").replace(/\s+/g, " ").trim();

function assertCurrentRnBoundary(packageReadme, installGuide, spec) {
  assert.match(
    packageReadme,
    /narrow alpha rather than general React Native support/,
    "the package must not claim broad RN support",
  );
  assert.match(
    packageReadme,
    /matching native development or release build.*capability issued by trusted platform admission/i,
    "persistent foreground use must retain both native-build and trusted-admission requirements",
  );
  assert.match(
    packageReadme,
    /two physical JSI runtimes.*remains pending/i,
    "same-runtime alias coverage must not be advertised as a multi-runtime device proof",
  );
  assert.match(
    installGuide,
    /narrow, capability-gated foreground alpha.*matching native development\/release build.*trusted platform admission/i,
    "the public install guide must describe the same constrained boundary",
  );
  assert.match(
    spec,
    /physical JSI runtime.*pending/i,
    "the normative implementation sequence must retain the missing physical-runtime proof",
  );
  assert.doesNotMatch(
    packageReadme,
    /high-level jazz client.*under restoration|modules deliberately report that the Rust relay artifact is unavailable/i,
    "removed pre-foreground claims must not survive in the packaging README",
  );
}

function jobIfCondition(workflow, job) {
  const escapedJob = job.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const section = new RegExp(
    `^  ${escapedJob}:\\n([\\s\\S]*?)(?=^  [A-Za-z0-9_-]+:|(?![\\s\\S]))`,
    "m",
  ).exec(workflow)?.[1];
  assert.ok(section, `workflow is missing ${job}`);
  const condition = /^    if: >-\n([\s\S]*?)(?=^    (?:name|runs-on):)/m.exec(section)?.[1];
  assert.ok(condition, `${job} must have a block scalar if condition before name/runs-on`);
  return condition.replace(/\s+/g, " ").trim();
}

function assertRnDeviceWorkflowContract(workflow) {
  const ios = jobIfCondition(workflow, "ios-simulator");
  const android = jobIfCondition(workflow, "android-device-acceptance");
  assert.equal(
    android,
    ios,
    "Android and iOS native device jobs must use one identical opt-in contract",
  );
  assert.match(
    android,
    /github\.event_name == 'workflow_dispatch' \|\| \( contains\(github\.event\.pull_request\.labels\.\*\.name, 'react-native\/rn-preview-release'\) && github\.event\.pull_request\.head\.repo\.full_name == github\.repository \)/,
    "native device jobs run only when manually dispatched or for a same-repository RN-preview PR",
  );
}

test("Android fixture BuildConfig fields and package registration remain compile-shaped", () => {
  const gradle = read("android/app/build.gradle");
  const fixture = read(
    "android/app/src/main/java/dev/jazz/rndeviceacceptance/JazzDeviceFixtureModule.kt",
  );
  assert.equal(
    fixture,
    read("native/android/JazzDeviceFixtureModule.kt"),
    "the checked-in Android host fixture must match the prebuild template",
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
    "VERIFIED_CLAIMS_JSON",
  ]) {
    assert.match(gradle, new RegExp(`buildConfigField "String", "JAZZ_DEVICE_${field}"`));
    assert.match(fixture, new RegExp(`BuildConfig\\.JAZZ_DEVICE_${field}`));
  }
  assert.match(gradle, /JAZZ_DEVICE_SCHEMA_JSON.*todos/);
  assert.match(registration, /class JazzDeviceFixturePackage : ReactPackage/);
  assert.match(registration, /listOf\(JazzDeviceFixtureModule\(context\)\)/);
  assert.match(host, /add\(JazzDeviceFixturePackage\(\)\)/);
  assert.match(fixture, /Build\.FINGERPRINT/);
  assert.match(fixture, /scopeConfig\(authScope: String\)/);
  assert.match(fixture, /fixture-user-b/);
  assert.match(fixture, /JazzRelayTrustedAdmission\.replace/);
  assert.match(fixture, /jazz-device-\$authScope\.sqlite/);
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

test("each native JSI runtime owns an independent foreground lease", () => {
  const androidBridge = fs.readFileSync(
    path.resolve(root, "../../crates/jazz-rn/android/cpp-relay.cpp"),
    "utf8",
  );
  const iosBridge = fs.readFileSync(
    path.resolve(root, "../../crates/jazz-rn/ios/JazzRelay.mm"),
    "utf8",
  );
  // Android's Kotlin wrapper already allocates a runtime token. This receipt
  // fails if JNI silently drops it and returns to one host-global lease.
  assert.match(androidBridge, /ForegroundRuntimeKey = std::pair<jazz_native_relay_host \*, jlong>/);
  assert.match(androidBridge, /nativeForegroundBindingsInstaller\([\s\S]*jlong runtime_token\)/);
  assert.match(androidBridge, /foregroundInstallation\(relay_host, runtime_token, callInvoker\)/);
  assert.match(androidBridge, /foreground_installations\.find\(\{relay_host, runtime_token\}\)/);
  assert.doesNotMatch(
    androidBridge,
    /std::unordered_map<jazz_native_relay_host \*, std::shared_ptr<ForegroundRuntimeInstallation>>/,
  );
  // iOS uses an opaque, monotonically allocated runtime token instead of an
  // Objective-C pointer identity. A pointer can be recycled after teardown;
  // the token is never reused and is also checked by the native lease. A
  // planted process-global `foregroundRuntimeLease` would make either bridge
  // teardown kill its sibling and fails this source/device-host receipt.
  assert.match(iosBridge, /static uint64_t nextForegroundRuntimeToken = 1/);
  assert.match(iosBridge, /std::unordered_map<uint64_t, ForegroundRuntimeInstallation>/);
  assert.match(iosBridge, /foregroundRuntimeLeases\.find\(runtimeToken\)/);
  assert.match(iosBridge, /foregroundRuntimeLeases\.erase\(found\)/);
  assert.doesNotMatch(iosBridge, /std::unordered_map<JazzRelay \*, ForegroundRuntimeInstallation>/);
  assert.doesNotMatch(
    iosBridge,
    /static std::shared_ptr<jazz::rn::ForegroundRuntimeLease> foregroundRuntimeLease/,
  );
});

test("foreground wake lifecycle clears native callbacks before close or runtime revocation", () => {
  const foregroundRuntime = fs.readFileSync(
    path.resolve(root, "../../crates/jazz-rn/native/foreground-runtime.cpp"),
    "utf8",
  );
  // A CallInvoker task may already be queued when the JS alias closes. The
  // registration must first become inactive, clear the Rust callback, and the
  // delayed task must re-check active state before it looks up JS values.
  assert.match(foregroundRuntime, /active_ = false;[\s\S]*pending_ = false;/);
  assert.match(
    foregroundRuntime,
    /jazz_native_relay_host_lease_set_foreground_wake_callback\([\s\S]*nullptr, nullptr\)/,
  );
  assert.match(foregroundRuntime, /if \(!active_ \|\| !pending_\) return;/);
  assert.match(foregroundRuntime, /if \(kind == kWakeCancelled\) \{[\s\S]*active_ = false;/);
  assert.match(foregroundRuntime, /catch \(\.\.\.\) \{[\s\S]*scheduled_ = false;/);
  // Native handle close/revoke reaches the same clear-before-free operation;
  // a late callback cannot retain a stale JSI function or escape as unhandled.
  assert.match(foregroundRuntime, /wake_->deactivateAndClear\(lease_->nativeLease\(\)\);/);
  assert.match(foregroundRuntime, /wake_->removeCallback\(runtime\);/);
});

test("Expo config plugin describes the real iOS receipt boundary without claiming TODO scenarios", () => {
  const plugin = read("plugins/with-jazz-device-fixture.cjs");
  assert.match(plugin, /JAZZ_DEVICE_SCHEMA_JSON.*todos/);
  assert.match(plugin, /label-gated iOS simulator workflow/);
  assert.match(plugin, /requires its linked/);
  assert.match(plugin, /ABI\/admission receipt/);
  assert.match(plugin, /Multi-peer acceptance remains TODO \(#2291\)/);
  assert.doesNotMatch(plugin, /this workflow is source-only/);
});

test("RN packaging and public docs describe the proven alpha boundary semantically", () => {
  const packageReadme = prose(
    fs.readFileSync(path.resolve(root, "../../crates/jazz-rn/README.md"), "utf8"),
  );
  const installGuide = prose(
    fs.readFileSync(path.resolve(root, "../../docs/content/docs/install/client.mdx"), "utf8"),
  );
  const spec = prose(
    fs.readFileSync(path.resolve(root, "../../crates/jazz/SPEC/19_native_relays.md"), "utf8"),
  );
  assertCurrentRnBoundary(packageReadme, installGuide, spec);

  // Plant the tempting but false extrapolation from two aliases in one JSI
  // runtime to two physical runtimes. The semantic receipt must reject it.
  assert.throws(
    () =>
      assertCurrentRnBoundary(
        packageReadme.replace("two physical JSI runtimes", "two aliases in one JSI runtime"),
        installGuide,
        spec,
      ),
    /same-runtime alias coverage/i,
  );
});

test("device fixture does not import internal jazz-tools relay-frame types", () => {
  const fixture = read("src/native-fixture.ts");
  assert.doesNotMatch(fixture, /NativeRelay(?:Capability|Executor)/);
  assert.match(fixture, /executor: \{ execute: executeNativeRelayCommand \}/);
});

test("process-restart acceptance has two disjoint, host-terminated phases", () => {
  const androidDriver = read("scripts/run-android.mjs");
  const iosDriver = read("scripts/run-ios.mjs");
  const app = read("App.tsx");
  const highLevelForeground = read("src/high-level-foreground.ts");
  const scenarios = read("src/scenarios.ts");
  const androidFixture = read("native/android/JazzDeviceFixtureModule.kt");
  const iosFixture = read("native/ios/JazzDeviceFixture.mm");

  for (const driver of [androidDriver, iosDriver]) {
    assert.match(driver, /launchAndAssert\("seed"\)/);
    assert.match(driver, /launchAndAssert\("verify"\)/);
    assert.match(driver, /scenariosForAcceptancePhase\(phase\)/);
  }
  assert.match(androidDriver, /am", "force-stop", "dev\.jazz\.rndeviceacceptance/);
  assert.match(iosDriver, /simctl\(\["terminate", udid, "dev\.jazz\.rndeviceacceptance"\]\)/);
  assert.match(scenarios, /phase === "verify"\s*\?\s*scenario\.scenario === "reopen"/);
  // The restart assertion must cross the public RN API. A byte-level `all`
  // after re-admission only proves the host transport; it does not prove a
  // fresh application can start, select its foreground runtime, and decode a
  // persisted row through `createJazzClient`.
  assert.match(app, /await proveHighLevelForegroundRestart\(reopened\.capability\)/);
  assert.match(highLevelForeground, /createJazzClient\(clientConfig\(capability\)\)/);
  assert.match(highLevelForeground, /client\.db\.all\(app\.todos\)/);
  assert.match(highLevelForeground, /prior process's persisted row/);
  assert.match(app, /\{\s*contains: \["a"\],\s*excludes: \["b"\],?\s*\}/);
  assert.match(app, /\{\s*contains: \["b"\],\s*excludes: \["a"\],?\s*\}/);
  assert.match(app, /\{\s*write: "a",\s*contains: \["a"\],\s*excludes: \["b"\],?\s*\}/);
  assert.match(app, /\{\s*write: "b",\s*contains: \["b"\],\s*excludes: \["a"\],?\s*\}/);
  assert.match(androidFixture, /@ReactMethod fun acceptancePhase/);
  assert.match(androidFixture, /jazzDeviceAcceptancePhase/);
  assert.match(iosFixture, /RCT_REMAP_METHOD\(acceptancePhase/);
  assert.match(iosFixture, /-JazzDeviceAcceptancePhase/);
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
  assertRnDeviceWorkflowContract(workflow);
  assert.throws(
    () =>
      assertRnDeviceWorkflowContract(
        workflow.replace(
          "contains(github.event.pull_request.labels.*.name, 'react-native/rn-preview-release')",
          "contains(github.event.pull_request.labels.*.name, 'react-native')",
        ),
      ),
    /identical opt-in contract|same-repository RN-preview PR/,
    "a planted broad Android label must not silently enable a different device gate",
  );
  assert.doesNotMatch(workflow, /labels\.\*\.name, 'react-native'/);
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
  assert.match(fixture, /replaceCapability:self\.capability withScopeJSON/);
  assert.match(fixture, /JazzDeviceScopeFixture\(NSString \*authScope\)/);
  assert.match(fixture, /RCT_REMAP_METHOD\(receiptContext/);
  assert.match(fixture, /@"schema_json": @"\{\\"tables\\":\{\\"todos\\":/);
  assert.match(fixture, /\\"column_type\\":\{\\"type\\":\\"Text\\"\}/);
  assert.doesNotMatch(fixture, /@"schema_json": @"\{\\"tables\\":\{\}\}"/);
  assert.match(fixture, /11111111-1111-4111-8111-111111111111/);
  assert.match(fixture, /fixture-user-a/);
  assert.match(fixture, /fixture-user-b/);
  assert.match(fixture, /22222222-2222-4222-8222-222222222222/);
  assert.match(fixture, /jazz-device-%@\.sqlite/);
  assert.match(fixture, /@"claims": @\{\}/);
  for (const key of ["-JazzDeviceRunNonce", "-JazzDeviceDeviceIdentifier"]) {
    assert.match(fixture, new RegExp(key));
  }
  assert.match(fixture, /#import <CommonCrypto\/CommonDigest\.h>/);
  assert.match(fixture, /NSBundle\.mainBundle\.executablePath/);
  assert.match(fixture, /CC_SHA256\(data\.bytes, \(CC_LONG\)data\.length, digest\)/);
  assert.doesNotMatch(fixture, /JazzDeviceBuildFingerprint/);
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
  assert.match(app, /installNativeForegroundRuntime/);
  assert.match(app, /proveForegroundByteAbi/);
  assert.match(app, /proveForegroundRevoked/);
  assert.match(app, /encodeNativeForegroundCommand/);
  assert.match(app, /decodeNativeForegroundResponse/);
  assert.match(app, /await proveLogoutRevocation/);
  assert.match(app, /await proveAuthScopeSwitch/);
  assert.match(app, /switchNativeRelayAuthScope/);
  assert.match(app, /logoutNativeRelay/);
  assert.match(app, /oldScopeForeground = foregroundFactory\.openAttached\(scopeA\.capability\)/);
  assert.match(app, /proveForegroundRevoked\(oldScopeForeground, foregroundCodec\.encode\)/);
  assert.match(
    app,
    /proveForegroundByteAbi\(foregroundFactory, scopeB\.capability, foregroundCodec\)/,
  );
  assert.match(app, /await recordDeviceReceipt\(results\.join\("\\n"\)\)/);
  assert.ok(
    app.indexOf("await observeTrustedAdmissionLifecycle()") <
      app.indexOf("await recordDeviceReceipt"),
    "the native receipt sink must run only after the complete JS relay lifecycle proof",
  );
});
