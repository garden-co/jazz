import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { assertFixtureNetworkPolicy } from "./android-network-policy.mjs";

const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url), "utf8");
const manifest = read("android/app/src/main/AndroidManifest.xml");
const policy = read("native/android/jazz_device_network_security.xml");

test("release fixture selects only emulator-host cleartext policy and preserves prebuild copies", () => {
  assertFixtureNetworkPolicy(manifest, policy);
  assert.equal(policy, read("android/app/src/main/res/xml/jazz_device_network_security.xml"));
  assert.match(read("plugins/with-jazz-device-fixture.cjs"), /withAndroidManifest/);
  assert.match(read("plugins/with-jazz-device-fixture.cjs"), /android:networkSecurityConfig/);
  assert.match(read("scripts/run-android.mjs"), /verifyAndroidReleaseNetworkPolicy/);
  const native = read("native/android/JazzDeviceFixtureModule.kt");
  assert.match(native, /isCleartextTrafficPermitted\(url.host\)/);
  assert.match(native, /Log.e\("JazzDeviceAcceptance", "core-observation-cleartext-denied"\)/);
  assert.equal(
    native,
    read("android/app/src/main/java/dev/jazz/rndeviceacceptance/JazzDeviceFixtureModule.kt"),
  );
});

test("planted missing release policy and broad or foreign-host grants fail closed", () => {
  assert.throws(
    () =>
      assertFixtureNetworkPolicy(
        manifest.replace('android:networkSecurityConfig="@xml/jazz_device_network_security"', ""),
        policy,
      ),
    /release merged manifest/,
  );
  for (const planted of [
    policy.replace(
      'base-config cleartextTrafficPermitted="false"',
      'base-config cleartextTrafficPermitted="true"',
    ),
    policy.replace("10.0.2.2", "example.com"),
    policy.replace('includeSubdomains="false"', 'includeSubdomains="true"'),
  ])
    assert.throws(() => assertFixtureNetworkPolicy(manifest, planted), /only the emulator host/);
});

test("native acknowledgement emits bounded phases and typed errors separately from JS stages", () => {
  const native = read("native/android/JazzDeviceFixtureModule.kt");
  const ack = native
    .split("@ReactMethod fun waitForCoreObservation")[1]
    .split("@ReactMethod fun receiptContext")[0];
  assert.match(ack, /val status = request.responseCode/);
  assert.match(ack, /check\(status == 204\)/);
  assert.match(ack, /is java.net.SocketTimeoutException -> "timeout"/);
  assert.match(ack, /Log.e\("JazzCoreObservation", "failure-\$phase-\$category"\)/);
  assert.match(ack, /promise.resolve\(null\)\s*Log.e\("JazzCoreObservation", "promise-resolved"\)/);
  assert.doesNotMatch(ack, /Log\.[a-z]\([^\n]*(?:error\.message|error.toString|endpoint|identity)/);
  assert.match(read("scripts/run-android.mjs"), /"JazzCoreObservation:E"/);
});
