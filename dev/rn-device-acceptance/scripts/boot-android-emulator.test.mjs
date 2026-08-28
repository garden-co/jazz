import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const script = new URL("./boot-android-emulator.sh", import.meta.url).pathname;

const withFixture = (name, adbBody, emulatorBody, assertion) => {
  const fixture = mkdtempSync(join(tmpdir(), `jazz-rn-boot-${name}-`));
  try {
    const adb = join(fixture, "adb");
    const emulator = join(fixture, "emulator");
    const log = join(fixture, "emulator.log");
    const config = join(fixture, "config.ini");
    writeFileSync(adb, `#!/usr/bin/env bash\nset -euo pipefail\n${adbBody}\n`);
    writeFileSync(emulator, `#!/usr/bin/env bash\nset -euo pipefail\n${emulatorBody}\n`);
    writeFileSync(config, "avd.id=acceptance\n");
    chmodSync(adb, 0o755);
    chmodSync(emulator, 0o755);
    const result = spawnSync("bash", [script, "test-avd", log, config], {
      encoding: "utf8",
      env: {
        ...process.env,
        JAZZ_DEVICE_ADB: adb,
        JAZZ_DEVICE_EMULATOR: emulator,
        JAZZ_ANDROID_BOOT_TIMEOUT_SECONDS: "1",
        JAZZ_ANDROID_BOOT_POLL_SECONDS: "0.05",
      },
      timeout: 5_000,
    });
    assertion(result);
  } finally {
    rmSync(fixture, { recursive: true, force: true });
  }
};

test("Android boot receipt fails under its own deadline when adb has no device", () => {
  withFixture("no-device", '[[ "$1" == get-state ]] && exit 1', 'echo "still starting"; sleep 10', (result) => {
    assert.equal(result.status, 1);
    assert.match(result.stderr, /within 1s/);
    assert.match(result.stderr, /emulator log/);
    assert.doesNotMatch(result.stderr, /wait-for-device/);
  });
});

test("Android boot receipt diagnoses an emulator that exits before adb registration", () => {
  withFixture("early-exit", 'exit 1', 'echo "fatal startup"; exit 17', (result) => {
    assert.equal(result.status, 1);
    assert.match(result.stderr, /process exited before boot completed/);
    assert.match(result.stderr, /fatal startup/);
    assert.match(result.stderr, /avd.id=acceptance/);
  });
});

test("Android boot receipt accepts the device-to-boot-complete transition", () => {
  withFixture(
    "booted",
    'case "$1" in get-state) echo device ;; shell) echo 1 ;; *) exit 2 ;; esac',
    'echo "started"; sleep 10',
    (result) => {
      assert.equal(result.status, 0, result.stderr);
      assert.match(result.stdout, /booted/);
    },
  );
});

test("workflow delegates Android boot to the bounded receipt script", () => {
  const workflow = new URL("../../../.github/workflows/rn-device-acceptance.yml", import.meta.url).pathname;
  const text = readFileSync(workflow, "utf8");
  assert.ok(workflow.endsWith("rn-device-acceptance.yml"));
  assert.match(text, /boot-android-emulator\.sh/);
  assert.doesNotMatch(text, /adb wait-for-device/);
});
