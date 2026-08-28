import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const script = new URL("./boot-android-emulator.sh", import.meta.url).pathname;

const withFixture = (name, adbBody, emulatorBody, assertion, options = {}) => {
  const fixture = mkdtempSync(join(tmpdir(), `jazz-rn-boot-${name}-`));
  try {
    const adb = join(fixture, "adb");
    const emulator = join(fixture, "emulator");
    const sessionLauncher = join(fixture, "session-launcher");
    const timeout = join(fixture, "timeout");
    const log = join(fixture, "emulator.log");
    const config = join(fixture, "config.ini");
    writeFileSync(adb, `#!/usr/bin/env bash\nset -euo pipefail\n${adbBody}\n`);
    writeFileSync(emulator, `#!/usr/bin/env bash\nset -euo pipefail\n${emulatorBody}\n`);
    // macOS has neither GNU `setsid` nor GNU `timeout`. These deliberately
    // small test doubles exercise the receipt's bounded state machine without
    // claiming to prove Linux process-group cleanup (covered below on Linux).
    writeFileSync(sessionLauncher, "#!/usr/bin/env bash\nexec \"$@\"\n");
    writeFileSync(
      timeout,
      "#!/usr/bin/env bash\nduration=${2%s}\nshift 2\nexec perl -e 'alarm shift; exec @ARGV' \"$duration\" \"$@\"\n",
    );
    writeFileSync(config, "avd.id=acceptance\n");
    chmodSync(adb, 0o755);
    chmodSync(emulator, 0o755);
    chmodSync(sessionLauncher, 0o755);
    chmodSync(timeout, 0o755);
    const result = spawnSync("bash", [script, "test-avd", log, config], {
      encoding: "utf8",
      env: {
        ...process.env,
        JAZZ_DEVICE_ADB: adb,
        JAZZ_DEVICE_EMULATOR: emulator,
        JAZZ_ANDROID_BOOT_TIMEOUT_SECONDS: "1",
        JAZZ_ANDROID_BOOT_POLL_SECONDS: "0.05",
        ...(process.platform === "linux"
          ? {}
          : {
              JAZZ_ANDROID_SESSION_LAUNCHER: sessionLauncher,
              JAZZ_ANDROID_SESSION_PROCESS_GROUP: "0",
              JAZZ_ANDROID_TIMEOUT_COMMAND: timeout,
            }),
        ...options.env,
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

test("Android boot receipt caps a long poll interval at its boot deadline", () => {
  const started = Date.now();
  withFixture(
    "long-poll",
    '[[ "$1" == get-state ]] && exit 1',
    'echo "still starting"; sleep 10',
    (result) => {
      assert.equal(result.status, 1);
      assert.match(result.stderr, /within 1s/);
    },
    { env: { JAZZ_ANDROID_BOOT_POLL_SECONDS: "10" } },
  );
  assert.ok(Date.now() - started < 2_500, "long polling must not outlive the boot deadline");
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
  const emulatorFile = join(tmpdir(), `jazz-rn-boot-emulator-${process.pid}-${Date.now()}`);
  withFixture(
    "booted",
    'case "$1" in get-state) echo device ;; shell) echo 1 ;; *) exit 2 ;; esac',
    'echo $$ > "$JAZZ_TEST_EMULATOR_PID"; echo "started"; sleep 10',
    (result) => {
      assert.equal(result.status, 0, result.stderr);
      assert.match(result.stdout, /booted/);
    },
    { env: { JAZZ_TEST_EMULATOR_PID: emulatorFile } },
  );
  const emulatorPid = Number(readFileSync(emulatorFile, "utf8").trim());
  try {
    assert.doesNotThrow(() => process.kill(emulatorPid, 0));
  } finally {
    process.kill(process.platform === "linux" ? -emulatorPid : emulatorPid, "SIGKILL");
    rmSync(emulatorFile, { force: true });
  }
});

test("Android boot receipt supports the macOS test launcher without relaxing the Linux default", () => {
  withFixture(
    "portable-launcher",
    'case "$1" in get-state) echo device ;; shell) echo 1 ;; *) exit 2 ;; esac',
    'echo "started"; exec sleep 1',
    (result) => assert.equal(result.status, 0, result.stderr),
    {
      env: {
        JAZZ_ANDROID_SESSION_LAUNCHER: "/bin/sh",
        JAZZ_ANDROID_SESSION_PROCESS_GROUP: "0",
      },
    },
  );
});

test("Android boot receipt cleans up the complete emulator process group on failure", { skip: process.platform !== "linux" }, () => {
  const childFile = join(tmpdir(), `jazz-rn-boot-child-${process.pid}-${Date.now()}`);
  try {
    withFixture(
      "child-cleanup",
      '[[ "$1" == get-state ]] && exit 1',
      'sleep 10 & echo $! > "$JAZZ_TEST_CHILD_PID"; wait',
      (result) => assert.equal(result.status, 1),
      { env: { JAZZ_TEST_CHILD_PID: childFile } },
    );
    const childPid = readFileSync(childFile, "utf8").trim();
    assert.throws(() => process.kill(Number(childPid), 0), { code: "ESRCH" });
  } finally {
    rmSync(childFile, { force: true });
  }
});

test("Android boot receipt removes its emulator process group when cancelled", { skip: process.platform !== "linux" }, async () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-rn-boot-cancel-"));
  const adb = join(fixture, "adb");
  const emulator = join(fixture, "emulator");
  const log = join(fixture, "emulator.log");
  const config = join(fixture, "config.ini");
  const childFile = join(fixture, "child.pid");
  writeFileSync(adb, '#!/usr/bin/env bash\nexit 1\n');
  writeFileSync(emulator, '#!/usr/bin/env bash\nsleep 30 & echo $! > "$JAZZ_TEST_CHILD_PID"; wait\n');
  writeFileSync(config, "avd.id=acceptance\n");
  chmodSync(adb, 0o755);
  chmodSync(emulator, 0o755);
  try {
    const receipt = spawn("bash", [script, "test-avd", log, config], {
      env: {
        ...process.env,
        JAZZ_DEVICE_ADB: adb,
        JAZZ_DEVICE_EMULATOR: emulator,
        JAZZ_TEST_CHILD_PID: childFile,
        JAZZ_ANDROID_BOOT_TIMEOUT_SECONDS: "30",
        JAZZ_ANDROID_BOOT_POLL_SECONDS: "0.05",
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 150));
    receipt.kill("SIGTERM");
    const status = await new Promise((resolve) => receipt.on("close", resolve));
    assert.equal(status, 143);
    const childPid = Number(readFileSync(childFile, "utf8").trim());
    assert.throws(() => process.kill(childPid, 0), { code: "ESRCH" });
  } finally {
    rmSync(fixture, { recursive: true, force: true });
  }
});

test("workflow delegates Android boot to the bounded receipt script", () => {
  const workflow = new URL("../../../.github/workflows/rn-device-acceptance.yml", import.meta.url).pathname;
  const text = readFileSync(workflow, "utf8");
  assert.ok(workflow.endsWith("rn-device-acceptance.yml"));
  assert.match(text, /boot-android-emulator\.sh/);
  assert.doesNotMatch(text, /adb wait-for-device/);
  assert.match(text, /JAZZ_ANDROID_SESSION_LAUNCHER=setsid/);
  assert.match(text, /JAZZ_ANDROID_SESSION_PROCESS_GROUP=1/);

  const receipt = readFileSync(script, "utf8");
  assert.match(receipt, /session_launcher=\$\{JAZZ_ANDROID_SESSION_LAUNCHER:-setsid\}/);
  assert.match(receipt, /session_process_group=\$\{JAZZ_ANDROID_SESSION_PROCESS_GROUP:-1\}/);
});
