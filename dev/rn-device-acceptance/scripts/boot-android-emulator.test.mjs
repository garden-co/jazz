import assert from "node:assert/strict";
import { execFileSync, spawn, spawnSync } from "node:child_process";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const script = new URL("./boot-android-emulator.sh", import.meta.url).pathname;

function hostCommand(command) {
  return execFileSync("sh", ["-lc", `command -v ${command}`], { encoding: "utf8" }).trim();
}

const withFixture = (name, adbBody, emulatorBody, assertion, options = {}) => {
  const fixture = mkdtempSync(join(tmpdir(), `jazz-rn-boot-${name}-`));
  try {
    const adb = join(fixture, "adb");
    const emulator = join(fixture, "emulator");
    const sessionLauncher = join(fixture, "session-launcher");
    const portableBin = join(fixture, "portable-bin");
    const log = join(fixture, "emulator.log");
    const config = join(fixture, "config.ini");
    writeFileSync(adb, `#!/usr/bin/env bash\nset -euo pipefail\n${adbBody}\n`);
    writeFileSync(emulator, `#!/usr/bin/env bash\nset -euo pipefail\n${emulatorBody}\n`);
    // macOS has no `setsid`. This deliberately small test double exercises
    // the portable direct-child cleanup path without claiming to prove Linux
    // process-group cleanup (covered below on Linux).
    writeFileSync(sessionLauncher, '#!/usr/bin/env bash\nexec "$@"\n');
    writeFileSync(config, "avd.id=acceptance\n");
    chmodSync(adb, 0o755);
    chmodSync(emulator, 0o755);
    chmodSync(sessionLauncher, 0o755);
    if (options.portableNoTimeout) {
      // Make `command -v timeout` fail even on this Linux host, while leaving
      // the exact commands the receipt needs available. The Perl wrapper is a
      // receipt: a green portable test must have taken the macOS fallback.
      mkdirSync(portableBin);
      for (const command of ["bash", "sleep", "tail", "tr"]) {
        symlinkSync(hostCommand(command), join(portableBin, command));
      }
      writeFileSync(
        join(portableBin, "perl"),
        `#!/bin/sh\nprintf "%s\\n" "\${JAZZ_TEST_PERL_MARKER:?}" > "$JAZZ_TEST_PERL_MARKER"\nexec ${JSON.stringify(hostCommand("perl"))} "$@"\n`,
      );
      chmodSync(join(portableBin, "perl"), 0o755);
    }
    const result = spawnSync("bash", [script, "test-avd", log, config], {
      encoding: "utf8",
      env: {
        ...process.env,
        JAZZ_DEVICE_ADB: adb,
        JAZZ_DEVICE_EMULATOR: emulator,
        JAZZ_ANDROID_BOOT_TIMEOUT_SECONDS: "1",
        JAZZ_ANDROID_BOOT_POLL_SECONDS: "0.05",
        ...(options.portableNoTimeout ? { PATH: portableBin } : {}),
        ...(options.portableNoTimeout || process.platform !== "linux"
          ? {
              JAZZ_ANDROID_SESSION_LAUNCHER: sessionLauncher,
              JAZZ_ANDROID_SESSION_PROCESS_GROUP: "0",
            }
          : {}),
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
  withFixture(
    "no-device",
    '[[ "$1" == get-state ]] && exit 1',
    'echo "still starting"; sleep 10',
    (result) => {
      assert.equal(result.status, 1);
      assert.match(result.stderr, /within 1s/);
      assert.match(result.stderr, /emulator log/);
      assert.doesNotMatch(result.stderr, /wait-for-device/);
    },
  );
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
  withFixture("early-exit", "exit 1", 'echo "fatal startup"; exit 17', (result) => {
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

test("Android boot receipt uses the Perl macOS fallback without GNU timeout", () => {
  const perlMarker = join(tmpdir(), `jazz-rn-perl-fallback-${process.pid}-${Date.now()}`);
  withFixture(
    "portable-launcher",
    'case "$1" in get-state) echo device ;; shell) echo 1 ;; *) exit 2 ;; esac',
    'echo "started"; exec sleep 1',
    (result) => assert.equal(result.status, 0, result.stderr),
    {
      env: {
        JAZZ_TEST_PERL_MARKER: perlMarker,
      },
      portableNoTimeout: true,
    },
  );
  // Plant the old macOS-only failure: no source receipt may hard-code the
  // Linux coreutils path. On macOS the launcher takes its Perl fallback.
  assert.doesNotMatch(readFileSync(script, "utf8"), /\/usr\/bin\/timeout/);
  assert.equal(existsSync(perlMarker), true, "the timeout-free fixture must invoke Perl");
  rmSync(perlMarker, { force: true });
});

test("Perl fallback bounds a wedged adb probe and cleans up the portable child", () => {
  const perlMarker = join(tmpdir(), `jazz-rn-perl-timeout-${process.pid}-${Date.now()}`);
  const emulatorPidFile = join(tmpdir(), `jazz-rn-perl-emulator-${process.pid}-${Date.now()}`);
  try {
    withFixture(
      "portable-timeout",
      'while [[ ! -e "$JAZZ_TEST_EMULATOR_PID" ]]; do sleep 0.01; done; sleep 10',
      'echo $$ > "$JAZZ_TEST_EMULATOR_PID"; sleep 10',
      (result) => {
        assert.equal(result.status, 1);
        assert.match(result.stderr, /within 1s/);
      },
      {
        env: {
          JAZZ_TEST_PERL_MARKER: perlMarker,
          JAZZ_TEST_EMULATOR_PID: emulatorPidFile,
        },
        portableNoTimeout: true,
      },
    );
    assert.equal(existsSync(perlMarker), true, "a wedged probe must be killed by the Perl fallback");
    const emulatorPid = Number(readFileSync(emulatorPidFile, "utf8").trim());
    assert.throws(() => process.kill(emulatorPid, 0), { code: "ESRCH" });
  } finally {
    rmSync(perlMarker, { force: true });
    rmSync(emulatorPidFile, { force: true });
  }
});

test(
  "Android boot receipt cleans up the complete emulator process group on failure",
  { skip: process.platform !== "linux" },
  () => {
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
  },
);

test(
  "Android boot receipt removes its emulator process group when cancelled",
  { skip: process.platform !== "linux" },
  async () => {
    const fixture = mkdtempSync(join(tmpdir(), "jazz-rn-boot-cancel-"));
    const adb = join(fixture, "adb");
    const emulator = join(fixture, "emulator");
    const log = join(fixture, "emulator.log");
    const config = join(fixture, "config.ini");
    const childFile = join(fixture, "child.pid");
    writeFileSync(adb, "#!/usr/bin/env bash\nexit 1\n");
    writeFileSync(
      emulator,
      '#!/usr/bin/env bash\nsleep 30 & echo $! > "$JAZZ_TEST_CHILD_PID"; wait\n',
    );
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
  },
);

test("workflow delegates Android boot to the bounded receipt script", () => {
  const workflow = new URL("../../../.github/workflows/rn-device-acceptance.yml", import.meta.url)
    .pathname;
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
