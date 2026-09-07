import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  captureAndroidFailure,
  summarizeAndroidExitInfo,
  summarizeAndroidCrashBuffer,
} from "./android-postmortem.mjs";

const exitInfo = `ApplicationExitInfo #0:
 timestamp=2026-09-04 19:00:00.000 pid=42 realUid=123
 process=dev.jazz.rndeviceacceptance reason=5 (CRASH NATIVE) subreason=0 (UNKNOWN) status=6
 description=secret-payload trace=/private/secret
ApplicationExitInfo #1:
 timestamp=2026-09-04 18:00:00.000 pid=43
 process=unrelated.app reason=4 (CRASH) status=0
 description=process=dev.jazz.rndeviceacceptance secret
`;
const crash = `*** *** ***
pid: 42, tid: 44, name: mqt_js >>> dev.jazz.rndeviceacceptance <<<
signal 6 (SIGABRT), code -1
Abort message: 'secret-payload'
 #00 pc abcd /private/libc.so (syscall+4)
 #01 pc def0 /private/base.apk!libjazzrelay.so (NativeRelay::pump+4)
*** *** ***
pid: 50, tid: 50, name: main >>> unrelated.app <<<
signal 11 (SIGSEGV), code -1
 #00 pc dead /private/libc.so (futex)
`;

test("postmortem retains only target exit status and signal/frames, not descriptions or payloads", () => {
  assert.deepEqual(summarizeAndroidExitInfo(exitInfo), [
    {
      reason: "native-crash",
      reasonCode: 5,
      status: 6,
      pid: 42,
      subreason: 0,
      timestamp: "2026-09-04 19:00:00.000",
    },
  ]);
  const result = summarizeAndroidCrashBuffer(crash);
  assert.equal(result.length, 1);
  assert.equal(result[0].signal, "SIGABRT");
  assert.equal(result[0].threads[0].thread, "mqt_js");
  assert.equal(result[0].threads[0].frames[1].symbol, "NativeRelay");
  assert.doesNotMatch(
    JSON.stringify([summarizeAndroidExitInfo(exitInfo), result]),
    /secret|private|unrelated|dead/,
  );
});

test("Java crash class is fixed and its exception text is never emitted", () => {
  assert.deepEqual(
    summarizeAndroidCrashBuffer(`FATAL EXCEPTION: main
Process: dev.jazz.rndeviceacceptance, PID: 42
java.lang.OutOfMemoryError: secret
at arbitrary.secret.frame(secret:1)
FATAL EXCEPTION: main
Process: unrelated.app, PID: 43
java.lang.IllegalStateException: another-secret`),
    [{ kind: "java", error: "java.lang.OutOfMemoryError" }],
  );
});

test("post-exit capture is read-only, bounded, and ordered before driver process cleanup", () => {
  const calls = [];
  const result = captureAndroidFailure({
    exec(_command, args, options) {
      calls.push({ args, options });
      if (calls.length === 1) return "";
      return calls.length === 2 ? exitInfo : crash;
    },
  });
  assert.equal(result.backtrace.status, "process-not-running");
  assert.equal(result.exitInfo.records[0].reason, "native-crash");
  assert.deepEqual(
    calls.map((call) => call.options.timeout),
    [2000, 3000, 3000],
  );
  assert.deepEqual(
    calls.map((call) => call.args),
    [
      ["shell", "pidof", "dev.jazz.rndeviceacceptance"],
      ["shell", "dumpsys", "activity", "exit-info", "dev.jazz.rndeviceacceptance"],
      ["logcat", "-b", "crash", "-d", "-v", "raw"],
    ],
  );
  const driver = readFileSync(new URL("./run-android.mjs", import.meta.url), "utf8");
  assert.match(
    driver,
    /if \(Date\.now\(\) >= deadline\)[\s\S]*?throw new Error\([\s\S]*?captureAndroidFailure/,
  );
  assert.equal((driver.match(/"force-stop"/g) ?? []).length, 1);
  assert.ok(driver.indexOf('await launchAndAssert("seed")') < driver.indexOf('"force-stop"'));
  assert.doesNotMatch(driver.slice(driver.lastIndexOf("} finally {")), /force-stop|emu.*kill/);
});

test("postmortem read failures remain safe and cannot replace the original timeout", () => {
  let calls = 0;
  const result = captureAndroidFailure({
    exec() {
      if (++calls === 1) return "";
      throw new Error("secret");
    },
  });
  assert.deepEqual(result, {
    backtrace: { status: "process-not-running" },
    exitInfo: { status: "unavailable" },
    crash: { status: "unavailable" },
  });
});

test("native main-thread crash names are normalized by pid/tid rather than exposed", () => {
  const result =
    summarizeAndroidCrashBuffer(`pid: 42, tid: 42, name: private-process >>> dev.jazz.rndeviceacceptance <<<
signal 6 (SIGABRT), code -1
 #00 pc abcd /private/libc.so (syscall+4)`);
  assert.equal(result[0].threads[0].thread, "main");
  assert.doesNotMatch(JSON.stringify(result), /private/);
});
