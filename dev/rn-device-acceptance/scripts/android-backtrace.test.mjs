import assert from "node:assert/strict";
import test from "node:test";
import { captureAndroidBacktrace, summarizeAndroidBacktrace } from "./android-backtrace.mjs";

test("native backtrace retains useful JS-thread offsets without raw process or payload text", () => {
  const summary = summarizeAndroidBacktrace(`Cmd line: secret-bearer
"mqt_js" sysTid=42
  #00 pc 0000AB /apex/private-path/libc.so (pthread_mutex_lock+8) secret-token
  #01 pc 0000CD /data/private-path/base.apk!libjazzrelay.so (jazz_native_relay_host_lease_execute_foreground+24)
  #02 pc 123abc /private/libjazz_secret.so (secret-token)
"secret-thread" sysTid=43
  #00 pc 123 /private/libc.so (futex)
"jazz-native-rel" sysTid=44
  #00 pc 456 /apex/libc.so (syscall+4)`);
  assert.deepEqual(summary, [
    {
      thread: "mqt_js",
      frames: [
        { library: "libc.so", pc: "0000ab", symbol: "pthread_mutex_lock" },
        {
          library: "libjazzrelay.so",
          pc: "0000cd",
          symbol: "jazz_native_relay_host_lease_execute_foreground",
        },
      ],
    },
    { thread: "jazz-native-rel", frames: [{ library: "libc.so", pc: "456", symbol: "syscall" }] },
  ]);
  assert.doesNotMatch(JSON.stringify(summary), /secret|private-path|Cmd line/);
});

test("backtrace capture is bounded and debugger failure cannot replace acceptance failure", () => {
  const calls = [];
  const result = captureAndroidBacktrace({
    serial: "emulator-test",
    exec(command, args, options) {
      calls.push({ command, args, options });
      if (calls.length === 1) return "42\n";
      throw new Error("permission denied secret-bearer");
    },
  });
  assert.deepEqual(result, {
    status: "unavailable",
    process: "running",
    attempts: [
      { mode: "shell", reason: "permission-denied" },
      { mode: "emulator-root", reason: "permission-denied" },
    ],
  });
  assert.deepEqual(
    calls.map((call) => call.options.timeout),
    [2_000, 8_000, 8_000],
  );
  assert.deepEqual(calls[1].args, ["-s", "emulator-test", "shell", "debuggerd", "-b", "42"]);
  assert.equal(calls[1].options.maxBuffer, 512 * 1024);
});

test("backtrace capture refuses ambiguous process ids and bounds emitted frames", () => {
  let calls = 0;
  assert.deepEqual(
    captureAndroidBacktrace({
      exec() {
        calls++;
        return "42 43";
      },
    }),
    { status: "unavailable", reason: "ambiguous-pid" },
  );
  assert.equal(calls, 1);
  const summary = summarizeAndroidBacktrace(
    '"mqt_js" sysTid=42\n' + " #00 pc 123 /lib/libc.so\n".repeat(100),
  );
  assert.equal(summary[0].frames.length, 12);
});

test("root fallback captures a release process after shell ptrace denial", () => {
  const calls = [];
  const result = captureAndroidBacktrace({
    exec(_command, args) {
      calls.push(args);
      if (calls.length === 1) return "42";
      if (calls.length === 2) throw new Error("Operation not permitted");
      return '"mqt_js" sysTid=42\n #00 pc abc /lib/libc.so (futex)';
    },
  });
  assert.equal(result.status, "captured");
  assert.equal(result.mode, "emulator-root");
  assert.deepEqual(calls[2], ["shell", "su", "0", "debuggerd", "-b", "42"]);
  assert.equal(result.threads[0].thread, "mqt_js");
});

test("process exit, pid-query failure and debugger timeout remain distinct safe outcomes", () => {
  assert.deepEqual(
    captureAndroidBacktrace({
      exec() {
        return "";
      },
    }),
    { status: "process-not-running" },
  );
  assert.deepEqual(
    captureAndroidBacktrace({
      exec() {
        throw Object.assign(new Error("secret"), { status: 1, stdout: "", stderr: "" });
      },
    }),
    { status: "process-not-running" },
  );
  assert.deepEqual(
    captureAndroidBacktrace({
      exec() {
        throw Object.assign(new Error("secret"), { code: "ETIMEDOUT" });
      },
    }),
    { status: "unavailable", reason: "pid-query-failed" },
  );
  let calls = 0;
  const timed = captureAndroidBacktrace({
    exec() {
      if (++calls === 1) return "42";
      throw Object.assign(new Error("secret"), { code: "ETIMEDOUT" });
    },
  });
  assert.deepEqual(
    timed.attempts.map((attempt) => attempt.reason),
    ["timed-out", "timed-out"],
  );
  assert.doesNotMatch(JSON.stringify(timed), /secret/);
});

test("actual API35 main-thread naming and truncated HTTP thread remain recognizable", () => {
  const summary = summarizeAndroidBacktrace(`----- pid 42 at synthetic-time -----
Cmd line: hidden-app-path secret
"hidden-app-name" sysTid=42
 #00 pc 123 /system/lib64/libandroid_runtime.so (android_os_MessageQueue_nativePollOnce+8)
"JazzCoreObserva" sysTid=43
 #00 pc 456 /system/framework/x86_64/boot.oat (com.android.okhttp.internal.http.Http1xStream.readResponse+8)
"hidden-other-thread" sysTid=44
 #00 pc 789 /system/lib64/libc.so (futex)`);
  assert.deepEqual(summary, [
    {
      thread: "main",
      frames: [{ library: "libandroid_runtime.so", pc: "123", symbol: "nativePollOnce" }],
    },
    {
      thread: "JazzCoreObserva",
      frames: [{ library: "boot.oat", pc: "456", symbol: "Http1xStream" }],
    },
  ]);
  assert.doesNotMatch(JSON.stringify(summary), /hidden|secret/);
});
