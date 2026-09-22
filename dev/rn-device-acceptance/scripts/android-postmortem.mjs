import { execFileSync } from "node:child_process";
import { captureAndroidBacktrace, summarizeAndroidBacktrace } from "./android-backtrace.mjs";

const PACKAGE = "dev.jazz.rndeviceacceptance";
const REASONS = new Map([
  [1, "exit-self"],
  [2, "signaled"],
  [3, "low-memory"],
  [4, "java-crash"],
  [5, "native-crash"],
  [6, "anr"],
  [7, "initialization-failure"],
  [8, "permission-change"],
  [9, "excessive-resource"],
  [10, "user-requested"],
  [11, "user-stopped"],
  [12, "dependency-died"],
  [13, "other"],
  [14, "freezer"],
  [15, "package-state-change"],
  [16, "package-updated"],
]);
const JAVA_ERRORS = [
  "java.lang.NullPointerException",
  "java.lang.IllegalStateException",
  "java.lang.OutOfMemoryError",
  "java.lang.RuntimeException",
  "java.lang.UnsatisfiedLinkError",
  "java.lang.SecurityException",
];

/** ApplicationExitInfo descriptions and trace paths are private. Keep only
 * numeric status/identity/time and fixed reason categories for this package. */
export function summarizeAndroidExitInfo(output) {
  const records = [];
  for (const block of String(output)
    .split(/ApplicationExitInfo #\d+:/)
    .slice(1)) {
    if (!/^\s*process=dev\.jazz\.rndeviceacceptance /m.test(block)) continue;
    const reason = /\breason=(\d+)\b/.exec(block)?.[1];
    const status = /\bstatus=(-?\d+)\b/.exec(block)?.[1];
    const pid = /\bpid=(\d+)\b/.exec(block)?.[1];
    const subreason = /\bsubreason=(\d+)\b/.exec(block)?.[1];
    const timestamp = /\btimestamp=(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\b/.exec(block)?.[1];
    if (!reason || status === undefined || !pid) continue;
    records.push({
      reason: REASONS.get(Number(reason)) ?? "unknown",
      reasonCode: Number(reason),
      status: Number(status),
      pid: Number(pid),
      ...(subreason ? { subreason: Number(subreason) } : {}),
      ...(timestamp ? { timestamp } : {}),
    });
    if (records.length === 2) break;
  }
  return records;
}

/** Retain only target-process crash blocks, fixed signal/error classes and
 * the same allowlisted frame summary as live debuggerd. No exception message,
 * abort payload, path, register dump or process argument reaches CI. */
export function summarizeAndroidCrashBuffer(output) {
  const crashes = [];
  let block;
  const finish = () => {
    if (!block) return;
    const threads = summarizeAndroidBacktrace(block.lines.join("\n"));
    crashes.push({
      kind: block.kind,
      ...(block.signal ? { signal: block.signal } : {}),
      ...(block.error ? { error: block.error } : {}),
      ...(threads.length ? { threads } : {}),
    });
    block = undefined;
  };
  for (const line of String(output).split(/\r?\n/)) {
    if (/^\*\*\* \*\*\*|^FATAL EXCEPTION:/.test(line)) {
      finish();
      continue;
    }
    if (/^pid:\s*\d+,\s*tid:/.test(line)) {
      finish();
      if (line.includes(`>>> ${PACKAGE} <<<`)) block = { kind: "native", lines: [line] };
      continue;
    }
    if (/^Process: /.test(line)) {
      finish();
      if (line.startsWith(`Process: ${PACKAGE}, PID:`)) block = { kind: "java", lines: [] };
      continue;
    }
    if (!block) continue;
    const signal = /\bsignal \d+ \((SIGABRT|SIGSEGV|SIGBUS|SIGILL|SIGFPE|SIGTRAP|SIGKILL)\)/.exec(
      line,
    )?.[1];
    if (signal) block.signal = signal;
    const error = JAVA_ERRORS.find((name) => line.startsWith(`${name}:`) || line === name);
    if (error) block.error = error;
    if (block.lines.length < 256) block.lines.push(line);
  }
  finish();
  return crashes.slice(-2);
}

/** Invoked while launchAndAssert still owns the failed process, before its
 * caller's force-stop/cleanup. Post-exit metadata is evidence, not an inferred
 * explanation of when or why the last JavaScript boundary stopped progressing. */
export function captureAndroidFailure({ serial, exec = execFileSync } = {}) {
  const backtrace = captureAndroidBacktrace({ serial, exec });
  if (backtrace.status !== "process-not-running") return { backtrace };
  const read = (args, summarize) => {
    try {
      const raw = exec("adb", serial ? ["-s", serial, ...args] : args, {
        encoding: "utf8",
        timeout: 3_000,
        maxBuffer: 512 * 1024,
        stdio: ["ignore", "pipe", "pipe"],
      });
      const records = summarize(raw);
      return records.length ? { status: "captured", records } : { status: "no-matching-records" };
    } catch {
      return { status: "unavailable" };
    }
  };
  return {
    backtrace,
    exitInfo: read(
      ["shell", "dumpsys", "activity", "exit-info", PACKAGE],
      summarizeAndroidExitInfo,
    ),
    crash: read(["logcat", "-b", "crash", "-d", "-v", "raw"], summarizeAndroidCrashBuffer),
  };
}
