import { execFileSync } from "node:child_process";

const THREADS = new Set([
  "main",
  "JazzCoreObserva",
  "JazzCoreObservation",
  "mqt_js",
  "js",
  "mqt_native_modu",
  "mqt_native_modules",
  "jazz-native-rel",
  "jazz-native-relay",
  "jazz-native-relay-socket",
]);
const LIBRARY =
  /^(?:libc|libart|libhermes|libhermes_executor|libreactnative|libjazzrelay|libfbjni|libc\+\+_shared|libopenjdk|libjavacore|libssl|libcrypto|libnetd_client|libandroid_runtime|libutils)\.so$|^(?:boot|boot-framework)\.oat$/;
const SYMBOLS = [
  "jazz_native_relay_host_lease_tick_attached_foreground",
  "jazz_native_relay_host_lease_execute_foreground",
  "jazz_native_relay_host_lease_close_attached_foreground",
  "ForegroundHandle",
  "ForegroundWakeRegistration",
  "RelayWorker",
  "NativeRelay",
  "RuntimeScheduler",
  "JavaTurboModule",
  "drainMicrotasks",
  "pthread_mutex_lock",
  "pthread_cond_wait",
  "syscall",
  "futex",
  "HttpURLConnection",
  "SocketInputStream",
  "SocketDispatcher",
  "Http1xStream",
  "PromiseImpl",
  "nativePollOnce",
];

/** Never return raw stack text: it can contain process arguments, paths or
 * arbitrary thread names. Keep only known thread/library labels, PC offsets and
 * fixed symbol categories; offsets can be symbolized against the exact APK. */
export function summarizeAndroidBacktrace(output) {
  const threads = [];
  let current;
  let processId;
  for (const line of String(output).split(/\r?\n/)) {
    const process = /^----- pid (\d+) at /.exec(line)?.[1];
    if (process) {
      processId = process;
      current = undefined;
      continue;
    }
    const quotedThread = /^"([^"]+)"\s+sysTid=(\d+)/.exec(line);
    const nativeThread = /^pid:\s*(\d+),\s*tid:\s*(\d+),\s*name:/.exec(line);
    const name =
      (processId && quotedThread?.[2] === processId) ||
      (nativeThread && nativeThread[1] === nativeThread[2])
        ? "main"
        : (/^"([^"]+)"\s+sysTid=\d+/.exec(line)?.[1] ??
          /\btid:\s*\d+,\s*name:\s*(\S+)/.exec(line)?.[1]);
    if (name !== undefined) {
      current = THREADS.has(name) && threads.length < 8 ? { thread: name, frames: [] } : undefined;
      if (current) threads.push(current);
      continue;
    }
    const frame = /^\s*#\d+\s+pc\s+([0-9a-fA-F]{1,16})\s+(\S+)(.*)$/.exec(line);
    if (!current || !frame || current.frames.length >= 12) continue;
    const library = frame[2].split(/[\/!]/).at(-1);
    if (!LIBRARY.test(library)) continue;
    const symbol = SYMBOLS.find((known) => frame[3].includes(known));
    current.frames.push({ library, pc: frame[1].toLowerCase(), ...(symbol ? { symbol } : {}) });
  }
  return threads.filter((thread) => thread.frames.length);
}

/** Keep failure classification fixed; raw debugger output and exceptions may
 * contain command arguments or app paths and never cross the CI boundary. */
function captureFailure(error) {
  if (error?.code === "ETIMEDOUT") return "timed-out";
  if (error?.code === "ENOBUFS") return "buffer-limit";
  const detail = `${error?.message ?? ""} ${error?.stdout ?? ""} ${error?.stderr ?? ""}`;
  if (/permission denied|operation not permitted|not dumpable|root is required/i.test(detail))
    return "permission-denied";
  if (/not found|inaccessible/i.test(detail)) return "unsupported";
  return "command-failed";
}

/** Timeout-only capture. The standard API35 Google APIs emulator supports su;
 * use it only for a failed diagnostic, never to change app scheduling or state.
 * Each attempt is bounded and cannot replace the original acceptance failure. */
export function captureAndroidBacktrace({ serial, exec = execFileSync } = {}) {
  const run = (args, timeout) =>
    exec("adb", serial ? ["-s", serial, ...args] : args, {
      encoding: "utf8",
      timeout,
      maxBuffer: 512 * 1024,
      stdio: ["ignore", "pipe", "pipe"],
    });
  let pid;
  try {
    pid = run(["shell", "pidof", "dev.jazz.rndeviceacceptance"], 2_000).trim();
  } catch (error) {
    // pidof exits 1 when the app is absent; other failures are not proof of exit.
    if (
      error?.status === 1 &&
      !String(error?.stdout ?? "").trim() &&
      !String(error?.stderr ?? "").trim()
    )
      return { status: "process-not-running" };
    return { status: "unavailable", reason: "pid-query-failed" };
  }
  if (!pid) return { status: "process-not-running" };
  if (!/^[1-9][0-9]{0,8}$/.test(pid)) return { status: "unavailable", reason: "ambiguous-pid" };
  const attempts = [];
  for (const mode of ["shell", "emulator-root"]) {
    try {
      const args =
        mode === "shell"
          ? ["shell", "debuggerd", "-b", pid]
          : ["shell", "su", "0", "debuggerd", "-b", pid];
      const threads = summarizeAndroidBacktrace(run(args, 8_000));
      if (threads.length) return { status: "captured", mode, threads };
      attempts.push({ mode, reason: "no-recognized-frames" });
    } catch (error) {
      attempts.push({ mode, reason: captureFailure(error) });
    }
  }
  return { status: "unavailable", process: "running", attempts };
}
