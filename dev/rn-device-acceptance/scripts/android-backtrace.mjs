import { execFileSync } from "node:child_process";

const THREADS = new Set([
  "main",
  "mqt_js",
  "js",
  "mqt_native_modu",
  "mqt_native_modules",
  "jazz-native-rel",
  "jazz-native-relay",
  "jazz-native-relay-socket",
]);
const LIBRARY =
  /^(?:libc|libart|libhermes|libhermes_executor|libreactnative|libjazzrelay|libfbjni|libc\+\+_shared)\.so$/;
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
];

/** Never return raw stack text: it can contain process arguments, paths or
 * arbitrary thread names. Keep only known thread/library labels, PC offsets and
 * fixed symbol categories; offsets can be symbolized against the exact APK. */
export function summarizeAndroidBacktrace(output) {
  const threads = [];
  let current;
  for (const line of String(output).split(/\r?\n/)) {
    const name =
      /^"([^"]+)"\s+sysTid=\d+/.exec(line)?.[1] ?? /\btid:\s*\d+,\s*name:\s*(\S+)/.exec(line)?.[1];
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

/** Best-effort timeout-only capture. A denied/failed debugger must not replace
 * the original acceptance failure or expose its raw command exception. */
export function captureAndroidBacktrace({ serial, exec = execFileSync } = {}) {
  const run = (args, timeout) =>
    exec("adb", serial ? ["-s", serial, ...args] : args, {
      encoding: "utf8",
      timeout,
      maxBuffer: 512 * 1024,
      stdio: ["ignore", "pipe", "pipe"],
    });
  try {
    const pid = run(["shell", "pidof", "dev.jazz.rndeviceacceptance"], 2_000).trim();
    if (!/^[1-9][0-9]{0,8}$/.test(pid)) return { status: "unavailable" };
    const threads = summarizeAndroidBacktrace(run(["shell", "debuggerd", "-b", pid], 8_000));
    return threads.length ? { status: "captured", threads } : { status: "no-recognized-frames" };
  } catch {
    return { status: "unavailable" };
  }
}
