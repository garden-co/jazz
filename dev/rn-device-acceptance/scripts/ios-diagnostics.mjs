import { isDeviceDiagnosticCode } from "../src/device-diagnostics.ts";

const MAX_DIAGNOSTIC_BYTES = 16 * 1024;
const MAX_DIAGNOSTIC_LINES = 120;

/** Never echo arbitrary app-sandbox contents into CI output. */
export const safeDeviceDiagnostic = (value) => {
  const code = String(value).trim();
  return isDeviceDiagnosticCode(code) ? code : "[no recognized device diagnostic]";
};

export const boundedDiagnostic = (value, { tail = false } = {}) => {
  const allLines = String(value).split("\n");
  const lines = tail
    ? allLines.slice(-MAX_DIAGNOSTIC_LINES)
    : allLines.slice(0, MAX_DIAGNOSTIC_LINES);
  const text = lines.join("\n");
  if (Buffer.byteLength(text) <= MAX_DIAGNOSTIC_BYTES) return text;
  const bytes = Buffer.from(text);
  const bounded = tail
    ? bytes.subarray(Math.max(0, bytes.length - MAX_DIAGNOSTIC_BYTES)).toString("utf8")
    : bytes.subarray(0, MAX_DIAGNOSTIC_BYTES).toString("utf8");
  return `${tail ? "[diagnostic truncated]\n" : ""}${bounded}${tail ? "" : "\n[diagnostic truncated]"}`;
};

export const relevantAppLogs = (value, processName) =>
  boundedDiagnostic(
    String(value)
      .split("\n")
      .filter((line) => line.includes(processName))
      .join("\n"),
    { tail: true },
  );

const foregroundWakeStages = new Set([
  "armed",
  "enabled",
  "requested",
  "scheduled",
  "delivered",
  "callback-invoked",
  "foreground-mismatch",
  "inactive",
]);

/** Parse only the fixed native wake vocabulary from a separately queried log. */
export const foregroundWakeDiagnostic = (value) => {
  let armed = false;
  const stages = [];
  for (const line of String(value).split("\n")) {
    const stage =
      /JazzForegroundWake\s+(armed|enabled|requested|scheduled|delivered|callback-invoked|foreground-mismatch|inactive)\b/.exec(
        line,
      )?.[1];
    if (!stage || !foregroundWakeStages.has(stage)) continue;
    if (stage === "armed") {
      armed = true;
      stages.length = 0;
    } else if (armed) stages.push(stage);
  }
  return stages.slice(-16).join(",") || (armed ? "armed-no-wake" : "no-arm");
};

export const sanitizedCommandFailure = (error) => {
  const status =
    error && typeof error === "object" && "status" in error && typeof error.status === "number"
      ? error.status
      : "unknown";
  return `command failed (exit ${status})`;
};

const acceptanceBundleId = "dev.jazz.rndeviceacceptance";

/** `simctl launch` returns `<bundle id>: <positive pid>` on success. */
export const parseLaunchProcessId = (value) => {
  const output = value.endsWith("\r\n")
    ? value.slice(0, -2)
    : value.endsWith("\n")
      ? value.slice(0, -1)
      : value;
  const prefix = `${acceptanceBundleId}: `;
  if (!output.startsWith(prefix))
    throw new Error("simctl launch returned an unexpected bundle/process id");
  const pid = output.slice(prefix.length);
  if (!/^[1-9]\d*$/.test(pid))
    throw new Error("simctl launch returned an unexpected bundle/process id");
  const processId = Number(pid);
  if (!Number.isSafeInteger(processId))
    throw new Error("simctl launch returned an unexpected bundle/process id");
  return processId;
};
