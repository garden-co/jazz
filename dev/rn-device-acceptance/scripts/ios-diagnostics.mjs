const MAX_DIAGNOSTIC_BYTES = 16 * 1024;
const MAX_DIAGNOSTIC_LINES = 120;

export const boundedDiagnostic = (value, { tail = false } = {}) => {
  const allLines = String(value).split("\n");
  const lines = tail ? allLines.slice(-MAX_DIAGNOSTIC_LINES) : allLines.slice(0, MAX_DIAGNOSTIC_LINES);
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
