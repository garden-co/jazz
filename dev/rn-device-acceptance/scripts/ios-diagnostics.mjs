const MAX_DIAGNOSTIC_BYTES = 16 * 1024;
const MAX_DIAGNOSTIC_LINES = 120;

export const boundedDiagnostic = (value) => {
  const lines = String(value).split("\n").slice(0, MAX_DIAGNOSTIC_LINES);
  const text = lines.join("\n");
  if (Buffer.byteLength(text) <= MAX_DIAGNOSTIC_BYTES) return text;
  return `${Buffer.from(text).subarray(0, MAX_DIAGNOSTIC_BYTES).toString("utf8")}\n[diagnostic truncated]`;
};

export const relevantAppLogs = (value, processName) =>
  boundedDiagnostic(
    String(value)
      .split("\n")
      .filter((line) => line.includes(processName))
      .join("\n"),
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
