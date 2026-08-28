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
