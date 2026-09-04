import { execFileSync } from "node:child_process";

const SENSITIVE_STRING_EXTRAS = new Set(["jazzDeviceBearerA", "jazzDeviceBearerB"]);

function sensitiveExtraValues(args) {
  const values = [];
  for (let index = 0; index < args.length - 2; index += 1) {
    if (args[index] === "--es" && SENSITIVE_STRING_EXTRAS.has(args[index + 1])) {
      values.push(args[index + 2]);
      index += 2;
    }
  }
  return values;
}

function redact(value, values) {
  let text = String(value ?? "");
  for (const sensitive of values) text = text.replaceAll(sensitive, "[redacted]");
  return text;
}

function redactedAdbError(error, values) {
  const redacted = new Error(redact(error?.message ?? error, values));
  redacted.name = error?.name ?? "Error";
  for (const field of ["stdout", "stderr", "output", "cmd", "command", "stack"]) {
    if (error?.[field] !== undefined) redacted[field] = redact(error[field], values);
  }
  for (const field of ["code", "status", "signal"]) {
    if (error?.[field] !== undefined) redacted[field] = error[field];
  }
  return redacted;
}

// `execFileSync` includes argv in its thrown message. Android trusted-session
// bearer extras are deliberately passed only to the activity, so redact every
// error surface before it can reach CI output.
export function adb(args, { serial, exec = execFileSync } = {}) {
  const sensitive = sensitiveExtraValues(args);
  try {
    return exec("adb", serial ? ["-s", serial, ...args] : args, { encoding: "utf8" });
  } catch (error) {
    throw redactedAdbError(error, sensitive);
  }
}
