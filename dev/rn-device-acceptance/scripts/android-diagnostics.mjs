import { isDeviceDiagnosticCode } from "../src/device-diagnostics.ts";

// `run-android` explicitly selects logcat's threadtime format. Parse the
// priority and tag fields rather than searching message payloads: a
// ReactNativeJS console line must never impersonate the native diagnostic tag.
const THREADTIME_DIAGNOSTIC =
  /^\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\d+\s+\d+\s+E\s+JazzDeviceAcceptance\s*:\s*(\S+)\s*$/;

// A separate tag preserves the HTTP outcome when JS re-emits its generic stage.
const THREADTIME_CORE_OBSERVATION =
  /^\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\d+\s+\d+\s+E\s+JazzCoreObservation\s*:\s*(request-started|request-sent|promise-resolved|http-status-(?:[1-5]\d{2}|invalid)|failure-(?:setup|request|response|promise)-(?:timeout|connection|dns|tls|protocol|io|state|other))\s*$/;

export function androidCoreObservationDiagnostic(output) {
  const codes = new Set();
  for (const line of String(output).split(/\r?\n/)) {
    const code = THREADTIME_CORE_OBSERVATION.exec(line)?.[1];
    if (code) codes.add(code);
  }
  return codes.size ? [...codes].slice(-8).join(",") : undefined;
}

/** Return only a fixed code emitted by the native fixture after this phase's
 * log buffer was cleared. Arbitrary log text never crosses into CI output. */
export function androidDeviceDiagnostic(output) {
  let latest;
  for (const line of String(output).split(/\r?\n/)) {
    const candidate = THREADTIME_DIAGNOSTIC.exec(line)?.[1];
    if (!candidate) continue;
    if (isDeviceDiagnosticCode(candidate)) latest = candidate;
  }
  return latest;
}

export function androidAcceptanceFailure(kind, phase, output) {
  if (phase !== "seed" && phase !== "verify") throw new Error("invalid Android acceptance phase");
  const summary =
    kind === "invalid-receipt"
      ? `Android app emitted an invalid ${phase} receipt`
      : kind === "timeout"
        ? `Timed out waiting for phase ${phase} from the launched Android app`
        : undefined;
  if (!summary) throw new Error("invalid Android acceptance failure kind");
  const diagnostic = androidDeviceDiagnostic(output);
  const stage = diagnostic
    ? `${summary}; device stage: ${diagnostic}`
    : `${summary}; no device stage`;
  const coreObservation = androidCoreObservationDiagnostic(output);
  return coreObservation ? `${stage}; native Core acknowledgement: ${coreObservation}` : stage;
}
