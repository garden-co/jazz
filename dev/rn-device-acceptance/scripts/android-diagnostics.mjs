import { isDeviceDiagnosticCode } from "../src/device-diagnostics.ts";

// `run-android` explicitly selects logcat's threadtime format. Parse the
// priority and tag fields rather than searching message payloads: a
// ReactNativeJS console line must never impersonate the native diagnostic tag.
const THREADTIME_DIAGNOSTIC =
  /^\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\d+\s+\d+\s+E\s+JazzDeviceAcceptance\s*:\s*(\S+)\s*$/;
const WRITER_READ_DETAIL =
  /^scope-isolation-writer-read-detail:last-(none|pending|subscription|rejected|closed|rows)-wakes-\d{1,6}-polls-\d{1,6}-row-responses-\d{1,6}-ready-(yes|no)$/;
const THREADTIME_WRITER_READ_DETAIL =
  /^\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\d+\s+\d+\s+E\s+JazzScopeWriterRead\s*:\s*(\S+)\s*$/;
const THREADTIME_FOREGROUND_WAKE =
  /^\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\d+\s+\d+\s+E\s+JazzForegroundWake\s*:\s*(armed|enabled|requested|scheduled|delivered|callback-invoked|foreground-mismatch|inactive)\s*$/;

// A separate tag preserves the HTTP outcome when JS re-emits its generic stage.
const THREADTIME_CORE_OBSERVATION =
  /^\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\d+\s+\d+\s+E\s+JazzCoreObservation\s*:\s*(request-started|request-sent|promise-resolved|js-(?:before-core-await|core-await-returned|before-unsubscribe|after-unsubscribe|before-shutdown|after-shutdown)|http-status-(?:[1-5]\d{2}|invalid)|failure-(?:setup|request|response|promise)-(?:timeout|connection|dns|tls|protocol|io|state|other))\s*$/;

// Separate from the JS stage: its final retry must not hide native causality.
const THREADTIME_FIXTURE_METADATA =
  /^\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\d+\s+\d+\s+E\s+JazzFixtureMetadata\s*:\s*(receipt-started|package-hash-started|receipt-resolved|receipt-failed-(?:activity|nonce|package-hash|device-identity|resolve)|phase-started|phase-activity-unavailable|phase-seed-resolved|phase-verify-resolved|phase-failed)\s*$/;

export function androidFixtureMetadataDiagnostic(output) {
  const codes = new Set();
  for (const line of String(output).split(/\r?\n/)) {
    const code = THREADTIME_FIXTURE_METADATA.exec(line)?.[1];
    if (code) codes.add(code);
  }
  return codes.size ? [...codes].join(",") : undefined;
}

export function androidCoreObservationDiagnostic(output) {
  const codes = new Set();
  for (const line of String(output).split(/\r?\n/)) {
    const code = THREADTIME_CORE_OBSERVATION.exec(line)?.[1];
    if (code) codes.add(code);
  }
  return codes.size ? [...codes].slice(-16).join(",") : undefined;
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

export function androidScopeWriterReadDiagnostic(output) {
  let latest;
  for (const line of String(output).split(/\r?\n/)) {
    const detail = THREADTIME_WRITER_READ_DETAIL.exec(line)?.[1];
    if (detail && WRITER_READ_DETAIL.test(detail)) latest = detail;
  }
  return latest;
}

export function androidForegroundWakeDiagnostic(output) {
  let armed = false;
  const stages = [];
  for (const line of String(output).split(/\r?\n/)) {
    const stage = THREADTIME_FOREGROUND_WAKE.exec(line)?.[1];
    if (stage === "armed") {
      armed = true;
      stages.length = 0;
    } else if (armed && stage) stages.push(stage);
  }
  const trace = stages.slice(-16).join(",");
  return trace || (armed ? "armed-no-wake" : undefined);
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
  const metadata = androidFixtureMetadataDiagnostic(output);
  const metadataSummary = metadata ? `${summary}; fixture metadata: ${metadata}` : summary;
  const stage = diagnostic
    ? `${metadataSummary}; device stage: ${diagnostic}`
    : `${metadataSummary}; no device stage`;
  const coreObservation = androidCoreObservationDiagnostic(output);
  const writerRead =
    diagnostic === "scope-isolation-writer-read-failed"
      ? androidScopeWriterReadDiagnostic(output)
      : undefined;
  const detailedStage = writerRead ? `${stage}; scope writer read: ${writerRead}` : stage;
  const foregroundWake =
    diagnostic === "same-runtime-postcommit-wake-failed"
      ? androidForegroundWakeDiagnostic(output)
      : undefined;
  const wakeStage = foregroundWake
    ? `${detailedStage}; foreground wake: ${foregroundWake}`
    : detailedStage;
  return coreObservation
    ? `${wakeStage}; native Core acknowledgement: ${coreObservation}`
    : wakeStage;
}
