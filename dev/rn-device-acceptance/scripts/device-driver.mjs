import { parseResult } from "../src/protocol.ts";

const MAX_RECEIPT_AGE_MS = 2 * 60 * 1000;
const CLOCK_SKEW_MS = 5 * 1000;

export function collectResults(output) {
  const results = [];
  for (const line of output.split(/\r?\n/)) {
    // parseResult is the sole decoder. Invalid prefixed JSON is a hard error.
    const parsed = parseResult(line);
    if (parsed) results.push(parsed);
  }
  return results;
}

/** Reject evidence unless it belongs exactly to this launched app, platform,
 * device, artifact, and scenario matrix. */
export function assertDeviceReceipt(output, expected) {
  const now = expected.now ?? Date.now();
  const results = collectResults(output);
  const expectedScenarios = new Set(expected.scenarios);
  if (expectedScenarios.size !== expected.scenarios.length)
    throw new Error("Expected scenario list has duplicates");
  if (results.length !== expectedScenarios.size)
    throw new Error(`Expected ${expectedScenarios.size} receipts, found ${results.length}`);

  const seen = new Set();
  let previousSequence = 0;
  for (const item of results) {
    if (item.state !== "passed" || !item.receipt)
      throw new Error(`Device acceptance is incomplete: ${item.scenario}=${item.state}`);
    if (!expectedScenarios.has(item.scenario))
      throw new Error(`Foreign device scenario receipt: ${item.scenario}`);
    if (seen.has(item.scenario))
      throw new Error(`Duplicate device scenario receipt: ${item.scenario}`);
    seen.add(item.scenario);
    const receipt = item.receipt;
    if (
      receipt.platform !== expected.platform ||
      receipt.deviceIdentifier !== expected.deviceIdentifier ||
      receipt.buildFingerprint !== expected.buildFingerprint ||
      receipt.runNonce !== expected.runNonce
    ) {
      throw new Error(`Foreign device receipt for ${item.scenario}`);
    }
    const observedAt = Date.parse(receipt.observedAt);
    if (
      observedAt < expected.startedAt - CLOCK_SKEW_MS ||
      observedAt > now + CLOCK_SKEW_MS ||
      now - observedAt > MAX_RECEIPT_AGE_MS
    ) {
      throw new Error(`Stale device receipt for ${item.scenario}`);
    }
    if (receipt.sequence <= previousSequence)
      throw new Error(`Non-monotonic device receipt sequence for ${item.scenario}`);
    previousSequence = receipt.sequence;
  }
  return results;
}
