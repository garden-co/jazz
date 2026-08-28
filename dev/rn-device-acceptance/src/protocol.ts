export const RESULT_PREFIX = "JAZZ_DEVICE_RESULT ";

export type ScenarioState = "passed" | "failed" | "todo" | "blocked";
export type Platform = "android" | "ios";

export interface DeviceReceipt {
  platform: Platform;
  /** Stable host identifier selected by the driver, never a display name. */
  deviceIdentifier: string;
  /** SHA-256 of the installed app artifact (or its immutable CI equivalent). */
  buildFingerprint: string;
  /** Per-launch unpredictable nonce supplied by the platform driver. */
  runNonce: string;
  /** Strictly increasing within one scenario run. */
  sequence: number;
  observedAt: string;
}

export interface ScenarioResult {
  protocol: 1;
  scenario: string;
  state: ScenarioState;
  detail: string;
  receipt?: DeviceReceipt;
}

/**
 * A passed device result is evidence, not an assertion. This makes a fixture
 * or UI placeholder unable to manufacture a green device receipt.
 */
export function result(value: ScenarioResult): ScenarioResult {
  if (
    !isRecord(value) ||
    value.protocol !== 1 ||
    !nonEmpty(value.scenario) ||
    !nonEmpty(value.detail)
  ) {
    throw new Error("Device result has an invalid protocol, scenario, or detail");
  }
  if (!(["passed", "failed", "todo", "blocked"] as const).includes(value.state)) {
    throw new Error(`Scenario ${value.scenario} has an invalid state`);
  }
  if (value.state === "passed") {
    const receipt = value.receipt;
    if (
      !receipt ||
      !(["android", "ios"] as const).includes(receipt.platform) ||
      !nonEmpty(receipt.deviceIdentifier) ||
      !nonEmpty(receipt.buildFingerprint) ||
      !nonEmpty(receipt.runNonce) ||
      !Number.isSafeInteger(receipt.sequence) ||
      receipt.sequence < 1 ||
      Number.isNaN(Date.parse(receipt.observedAt))
    ) {
      throw new Error(`Scenario ${value.scenario} cannot pass without a complete device receipt`);
    }
  } else if (value.receipt) {
    throw new Error(`Only passed scenario results may carry a device receipt`);
  }
  return value;
}

export function encodeResult(value: ScenarioResult): string {
  return `${RESULT_PREFIX}${JSON.stringify(result(value))}`;
}

/** Decode a prefixed result. Non-protocol lines return null; malformed protocol
 * lines throw so a driver cannot silently discard a forged/partial receipt. */
export function parseResult(line: string): ScenarioResult | null {
  if (!line.startsWith(RESULT_PREFIX)) return null;
  try {
    return result(JSON.parse(line.slice(RESULT_PREFIX.length)) as ScenarioResult);
  } catch (error) {
    throw new Error(
      `Invalid Jazz device result: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function nonEmpty(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}
