export const RESULT_PREFIX = "JAZZ_DEVICE_RESULT ";

export type ScenarioState = "passed" | "failed" | "todo" | "blocked";
export type Platform = "android" | "ios";

export interface DeviceReceipt {
  platform: Platform;
  device: string;
  buildId: string;
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
export function result(result: ScenarioResult): ScenarioResult {
  if (result.state === "passed") {
    if (!result.receipt?.platform || !result.receipt.device || !result.receipt.buildId) {
      throw new Error(`Scenario ${result.scenario} cannot pass without a device receipt`);
    }
    if (Number.isNaN(Date.parse(result.receipt.observedAt))) {
      throw new Error(`Scenario ${result.scenario} has an invalid receipt timestamp`);
    }
  } else if (result.receipt) {
    throw new Error(`Only passed scenario results may carry a device receipt`);
  }
  return result;
}

export function encodeResult(value: ScenarioResult): string {
  return `${RESULT_PREFIX}${JSON.stringify(result(value))}`;
}

export function parseResult(line: string): ScenarioResult | null {
  if (!line.startsWith(RESULT_PREFIX)) return null;
  try {
    return result(JSON.parse(line.slice(RESULT_PREFIX.length)) as ScenarioResult);
  } catch {
    return null;
  }
}
