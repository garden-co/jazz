export type Level = "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR";
export const LEVEL_RANK: Record<Level, number> = { ERROR: 1, WARN: 2, INFO: 3, DEBUG: 4, TRACE: 5 };

/** Configuration parameters for the WasmLayer. */
export class WasmLayerConfig {
  enabled = true;
  maxLevel: Level = "TRACE";
  consoleGroupSpans = false;
  disable(): this {
    this.enabled = false;
    return this;
  }
  withMaxLevel(maxLevel: Level): this {
    this.maxLevel = maxLevel;
    return this;
  }
  withConsoleGroupSpans(): this {
    this.consoleGroupSpans = true;
    return this;
  }
}
