import type { ScenarioDefinition } from "./types.ts";
import { gridScenario } from "./grid.ts";

export * from "./types.ts";
export { gridScenario, defaultGridOptions } from "./grid.ts";

/**
 * Registry of all available scenarios
 */
export const scenarios: Record<string, ScenarioDefinition> = {
  grid: gridScenario,
};

/**
 * Get a scenario by name
 */
export function getScenario(name: string): ScenarioDefinition | undefined {
  return scenarios[name];
}

/**
 * List all available scenario names
 */
export function listScenarios(): string[] {
  return Object.keys(scenarios);
}
