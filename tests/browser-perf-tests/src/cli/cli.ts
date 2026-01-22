import { parseArgs } from "node:util";
import { runBenchmark } from "./runner.ts";
import { printResults, printUsage, printError } from "./reporter.ts";
import { listScenarios } from "./scenarios/index.ts";
import type { BenchmarkConfig } from "./scenarios/types.ts";

/**
 * CLI argument options configuration
 */
const argsConfig = {
  options: {
    runs: { type: "string" as const, short: "n" },
    id: { type: "string" as const },
    sync: { type: "string" as const, short: "s" },
    headful: { type: "boolean" as const, default: false },
    "cold-storage": { type: "boolean" as const, default: false },
    timeout: { type: "string" as const, short: "t" },
    help: { type: "boolean" as const, short: "h" },
    // Grid scenario options
    size: { type: "string" as const },
    "min-padding": { type: "string" as const },
    "max-padding": { type: "string" as const },
  },
  allowPositionals: true,
};

/**
 * Build benchmark config from parsed arguments
 */
function buildConfig(
  values: Record<string, unknown>,
  positionals: string[],
): BenchmarkConfig {
  const scenario = positionals[0];

  if (!scenario) {
    throw new Error("No scenario specified");
  }

  const availableScenarios = listScenarios();
  if (!availableScenarios.includes(scenario)) {
    throw new Error(
      `Unknown scenario: ${scenario}. Available: ${availableScenarios.join(", ")}`,
    );
  }

  // Parse common options
  const runs = values.runs ? parseInt(String(values.runs), 10) : 50;
  const id = values.id ? String(values.id) : undefined;
  const sync = values.sync ? String(values.sync) : undefined;
  const headless = values.headful !== true; // Default to headless (true)
  const coldStorage = values["cold-storage"] === true; // Default to false (warm storage)
  const timeout = values.timeout
    ? parseInt(String(values.timeout), 10)
    : undefined;

  // Build scenario-specific options
  const scenarioOptions: Record<string, unknown> = {};

  if (scenario === "grid") {
    if (values.size) {
      scenarioOptions.size = parseInt(String(values.size), 10);
    }
    if (values["min-padding"]) {
      scenarioOptions.minPadding = parseInt(String(values["min-padding"]), 10);
    }
    if (values["max-padding"]) {
      scenarioOptions.maxPadding = parseInt(String(values["max-padding"]), 10);
    }
  }

  return {
    scenario,
    runs,
    id,
    sync,
    headless,
    coldStorage,
    timeout,
    scenarioOptions,
  };
}

/**
 * Main entry point
 */
async function main(): Promise<void> {
  try {
    const { values, positionals } = parseArgs({
      args: process.argv.slice(2),
      ...argsConfig,
    });

    // Handle help flag or no arguments
    if (values.help || positionals.length === 0) {
      printUsage();
      process.exit(0);
    }

    const config = buildConfig(values, positionals);

    console.log(`\nBenchmark Configuration:`);
    console.log(`  Scenario: ${config.scenario}`);
    console.log(`  Runs: ${config.runs}`);
    if (config.id) {
      console.log(`  CoValue ID: ${config.id}`);
    }
    if (config.sync) {
      console.log(`  Sync Server: ${config.sync}`);
    }
    console.log(`  Headful: ${!config.headless}`);
    console.log(
      `  Storage: ${config.coldStorage ? "cold (fresh each run)" : "warm (reused)"}`,
    );
    console.log(
      `  Timeout: ${config.timeout === 0 ? "disabled" : config.timeout ? `${config.timeout}ms` : "default"}`,
    );
    if (Object.keys(config.scenarioOptions).length > 0) {
      console.log(`  Options: ${JSON.stringify(config.scenarioOptions)}`);
    }

    const results = await runBenchmark(config);
    printResults(results);

    process.exit(0);
  } catch (error) {
    printError(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

main();
