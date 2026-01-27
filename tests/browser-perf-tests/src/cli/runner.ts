import { chromium, type Browser, type BrowserContext } from "@playwright/test";
import type {
  BenchmarkConfig,
  BenchmarkResults,
  ScenarioDefinition,
} from "./scenarios/types.ts";
import { getScenario } from "./scenarios/index.ts";
import { calculateBenchmarkStats } from "./stats.ts";
import {
  printProgress,
  printGeneratedFixture,
  printWarmupStart,
  printWarmupComplete,
} from "./reporter.ts";

/**
 * Run a benchmark session
 */
export async function runBenchmark(
  config: BenchmarkConfig,
): Promise<BenchmarkResults> {
  const scenario = getScenario(config.scenario);
  if (!scenario) {
    throw new Error(
      `Unknown scenario: ${config.scenario}. Available: grid, todo`,
    );
  }

  // Build the base URL with sync parameter if provided
  const baseURL = config.sync
    ? `http://localhost:5173/?sync=${encodeURIComponent(config.sync)}`
    : "http://localhost:5173/";

  // Launch browser
  const browser = await chromium.launch({
    headless: config.headless ?? true,
  });

  try {
    // Get or generate the CoValue ID
    const coValueId = await getOrGenerateCoValueId(
      browser,
      scenario,
      config,
      baseURL,
    );

    // Perform warmup run (not included in results)
    await performWarmupRun(
      browser,
      scenario,
      coValueId,
      baseURL,
      config.timeout,
    );

    // Run the benchmark iterations
    const rawMetrics = await runIterations(
      browser,
      scenario,
      coValueId,
      config.runs,
      baseURL,
      config.coldStorage ?? false,
      config.timeout,
    );

    // Calculate statistics for each metric
    const stats: Record<
      string,
      ReturnType<typeof calculateBenchmarkStats>
    > = {};
    for (const [metricName, values] of Object.entries(rawMetrics)) {
      stats[metricName] = calculateBenchmarkStats(values);
    }

    return {
      scenario: config.scenario,
      coValueId,
      runs: config.runs,
      rawMetrics,
      stats,
    };
  } finally {
    await browser.close();
  }
}

/**
 * Get existing CoValue ID or generate a new fixture
 */
async function getOrGenerateCoValueId(
  browser: Browser,
  scenario: ScenarioDefinition,
  config: BenchmarkConfig,
  baseURL: string,
): Promise<string> {
  if (config.id) {
    return config.id;
  }

  console.log(`\nGenerating ${scenario.name} fixture...`);

  // Create a context for generation
  const context = await browser.newContext({ baseURL });
  const page = await context.newPage();

  try {
    // Navigate to the app first to initialize
    await page.goto("/");
    // Wait for the app to load
    await page.waitForSelector("h1", { timeout: 30000 });

    // Generate the fixture
    const coValueId = await scenario.generate(page, config.scenarioOptions);

    printGeneratedFixture(scenario.name, coValueId);

    return coValueId;
  } finally {
    await context.close();
  }
}

/**
 * Perform a warmup run (not included in benchmark results)
 */
async function performWarmupRun(
  browser: Browser,
  scenario: ScenarioDefinition,
  coValueId: string,
  baseURL: string,
  timeout?: number,
): Promise<void> {
  printWarmupStart();

  const startTime = Date.now();
  const context = await browser.newContext({ baseURL });
  context.setDefaultTimeout(timeout ?? 120000);

  try {
    const page = await context.newPage();
    await scenario.run(page, coValueId);
  } finally {
    await context.close();
  }

  printWarmupComplete(Date.now() - startTime);
}

/**
 * Run benchmark iterations
 */
async function runIterations(
  browser: Browser,
  scenario: ScenarioDefinition,
  coValueId: string,
  runs: number,
  baseURL: string,
  coldStorage: boolean,
  timeout?: number,
): Promise<Record<string, number[]>> {
  const rawMetrics: Record<string, number[]> = {};

  console.log(`Running ${runs} iterations...`);

  // For warm storage mode (default), create a single context and reuse it
  let sharedContext: BrowserContext | null = null;
  if (!coldStorage) {
    sharedContext = await browser.newContext({ baseURL });
  }

  try {
    for (let i = 0; i < runs; i++) {
      printProgress(i + 1, runs);

      // Either use shared context (warm, default) or create fresh context (cold)
      const context = coldStorage
        ? await browser.newContext({
            baseURL,
            // Clear storage to simulate cold load
            storageState: undefined,
          })
        : sharedContext!;

      context.setDefaultTimeout(timeout ?? 120000);

      try {
        const page = await context.newPage();

        // Run the scenario
        const result = await scenario.run(page, coValueId);

        // Collect metrics
        for (const [metricName, value] of Object.entries(result.metrics)) {
          if (!rawMetrics[metricName]) {
            rawMetrics[metricName] = [];
          }
          rawMetrics[metricName].push(value);
        }

        // Close the page (but not context in warm mode)
        await page.close();
      } finally {
        // Only close context in cold mode
        if (coldStorage) {
          await context.close();
        }
      }
    }
  } finally {
    // Close shared context at the end in warm mode
    if (sharedContext) {
      await sharedContext.close();
    }
  }

  return rawMetrics;
}
