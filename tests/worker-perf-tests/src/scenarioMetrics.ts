import { metrics, type UpDownCounter } from "@opentelemetry/api";

/**
 * Scenario metrics state - updated by run scripts.
 */
export type DurationMetricsState = {
  scenario: "duration";
  workers: number;
  elapsedMs: number;
  targetMs: number;
  opsTotal: number;
  opsPerSecond: number;
  fileOps: number;
  mapOps: number;
  fullFileOps: number;
  unavailable: number;
};

export type BatchMetricsState = {
  scenario: "batch";
  workers: number;
  runCurrent: number;
  runsTotal: number;
  mapsLoaded: number;
  mapsTotal: number;
  // Time stats (min/median/max across completed runs)
  timeMsMin: number;
  timeMsMedian: number;
  timeMsMax: number;
  // Throughput stats (min/median/max across completed runs)
  throughputMin: number;
  throughputMedian: number;
  throughputMax: number;
  errors: number;
};

export type ScenarioMetricsState = DurationMetricsState | BatchMetricsState;

/**
 * Gauge wrapper using UpDownCounter to allow imperative updates.
 * Tracks the current value and uses add() to adjust to new values.
 */
class Gauge {
  private counter: UpDownCounter;
  private currentValue = 0;
  private name: string;

  constructor(counter: UpDownCounter, name: string) {
    this.counter = counter;
    this.name = name;
  }

  set(value: number): void {
    const delta = value - this.currentValue;
    if (delta !== 0) {
      this.counter.add(delta);
      this.currentValue = value;
    }
  }

  get(): number {
    return this.currentValue;
  }

  reset(): void {
    this.set(0);
  }
}

// Metric instances (initialized lazily)
let gauges: {
  scenarioActive: Gauge;
  workers: Gauge;
  // Duration
  durationElapsedMs: Gauge;
  durationTargetMs: Gauge;
  opsTotal: Gauge;
  opsPerSecond: Gauge;
  fileOps: Gauge;
  mapOps: Gauge;
  fullFileOps: Gauge;
  unavailable: Gauge;
  // Batch
  batchRunCurrent: Gauge;
  batchRunsTotal: Gauge;
  batchMapsLoaded: Gauge;
  batchMapsTotal: Gauge;
  batchTimeMsMin: Gauge;
  batchTimeMsMedian: Gauge;
  batchTimeMsMax: Gauge;
  batchThroughputMin: Gauge;
  batchThroughputMedian: Gauge;
  batchThroughputMax: Gauge;
  batchErrors: Gauge;
} | null = null;

/**
 * Register all scenario metrics with the OpenTelemetry meter.
 * Call this once after setting up the meter provider.
 */
export function registerScenarioMetrics(): void {
  if (gauges) return; // Already registered

  const meter = metrics.getMeter("loadtest");

  const createGauge = (name: string, description: string): Gauge => {
    return new Gauge(meter.createUpDownCounter(name, { description }), name);
  };

  gauges = {
    scenarioActive: createGauge(
      "loadtest_scenario_active",
      "Active scenario (0=none, 1=duration, 2=batch)",
    ),
    workers: createGauge("loadtest_workers", "Number of worker threads"),
    // Duration
    durationElapsedMs: createGauge(
      "loadtest_duration_elapsed_ms",
      "Elapsed time in milliseconds",
    ),
    durationTargetMs: createGauge(
      "loadtest_duration_target_ms",
      "Target duration in milliseconds",
    ),
    opsTotal: createGauge("loadtest_ops_total", "Total operations completed"),
    opsPerSecond: createGauge(
      "loadtest_ops_per_second",
      "Operations per second",
    ),
    fileOps: createGauge("loadtest_file_ops", "File operations completed"),
    mapOps: createGauge("loadtest_map_ops", "Map operations completed"),
    fullFileOps: createGauge(
      "loadtest_full_file_ops",
      "Completed file stream operations",
    ),
    unavailable: createGauge("loadtest_unavailable", "Unavailable errors"),
    // Batch
    batchRunCurrent: createGauge(
      "loadtest_batch_run_current",
      "Current run index (1-based)",
    ),
    batchRunsTotal: createGauge(
      "loadtest_batch_runs_total",
      "Total number of runs",
    ),
    batchMapsLoaded: createGauge(
      "loadtest_batch_maps_loaded",
      "Maps loaded in current/last run",
    ),
    batchMapsTotal: createGauge(
      "loadtest_batch_maps_total",
      "Total maps to load per run",
    ),
    batchTimeMsMin: createGauge(
      "loadtest_batch_time_ms_min",
      "Minimum run time in milliseconds",
    ),
    batchTimeMsMedian: createGauge(
      "loadtest_batch_time_ms_median",
      "Median run time in milliseconds",
    ),
    batchTimeMsMax: createGauge(
      "loadtest_batch_time_ms_max",
      "Maximum run time in milliseconds",
    ),
    batchThroughputMin: createGauge(
      "loadtest_batch_throughput_min",
      "Minimum throughput (maps/sec)",
    ),
    batchThroughputMedian: createGauge(
      "loadtest_batch_throughput_median",
      "Median throughput (maps/sec)",
    ),
    batchThroughputMax: createGauge(
      "loadtest_batch_throughput_max",
      "Maximum throughput (maps/sec)",
    ),
    batchErrors: createGauge(
      "loadtest_batch_errors",
      "Errors in current/last run",
    ),
  };
}

/**
 * Update the scenario metrics state.
 * Imperatively updates all Prometheus metrics.
 */
export function updateScenarioState(state: ScenarioMetricsState): void {
  if (!gauges) {
    console.warn(
      "Metrics not registered. Call registerScenarioMetrics() first.",
    );
    return;
  }

  gauges.workers.set(state.workers);

  if (state.scenario === "duration") {
    gauges.scenarioActive.set(1);
    gauges.durationElapsedMs.set(state.elapsedMs);
    gauges.durationTargetMs.set(state.targetMs);
    gauges.opsTotal.set(state.opsTotal);
    gauges.opsPerSecond.set(state.opsPerSecond);
    gauges.fileOps.set(state.fileOps);
    gauges.mapOps.set(state.mapOps);
    gauges.fullFileOps.set(state.fullFileOps);
    gauges.unavailable.set(state.unavailable);
    // Reset batch metrics
    gauges.batchRunCurrent.set(0);
    gauges.batchRunsTotal.set(0);
    gauges.batchMapsLoaded.set(0);
    gauges.batchMapsTotal.set(0);
    gauges.batchTimeMsMin.set(0);
    gauges.batchTimeMsMedian.set(0);
    gauges.batchTimeMsMax.set(0);
    gauges.batchThroughputMin.set(0);
    gauges.batchThroughputMedian.set(0);
    gauges.batchThroughputMax.set(0);
    gauges.batchErrors.set(0);
  } else {
    gauges.scenarioActive.set(2);
    gauges.batchRunCurrent.set(state.runCurrent);
    gauges.batchRunsTotal.set(state.runsTotal);
    gauges.batchMapsLoaded.set(state.mapsLoaded);
    gauges.batchMapsTotal.set(state.mapsTotal);
    gauges.batchTimeMsMin.set(state.timeMsMin);
    gauges.batchTimeMsMedian.set(state.timeMsMedian);
    gauges.batchTimeMsMax.set(state.timeMsMax);
    gauges.batchThroughputMin.set(state.throughputMin);
    gauges.batchThroughputMedian.set(state.throughputMedian);
    gauges.batchThroughputMax.set(state.throughputMax);
    gauges.batchErrors.set(state.errors);
    // Reset duration metrics
    gauges.durationElapsedMs.set(0);
    gauges.durationTargetMs.set(0);
    gauges.opsTotal.set(0);
    gauges.opsPerSecond.set(0);
    gauges.fileOps.set(0);
    gauges.mapOps.set(0);
    gauges.fullFileOps.set(0);
    gauges.unavailable.set(0);
  }
}

/**
 * Clear the scenario metrics state.
 * Resets all metrics to 0.
 */
export function clearScenarioState(): void {
  if (!gauges) return;

  gauges.scenarioActive.reset();
  gauges.workers.reset();
  gauges.durationElapsedMs.reset();
  gauges.durationTargetMs.reset();
  gauges.opsTotal.reset();
  gauges.opsPerSecond.reset();
  gauges.fileOps.reset();
  gauges.mapOps.reset();
  gauges.fullFileOps.reset();
  gauges.unavailable.reset();
  gauges.batchRunCurrent.reset();
  gauges.batchRunsTotal.reset();
  gauges.batchMapsLoaded.reset();
  gauges.batchMapsTotal.reset();
  gauges.batchTimeMsMin.reset();
  gauges.batchTimeMsMedian.reset();
  gauges.batchTimeMsMax.reset();
  gauges.batchThroughputMin.reset();
  gauges.batchThroughputMedian.reset();
  gauges.batchThroughputMax.reset();
  gauges.batchErrors.reset();
}

/**
 * Debug: Get current gauge values (for troubleshooting).
 */
export function debugGetGaugeValues(): Record<string, number> | null {
  if (!gauges) return null;
  return {
    scenarioActive: gauges.scenarioActive.get(),
    workers: gauges.workers.get(),
    batchRunCurrent: gauges.batchRunCurrent.get(),
    batchRunsTotal: gauges.batchRunsTotal.get(),
    batchMapsLoaded: gauges.batchMapsLoaded.get(),
    batchMapsTotal: gauges.batchMapsTotal.get(),
    batchTimeMsMin: gauges.batchTimeMsMin.get(),
    batchTimeMsMedian: gauges.batchTimeMsMedian.get(),
    batchTimeMsMax: gauges.batchTimeMsMax.get(),
    batchThroughputMin: gauges.batchThroughputMin.get(),
    batchThroughputMedian: gauges.batchThroughputMedian.get(),
    batchThroughputMax: gauges.batchThroughputMax.get(),
    batchErrors: gauges.batchErrors.get(),
  };
}
