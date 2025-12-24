import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  MetricReader,
} from "@opentelemetry/sdk-metrics";
import { assert, expect } from "vitest";

class TestMetricReader extends MetricReader {
  private _exporter = new InMemoryMetricExporter(
    AggregationTemporality.CUMULATIVE,
  );

  protected onShutdown(): Promise<void> {
    throw new Error("Method not implemented.");
  }
  protected onForceFlush(): Promise<void> {
    throw new Error("Method not implemented.");
  }

  async getMetricDataPoints(scope: string, name: string) {
    await this.collectAndExport();

    const metric = this._exporter
      .getMetrics()[0]
      ?.scopeMetrics?.find((sm) => sm.scope.name === scope)
      ?.metrics.find((m) => m.descriptor.name === name);

    this._exporter.reset();

    const dp = metric?.dataPoints;

    assert(dp, `Metric ${name} not found in scope ${scope}`);

    return dp;
  }

  async getSumOfCounterMetric(scope: string, name: string) {
    const dp = await this.getMetricDataPoints(scope, name);
    return dp.reduce((acc, dp) => {
      if (typeof dp.value !== "number") {
        throw new Error(`Metric ${name} has a value that is not a number`);
      }

      return acc + dp.value;
    }, 0);
  }

  async getMetricValue(
    scope: string,
    name: string,
    attributes: { [key: string]: string | number } | null = null,
  ) {
    const dp1 = await this.getMetricDataPoints(scope, name);

    console.log(dp1);

    const dp = attributes
      ? dp1.find(
          (dp) => JSON.stringify(dp.attributes) === JSON.stringify(attributes),
        )
      : dp1[0];

    if (typeof dp?.value === "number") {
      return dp.value;
    }

    if (typeof dp?.value === "object") {
      return dp.value.sum;
    }

    return dp?.value;
  }

  async collectAndExport(): Promise<void> {
    const result = await this.collect();
    await new Promise<void>((resolve, reject) => {
      this._exporter.export(result.resourceMetrics, (result) => {
        if (result.error != null) {
          reject(result.error);
        } else {
          resolve();
        }
      });
    });
  }
}

export function createTestMetricReader() {
  const metricReader = new TestMetricReader();
  const success = metrics.setGlobalMeterProvider(
    new MeterProvider({
      readers: [metricReader],
    }),
  );

  expect(success).toBe(true);

  return metricReader;
}

export function tearDownTestMetricReader() {
  metrics.disable();
}
