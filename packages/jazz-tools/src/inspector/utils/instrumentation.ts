import { MeterProvider } from "@opentelemetry/sdk-metrics";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { metrics } from "@opentelemetry/api";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
import {
  MetricReader,
  InMemoryMetricExporter,
  AggregationTemporality,
} from "@opentelemetry/sdk-metrics";

export class JazzOTelMetricReader extends MetricReader {
  private initialized = false;
  private exporter = new InMemoryMetricExporter(
    AggregationTemporality.CUMULATIVE,
  );

  protected onInitialized(): void {
    this.initialized = true;
  }

  protected onShutdown(): Promise<void> {
    throw new Error("Method not implemented.");
  }
  protected onForceFlush(): Promise<void> {
    throw new Error("Method not implemented.");
  }

  async getMetricDataPoints(scope: string, name: string) {
    await this.collectAndExport();

    const metric = this.exporter
      .getMetrics()[0]
      ?.scopeMetrics?.find((sm) => sm.scope.name === scope)
      ?.metrics.find((m) => m.descriptor.name === name);

    this.exporter.reset();

    const dp = metric?.dataPoints;

    return dp;
  }

  async collectMetrics() {
    if (!this.initialized) {
      throw new Error("JazzOTelMetricReader not initialized.");
    }

    await this.collectAndExport();
    const metrics = this.exporter.getMetrics();
    this.exporter.reset();
    return metrics;
  }

  async collectAndExport(): Promise<void> {
    const result = await this.collect();
    await new Promise<void>((resolve, reject) => {
      this.exporter.export(result.resourceMetrics, (result) => {
        if (result.error != null) {
          reject(result.error);
        } else {
          resolve();
        }
      });
    });
  }
}

export const jazzMetricReader = new JazzOTelMetricReader();

export function recordMetrics() {
  const globalMeterHasBeenSet = !metrics
    .getMeterProvider()
    .constructor.name.startsWith("Noop");

  if (globalMeterHasBeenSet) {
    return;
  }

  const meterProvider = new MeterProvider({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: "jazz-tools",
    }),
    readers: [jazzMetricReader],
  });

  // Register the global meter provider
  const res = metrics.setGlobalMeterProvider(meterProvider);

  if (res !== true) {
    console.error("Failed to set OTel meter provider");
    return;
  }
}
