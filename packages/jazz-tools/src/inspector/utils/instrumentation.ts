import {
  MeterProvider,
  PeriodicExportingMetricReader,
  ConsoleMetricExporter,
  PushMetricExporter,
  ResourceMetrics,
  MetricReader,
  InMemoryMetricExporter,
  AggregationTemporality,
} from "@opentelemetry/sdk-metrics";
import {
  defaultResource,
  resourceFromAttributes,
} from "@opentelemetry/resources";
import { type ExportResult, ExportResultCode } from "@opentelemetry/core";
import { metrics } from "@opentelemetry/api";
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import { jazzMetricReader } from "./otel";

export function recordMetrics() {
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
