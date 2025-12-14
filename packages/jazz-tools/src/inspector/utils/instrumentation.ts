import { MeterProvider } from "@opentelemetry/sdk-metrics";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { metrics } from "@opentelemetry/api";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
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
