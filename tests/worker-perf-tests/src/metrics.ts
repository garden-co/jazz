import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { metrics } from "@opentelemetry/api";
import { PrometheusExporter } from "@opentelemetry/exporter-prometheus";
import { MeterProvider } from "@opentelemetry/sdk-metrics";
import { IncomingMessage, ServerResponse } from "http";

import { registerScenarioMetrics } from "./scenarioMetrics.ts";

let metricsInitialized = false;

export function setupMetrics() {
  // Set up OpenTelemetry metrics with Prometheus exporter
  const exporter = new PrometheusExporter({ preventServerStart: true });
  const meterProvider = new MeterProvider({ readers: [exporter] });
  metrics.setGlobalMeterProvider(meterProvider);

  // Register scenario metrics only once
  if (!metricsInitialized) {
    registerScenarioMetrics();
    metricsInitialized = true;
  }

  return {
    middleware(req: IncomingMessage, res: ServerResponse) {
      if (req.url === "/metrics") {
        res.setHeader("Content-Type", "text/html");
        res.end(
          readFileSync(
            join(dirname(fileURLToPath(import.meta.url)), "dashboard.html"),
            "utf-8",
          ),
        );
        return true;
      }
      if (req.url === "/api/metrics") {
        exporter.getMetricsRequestHandler(req, res);
        return true;
      }

      return false;
    },
  };
}
