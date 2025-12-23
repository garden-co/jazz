// #region Metrics
import { unstable_setOpenTelemetryInstrumentationEnabled } from "jazz-tools";
import {
  MeterProvider,
  PeriodicExportingMetricReader,
  ConsoleMetricExporter,
} from "@opentelemetry/sdk-metrics";
import { metrics } from "@opentelemetry/api";

// Enable instrumentation (required for metrics and tracing)
unstable_setOpenTelemetryInstrumentationEnabled(true);

// Create a console exporter for development
const metricExporter = new ConsoleMetricExporter();

// Set up the meter provider with periodic export
const meterProvider = new MeterProvider({
  readers: [
    new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 10000, // Export every 10 seconds
    }),
  ],
});

// Register the provider globally
metrics.setGlobalMeterProvider(meterProvider);
// #endregion

// #region Tracing
import { unstable_setOpenTelemetryInstrumentationEnabled } from "jazz-tools";
import {
  BasicTracerProvider,
  SimpleSpanProcessor,
  ConsoleSpanExporter,
} from "@opentelemetry/sdk-trace-base";
import { trace } from "@opentelemetry/api";

// Enable instrumentation (required for metrics and tracing)
unstable_setOpenTelemetryInstrumentationEnabled(true);

// Create a console exporter for development
const spanExporter = new ConsoleSpanExporter();

// Set up the tracer provider
const tracerProvider = new BasicTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(spanExporter)],
});

// Register the provider globally
trace.setGlobalTracerProvider(tracerProvider);
// #endregion

// #region Production
import { unstable_setOpenTelemetryInstrumentationEnabled } from "jazz-tools";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-http";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import {
  MeterProvider as MeterProviderProd,
  PeriodicExportingMetricReader as PeriodicExportingMetricReaderProd,
} from "@opentelemetry/sdk-metrics";
import {
  BasicTracerProvider as BasicTracerProviderProd,
  BatchSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { metrics as metricsProd, trace as traceProd } from "@opentelemetry/api";

// Enable instrumentation (required for metrics and tracing)
unstable_setOpenTelemetryInstrumentationEnabled(true);

// Configure OTLP exporters pointing to your collector
const otlpMetricExporter = new OTLPMetricExporter({
  url: "https://your-collector.example.com/v1/metrics",
});

const otlpTraceExporter = new OTLPTraceExporter({
  url: "https://your-collector.example.com/v1/traces",
});

// Set up providers with production exporters
const prodMeterProvider = new MeterProviderProd({
  readers: [
    new PeriodicExportingMetricReaderProd({
      exporter: otlpMetricExporter,
      exportIntervalMillis: 60000, // Export every minute
    }),
  ],
});

const prodTracerProvider = new BasicTracerProviderProd({
  spanProcessors: [new BatchSpanProcessor(otlpTraceExporter)],
});

metricsProd.setGlobalMeterProvider(prodMeterProvider);
traceProd.setGlobalTracerProvider(prodTracerProvider);
// #endregion
