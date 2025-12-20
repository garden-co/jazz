// #region Metrics
import {
  MeterProvider,
  PeriodicExportingMetricReader,
  ConsoleMetricExporter,
} from "@opentelemetry/sdk-metrics";
import { metrics } from "@opentelemetry/api";

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
import {
  BasicTracerProvider,
  SimpleSpanProcessor,
  ConsoleSpanExporter,
} from "@opentelemetry/sdk-trace-base";
import { trace } from "@opentelemetry/api";

// Create a console exporter for development
const spanExporter = new ConsoleSpanExporter();

// Set up the tracer provider
const tracerProvider = new BasicTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(spanExporter)],
});

// Register the provider globally
trace.setTracerProvider(tracerProvider);
// #endregion

// #region Production
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
traceProd.setTracerProvider(prodTracerProvider);
// #endregion
