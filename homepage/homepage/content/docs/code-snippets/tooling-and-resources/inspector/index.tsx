// #region WithOpenTelemetry
import React from "react";
import { jazzMetricReader, JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider } from "jazz-tools/react";
import {
  MeterProvider,
  PeriodicExportingMetricReader,
  ConsoleMetricExporter,
} from "@opentelemetry/sdk-metrics";
import { metrics } from "@opentelemetry/api";

// Include jazzMetricReader alongside your other readers
const meterProvider = new MeterProvider({
  readers: [
    // Your existing metric reader
    new PeriodicExportingMetricReader({
      exporter: new ConsoleMetricExporter(),
      exportIntervalMillis: 10000,
    }),
    // Add this to enable the Inspector's Performance tab
    jazzMetricReader,
  ],
});

metrics.setGlobalMeterProvider(meterProvider);

function App() {
  return (
    // @ts-expect-error No sync prop
    <JazzReactProvider>
      {/* Your app components */}
      <JazzInspector />
    </JazzReactProvider>
  );
}
// #endregion

export {};
