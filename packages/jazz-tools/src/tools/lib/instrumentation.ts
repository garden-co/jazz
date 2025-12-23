import { trace, type Span, context, type Histogram } from "@opentelemetry/api";
import type { LoadMeasureDetail } from "./perf-utils";

let otelEnabled = false;

export function setOpenTelemetryInstrumentationEnabled(enable: boolean) {
  otelEnabled = enable;
}

export function isOpenTelemetryInstrumentationEnabled() {
  return otelEnabled;
}

export function recordLoadTimeToOTelMetric(
  metric: Histogram,
  loadMeasureDetail: LoadMeasureDetail,
  coId: string,
  error: boolean,
) {
  if (!otelEnabled) {
    return;
  }

  metric.record(loadMeasureDetail.firstLoad.duration, {
    id: coId,
    result: error ? "error" : "loaded",
  });
}

export function recordLoadTimeToOTelSpan(
  subscriptionSpan: Span,
  loadMeasureDetail: LoadMeasureDetail,
) {
  if (!otelEnabled) {
    return;
  }

  const tracer = trace.getTracer("jazz-tools");
  const subscriptionContext = trace.setSpan(context.active(), subscriptionSpan);

  // Record the load span
  const firstLoadSpan = tracer.startSpan(
    "jazz.subscription.first_load",
    {
      startTime: performance.timeOrigin + loadMeasureDetail.firstLoad.startTime,
    },
    subscriptionContext,
  );

  const loadContext = trace.setSpan(subscriptionContext, firstLoadSpan);

  if (loadMeasureDetail.loadFromStorage) {
    tracer
      .startSpan(
        "jazz.subscription.first_load.from_storage",
        {
          startTime:
            performance.timeOrigin +
            loadMeasureDetail.loadFromStorage?.startTime,
        },
        loadContext,
      )
      .end(
        performance.timeOrigin +
          loadMeasureDetail.loadFromStorage?.startTime +
          loadMeasureDetail.loadFromStorage?.duration,
      );
  }

  if (loadMeasureDetail.loadFromPeer) {
    tracer
      .startSpan(
        "jazz.subscription.first_load.load_from_peer",
        {
          startTime:
            performance.timeOrigin + loadMeasureDetail.loadFromPeer?.startTime,
        },
        loadContext,
      )
      .end(
        performance.timeOrigin +
          loadMeasureDetail.loadFromPeer?.startTime +
          loadMeasureDetail.loadFromPeer?.duration,
      );
  }

  firstLoadSpan.end(
    performance.timeOrigin +
      loadMeasureDetail.firstLoad.startTime +
      loadMeasureDetail.firstLoad.duration,
  );
}
