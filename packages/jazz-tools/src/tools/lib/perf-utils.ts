import { perf as coJsonPerf } from "cojson";
import { trace, type Span, context } from "@opentelemetry/api";

export const performanceMarks = {
  subscriptionLoadStart: "jazz.subscription.first_load.start",
  subscriptionLoadEnd: "jazz.subscription.first_load.end",
} as const;

export const performanceMeasures = {
  subscriptionLoad: "jazz.subscription.first_load",
  subscriptionLoadFromStorage: "jazz.subscription.first_load.from_storage",
  subscriptionLoadFromPeer: "jazz.subscription.first_load.load_from_peer",
  subscriptionLoadTransactionParsing:
    "jazz.subscription.first_load.transaction_parsing",
} as const;

export function trackPerformanceMark(
  mark: keyof typeof performanceMarks,
  coId: string,
  detail?: Record<string, string>,
) {
  performance.mark(performanceMarks[mark] + "." + coId, {
    detail,
  });
}

interface LoadMeasureDetail {
  firstLoad: PerformanceMeasure;
  loadFromStorage?: PerformanceMeasure;
  loadFromPeer?: PerformanceMeasure;
  transactionParsing?: PerformanceMeasure;
}

export function measureSubscriptionLoad(
  coId: string,
  sourceId: string,
  resolve: any,
): LoadMeasureDetail {
  const loadMeasureDetail: Omit<LoadMeasureDetail, "firstLoad"> = {};

  if (
    hasStartEndMarks(
      coJsonPerf.performanceMarks.loadFromStorageStart,
      coJsonPerf.performanceMarks.loadFromStorageEnd,
      coId,
    )
  ) {
    loadMeasureDetail.loadFromStorage = performance.measure(
      performanceMeasures.subscriptionLoadFromStorage + "." + coId,
      {
        start: coJsonPerf.performanceMarks.loadFromStorageStart + "." + coId,
        end: coJsonPerf.performanceMarks.loadFromStorageEnd + "." + coId,
        detail: {
          id: coId,
          source_id: sourceId,
          resolve,
          devtools: {
            track: sourceId,
            trackGroup: "SubscriptionScopes",
            tooltipText: "Load from storage",
            color: "secondary",
          },
        },
      },
    );

    clearMarks([
      coJsonPerf.performanceMarks.loadFromStorageStart + "." + coId,
      coJsonPerf.performanceMarks.loadFromStorageEnd + "." + coId,
    ]);
  }

  if (
    hasStartEndMarks(
      coJsonPerf.performanceMarks.loadFromPeerStart,
      coJsonPerf.performanceMarks.loadFromPeerEnd,
      coId,
    )
  ) {
    loadMeasureDetail.loadFromPeer = performance.measure(
      performanceMeasures.subscriptionLoadFromPeer + "." + coId,
      {
        start: coJsonPerf.performanceMarks.loadFromPeerStart + "." + coId,
        end: coJsonPerf.performanceMarks.loadFromPeerEnd + "." + coId,
        detail: {
          id: coId,
          source_id: sourceId,
          resolve,
          devtools: {
            track: sourceId,
            trackGroup: "SubscriptionScopes",
            tooltipText: "Internal load from peer",
            color: "secondary",
          },
        },
      },
    );

    clearMarks([
      coJsonPerf.performanceMarks.loadFromPeerStart + "." + coId,
      coJsonPerf.performanceMarks.loadFromPeerEnd + "." + coId,
    ]);
  }

  if (
    hasStartEndMarks(
      coJsonPerf.performanceMarks.transactionParsingStart,
      coJsonPerf.performanceMarks.transactionParsingEnd,
      coId,
    )
  ) {
    loadMeasureDetail.transactionParsing = performance.measure(
      performanceMeasures.subscriptionLoadTransactionParsing + "." + coId,
      {
        start: coJsonPerf.performanceMarks.transactionParsingStart + "." + coId,
        end: coJsonPerf.performanceMarks.transactionParsingEnd + "." + coId,
        detail: {
          id: coId,
          source_id: sourceId,
          resolve,
          devtools: {
            track: sourceId,
            trackGroup: "SubscriptionScopes",
            tooltipText: "Transaction parsing",
            color: "secondary",
          },
        },
      },
    );

    clearMarks([
      coJsonPerf.performanceMarks.transactionParsingStart + "." + coId,
      coJsonPerf.performanceMarks.transactionParsingEnd + "." + coId,
    ]);
  }

  const loadMeasure = performance.measure(
    performanceMeasures.subscriptionLoad + "." + coId,
    {
      start: performanceMarks.subscriptionLoadStart + "." + coId,
      end: performanceMarks.subscriptionLoadEnd + "." + coId,
      detail: {
        id: coId,
        source_id: sourceId,
        resolve,
        loadFromStorage: loadMeasureDetail.loadFromStorage?.duration,
        loadFromPeer: loadMeasureDetail.loadFromPeer?.duration,
        transactionParsing: loadMeasureDetail.transactionParsing?.duration,
        devtools: {
          track: sourceId,
          trackGroup: "SubscriptionScopes",
          tooltipText: "First load time",
          color: "primary",
        },
      },
    },
  );

  clearMarks([
    performanceMarks.subscriptionLoadStart + "." + coId,
    performanceMarks.subscriptionLoadEnd + "." + coId,
  ]);

  return {
    ...loadMeasureDetail,
    firstLoad: loadMeasure,
  };
}

function hasPerformanceMark(mark: string, coId: string) {
  return performance.getEntriesByName(mark + "." + coId, "mark").length > 0;
}

function hasStartEndMarks(startMark: string, endMark: string, coId: string) {
  return (
    hasPerformanceMark(startMark, coId) && hasPerformanceMark(endMark, coId)
  );
}

function clearMarks(marks: string[]): void {
  marks.forEach((mark) => performance.clearMarks(mark));
}

export function trackSubscriptionLoadSpans(
  subscriptionSpan: Span,
  loadMeasureDetail: LoadMeasureDetail,
) {
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
  if (loadMeasureDetail.transactionParsing) {
    tracer
      .startSpan(
        "jazz.subscription.first_load.transaction_parsing",
        {
          startTime:
            performance.timeOrigin +
            loadMeasureDetail.transactionParsing?.startTime,
        },
        loadContext,
      )
      .end(
        performance.timeOrigin +
          loadMeasureDetail.transactionParsing?.startTime +
          loadMeasureDetail.transactionParsing?.duration,
      );
  }

  firstLoadSpan.end(
    performance.timeOrigin +
      loadMeasureDetail.firstLoad.startTime +
      loadMeasureDetail.firstLoad.duration,
  );
}
