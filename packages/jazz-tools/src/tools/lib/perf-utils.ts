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

const performanceMarkAvailable = "mark" in performance;

export function trackPerformanceMark(
  mark: keyof typeof performanceMarks,
  coId: string,
  detail?: Record<string, string>,
) {
  if (!performanceMarkAvailable) {
    return;
  }

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
): LoadMeasureDetail | null {
  if (!performanceMarkAvailable) {
    return null;
  }

  const loadMeasureDetail: Omit<LoadMeasureDetail, "firstLoad"> = {};

  const loadFromStorage = extractStartEndMarks(
    coJsonPerf.performanceMarks.loadFromStorageStart,
    coJsonPerf.performanceMarks.loadFromStorageEnd,
    coId,
  );
  if (loadFromStorage) {
    loadMeasureDetail.loadFromStorage = performance.measure(
      performanceMeasures.subscriptionLoadFromStorage + "." + coId,
      {
        start: loadFromStorage.start.startTime,
        end: loadFromStorage.end.startTime,
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
  }

  const loadFromPeer = extractStartEndMarks(
    coJsonPerf.performanceMarks.loadFromPeerStart,
    coJsonPerf.performanceMarks.loadFromPeerEnd,
    coId,
  );
  if (loadFromPeer) {
    loadMeasureDetail.loadFromPeer = performance.measure(
      performanceMeasures.subscriptionLoadFromPeer + "." + coId,
      {
        start: loadFromPeer.start.startTime,
        end: loadFromPeer.end.startTime,
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
  }

  const transactionParsing = extractStartEndMarks(
    coJsonPerf.performanceMarks.transactionParsingStart,
    coJsonPerf.performanceMarks.transactionParsingEnd,
    coId,
  );
  if (transactionParsing) {
    loadMeasureDetail.transactionParsing = performance.measure(
      performanceMeasures.subscriptionLoadTransactionParsing + "." + coId,
      {
        start: transactionParsing.start.startTime,
        end: transactionParsing.end.startTime,
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
  }

  const firstLoad = extractStartEndMarks(
    performanceMarks.subscriptionLoadStart,
    performanceMarks.subscriptionLoadEnd,
    coId,
  );

  if (!firstLoad) {
    throw new Error("First load mark not found");
  }

  const loadMeasure = performance.measure(
    performanceMeasures.subscriptionLoad + "." + coId,
    {
      start: firstLoad.start.startTime,
      end: firstLoad.end.startTime,
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

  return {
    ...loadMeasureDetail,
    firstLoad: loadMeasure,
  };
}

function extractStartEndMarks(
  startMark: string,
  endMark: string,
  coId: string,
): { start: PerformanceEntry; end: PerformanceEntry } | null {
  const endMarks = performance.getEntriesByName(endMark + "." + coId, "mark");

  // Assuming they are all sync, pick the last endMark entry position
  const startMarkEntry = performance
    .getEntriesByName(startMark + "." + coId, "mark")
    .at(endMarks.length - 1);
  const endMarkEntry = endMarks.at(-1);

  if (!startMarkEntry || !endMarkEntry) {
    return null;
  }

  return {
    start: startMarkEntry,
    end: endMarkEntry,
  };
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
