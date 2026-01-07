import { perf as coJsonPerf } from "cojson";

export const performanceMarks = {
  subscriptionLoadStart: "jazz.subscription.first_load.start",
  subscriptionLoadEnd: "jazz.subscription.first_load.end",
} as const;

export const performanceMeasures = {
  subscriptionLoad: "jazz.subscription.first_load",
  subscriptionLoadFromStorage: "jazz.subscription.load_from_storage",
  subscriptionLoadFromPeer: "jazz.subscription.load_load_from_peer",
} as const;

const performanceMarkAvailable =
  "mark" in performance && "getEntriesByName" in performance;

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

export interface LoadMeasureDetail {
  firstLoad: PerformanceMeasure;
  loadFromStorage?: PerformanceMeasure;
  loadFromPeer?: PerformanceMeasure;
}

export function measureSubscriptionLoad(
  coId: string,
  sourceId: string | undefined,
  parentId: string | undefined,
  parentKey: string | undefined,
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
    // In case of missed value, we don't want to measure the load from storage
    if (loadFromStorage.end.detail.found) {
      loadMeasureDetail.loadFromStorage = performance.measure(
        performanceMeasures.subscriptionLoadFromStorage + "." + coId,
        {
          start: loadFromStorage.start.startTime,
          end: loadFromStorage.end.startTime,
          detail: {
            id: coId,
            source_id: sourceId,
            parent_id: parentId,
            parent_key: parentKey,
            resolve,
            // devtools: {
            //   track: sourceId,
            //   trackGroup: "SubscriptionScopes",
            //   tooltipText: "Load from storage",
            //   color: "secondary",
            // },
          },
        },
      );
    }
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
          parent_id: parentId,
          parent_key: parentKey,
          resolve,
          // devtools: {
          //   track: sourceId,
          //   trackGroup: "SubscriptionScopes",
          //   tooltipText: "Internal load from peer",
          //   color: "secondary",
          // },
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
    return null;
  }

  const loadMeasure = performance.measure(
    performanceMeasures.subscriptionLoad + "." + coId,
    {
      start: firstLoad.start.startTime,
      end: firstLoad.end.startTime,
      detail: {
        id: coId,
        source_id: sourceId,
        parent_id: parentId,
        parent_key: parentKey,
        resolve,
        loadFromStorage: loadMeasureDetail.loadFromStorage?.duration,
        loadFromPeer: loadMeasureDetail.loadFromPeer?.duration,
        // Show in devtools only top-level subscriptions
        devtools: parentId
          ? undefined
          : {
              track: sourceId ?? coId,
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
): { start: PerformanceMark; end: PerformanceMark } | null {
  const endMarks = performance.getEntriesByName(endMark + "." + coId, "mark");
  const startMarks = performance.getEntriesByName(
    startMark + "." + coId,
    "mark",
  );

  // Assuming they are all sync, pick the last endMark entry position
  const startMarkEntry = startMarks.at(startMarks.length - 1);
  const endMarkEntry = endMarks.at(-1);

  if (!startMarkEntry || !endMarkEntry) {
    return null;
  }

  // clean up marks once they are synchronized
  if (endMarks.length === startMarks.length) {
    performance.clearMarks(startMark + "." + coId);
    performance.clearMarks(endMark + "." + coId);
  }

  return {
    start: startMarkEntry as PerformanceMark,
    end: endMarkEntry as PerformanceMark,
  };
}
