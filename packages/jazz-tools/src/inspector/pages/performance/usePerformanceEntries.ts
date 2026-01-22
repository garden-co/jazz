import { useState, useEffect } from "react";
import { SubscriptionPerformanceDetail } from "jazz-tools";
import type { SubscriptionEntry } from "./types.js";

export function usePerformanceEntries(): SubscriptionEntry[] {
  const [entries, setEntries] = useState<SubscriptionEntry[]>([]);

  useEffect(() => {
    const entriesByUuid = new Map<string, SubscriptionEntry>();

    const handlePerformanceEntries = (entries: PerformanceEntry[]) => {
      for (const mark of entries) {
        const detail = (mark as PerformanceMark)
          .detail as SubscriptionPerformanceDetail;

        if (detail?.type !== "jazz-subscription") continue;

        const prevEntry = entriesByUuid.get(detail.uuid);

        if (mark.entryType === "mark" && prevEntry) continue;

        entriesByUuid.set(detail.uuid, {
          uuid: detail.uuid,
          id: detail.id,
          source: detail.source,
          resolve: JSON.stringify(detail.resolve),
          status: detail.status,
          startTime: mark.startTime,
          callerStack: detail.callerStack ?? prevEntry?.callerStack,
          duration: mark.entryType === "mark" ? undefined : mark.duration,
          endTime: mark.startTime + mark.duration,
          errorType: detail.errorType,
        });
      }
    };

    handlePerformanceEntries(performance.getEntriesByType("mark"));
    handlePerformanceEntries(performance.getEntriesByType("measure"));

    setEntries(Array.from(entriesByUuid.values()));

    const observer = new PerformanceObserver((list) => {
      handlePerformanceEntries(list.getEntries());
      setEntries(Array.from(entriesByUuid.values()));
    });

    observer.observe({ entryTypes: ["mark", "measure"] });

    return () => observer.disconnect();
  }, []);

  return entries;
}
