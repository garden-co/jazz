import { useEffect, useRef, useState } from "react";
import { shouldStartTimelineHydration } from "./timeline-startup.js";

const pollInterval = 15_000;

type TimelinePayload = {
  cursor?: string;
  hasMore?: boolean;
  count?: number;
};

export function shouldApplyPaginationCursor(requestedCursor: string | null, paginationStarted: boolean) {
  return requestedCursor !== null || !paginationStarted;
}

export function useTimelineHydration({
  did,
  itemCount,
  hasLocalRows,
  localQueryReady,
  browserOnline,
  reportApiReachable,
}: {
  did: string;
  itemCount: number;
  hasLocalRows: boolean;
  localQueryReady: boolean;
  browserOnline: boolean;
  reportApiReachable: (reachable: boolean) => void;
}) {
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const refreshInFlight = useRef(false);
  const paginationInFlight = useRef(false);
  const paginationStarted = useRef(false);
  const loadingFallbackTimer = useRef<number | undefined>(undefined);
  const loadMoreRef = useRef<HTMLDivElement>(null);

  function finishInitialLoading() {
    if (loadingFallbackTimer.current !== undefined) {
      window.clearTimeout(loadingFallbackTimer.current);
      loadingFallbackTimer.current = undefined;
    }
    setInitialLoading(false);
  }

  async function loadPage(cursor: string | null) {
    const inFlight = cursor ? paginationInFlight : refreshInFlight;
    if (inFlight.current) return;
    if (!browserOnline) {
      setInitialLoading(false);
      return;
    }
    inFlight.current = true;
    const paginationWasStarted = paginationStarted.current;
    if (cursor) paginationStarted.current = true;
    if (cursor) setLoadingMore(true);
    try {
      const response = await fetch(cursor ? `/api/timeline?cursor=${encodeURIComponent(cursor)}` : "/api/timeline");
      if (!response.ok) throw new Error("Timeline refresh failed");
      const result = await response.json() as TimelinePayload;
      reportApiReachable(true);
      if (shouldApplyPaginationCursor(cursor, paginationStarted.current)) {
        setNextCursor(result.cursor ?? null);
        setHasMore(Boolean(result.hasMore));
      }
      if (!cursor) {
        if (!result.count) finishInitialLoading();
        else loadingFallbackTimer.current = window.setTimeout(finishInitialLoading, 5_000);
      }
    } catch {
      if (cursor && !paginationWasStarted) paginationStarted.current = false;
      reportApiReachable(false);
    } finally {
      inFlight.current = false;
      if (cursor) setLoadingMore(false);
    }
  }

  useEffect(() => {
    if (hasLocalRows) finishInitialLoading();
  }, [hasLocalRows]);

  useEffect(() => {
    paginationStarted.current = false;
    setNextCursor(null);
    setHasMore(false);
  }, [did]);

  useEffect(() => {
    if (!shouldStartTimelineHydration(localQueryReady, browserOnline)) return;
    loadPage(null);
    const timer = window.setInterval(() => loadPage(null), pollInterval);
    return () => {
      window.clearInterval(timer);
      if (loadingFallbackTimer.current !== undefined) window.clearTimeout(loadingFallbackTimer.current);
    };
  }, [did, browserOnline, localQueryReady]);

  useEffect(() => {
    const sentinel = loadMoreRef.current;
    if (!sentinel) return;
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting) && itemCount > 0 && nextCursor && hasMore && !loadingMore) {
        loadPage(nextCursor);
      }
    }, { rootMargin: "500px" });
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [hasMore, itemCount, loadingMore, nextCursor]);

  return { hasMore, loadingMore, initialLoading, loadMoreRef };
}
