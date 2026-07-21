import { useEffect, useRef, useState } from "react";

const pollInterval = 15_000;

type TimelinePayload = {
  cursor?: string;
  hasMore?: boolean;
  count?: number;
};

export function nextTimelinePageSource({
  cachedRowsRemaining,
  localQueryRefreshing,
  remoteRowsRemaining,
}: {
  cachedRowsRemaining: boolean;
  localQueryRefreshing: boolean;
  remoteRowsRemaining: boolean;
}) {
  if (cachedRowsRemaining) return "local" as const;
  if (!localQueryRefreshing && remoteRowsRemaining) return "remote" as const;
  return undefined;
}

export function useTimelineProjection({
  did,
  itemCount,
  hasLocalRows,
  cachedRowsRemaining,
  localQueryRefreshing,
  localQueryReady,
  browserOnline,
  revealCachedRows,
  reportApiReachable,
}: {
  did: string;
  itemCount: number;
  hasLocalRows: boolean;
  cachedRowsRemaining: boolean;
  localQueryRefreshing: boolean;
  localQueryReady: boolean;
  browserOnline: boolean;
  revealCachedRows: () => void;
  reportApiReachable: (reachable: boolean) => void;
}) {
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const refreshInFlight = useRef(false);
  const paginationInFlight = useRef(false);
  const paginationStarted = useRef(false);
  const requestGeneration = useRef(0);
  const loadMoreRef = useRef<HTMLDivElement>(null);

  async function loadPage(cursor: string | null) {
    const generation = requestGeneration.current;
    const inFlight = cursor ? paginationInFlight : refreshInFlight;
    if (inFlight.current || !browserOnline) return;
    inFlight.current = true;
    const paginationWasStarted = paginationStarted.current;
    if (cursor) {
      paginationStarted.current = true;
      setLoadingMore(true);
    }
    try {
      // This endpoint returns only projection metadata. Timeline rows still
      // arrive through the reactive Jazz query in Timeline.tsx.
      const response = await fetch(
        cursor ? `/api/timeline?cursor=${encodeURIComponent(cursor)}` : "/api/timeline",
      );
      if (!response.ok) throw new Error("Timeline projection failed");
      const result = await response.json() as TimelinePayload;
      if (generation !== requestGeneration.current) return;
      reportApiReachable(true);
      if (cursor !== null || !paginationStarted.current) {
        setNextCursor(result.cursor ?? null);
        setHasMore(Boolean(result.hasMore));
      }
      // A non-empty response is not the data itself. Keep showing the honest
      // waiting state until Jazz delivers the first projected row.
      if (!cursor && !result.count) setInitialLoading(false);
    } catch {
      if (generation !== requestGeneration.current) return;
      if (cursor && !paginationWasStarted) paginationStarted.current = false;
      if (!cursor) setInitialLoading(false);
      reportApiReachable(false);
    } finally {
      if (generation === requestGeneration.current) {
        inFlight.current = false;
        if (cursor) setLoadingMore(false);
      }
    }
  }

  useEffect(() => {
    requestGeneration.current += 1;
    refreshInFlight.current = false;
    paginationInFlight.current = false;
    paginationStarted.current = false;
    setNextCursor(null);
    setHasMore(false);
    setLoadingMore(false);
    setInitialLoading(true);
  }, [did]);

  useEffect(() => {
    if (hasLocalRows || (localQueryReady && !browserOnline)) setInitialLoading(false);
  }, [browserOnline, hasLocalRows, localQueryReady]);

  useEffect(() => {
    if (!localQueryReady || !browserOnline) return;
    loadPage(null);
    const timer = window.setInterval(() => loadPage(null), pollInterval);
    return () => window.clearInterval(timer);
  }, [did, browserOnline, localQueryReady]);

  useEffect(() => {
    const sentinel = loadMoreRef.current;
    if (!sentinel) return;
    const observer = new IntersectionObserver((entries) => {
      if (!entries.some((entry) => entry.isIntersecting) || itemCount === 0) return;
      const source = nextTimelinePageSource({
        cachedRowsRemaining,
        localQueryRefreshing,
        remoteRowsRemaining: Boolean(nextCursor && hasMore),
      });
      if (source === "local") revealCachedRows();
      else if (source === "remote" && nextCursor && !loadingMore) loadPage(nextCursor);
    }, { rootMargin: "500px" });
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [
    cachedRowsRemaining,
    hasMore,
    itemCount,
    loadingMore,
    localQueryRefreshing,
    nextCursor,
    revealCachedRows,
  ]);

  return { hasMore, loadingMore, initialLoading, loadMoreRef };
}
