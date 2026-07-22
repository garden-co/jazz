import { useEffect, useRef, useState } from "react";

const pollInterval = 15_000;
const minimumSpinnerDuration = 300;

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

export function fetchingMorePosts({
  startCount,
  itemCount,
  remote,
}: {
  startCount: number | null;
  itemCount: number;
  remote: boolean;
}) {
  return remote || (startCount !== null && itemCount <= startCount);
}

export function shouldLoadNextPage({
  intersecting,
  canLoad,
  loadingMore,
}: {
  intersecting: boolean;
  canLoad: boolean;
  loadingMore: boolean;
}) {
  return intersecting && canLoad && !loadingMore;
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
  const [remoteLoadingMore, setRemoteLoadingMore] = useState(false);
  const [pageStartCount, setPageStartCount] = useState<number | null>(null);
  const [pageFetchStartedAt, setPageFetchStartedAt] = useState<number | null>(null);
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
      setRemoteLoadingMore(true);
      setPageStartCount(itemCount);
      setPageFetchStartedAt(Date.now());
    }
    try {
      // This endpoint returns only projection metadata. Timeline rows still
      // arrive through the reactive Jazz query in Timeline.tsx.
      const response = await fetch(
        cursor ? `/api/timeline?cursor=${encodeURIComponent(cursor)}` : "/api/timeline",
      );
      if (!response.ok) throw new Error("Timeline projection failed");
      const result = (await response.json()) as TimelinePayload;
      if (generation !== requestGeneration.current) return;
      reportApiReachable(true);
      if (cursor !== null || !paginationStarted.current) {
        setNextCursor(result.cursor ?? null);
        setHasMore(Boolean(result.hasMore));
      }
      if (cursor && !result.count) {
        setPageStartCount(null);
        setPageFetchStartedAt(null);
      }
      // A non-empty response is not the data itself. Keep showing the honest
      // waiting state until Jazz delivers the first projected row.
      if (!cursor && !result.count) setInitialLoading(false);
    } catch {
      if (generation !== requestGeneration.current) return;
      if (cursor && !paginationWasStarted) paginationStarted.current = false;
      if (cursor) {
        setPageStartCount(null);
        setPageFetchStartedAt(null);
      }
      if (!cursor) setInitialLoading(false);
      reportApiReachable(false);
    } finally {
      if (generation === requestGeneration.current) {
        inFlight.current = false;
        if (cursor) setRemoteLoadingMore(false);
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
    setRemoteLoadingMore(false);
    setPageStartCount(null);
    setPageFetchStartedAt(null);
    setInitialLoading(true);
  }, [did]);

  useEffect(() => {
    if (hasLocalRows || (localQueryReady && !browserOnline)) setInitialLoading(false);
  }, [browserOnline, hasLocalRows, localQueryReady]);

  useEffect(() => {
    if (pageStartCount === null || pageFetchStartedAt === null || itemCount <= pageStartCount)
      return;
    const remaining = Math.max(0, minimumSpinnerDuration - (Date.now() - pageFetchStartedAt));
    const timer = window.setTimeout(() => {
      setPageStartCount(null);
      setPageFetchStartedAt(null);
    }, remaining);
    return () => window.clearTimeout(timer);
  }, [itemCount, pageFetchStartedAt, pageStartCount]);

  useEffect(() => {
    if (!localQueryReady || !browserOnline) return;
    loadPage(null);
    const timer = window.setInterval(() => loadPage(null), pollInterval);
    return () => window.clearInterval(timer);
  }, [did, browserOnline, localQueryReady]);

  useEffect(() => {
    const sentinel = loadMoreRef.current;
    if (!sentinel) return;
    const observer = new IntersectionObserver(
      (entries) => {
        const intersecting = entries.some((entry) => entry.isIntersecting);
        const source = nextTimelinePageSource({
          cachedRowsRemaining,
          localQueryRefreshing,
          remoteRowsRemaining: Boolean(nextCursor && hasMore),
        });
        const loadingMore = fetchingMorePosts({
          startCount: pageStartCount,
          itemCount,
          remote: remoteLoadingMore,
        });
        const shouldLoad = shouldLoadNextPage({
          intersecting,
          canLoad: itemCount > 0 && source !== undefined,
          loadingMore,
        });
        if (!shouldLoad) return;
        if (source === "local") {
          setPageStartCount(itemCount);
          setPageFetchStartedAt(Date.now());
          revealCachedRows();
        } else if (source === "remote" && nextCursor) loadPage(nextCursor);
      },
      { rootMargin: "500px" },
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [
    cachedRowsRemaining,
    hasMore,
    itemCount,
    pageStartCount,
    localQueryRefreshing,
    nextCursor,
    remoteLoadingMore,
    revealCachedRows,
  ]);

  const loadingMore = fetchingMorePosts({
    startCount: pageStartCount,
    itemCount,
    remote: remoteLoadingMore,
  });
  return { hasMore, loadingMore, initialLoading, loadMoreRef };
}
