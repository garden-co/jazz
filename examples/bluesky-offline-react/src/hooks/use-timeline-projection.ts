import { useEffect, useRef, useState } from "react";

const pollInterval = 15_000;
const minimumSpinnerDuration = 300;
const rootCardsPerPage = 20;

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

export function needsMoreRootCards({
  itemCount,
  targetItemCount,
}: {
  itemCount: number;
  targetItemCount: number;
}) {
  return itemCount < targetItemCount;
}

export function visibleRootCards<Item>(items: Item[], visibleItemCount: number) {
  return items.slice(0, visibleItemCount);
}

export function nextVisibleRootCount(itemCount: number, visibleItemCount: number) {
  return Math.min(itemCount, visibleItemCount) + rootCardsPerPage;
}

export function canLoadNextPage({
  source,
  loadingMore,
}: {
  source: "local" | "remote" | undefined;
  loadingMore: boolean;
}) {
  return source !== undefined && !loadingMore;
}

export function useTimelineProjection({
  did,
  itemCount,
  rowCount,
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
  rowCount: number;
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
  const [targetItemCount, setTargetItemCount] = useState<number | null>(null);
  const [visibleItemCount, setVisibleItemCount] = useState(rootCardsPerPage);
  const [initialLoading, setInitialLoading] = useState(true);
  const refreshInFlight = useRef(false);
  const paginationInFlight = useRef(false);
  const paginationStarted = useRef(false);
  const requestGeneration = useRef(0);

  async function loadPage(cursor: string | null) {
    const generation = requestGeneration.current;
    const inFlight = cursor ? paginationInFlight : refreshInFlight;
    if (inFlight.current || !browserOnline) return;
    inFlight.current = true;
    const paginationWasStarted = paginationStarted.current;
    if (cursor) {
      paginationStarted.current = true;
      setRemoteLoadingMore(true);
      setPageStartCount(rowCount);
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
    setTargetItemCount(null);
    setVisibleItemCount(rootCardsPerPage);
    setInitialLoading(true);
  }, [did]);

  useEffect(() => {
    if (hasLocalRows || (localQueryReady && !browserOnline)) setInitialLoading(false);
  }, [browserOnline, hasLocalRows, localQueryReady]);

  useEffect(() => {
    if (pageStartCount === null || pageFetchStartedAt === null || rowCount <= pageStartCount)
      return;
    const remaining = Math.max(0, minimumSpinnerDuration - (Date.now() - pageFetchStartedAt));
    const timer = window.setTimeout(() => {
      setPageStartCount(null);
      setPageFetchStartedAt(null);
    }, remaining);
    return () => window.clearTimeout(timer);
  }, [rowCount, pageFetchStartedAt, pageStartCount]);

  useEffect(() => {
    if (!localQueryReady || !browserOnline) return;
    loadPage(null);
    const timer = window.setInterval(() => loadPage(null), pollInterval);
    return () => window.clearInterval(timer);
  }, [did, browserOnline, localQueryReady]);

  const source = nextTimelinePageSource({
    cachedRowsRemaining,
    localQueryRefreshing,
    remoteRowsRemaining: Boolean(nextCursor && hasMore),
  });

  const loadingMore =
    targetItemCount !== null && needsMoreRootCards({ itemCount, targetItemCount });
  const hasBufferedRootCards = itemCount > visibleItemCount;
  const canLoadMore =
    rowCount > 0 && (hasBufferedRootCards || canLoadNextPage({ source, loadingMore }));

  async function loadNextPage(nextSource: "local" | "remote") {
    if (nextSource === "local") {
      setPageStartCount(rowCount);
      setPageFetchStartedAt(Date.now());
      revealCachedRows();
    } else if (nextCursor) {
      await loadPage(nextCursor);
    }
  }

  async function loadMore() {
    if (!canLoadMore) return;
    const nextTarget = nextVisibleRootCount(itemCount, visibleItemCount);
    if (itemCount >= nextTarget) {
      setVisibleItemCount(nextTarget);
      return;
    }
    if (!source) {
      setVisibleItemCount(itemCount);
      return;
    }
    setTargetItemCount(nextTarget);
    await loadNextPage(source);
  }

  useEffect(() => {
    if (targetItemCount === null) return;
    if (!needsMoreRootCards({ itemCount, targetItemCount })) {
      setVisibleItemCount(targetItemCount);
      setTargetItemCount(null);
      return;
    }
    if (pageStartCount !== null || remoteLoadingMore || localQueryRefreshing) return;
    if (!source) {
      setVisibleItemCount(itemCount);
      setTargetItemCount(null);
      return;
    }
    loadNextPage(source);
  }, [
    cachedRowsRemaining,
    hasMore,
    itemCount,
    localQueryRefreshing,
    nextCursor,
    pageStartCount,
    remoteLoadingMore,
    rowCount,
    targetItemCount,
  ]);

  return {
    hasMore: hasMore || hasBufferedRootCards,
    canLoadMore,
    loadMore,
    loadingMore,
    initialLoading,
    visibleItemCount,
  };
}
