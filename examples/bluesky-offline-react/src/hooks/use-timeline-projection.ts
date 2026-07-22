import { useEffect, useRef, useState } from "react";

const rootCardsPerPage = 20;
const projectionDeliveryTimeout = 2_000;

type TimelinePayload = {
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
  const [hasMore, setHasMore] = useState(true);
  const [remoteLoadingMore, setRemoteLoadingMore] = useState(false);
  const [pageStartCount, setPageStartCount] = useState<number | null>(null);
  const [targetItemCount, setTargetItemCount] = useState<number | null>(null);
  const [visibleItemCount, setVisibleItemCount] = useState(rootCardsPerPage);
  const paginationInFlight = useRef(false);
  const requestGeneration = useRef(0);

  async function loadRemotePage() {
    const generation = requestGeneration.current;
    if (paginationInFlight.current || !browserOnline) return;
    paginationInFlight.current = true;
    setRemoteLoadingMore(true);
    setPageStartCount(rowCount);
    try {
      // This is a semantic command. The BFF owns AppView cursors, while the
      // resulting rows still arrive exclusively through Jazz.
      const response = await fetch("/api/timeline/more", { method: "POST" });
      if (!response.ok) throw new Error("Timeline projection failed");
      const result = (await response.json()) as TimelinePayload;
      if (generation !== requestGeneration.current) return;
      reportApiReachable(true);
      setHasMore(Boolean(result.hasMore));
      if (!result.count) setPageStartCount(null);
    } catch {
      if (generation !== requestGeneration.current) return;
      setPageStartCount(null);
      reportApiReachable(false);
    } finally {
      if (generation === requestGeneration.current) {
        paginationInFlight.current = false;
        setRemoteLoadingMore(false);
      }
    }
  }

  useEffect(() => {
    requestGeneration.current += 1;
    paginationInFlight.current = false;
    setHasMore(true);
    setRemoteLoadingMore(false);
    setPageStartCount(null);
    setTargetItemCount(null);
    setVisibleItemCount(rootCardsPerPage);
  }, [did]);

  useEffect(() => {
    if (pageStartCount === null) return;
    if (rowCount > pageStartCount) {
      setPageStartCount(null);
      return;
    }
    const timer = window.setTimeout(() => {
      setPageStartCount(null);
    }, projectionDeliveryTimeout);
    return () => window.clearTimeout(timer);
  }, [rowCount, pageStartCount]);

  const source = nextTimelinePageSource({
    cachedRowsRemaining,
    localQueryRefreshing,
    remoteRowsRemaining: browserOnline && hasMore,
  });

  const loadingMore =
    targetItemCount !== null && needsMoreRootCards({ itemCount, targetItemCount });
  const hasBufferedRootCards = itemCount > visibleItemCount;
  const canLoadMore =
    rowCount > 0 && (hasBufferedRootCards || canLoadNextPage({ source, loadingMore }));

  async function loadNextPage(nextSource: "local" | "remote") {
    if (nextSource === "local") {
      setPageStartCount(rowCount);
      revealCachedRows();
    } else {
      await loadRemotePage();
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
    initialLoading: !hasLocalRows && (!localQueryReady || browserOnline),
    visibleItemCount,
  };
}
