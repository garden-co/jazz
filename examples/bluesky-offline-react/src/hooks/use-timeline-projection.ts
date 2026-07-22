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
  localStartCount,
  itemCount,
  remote,
}: {
  localStartCount: number | null;
  itemCount: number;
  remote: boolean;
}) {
  return remote || (localStartCount !== null && itemCount <= localStartCount);
}

export function nextInfiniteScrollState({
  armed,
  intersecting,
  canLoad,
}: {
  armed: boolean;
  intersecting: boolean;
  canLoad: boolean;
}) {
  if (!intersecting) return { armed: true, trigger: false };
  if (!armed || !canLoad) return { armed, trigger: false };
  return { armed: false, trigger: true };
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
  const [localStartCount, setLocalStartCount] = useState<number | null>(null);
  const [localFetchStartedAt, setLocalFetchStartedAt] = useState<number | null>(null);
  const [initialLoading, setInitialLoading] = useState(true);
  const refreshInFlight = useRef(false);
  const paginationInFlight = useRef(false);
  const paginationStarted = useRef(false);
  const loadMoreArmed = useRef(true);
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
        if (cursor) setRemoteLoadingMore(false);
      }
    }
  }

  useEffect(() => {
    requestGeneration.current += 1;
    refreshInFlight.current = false;
    paginationInFlight.current = false;
    paginationStarted.current = false;
    loadMoreArmed.current = true;
    setNextCursor(null);
    setHasMore(false);
    setRemoteLoadingMore(false);
    setLocalStartCount(null);
    setLocalFetchStartedAt(null);
    setInitialLoading(true);
  }, [did]);

  useEffect(() => {
    if (hasLocalRows || (localQueryReady && !browserOnline)) setInitialLoading(false);
  }, [browserOnline, hasLocalRows, localQueryReady]);

  useEffect(() => {
    if (localStartCount === null || localFetchStartedAt === null || itemCount <= localStartCount)
      return;
    const remaining = Math.max(0, minimumSpinnerDuration - (Date.now() - localFetchStartedAt));
    const timer = window.setTimeout(() => {
      setLocalStartCount(null);
      setLocalFetchStartedAt(null);
    }, remaining);
    return () => window.clearTimeout(timer);
  }, [itemCount, localFetchStartedAt, localStartCount]);

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
          localStartCount,
          itemCount,
          remote: remoteLoadingMore,
        });
        const nextState = nextInfiniteScrollState({
          armed: loadMoreArmed.current,
          intersecting,
          canLoad: itemCount > 0 && source !== undefined && !loadingMore,
        });
        loadMoreArmed.current = nextState.armed;
        if (!nextState.trigger) return;
        if (source === "local") {
          setLocalStartCount(itemCount);
          setLocalFetchStartedAt(Date.now());
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
    localStartCount,
    localQueryRefreshing,
    nextCursor,
    remoteLoadingMore,
    revealCachedRows,
  ]);

  const loadingMore = fetchingMorePosts({
    localStartCount,
    itemCount,
    remote: remoteLoadingMore,
  });
  return { hasMore, loadingMore, initialLoading, loadMoreRef };
}
