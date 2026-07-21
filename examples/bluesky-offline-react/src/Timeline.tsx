import { useAll } from "jazz-tools/react";
import { useEffect, useRef, useState } from "react";
import { app } from "../schema.js";
import { decodeOperation } from "../shared/pending-operations.js";
import {
  buildTimeline,
  initialTimelineLimit,
  nextTimelineLimit,
  timelineQuery,
  timelineQueryLimit,
  windowTimelineRows,
  type DisplayPost,
  type TimelineEntryView,
} from "./model/timeline-data.js";
import { stableObjectId } from "./model/object-id.js";
import { useConnectivity } from "./hooks/use-connectivity.js";
import { useTimelineActions } from "./hooks/use-timeline-actions.js";
import { useTimelineProjection } from "./hooks/use-timeline-projection.js";
import {
  AppFooter,
  AppHeader,
  Composer,
  Intro,
  SyncBanner,
  TimelineFeed,
} from "./components/TimelineView.js";

export function Timeline({ did, onSignOut }: { did: string; onSignOut: () => void }) {
  const [text, setText] = useState("");
  const [loadingThreadUris, setLoadingThreadUris] = useState(new Set<string>());
  const [localTimelineLimit, setLocalTimelineLimit] = useState(initialTimelineLimit);
  const [includeThreadDetails, setIncludeThreadDetails] = useState(false);
  const { browserOnline, status: connectivity, reportApiReachable } = useConnectivity();
  const online = connectivity === "online";
  // Keep the feed mounted while an included Jazz query briefly recomputes.
  const lastTimelineRows = useRef<TimelineEntryView[]>([]);

  // This is the client-side seam: one reactive Jazz query supplies the entire view.
  const timelineRows = useAll(timelineQuery(did, includeThreadDetails)
    .limit(timelineQueryLimit(localTimelineLimit)));
  const pending = useAll(app.pendingOperations.where({ ownerDid: { eq: did } }));
  const ownProfileRows = useAll(app.profiles
    .where({ did: { eq: did } })
    .orderBy("indexedAt", "desc")
    .limit(1));
  const ownProfile = ownProfileRows?.[0];
  const ownHandle = ownProfile?.handle ?? ownProfile?.displayName ?? did;
  const availableTimelineRows = timelineRows?.length ? timelineRows : lastTimelineRows.current;
  const localTimelineWindow = windowTimelineRows(availableTimelineRows, localTimelineLimit);
  const visibleTimelineRows = localTimelineWindow.rows;
  const timelineItems = buildTimeline(visibleTimelineRows);
  const localQueryRefreshing = timelineRows === undefined && lastTimelineRows.current.length > 0;
  const {
    hasMore: hasMoreRemoteRows,
    loadingMore,
    initialLoading,
    loadMoreRef,
  } = useTimelineProjection({
    did,
    itemCount: timelineItems.length,
    hasLocalRows: visibleTimelineRows.length > 0,
    cachedRowsRemaining: localTimelineWindow.hasMore,
    localQueryRefreshing,
    localQueryReady: timelineRows !== undefined,
    browserOnline,
    revealCachedRows: () => setLocalTimelineLimit(nextTimelineLimit),
    reportApiReachable,
  });
  const hasMore = localTimelineWindow.hasMore || localQueryRefreshing || hasMoreRemoteRows;

  useEffect(() => {
    setLocalTimelineLimit(initialTimelineLimit);
    setIncludeThreadDetails(false);
  }, [did]);

  useEffect(() => {
    if (!includeThreadDetails && timelineRows?.length) {
      requestAnimationFrame(() => setIncludeThreadDetails(true));
    }
  }, [includeThreadDetails, timelineRows?.length]);

  const { flushOperations, publishPost, toggleReaction } = useTimelineActions(
    did,
    browserOnline,
    reportApiReachable,
  );
  const visiblePendingOperations = (pending ?? []).filter((operation) =>
    operation.state === "failed"
      || (operation.state === "queued" && (connectivity === "offline" || Boolean(operation.error))),
  );
  const [pendingObjectIds, setPendingObjectIds] = useState({
    posts: new Set<string>(),
    likes: new Set<string>(),
    reposts: new Set<string>(),
  });
  useEffect(() => {
    let stopped = false;
    const postUris = visiblePendingOperations
      .filter((operation) => operation.kind === "post")
      .map((operation) => `at://${operation.ownerDid}/app.bsky.feed.post/${operation.rkey}`);
    const reactionUris = (kind: "like" | "repost") => visiblePendingOperations.flatMap((operation) => {
      if (operation.kind !== kind) return [];
      try {
        const decoded = decodeOperation(operation);
        return decoded.kind === "post" ? [] : [decoded.payload.subjectUri];
      } catch {
        return [];
      }
    });
    Promise.all([
      Promise.all(postUris.map((uri) => stableObjectId("bluesky-post", uri))),
      Promise.all(reactionUris("like").map((uri) => stableObjectId("bluesky-post", uri))),
      Promise.all(reactionUris("repost").map((uri) => stableObjectId("bluesky-post", uri))),
    ]).then(([posts, likes, reposts]) => {
      if (!stopped) setPendingObjectIds({ posts: new Set(posts), likes: new Set(likes), reposts: new Set(reposts) });
    });
    return () => { stopped = true; };
  }, [online, pending]);
  const seenEntries = useRef(new Set(visibleTimelineRows.map((entry) => entry.id)));
  const newEntryPostIds = new Set(visibleTimelineRows
    .filter((entry) => !seenEntries.current.has(entry.id))
    .map((entry) => entry.postId));
  useEffect(() => {
    if (timelineRows?.length) lastTimelineRows.current = timelineRows;
    for (const entry of visibleTimelineRows) seenEntries.current.add(entry.id);
  }, [timelineRows]);

  async function publish() {
    const value = text.trim();
    if (!value) return;
    await publishPost(value);
    setText("");
  }

  async function publishReply(parent: DisplayPost, root: DisplayPost, value: string) {
    await publishPost(value, { parent, root });
  }

  async function loadThread(post: DisplayPost) {
    if (!browserOnline || loadingThreadUris.has(post.uri)) return;
    setLoadingThreadUris((current) => new Set(current).add(post.uri));
    try {
      const response = await fetch(`/api/thread?uri=${encodeURIComponent(post.uri)}`);
      if (!response.ok) throw new Error("Thread load failed");
      reportApiReachable(true);
    } catch {
      reportApiReachable(false);
    } finally {
      setLoadingThreadUris((current) => {
        const next = new Set(current);
        next.delete(post.uri);
        return next;
      });
    }
  }

  const waitingForTimeline = visibleTimelineRows.length === 0 && (timelineRows === undefined || initialLoading);
  return <main className="app-shell">
    <AppHeader profile={ownProfile} handle={ownHandle} onSignOut={onSignOut} />
    <Intro />
    <Composer text={text} onChange={setText} onPublish={publish} />
    <SyncBanner count={visiblePendingOperations.length} online={online} onSync={flushOperations} />
    <TimelineFeed
      items={timelineItems}
      waiting={waitingForTimeline}
      hasMore={hasMore}
      loadingMore={loadingMore}
      loadMoreRef={loadMoreRef}
      pendingLikePostIds={pendingObjectIds.likes}
      pendingRepostPostIds={pendingObjectIds.reposts}
      pendingPostIds={pendingObjectIds.posts}
      newEntryPostIds={newEntryPostIds}
      loadingThreadUris={loadingThreadUris}
      online={online}
      connectivity={connectivity}
      onToggleReaction={toggleReaction}
      onReply={publishReply}
      onLoadThread={loadThread}
    />
    <AppFooter onSignOut={onSignOut} />
  </main>;
}
