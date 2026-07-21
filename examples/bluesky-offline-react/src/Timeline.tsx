import { TID } from "@atproto/common-web";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { useEffect, useRef, useState } from "react";
import { app } from "../shared/schema.js";
import { parseAtRecordUri } from "../shared/identifiers.js";
import { decodeOperation, encodeOperationPayload, type Operation } from "../shared/operations.js";
import {
  initialTimelineLimit,
  nextTimelineLimit,
  timelineQueryLimit,
  windowTimelineRows,
} from "./local-timeline-window.js";
import { stableObjectId } from "./object-id.js";
import { nextReactionIntent } from "./reactions.js";
import {
  buildTimeline,
  optimisticReplyCount,
  type DisplayPost,
  type TimelineEntryView,
} from "./timeline-model.js";
import { timelineQuery } from "./timeline-query.js";
import { useConnectivity } from "./use-connectivity.js";
import { useOutbox } from "./use-outbox.js";
import { useTimelineHydration } from "./use-timeline-hydration.js";
import {
  AppFooter,
  AppHeader,
  Composer,
  Intro,
  SyncBanner,
  TimelineFeed,
} from "./TimelineView.js";

function recordKey(uri: string | null | undefined, kind: "like" | "repost") {
  const parsed = parseAtRecordUri(uri);
  return parsed?.collection === `app.bsky.feed.${kind}` ? parsed.rkey : undefined;
}

export function Timeline({ did, onSignOut }: { did: string; onSignOut: () => void }) {
  const db = useDb();
  const jazzSession = useSession();
  const [text, setText] = useState("");
  const [loadingThreadUris, setLoadingThreadUris] = useState(new Set<string>());
  const [localTimelineLimit, setLocalTimelineLimit] = useState(initialTimelineLimit);
  const [includeThreadDetails, setIncludeThreadDetails] = useState(false);
  const { browserOnline, online, reportApiReachable } = useConnectivity();
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
  } = useTimelineHydration({
    did,
    // The BFF cursor is only used after every currently cached Jazz root is visible.
    itemCount: localTimelineWindow.hasMore || localQueryRefreshing ? 0 : timelineItems.length,
    hasLocalRows: visibleTimelineRows.length > 0,
    localQueryReady: timelineRows !== undefined,
    browserOnline,
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

  useEffect(() => {
    const sentinel = loadMoreRef.current;
    if (!sentinel || !localTimelineWindow.hasMore || timelineItems.length === 0) return;
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        setLocalTimelineLimit(nextTimelineLimit);
      }
    }, { rootMargin: "500px" });
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [localTimelineWindow.hasMore, timelineItems.length]);
  const flushOperations = useOutbox(did, browserOnline, reportApiReachable);
  const visiblePendingOperations = (pending ?? []).filter((operation) =>
    operation.state === "failed" || (operation.state === "queued" && (!online || Boolean(operation.error))),
  );
  const [pendingObjectIds, setPendingObjectIds] = useState({
    posts: new Set<string>(),
    likes: new Set<string>(),
    reposts: new Set<string>(),
  });
  useEffect(() => {
    let stopped = false;
    const visible = (pending ?? []).filter((operation) =>
      operation.state === "failed" || (operation.state === "queued" && (!online || Boolean(operation.error))),
    );
    const postUris = visible
      .filter((operation) => operation.kind === "post")
      .map((operation) => `at://${operation.ownerDid}/app.bsky.feed.post/${operation.rkey}`);
    const reactionUris = (kind: "like" | "repost") => visible.flatMap((operation) => {
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

  async function publishPost(value: string, replyTo?: { parent: DisplayPost; root: DisplayPost }) {
    if (!value || !jazzSession?.user_id) return;
    const parentCid = replyTo?.parent.cid;
    const rootCid = replyTo?.root.cid;
    const reply = replyTo && parentCid && rootCid ? {
      root: { uri: replyTo.root.uri, cid: rootCid },
      parent: { uri: replyTo.parent.uri, cid: parentCid },
    } : undefined;
    if (replyTo && !reply) return;
    const rkey = TID.nextStr();
    const now = new Date().toISOString();
    const uri = `at://${did}/app.bsky.feed.post/${rkey}`;
    const [postId, profileId, entryId, operationId, threadEntryId] = await Promise.all([
      stableObjectId("bluesky-post", uri),
      stableObjectId("bluesky-profile", did),
      stableObjectId("timeline-entry", `${did}:${uri}`),
      stableObjectId("post-operation", `${did}:${uri}`),
      replyTo ? stableObjectId("thread-entry", `${replyTo.root.id}:${uri}`) : Promise.resolve(undefined),
    ]);
    db.upsert(app.posts, {
      uri,
      authorDid: did,
      authorProfileId: profileId,
      text: value,
      createdAt: now,
      indexedAt: now,
      ...(replyTo ? { replyParentId: replyTo.parent.id, replyRootId: replyTo.root.id } : {}),
      replyCount: 0,
      likeCount: 0,
      repostCount: 0,
      state: "pending",
    }, { id: postId });
    const operation: Operation = {
      id: operationId,
      ownerDid: did,
      kind: "post",
      rkey,
      payload: {
        text: value,
        createdAt: now,
        ...(reply ? { reply } : {}),
      },
      state: "queued",
      createdAt: now,
    };
    db.upsert(app.pendingOperations, {
      ...operation,
      payload: encodeOperationPayload(operation),
    }, { id: operationId });
    db.upsert(app.timelineEntries, {
      ownerDid: did,
      postId,
      threadRootId: replyTo?.root.id ?? postId,
      sortAt: replyTo?.root.indexedAt ?? now,
      active: true,
    }, { id: entryId });
    if (replyTo && threadEntryId) {
      db.upsert(app.threadEntries, {
        rootPostId: replyTo.root.id,
        postId,
        parentPostId: replyTo.parent.id,
        sortOrder: 0,
        state: "post",
        indexedAt: now,
      }, { id: threadEntryId });
      const replyCount = optimisticReplyCount(replyTo.parent, did);
      if (replyCount !== undefined) db.update(app.posts, replyTo.parent.id, { replyCount });
    }
    flushOperations();
  }

  async function publish() {
    const value = text.trim();
    if (!value) return;
    await publishPost(value);
    setText("");
  }

  async function publishReply(parent: DisplayPost, root: DisplayPost, value: string) {
    await publishPost(value, { parent, root });
  }

  async function toggleReaction(kind: "like" | "repost", post: DisplayPost) {
    if (!post.cid || !jazzSession?.user_id) return;
    const [reactionId, operationId, actorProfileId] = await Promise.all([
      stableObjectId(`bluesky-${kind}`, `${did}:${post.uri}`),
      stableObjectId(`${kind}-operation`, `${did}:${post.uri}`),
      stableObjectId("bluesky-profile", did),
    ]);
    const allOperations = await db.all(app.pendingOperations.where({ ownerDid: { eq: did } }));
    const queued = allOperations.find((operation) => operation.id === operationId && operation.state === "queued");
    const current = kind === "like" ? post.like : post.repost;
    const decodedQueued = queued ? decodeOperation(queued) : undefined;
    const queuedPayload = decodedQueued?.kind === "like" || decodedQueued?.kind === "repost"
      ? decodedQueued.payload
      : undefined;
    const intent = nextReactionIntent(current?.active ?? false, queuedPayload);
    const { active, syncedActive } = intent;
    if (queued && !intent.keepPending) {
      if (kind === "like") {
        const row = await db.one(app.likes.where({ id: { eq: reactionId } }));
        if (row) db.update(app.likes, row.id, { active });
      } else {
        const row = await db.one(app.reposts.where({ id: { eq: reactionId } }));
        if (row) db.update(app.reposts, row.id, { active });
      }
      db.delete(app.pendingOperations, queued.id);
      return;
    }
    const rkey = queued?.rkey ?? (active && !syncedActive ? TID.nextStr() : recordKey(current?.uri, kind) ?? TID.nextStr());
    const now = new Date().toISOString();
    const uri = current?.uri ?? `at://${did}/app.bsky.feed.${kind}/${rkey}`;
    if (kind === "like") {
      db.upsert(app.likes, { uri, actorDid: did, subjectPostId: post.id, createdAt: now, active }, { id: reactionId });
    } else {
      db.upsert(app.reposts, { uri, actorDid: did, actorProfileId, subjectPostId: post.id, createdAt: now, active }, { id: reactionId });
    }
    const operation: Operation = {
      id: operationId,
      ownerDid: did,
      kind,
      rkey,
      payload: { subjectUri: post.uri, subjectCid: post.cid, active, syncedActive, createdAt: now },
      state: "queued",
      createdAt: now,
    };
    db.upsert(app.pendingOperations, {
      ...operation,
      payload: encodeOperationPayload(operation),
    }, { id: operationId });
    flushOperations();
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
      onToggleReaction={toggleReaction}
      onReply={publishReply}
      onLoadThread={loadThread}
    />
    <AppFooter onSignOut={onSignOut} />
  </main>;
}
