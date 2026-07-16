import { TID } from "@atproto/common-web";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { useEffect, useMemo, useRef, useState } from "react";
import { app } from "../schema.js";
import { nextReactionIntent } from "./reactions.js";
import {
  buildTimeline,
  type DisplayPost,
  type PendingOperationView,
  type ProfileView,
  type TimelineEntryView,
} from "./timeline-model.js";
import {
  AppFooter,
  AppHeader,
  Composer,
  Intro,
  SyncBanner,
  TimelineFeed,
} from "./TimelineView.js";

const pollInterval = 15_000;
type TimelinePayload = { cursor?: string; hasMore?: boolean; count?: number };
const stableObjectIds = new Map<string, Promise<string>>();

function stableObjectId(namespace: string, value: string) {
  const key = `${namespace}:${value}`;
  let id = stableObjectIds.get(key);
  if (!id) {
    id = crypto.subtle.digest("SHA-256", new TextEncoder().encode(key)).then((digest) => {
      const bytes = new Uint8Array(digest).slice(0, 16);
      bytes[6] = (bytes[6] & 0x0f) | 0x50;
      bytes[8] = (bytes[8] & 0x3f) | 0x80;
      const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    });
    stableObjectIds.set(key, id);
  }
  return id;
}

function recordKey(uri: string | null | undefined, kind: "like" | "repost") {
  return uri?.match(new RegExp(`^at://[^/]+/app\\.bsky\\.feed\\.${kind}/([^/]+)$`))?.[1];
}

export function Timeline({ did, onSignOut }: { did: string; onSignOut: () => void }) {
  const db = useDb();
  const jazzSession = useSession();
  const [text, setText] = useState("");
  const [online, setOnline] = useState(navigator.onLine);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [initialTimelineLoading, setInitialTimelineLoading] = useState(true);
  const [loadingThreadUris, setLoadingThreadUris] = useState(new Set<string>());
  const requestInFlight = useRef(false);
  const initialPageLoaded = useRef(false);
  const loadMoreRef = useRef<HTMLDivElement>(null);

  // This is the client-side seam: one reactive Jazz query supplies the entire view.
  const postQuery = app.posts.include({
    authorProfile: true,
    postImagesViaPost: true,
    likesViaSubjectPost: app.likes.where({ actorDid: { eq: did } }),
    repostsViaSubjectPost: app.reposts.where({ actorDid: { eq: did } }),
  });
  const timelineRows = useAll(app.timelineEntries
    .where({ ownerDid: { eq: did }, active: { eq: true } })
    .orderBy("sortAt", "desc")
    .include({
      post: postQuery,
      repost: app.reposts.include({ actorProfile: true }),
      threadRoot: app.posts.include({
        authorProfile: true,
        postImagesViaPost: true,
        threadEntriesViaRootPost: app.threadEntries.orderBy("sortOrder", "asc").include({ post: postQuery }),
      }),
    })) as TimelineEntryView[] | undefined;
  const pending = useAll(app.pendingOperations.where({ ownerDid: { eq: did } })) as PendingOperationView[] | undefined;
  const ownProfileRows = useAll(app.profiles.where({ did: { eq: did } }).limit(1));
  const ownProfile = ownProfileRows?.[0] as ProfileView | undefined;
  const ownHandle = ownProfile?.handle ?? ownProfile?.displayName ?? did;
  const timelineItems = useMemo(() => buildTimeline(timelineRows ?? []), [timelineRows]);
  const visiblePendingOperations = useMemo(() => (pending ?? []).filter((operation) =>
    operation.state === "failed" || (operation.state === "queued" && (!online || Boolean(operation.error))),
  ), [online, pending]);
  const pendingPostIds = useMemo(() => new Set(visiblePendingOperations
    .filter((operation) => operation.kind === "post")
    .map((operation) => `at://${operation.ownerDid}/app.bsky.feed.post/${operation.rkey}`)), [visiblePendingOperations]);
  const pendingLikePostIds = useMemo(() => new Set(visiblePendingOperations.flatMap((operation) => {
    if (operation.kind !== "like") return [];
    try {
      const uri = (JSON.parse(operation.payload) as { subjectUri?: string }).subjectUri;
      return uri ? [uri] : [];
    } catch {
      return [];
    }
  })), [visiblePendingOperations]);
  const pendingRepostPostIds = useMemo(() => new Set(visiblePendingOperations.flatMap((operation) => {
    if (operation.kind !== "repost") return [];
    try {
      const uri = (JSON.parse(operation.payload) as { subjectUri?: string }).subjectUri;
      return uri ? [uri] : [];
    } catch {
      return [];
    }
  })), [visiblePendingOperations]);
  const [pendingObjectIds, setPendingObjectIds] = useState({
    posts: new Set<string>(),
    likes: new Set<string>(),
    reposts: new Set<string>(),
  });
  useEffect(() => {
    let stopped = false;
    void Promise.all([
      Promise.all([...pendingPostIds].map((uri) => stableObjectId("bluesky-post", uri))),
      Promise.all([...pendingLikePostIds].map((uri) => stableObjectId("bluesky-post", uri))),
      Promise.all([...pendingRepostPostIds].map((uri) => stableObjectId("bluesky-post", uri))),
    ]).then(([posts, likes, reposts]) => {
      if (!stopped) setPendingObjectIds({ posts: new Set(posts), likes: new Set(likes), reposts: new Set(reposts) });
    });
    return () => { stopped = true; };
  }, [pendingLikePostIds, pendingPostIds, pendingRepostPostIds]);
  const seenEntries = useRef(new Set((timelineRows ?? []).map((entry) => entry.id)));
  const newEntryPostIds = useMemo(() => new Set((timelineRows ?? [])
    .filter((entry) => !seenEntries.current.has(entry.id))
    .map((entry) => entry.postId)), [timelineRows]);
  useEffect(() => {
    for (const entry of timelineRows ?? []) seenEntries.current.add(entry.id);
  }, [timelineRows]);

  useEffect(() => {
    const update = () => setOnline(navigator.onLine);
    addEventListener("online", update);
    addEventListener("offline", update);
    return () => {
      removeEventListener("online", update);
      removeEventListener("offline", update);
    };
  }, []);

  async function loadTimelinePage(cursor: string | null, polling = false) {
    if (requestInFlight.current || !navigator.onLine) return;
    requestInFlight.current = true;
    if (cursor) setLoadingMore(true);
    let projectedCount = 0;
    try {
      const response = await fetch(cursor ? `/api/timeline?cursor=${encodeURIComponent(cursor)}` : "/api/timeline");
      if (!response.ok) throw new Error("Timeline refresh failed");
      const result = await response.json() as TimelinePayload;
      projectedCount = result.count ?? 0;
      setOnline(true);
      if (cursor || !initialPageLoaded.current) {
        setNextCursor(result.cursor ?? null);
        setHasMore(Boolean(result.hasMore));
      }
      initialPageLoaded.current = true;
    } catch {
      setOnline(false);
    } finally {
      requestInFlight.current = false;
      if (cursor) setLoadingMore(false);
      if (!polling && projectedCount === 0) setInitialTimelineLoading(false);
    }
  }

  useEffect(() => {
    if (timelineRows?.length) setInitialTimelineLoading(false);
  }, [timelineRows?.length]);

  useEffect(() => {
    void loadTimelinePage(null);
    const timer = window.setInterval(() => void loadTimelinePage(null, true), pollInterval);
    return () => window.clearInterval(timer);
  }, [did]);

  useEffect(() => {
    const sentinel = loadMoreRef.current;
    if (!sentinel) return;
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting) && timelineItems.length > 0 && nextCursor && hasMore && !loadingMore) {
        void loadTimelinePage(nextCursor);
      }
    }, { rootMargin: "500px" });
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [hasMore, loadingMore, nextCursor, timelineItems.length]);

  async function flushOperations(ownerDid: string) {
    const operations = await db.all(app.pendingOperations.where({ ownerDid: { eq: ownerDid }, state: { eq: "queued" } }));
    if (!operations.length || !navigator.onLine) return;
    try {
      const response = await fetch("/api/operations", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(operations),
      });
      const result = await response.json().catch(() => ({ error: "Sync failed" })) as { error?: string };
      if (!response.ok) {
        const permanent = response.status === 400 || response.status === 401 || response.status === 403;
        for (const operation of operations) {
          db.update(app.pendingOperations, operation.id, {
            state: permanent ? "failed" : "queued",
            error: result.error ?? "Sync failed",
          });
        }
        if (!permanent) setOnline(false);
        return;
      }
      setOnline(true);
      for (const operation of operations) {
        db.update(app.pendingOperations, operation.id, { state: "sent", error: "" });
        if (operation.kind === "post") {
          const postId = await stableObjectId("bluesky-post", `at://${ownerDid}/app.bsky.feed.post/${operation.rkey}`);
          const post = await db.one(app.posts.where({ id: { eq: postId } }));
          if (post) db.update(app.posts, post.id, { state: "synced" });
        }
      }
    } catch {
      for (const operation of operations) db.update(app.pendingOperations, operation.id, { error: "Sync failed" });
      setOnline(false);
    }
  }

  useEffect(() => {
    if (!online) return;
    void flushOperations(did);
    const timer = window.setInterval(() => void flushOperations(did), pollInterval);
    return () => window.clearInterval(timer);
  }, [did, online]);

  async function publish() {
    const value = text.trim();
    if (!value || !jazzSession?.user_id) return;
    const rkey = TID.nextStr();
    const now = new Date().toISOString();
    const uri = `at://${did}/app.bsky.feed.post/${rkey}`;
    const [postId, profileId, entryId, operationId] = await Promise.all([
      stableObjectId("bluesky-post", uri),
      stableObjectId("bluesky-profile", did),
      stableObjectId("timeline-entry", `${did}:${uri}`),
      stableObjectId("post-operation", `${did}:${uri}`),
    ]);
    db.upsert(app.posts, {
      uri,
      authorDid: did,
      authorProfileId: profileId,
      text: value,
      createdAt: now,
      createdAtMs: Math.floor(Date.parse(now) / 1_000),
      indexedAt: now,
      replyCount: 0,
      likeCount: 0,
      repostCount: 0,
      state: "pending",
    }, { id: postId });
    db.upsert(app.pendingOperations, {
      operationId,
      ownerDid: did,
      kind: "post",
      target: "app.bsky.feed.post",
      rkey,
      payload: JSON.stringify({ text: value, createdAt: now }),
      state: "queued",
      createdAt: now,
    }, { id: operationId });
    db.upsert(app.timelineEntries, {
      ownerDid: did,
      postId,
      threadRootId: postId,
      sortAt: now,
      active: true,
    }, { id: entryId });
    setText("");
    void flushOperations(did);
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
    const queuedPayload = queued ? JSON.parse(queued.payload) as { syncedActive?: boolean } : undefined;
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
    db.upsert(app.pendingOperations, {
      operationId,
      ownerDid: did,
      kind,
      target: `app.bsky.feed.${kind}`,
      rkey,
      payload: JSON.stringify({ subjectUri: post.uri, subjectCid: post.cid, active, syncedActive, createdAt: now }),
      state: "queued",
      createdAt: now,
    }, { id: operationId });
    void flushOperations(did);
  }

  async function loadThread(post: DisplayPost) {
    if (!navigator.onLine || loadingThreadUris.has(post.uri)) return;
    setLoadingThreadUris((current) => new Set(current).add(post.uri));
    try {
      const response = await fetch(`/api/thread?uri=${encodeURIComponent(post.uri)}`);
      if (!response.ok) throw new Error("Thread load failed");
      setOnline(true);
    } catch {
      setOnline(false);
    } finally {
      setLoadingThreadUris((current) => {
        const next = new Set(current);
        next.delete(post.uri);
        return next;
      });
    }
  }

  const waitingForTimeline = timelineRows === undefined || (!timelineRows.length && initialTimelineLoading);
  return <main className="app-shell">
    <AppHeader online={online} profile={ownProfile} handle={ownHandle} onSignOut={onSignOut} />
    <Intro />
    <Composer text={text} onChange={setText} onPublish={() => void publish()} />
    <SyncBanner count={visiblePendingOperations.length} online={online} onSync={() => void flushOperations(did)} />
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
      onToggleReaction={(kind, post) => void toggleReaction(kind, post)}
      onLoadThread={(post) => void loadThread(post)}
    />
    <AppFooter onSignOut={onSignOut} />
  </main>;
}
