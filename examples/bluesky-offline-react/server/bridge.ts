import { createJazzContext } from "jazz-tools/backend";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import permissions from "../permissions.js";
import { app } from "../schema.js";
import type { OAuthSession } from "./auth.js";
import {
  deleteRecord,
  fetchPostThread,
  fetchProfile,
  fetchTimelineFeed,
  fetchViewerPosts,
  OperationError,
  putRecord,
  recordKey,
  type ThreadViewNode,
} from "./bluesky.js";
import {
  normalizePost,
  normalizeTimelineItem,
  stableObjectId,
  type FeedViewPost,
  type PostView,
} from "./timeline.js";

const jazzDbPath = process.env.JAZZ_DB ?? "./data/jazz.db";
mkdirSync(dirname(jazzDbPath), { recursive: true });
const context = createJazzContext({
  appId: process.env.JAZZ_APP_ID ?? "bluesky-offline-react-v2",
  app,
  permissions,
  driver: { type: "persistent", dataPath: jazzDbPath },
  serverUrl: process.env.JAZZ_SERVER_URL,
  adminSecret: process.env.JAZZ_ADMIN_SECRET,
  env: "dev",
  userBranch: "main",
});
const db = context.db();

type AnyTable = {
  where: (filter: unknown) => unknown;
};

export type QueuedOperation = {
  ownerDid: string;
  kind: string;
  target: string;
  rkey: string;
  payload: string;
};

async function upsertChanged(table: AnyTable, id: string, data: Record<string, unknown>) {
  const query = table.where({ id: { eq: id } });
  const existing = await db.one(query as never) as Record<string, unknown> | null;
  const unchanged = existing && Object.entries(data).every(([key, value]) =>
    (existing[key] ?? undefined) === (value ?? undefined));
  if (!unchanged) db.upsert(table as never, data as never, { id });
  return existing;
}

async function writeProfile(profile: {
  id: string;
  did: string;
  handle?: string;
  displayName?: string;
  description?: string;
  avatar?: string;
  indexedAt: string;
}) {
  const existing = await db.one(app.profiles.where({ id: { eq: profile.id } }));
  const data = {
    did: profile.did,
    handle: profile.handle ?? existing?.handle,
    displayName: profile.displayName ?? existing?.displayName,
    description: profile.description ?? existing?.description,
    avatar: profile.avatar ?? existing?.avatar,
    indexedAt: profile.indexedAt,
  };
  const profileChanged = !existing
    || existing.did !== data.did
    || existing.handle !== (data.handle ?? null)
    || existing.displayName !== (data.displayName ?? null)
    || existing.description !== (data.description ?? null)
    || existing.avatar !== (data.avatar ?? null);
  if (profileChanged) db.upsert(app.profiles, data, { id: profile.id });
}

async function writePostBundle(bundle: NonNullable<ReturnType<typeof normalizePost>>) {
  if (bundle.profile) await writeProfile(bundle.profile);
  const { id, ...post } = bundle.post;
  await upsertChanged(app.posts as unknown as AnyTable, id, post);
  const expectedImages = new Set(bundle.images.map((image) => image.id));
  const existingImages = await db.all(app.postImages.where({ postId: { eq: id } }));
  for (const image of bundle.images) {
    const { id: imageId, ...data } = image;
    await upsertChanged(app.postImages as unknown as AnyTable, imageId, data);
  }
  for (const image of existingImages) {
    if (!expectedImages.has(image.id)) db.delete(app.postImages, image.id);
  }
}

async function writeLike(data: {
  id: string;
  uri: string;
  actorDid: string;
  subjectPostId: string;
  createdAt: string;
  active: boolean;
}) {
  const { id, ...row } = data;
  await upsertChanged(app.likes as unknown as AnyTable, id, row);
}

async function writeRepost(data: {
  id: string;
  uri?: string;
  cid?: string;
  actorDid: string;
  actorProfileId: string;
  subjectPostId: string;
  createdAt: string;
  active: boolean;
}) {
  const { id, ...row } = data;
  await upsertChanged(app.reposts as unknown as AnyTable, id, row);
}

function operationSubjectUri(operation: { kind: string; payload: string }) {
  if (operation.kind !== "like" && operation.kind !== "repost") return undefined;
  try {
    return (JSON.parse(operation.payload) as { subjectUri?: string }).subjectUri;
  } catch {
    return undefined;
  }
}

async function pendingReactionKeys(ownerDid: string) {
  const pending = await db.all(app.pendingOperations.where({ ownerDid: { eq: ownerDid }, state: { eq: "queued" } }));
  return new Set(pending.flatMap((operation) => {
    const subjectUri = operationSubjectUri(operation);
    return subjectUri ? [`${operation.kind}:${stableObjectId("bluesky-post", subjectUri)}`] : [];
  }));
}

const recentlyMutatedReactions = new Map<string, number>();

async function writeViewerState(
  ownerDid: string,
  post: NonNullable<ReturnType<typeof normalizePost>>["post"],
  viewer: PostView["viewer"],
  pending: Set<string>,
) {
  const now = Date.now();
  for (const kind of ["like", "repost"] as const) {
    const key = `${kind}:${post.id}`;
    if (pending.has(key) || (recentlyMutatedReactions.get(`${ownerDid}:${key}`) ?? 0) > now) continue;
    const id = stableObjectId(`bluesky-${kind}`, `${ownerDid}:${post.uri}`);
    const uri = viewer?.[kind];
    if (kind === "like") {
      const existing = await db.one(app.likes.where({ id: { eq: id } }));
      if (uri) {
        await writeLike({ id, uri, actorDid: ownerDid, subjectPostId: post.id, createdAt: existing?.createdAt ?? post.indexedAt, active: true });
      } else if (existing?.active) {
        db.update(app.likes, id, { active: false });
      }
    } else {
      const existing = await db.one(app.reposts.where({ id: { eq: id } }));
      if (uri) {
        await writeRepost({
          id,
          uri,
          cid: existing?.cid ?? undefined,
          actorDid: ownerDid,
          actorProfileId: stableObjectId("bluesky-profile", ownerDid),
          subjectPostId: post.id,
          createdAt: existing?.createdAt ?? post.indexedAt,
          active: true,
        });
      } else if (existing?.active) {
        db.update(app.reposts, id, { active: false });
      }
    }
  }
}

async function writeThreadEntry(rootPostId: string, bundle: NonNullable<ReturnType<typeof normalizePost>>, sortOrder: number) {
  const id = stableObjectId("thread-entry", `${rootPostId}:${bundle.post.id}`);
  await upsertChanged(app.threadEntries as unknown as AnyTable, id, {
    rootPostId,
    postId: bundle.post.id,
    parentPostId: bundle.post.replyParentId,
    sortOrder,
    state: "post",
    indexedAt: bundle.post.indexedAt,
  });
}

async function writeTimelineItem(ownerDid: string, item: FeedViewPost, pending: Set<string>) {
  const normalized = normalizeTimelineItem(ownerDid, item);
  if (!normalized) return undefined;
  const bundles = new Map(normalized.context.map((bundle) => [bundle.post.id, bundle]));
  bundles.set(normalized.post.id, {
    profile: normalized.profiles.find((profile) => profile.did === normalized.post.authorDid),
    post: normalized.post,
    images: normalized.images,
  });
  for (const bundle of bundles.values()) await writePostBundle(bundle);
  for (const profile of normalized.profiles) await writeProfile(profile);
  for (const like of normalized.likes) await writeLike(like);
  for (const repost of normalized.reposts) await writeRepost(repost);
  await writeViewerState(ownerDid, normalized.post, item.post?.viewer, pending);
  const { id, ...entry } = normalized.timelineEntry;
  await upsertChanged(app.timelineEntries as unknown as AnyTable, id, entry);
  const orderedBundles = [...bundles.values()].sort((a, b) => a.post.createdAt.localeCompare(b.post.createdAt));
  for (const [index, bundle] of orderedBundles.entries()) {
    await writeThreadEntry(normalized.timelineEntry.threadRootId, bundle, index);
  }
  return normalized.timelineEntry;
}

async function finishTimelinePage(
  ownerDid: string,
  items: FeedViewPost[],
  cursor: string | undefined,
  pending: Set<string>,
) {
  const entries = [];
  for (const item of items) {
    const entry = await writeTimelineItem(ownerDid, item, pending);
    if (entry) entries.push(entry);
  }
  if (!cursor && entries.length) {
    const returnedIds = new Set(entries.map((entry) => entry.id));
    const boundary = entries.reduce((oldest, entry) => entry.sortAt < oldest ? entry.sortAt : oldest, entries[0]!.sortAt);
    const queuedPosts = new Set((await db.all(app.pendingOperations.where({ ownerDid, kind: { eq: "post" }, state: { eq: "queued" } })))
      .map((operation) => stableObjectId("bluesky-post", `at://${ownerDid}/app.bsky.feed.post/${operation.rkey}`)));
    const active = await db.all(app.timelineEntries.where({ ownerDid, active: { eq: true } }));
    for (const entry of active) {
      if (entry.sortAt >= boundary && !returnedIds.has(entry.id) && !queuedPosts.has(entry.postId)) {
        db.update(app.timelineEntries, entry.id, { active: false });
      }
    }
  }
  const profile = await fetchProfile(ownerDid);
  if (profile?.did) {
    await writeProfile({
      id: stableObjectId("bluesky-profile", profile.did),
      did: profile.did,
      handle: profile.handle,
      displayName: profile.displayName,
      description: profile.description,
      avatar: profile.avatar,
      indexedAt: profile.indexedAt ?? new Date().toISOString(),
    });
  }
}

async function writeThread(ownerDid: string, requestedUri: string, thread: ThreadViewNode, pending: Set<string>) {
  const ancestors: ThreadViewNode[] = [];
  for (let node: ThreadViewNode | undefined = thread; node; node = node.parent) ancestors.unshift(node);
  const rootUri = thread.post?.record?.reply?.root?.uri ?? ancestors[0]?.post?.uri ?? requestedUri;
  const rootPostId = stableObjectId("bluesky-post", rootUri);
  let sortOrder = 0;
  let count = 0;
  const seen = new Set<string>();

  const addNode = async (node: ThreadViewNode, fallbackParentId?: string) => {
    const uri = node.post?.uri ?? node.uri;
    if (!uri || seen.has(uri)) return uri ? stableObjectId("bluesky-post", uri) : undefined;
    seen.add(uri);
    const postId = stableObjectId("bluesky-post", uri);
    const bundle = normalizePost(node.post);
    if (bundle) {
      await writePostBundle(bundle);
      await writeViewerState(ownerDid, bundle.post, node.post?.viewer, pending);
      count += 1;
    }
    await upsertChanged(app.threadEntries as unknown as AnyTable, stableObjectId("thread-entry", `${rootPostId}:${postId}`), {
      rootPostId,
      postId,
      parentPostId: bundle?.post.replyParentId ?? fallbackParentId,
      sortOrder: sortOrder++,
      state: bundle ? "post" : node.blocked || node.$type?.endsWith("#blockedPost") ? "blocked" : "not-found",
      indexedAt: bundle?.post.indexedAt ?? new Date().toISOString(),
    });
    return postId;
  };

  let parentId: string | undefined;
  for (const node of ancestors) parentId = await addNode(node, parentId) ?? parentId;
  const selectedId = stableObjectId("bluesky-post", thread.post?.uri ?? requestedUri);
  const addReplies = async (nodes: ThreadViewNode[] | undefined, replyParentId: string) => {
    for (const node of nodes ?? []) {
      const postId = await addNode(node, replyParentId);
      if (postId) await addReplies(node.replies, postId);
    }
  };
  await addReplies(thread.replies, selectedId);
  return { rootPostId, count };
}

async function reconcilePostOperation(did: string, session: OAuthSession, operation: QueuedOperation) {
  if (operation.target !== "app.bsky.feed.post") throw new OperationError("invalid post collection", 400);
  const payload = JSON.parse(operation.payload) as { text?: string; createdAt?: string };
  if (typeof payload.text !== "string" || !payload.text.trim() || !payload.createdAt) {
    throw new OperationError("invalid post operation", 400);
  }
  const created = await putRecord(session, {
    repo: did,
    collection: "app.bsky.feed.post",
    rkey: operation.rkey,
    record: { $type: "app.bsky.feed.post", text: payload.text, createdAt: payload.createdAt },
  });
  const bundle = normalizePost({
    uri: created.uri,
    cid: created.cid,
    author: { did },
    record: { text: payload.text, createdAt: payload.createdAt },
    indexedAt: payload.createdAt,
  });
  if (bundle) await writePostBundle(bundle);
}

async function reconcileReactionOperation(
  did: string,
  session: OAuthSession,
  operation: QueuedOperation,
  kind: "like" | "repost",
) {
  const collection = `app.bsky.feed.${kind}`;
  if (operation.target !== collection) throw new OperationError(`invalid ${kind} collection`, 400);
  const payload = JSON.parse(operation.payload) as {
    subjectUri?: string;
    subjectCid?: string;
    active?: boolean;
    createdAt?: string;
  };
  if (!payload.subjectUri?.startsWith("at://") || !payload.subjectCid || typeof payload.active !== "boolean" || !payload.createdAt) {
    throw new OperationError(`invalid ${kind} operation`, 400);
  }
  const [post] = await fetchViewerPosts(session, [payload.subjectUri]);
  if (!post?.uri || !post.cid) throw new OperationError("subject post is unavailable", 502);
  const postId = stableObjectId("bluesky-post", post.uri);
  const viewerUri = post.viewer?.[kind];
  const id = stableObjectId(`bluesky-${kind}`, `${did}:${post.uri}`);
  const existing = kind === "like"
    ? await db.one(app.likes.where({ id: { eq: id } }))
    : await db.one(app.reposts.where({ id: { eq: id } }));
  let uri = viewerUri ?? existing?.uri ?? undefined;
  let cid = kind === "repost" && existing && "cid" in existing ? existing.cid ?? undefined : undefined;
  const wasActive = Boolean(viewerUri);
  if (payload.active && !wasActive) {
    const created = await putRecord(session, {
      repo: did,
      collection,
      rkey: operation.rkey,
      record: {
        $type: collection,
        subject: { uri: post.uri, cid: post.cid },
        createdAt: payload.createdAt,
      },
    });
    uri = created.uri;
    cid = created.cid;
  } else if (!payload.active) {
    const rkey = recordKey(uri, did, collection);
    if (rkey) {
      try {
        await deleteRecord(session, { repo: did, collection, rkey });
      } catch (error) {
        if (!(error instanceof OperationError) || !error.message.includes("RecordNotFound")) throw error;
      }
    }
  }
  const bundle = normalizePost({
    ...post,
    likeCount: kind === "like" ? Math.max(0, (post.likeCount ?? 0) + Number(payload.active) - Number(wasActive)) : post.likeCount,
    repostCount: kind === "repost" ? Math.max(0, (post.repostCount ?? 0) + Number(payload.active) - Number(wasActive)) : post.repostCount,
  });
  if (bundle) await writePostBundle(bundle);
  if (kind === "like") {
    await writeLike({
      id,
      uri: uri ?? `at://${did}/${collection}/${operation.rkey}`,
      actorDid: did,
      subjectPostId: postId,
      createdAt: payload.createdAt,
      active: payload.active,
    });
  } else {
    await writeRepost({
      id,
      uri,
      cid,
      actorDid: did,
      actorProfileId: stableObjectId("bluesky-profile", did),
      subjectPostId: postId,
      createdAt: payload.createdAt,
      active: payload.active,
    });
    if (!payload.active) {
      const entries = await db.all(app.timelineEntries.where({ ownerDid: { eq: did }, repostId: { eq: id }, active: { eq: true } }));
      for (const entry of entries) db.update(app.timelineEntries, entry.id, { active: false });
    }
  }
  recentlyMutatedReactions.set(`${did}:${kind}:${postId}`, Date.now() + 30_000);
}

export async function projectTimelinePage(did: string, session: OAuthSession, cursor?: string) {
  const timeline = await fetchTimelineFeed(session, cursor);
  const pending = await pendingReactionKeys(did);
  void finishTimelinePage(did, timeline.feed ?? [], cursor, pending)
    .catch((error) => console.error("Timeline projection failed", error));
  return {
    cursor: timeline.cursor,
    hasMore: Boolean(timeline.cursor),
    count: timeline.feed?.length ?? 0,
  };
}

export async function projectThread(did: string, session: OAuthSession, uri: string) {
  const thread = await fetchPostThread(session, uri);
  if (!thread) throw new Error("thread fetch failed");
  return { ok: true, ...await writeThread(did, uri, thread, await pendingReactionKeys(did)) };
}

export async function reconcileOperations(did: string, session: OAuthSession, operations: QueuedOperation[]) {
  for (const operation of operations) {
    if (operation.kind === "post") await reconcilePostOperation(did, session, operation);
    else if (operation.kind === "like" || operation.kind === "repost") {
      await reconcileReactionOperation(did, session, operation, operation.kind);
    } else {
      throw new OperationError("unsupported operation", 400);
    }
  }
}

export { OperationError } from "./bluesky.js";
