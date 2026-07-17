import type { Operation, ReactionOperation } from "../operations.js";
import { decodeOperation, encodeOperationPayload } from "../operations.js";
import { app } from "../schema.js";
import type { TableProxy } from "jazz-tools/backend";
import type { ThreadViewNode } from "./bluesky.js";
import {
  normalizePost,
  normalizeTimelineItem,
  stableObjectId,
  type FeedViewPost,
  type PostView,
  type ProfileView,
} from "./timeline.js";
import { db } from "./jazz.js";
import type { FlatThread } from "./thread-normalizer.js";

type ProjectionDatabase = typeof db;
type PostBundle = NonNullable<ReturnType<typeof normalizePost>>;
type NormalizedPost = PostBundle["post"];
export type ReactionIntents = Map<string, ReactionOperation>;

function projectRow<TRow, TInit>(
  database: ProjectionDatabase,
  table: TableProxy<TRow, TInit>,
  id: string,
  data: Partial<TInit>,
) {
  try {
    database.upsert(table, data, { id });
  } catch (error) {
    if (!(error instanceof Error) || !error.message.includes("object already exists")) throw error;
    database.update(table, id, data);
  }
}

type ProfileProjection = {
  id: string;
  did: string;
  handle?: string;
  displayName?: string;
  description?: string;
  avatar?: string;
  indexedAt: string;
};

export function mergeProfileProjection(
  existing: {
    did: string;
    handle: string | null;
    displayName: string | null;
    description: string | null;
    avatar: string | null;
    indexedAt: string;
  } | null,
  incoming: ProfileProjection,
) {
  return {
    did: incoming.did,
    // Undefined enrichment fields deliberately leave an existing Jazz value untouched.
    handle: incoming.handle,
    displayName: incoming.displayName,
    description: incoming.description,
    avatar: incoming.avatar,
    indexedAt: existing && existing.indexedAt > incoming.indexedAt
      ? existing.indexedAt
      : incoming.indexedAt,
  };
}

function reactionKey(kind: ReactionOperation["kind"], postId: string) {
  return `${kind}:${postId}`;
}

export function createProjectionWriter(database: ProjectionDatabase = db) {
  async function writeProfile(profile: ProfileProjection) {
    const existing = await database.one(app.profiles.where({ id: { eq: profile.id } }));
    projectRow(database, app.profiles, profile.id, mergeProfileProjection(existing, profile));
  }

  async function writePostBundle(bundle: PostBundle) {
    if (bundle.quote) await writePostBundle(bundle.quote);
    if (bundle.profile) await writeProfile(bundle.profile);
    const { id, ...post } = bundle.post;
    projectRow(database, app.posts, id, { ...post, state: "synced" });

    const expectedImages = new Set(bundle.images.map((image) => image.id));
    const existingImages = await database.all(app.postImages.where({ postId: { eq: id } }));
    for (const image of bundle.images) {
      const { id: imageId, ...data } = image;
      projectRow(database, app.postImages, imageId, data);
    }
    for (const image of existingImages) {
      if (!expectedImages.has(image.id)) database.delete(app.postImages, image.id);
    }
  }

  function writeLike(data: {
    id: string;
    uri: string;
    actorDid: string;
    subjectPostId: string;
    createdAt: string;
    active: boolean;
  }) {
    const { id, ...row } = data;
    projectRow(database, app.likes, id, row);
  }

  function writeRepost(data: {
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
    projectRow(database, app.reposts, id, row);
  }

  async function loadReactionIntents(ownerDid: string): Promise<ReactionIntents> {
    const rows = await database.all(app.pendingOperations.where({ ownerDid: { eq: ownerDid } }));
    const operations = rows
      .map(decodeOperation)
      .filter((operation): operation is ReactionOperation =>
        operation.kind !== "post" && (operation.state === "queued" || operation.state === "sent"))
      .sort((left, right) => left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id));
    const intents: ReactionIntents = new Map();
    for (const operation of operations) {
      const postId = stableObjectId("bluesky-post", operation.payload.subjectUri);
      intents.set(reactionKey(operation.kind, postId), operation);
    }
    return intents;
  }

  async function projectViewerState(
    ownerDid: string,
    post: NormalizedPost,
    viewer: PostView["viewer"],
    intents: ReactionIntents,
  ) {
    for (const kind of ["like", "repost"] as const) {
      const intent = intents.get(reactionKey(kind, post.id));
      const viewerUri = viewer?.[kind];
      const viewerActive = Boolean(viewerUri);

      if (intent && viewerActive !== intent.payload.active) continue;

      const id = stableObjectId(`bluesky-${kind}`, `${ownerDid}:${post.uri}`);
      if (kind === "like") {
        const existing = await database.one(app.likes.where({ id: { eq: id } }));
        if (viewerUri) {
          writeLike({
            id,
            uri: viewerUri,
            actorDid: ownerDid,
            subjectPostId: post.id,
            createdAt: existing?.createdAt ?? post.indexedAt,
            active: true,
          });
        } else if (existing) {
          writeLike({ ...existing, active: false });
        }
      } else {
        const existing = await database.one(app.reposts.where({ id: { eq: id } }));
        if (viewerUri) {
          writeRepost({
            id,
            uri: viewerUri,
            cid: existing?.cid ?? undefined,
            actorDid: ownerDid,
            actorProfileId: stableObjectId("bluesky-profile", ownerDid),
            subjectPostId: post.id,
            createdAt: existing?.createdAt ?? post.indexedAt,
            active: true,
          });
        } else if (existing) {
          writeRepost({
            ...existing,
            uri: existing.uri ?? undefined,
            cid: existing.cid ?? undefined,
            active: false,
          });
        }
      }

      if (intent) {
        database.delete(app.pendingOperations, intent.id);
        intents.delete(reactionKey(kind, post.id));
      }
    }
  }

  function writeThreadEntry(rootPostId: string, bundle: PostBundle, sortOrder: number) {
    const id = stableObjectId("thread-entry", `${rootPostId}:${bundle.post.id}`);
    projectRow(database, app.threadEntries, id, {
      rootPostId,
      postId: bundle.post.id,
      parentPostId: bundle.post.replyParentId,
      sortOrder,
      state: "post",
      indexedAt: bundle.post.indexedAt,
    });
  }

  async function projectTimelineItem(ownerDid: string, item: FeedViewPost, intents: ReactionIntents) {
    const normalized = normalizeTimelineItem(ownerDid, item);
    if (!normalized) throw new Error(`Invalid timeline item: ${item.post?.uri ?? "missing post"}`);
    const bundles = new Map(normalized.context.map((bundle) => [bundle.post.id, bundle]));
    bundles.set(normalized.post.id, {
      profile: normalized.profiles.find((profile) => profile.did === normalized.post.authorDid),
      post: normalized.post,
      images: normalized.images,
      quote: normalized.quote,
    });

    const profiles = new Map(normalized.profiles.map((profile) => [profile.id, profile]));
    for (const bundle of bundles.values()) {
      if (bundle.profile) profiles.set(bundle.profile.id, bundle.profile);
    }
    await Promise.all([
      ...[...profiles.values()].map(writeProfile),
      ...[...bundles.values()].map((bundle) => writePostBundle({ ...bundle, profile: undefined })),
    ]);
    for (const like of normalized.likes) writeLike(like);
    for (const repost of normalized.reposts) writeRepost(repost);
    await projectViewerState(ownerDid, normalized.post, item.post?.viewer, intents);
    const { id, ...entry } = normalized.timelineEntry;
    projectRow(database, app.timelineEntries, id, entry);

    const orderedBundles = [...bundles.values()]
      .sort((left, right) => left.post.createdAt.localeCompare(right.post.createdAt));
    for (const [index, bundle] of orderedBundles.entries()) {
      writeThreadEntry(normalized.timelineEntry.threadRootId, bundle, index);
    }
    return normalized.timelineEntry;
  }

  async function projectTimelinePage(
    ownerDid: string,
    items: FeedViewPost[],
    cursor: string | undefined,
    intents: ReactionIntents,
  ) {
    const entries = await Promise.all(items.map((item) => projectTimelineItem(ownerDid, item, intents)));
    if (cursor || entries.length === 0) return;

    const returnedIds = new Set(entries.map((entry) => entry.id));
    const boundary = entries.reduce(
      (oldest, entry) => entry.sortAt < oldest ? entry.sortAt : oldest,
      entries[0]!.sortAt,
    );
    const operationRows = await database.all(app.pendingOperations.where({ ownerDid: { eq: ownerDid } }));
    const queuedPostIds = new Set(operationRows.map(decodeOperation)
      .filter((operation) => operation.kind === "post" && operation.state === "queued")
      .map((operation) => stableObjectId(
        "bluesky-post",
        `at://${ownerDid}/app.bsky.feed.post/${operation.rkey}`,
      )));
    const active = await database.all(app.timelineEntries.where({ ownerDid: { eq: ownerDid }, active: { eq: true } }));
    for (const entry of active) {
      if (entry.sortAt >= boundary && !returnedIds.has(entry.id) && !queuedPostIds.has(entry.postId)) {
        projectRow(database, app.timelineEntries, entry.id, { active: false });
      }
    }
  }

  async function projectProfile(profile: (ProfileView & { indexedAt?: string }) | undefined) {
    if (!profile?.did) return;
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

  async function projectThread(ownerDid: string, thread: FlatThread, intents: ReactionIntents) {
    let count = 0;
    await Promise.all(thread.entries.map(async (entry) => {
      const bundle = normalizePost(entry.node.post);
      if (bundle) {
        await writePostBundle(bundle);
        await projectViewerState(ownerDid, bundle.post, entry.node.post?.viewer, intents);
        count += 1;
      }
      projectRow(database, app.threadEntries, stableObjectId(
        "thread-entry",
        `${thread.rootPostId}:${entry.postId}`,
      ), {
        rootPostId: thread.rootPostId,
        postId: entry.postId,
        parentPostId: bundle?.post.replyParentId ?? entry.parentPostId,
        sortOrder: entry.sortOrder,
        state: bundle ? "post" : entry.state,
        indexedAt: bundle?.post.indexedAt ?? new Date().toISOString(),
      });
    }));
    return { rootPostId: thread.rootPostId, count };
  }

  function markOperationSent(operation: Operation) {
    projectRow(database, app.pendingOperations, operation.id, {
      ownerDid: operation.ownerDid,
      kind: operation.kind,
      rkey: operation.rkey,
      payload: encodeOperationPayload(operation),
      state: "sent",
      error: null,
      createdAt: operation.createdAt,
    });
  }

  async function deactivateRepostTimelineEntries(ownerDid: string, repostId: string) {
    const entries = await database.all(app.timelineEntries.where({
      ownerDid: { eq: ownerDid },
      repostId: { eq: repostId },
      active: { eq: true },
    }));
    for (const entry of entries) {
      projectRow(database, app.timelineEntries, entry.id, { active: false });
    }
  }

  return {
    deactivateRepostTimelineEntries,
    loadReactionIntents,
    markOperationSent,
    projectProfile,
    projectThread,
    projectTimelineItem,
    projectTimelinePage,
    projectViewerState,
    writeLike,
    writePostBundle,
    writeRepost,
  };
}

export type ProjectionWriter = ReturnType<typeof createProjectionWriter>;
