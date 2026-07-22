import { AppBskyActorDefs, AppBskyFeedDefs } from "@atproto/api";
import type { Operation, PostOperation, ReactionOperation } from "../shared/pending-operations.js";
import { decodeOperation, encodeOperationPayload } from "../shared/pending-operations.js";
import { app } from "../schema.js";
import type { QueryBuilder, TableProxy } from "jazz-tools/backend";
import type { projectionDb } from "./jazz.js";
import {
  flattenThread,
  normalizePost,
  normalizeTimelineItem,
  stableObjectId,
  type NormalizedPost,
  type PostBundle,
  type ThreadNode,
} from "./projection-input.js";

type ProjectionDatabase = typeof projectionDb;

const unknownIndexedAt = new Date(0).toISOString();

type ReactionIntents = Map<string, ReactionOperation>;

// Shared idempotent row writer used by the persistence stage below.
type ProjectionTable<TRow, TInit> = TableProxy<TRow, TInit> & {
  where(filter: { id: { eq: string } }): QueryBuilder<TRow>;
};

function projectionMatches<TRow extends object, TInit extends object>(
  existing: TRow,
  data: Partial<TInit>,
) {
  return Object.entries(data).every(
    ([key, value]) => value === undefined || Object.is(Reflect.get(existing, key), value),
  );
}

async function projectRow<TRow extends object, TInit extends object>(
  database: ProjectionDatabase,
  table: ProjectionTable<TRow, TInit>,
  id: string,
  data: Partial<TInit>,
  durability: "local" | "edge" = "local",
) {
  const existing = await database.one(table.where({ id: { eq: id } }));
  if (existing && projectionMatches(existing, data)) return;
  if (existing) {
    const write = database.update(table, id, data);
    if (durability === "edge") await write.wait({ tier: "edge" });
    else await write;
    return;
  }
  const write = database.upsert(table, data, { id });
  if (durability === "edge") await write.wait({ tier: "edge" });
  else await write;
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

function mergeProfileProjection(
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
    indexedAt:
      existing && existing.indexedAt > incoming.indexedAt ? existing.indexedAt : incoming.indexedAt,
  };
}

function reactionKey(kind: ReactionOperation["kind"], postId: string) {
  return `${kind}:${postId}`;
}

// Persistence stage: write mapped rows and reconcile optimistic intentions.
export function createProjection(database: ProjectionDatabase) {
  const projection = {
    projectPostOperation,
    projectProfile,
    projectReactionOperation,
    projectThread,
    projectTimelinePage,
  };

  async function writeProfile(profile: ProfileProjection) {
    const existing = await database.one(app.profiles.where({ id: { eq: profile.id } }));
    await projectRow(database, app.profiles, profile.id, mergeProfileProjection(existing, profile));
  }

  async function writePostBundle(bundle: PostBundle) {
    if (bundle.quote) await writePostBundle(bundle.quote);
    if (bundle.profile) await writeProfile(bundle.profile);
    const { id, ...post } = bundle.post;
    await projectRow(database, app.posts, id, { ...post, state: "synced" });

    const expectedImages = new Set(bundle.images.map((image) => image.id));
    const existingImages = await database.all(app.postImages.where({ postId: { eq: id } }));
    await Promise.all(
      bundle.images.map(async (image) => {
        const { id: imageId, ...data } = image;
        await projectRow(database, app.postImages, imageId, data);
      }),
    );
    await Promise.all(
      existingImages
        .filter((image) => !expectedImages.has(image.id))
        .map((image) => database.delete(app.postImages, image.id)),
    );
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
    await projectRow(database, app.likes, id, row);
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
    await projectRow(database, app.reposts, id, row);
  }

  async function loadReactionIntents(ownerDid: string): Promise<ReactionIntents> {
    const rows = await database.all(app.pendingOperations.where({ ownerDid: { eq: ownerDid } }));
    const operations = rows
      .map(decodeOperation)
      .filter(
        (operation): operation is ReactionOperation =>
          operation.kind !== "post" && (operation.state === "queued" || operation.state === "sent"),
      )
      .sort(
        (left, right) =>
          left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
      );
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
    viewer: AppBskyFeedDefs.PostView["viewer"],
    intents: ReactionIntents,
  ) {
    for (const kind of ["like", "repost"] as const) {
      const intent = intents.get(reactionKey(kind, post.id));
      const viewerUri = viewer?.[kind];
      const viewerActive = Boolean(viewerUri);
      if (intent && viewerActive !== intent.payload.active) continue;
      if (intent) intents.delete(reactionKey(kind, post.id));

      const id = stableObjectId(`bluesky-${kind}`, `${ownerDid}:${post.uri}`);
      if (kind === "like") {
        const existing = await database.one(app.likes.where({ id: { eq: id } }));
        if (viewerUri) {
          await writeLike({
            id,
            uri: viewerUri,
            actorDid: ownerDid,
            subjectPostId: post.id,
            createdAt: existing?.createdAt ?? post.indexedAt,
            active: true,
          });
        } else if (existing) {
          await writeLike({ ...existing, active: false });
        }
      } else {
        const existing = await database.one(app.reposts.where({ id: { eq: id } }));
        if (viewerUri) {
          await writeRepost({
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
          await writeRepost({
            ...existing,
            uri: existing.uri ?? undefined,
            cid: existing.cid ?? undefined,
            active: false,
          });
        }
      }
      if (intent?.state === "sent") await database.delete(app.pendingOperations, intent.id);
    }
  }

  async function writeThreadEntry(rootPostId: string, bundle: PostBundle, sortOrder: number) {
    const id = stableObjectId("thread-entry", `${rootPostId}:${bundle.post.id}`);
    await projectRow(database, app.threadEntries, id, {
      rootPostId,
      postId: bundle.post.id,
      parentPostId: bundle.post.replyParentId,
      sortOrder,
      state: "post",
      indexedAt: bundle.post.indexedAt,
    });
  }

  async function projectTimelineItem(
    ownerDid: string,
    item: AppBskyFeedDefs.FeedViewPost,
    intents: ReactionIntents,
  ) {
    const normalized = normalizeTimelineItem(ownerDid, item);
    if (!normalized) throw new Error(`Invalid timeline item: ${item.post?.uri ?? "missing post"}`);
    const bundles = new Map(normalized.context.map((bundle) => [bundle.post.id, bundle]));
    bundles.set(normalized.postBundle.post.id, normalized.postBundle);

    const profiles = new Map<string, ProfileProjection>();
    if (normalized.reposterProfile)
      profiles.set(normalized.reposterProfile.id, normalized.reposterProfile);
    for (const bundle of bundles.values()) {
      if (bundle.profile) profiles.set(bundle.profile.id, bundle.profile);
    }
    // Profiles must settle before posts because authorProfileId is a required relation.
    await Promise.all([...profiles.values()].map(writeProfile));
    await Promise.all(
      [...bundles.values()].map((bundle) => writePostBundle({ ...bundle, profile: undefined })),
    );
    if (normalized.repost) await writeRepost(normalized.repost);
    await projectViewerState(ownerDid, normalized.postBundle.post, item.post?.viewer, intents);
    const { id, ...entry } = normalized.timelineEntry;
    await projectRow(database, app.timelineEntries, id, entry);

    const orderedBundles = [...bundles.values()].sort((left, right) =>
      left.post.createdAt.localeCompare(right.post.createdAt),
    );
    await Promise.all(
      orderedBundles.map((bundle, index) =>
        writeThreadEntry(normalized.timelineEntry.threadRootId, bundle, index),
      ),
    );
    return normalized.timelineEntry;
  }

  async function projectTimelinePage(
    ownerDid: string,
    items: AppBskyFeedDefs.FeedViewPost[],
    cursor?: string,
  ) {
    const intents = await loadReactionIntents(ownerDid);
    const entries = await Promise.all(
      items.map((item) => projectTimelineItem(ownerDid, item, intents)),
    );
    if (cursor || entries.length === 0) return;

    const returnedIds = new Set(entries.map((entry) => entry.id));
    const boundary = entries.reduce(
      (oldest, entry) => (entry.sortAt < oldest ? entry.sortAt : oldest),
      entries[0]!.sortAt,
    );
    const operationRows = await database.all(
      app.pendingOperations.where({ ownerDid: { eq: ownerDid } }),
    );
    const queuedPostIds = new Set(
      operationRows
        .map(decodeOperation)
        .filter((operation) => operation.kind === "post" && operation.state === "queued")
        .map((operation) =>
          stableObjectId("bluesky-post", `at://${ownerDid}/app.bsky.feed.post/${operation.rkey}`),
        ),
    );
    const active = await database.all(
      app.timelineEntries.where({ ownerDid: { eq: ownerDid }, active: { eq: true } }),
    );
    await Promise.all(
      active
        .filter(
          (entry) =>
            entry.sortAt >= boundary &&
            !returnedIds.has(entry.id) &&
            !queuedPostIds.has(entry.postId),
        )
        .map((entry) => projectRow(database, app.timelineEntries, entry.id, { active: false })),
    );
  }

  async function projectProfile(profile: AppBskyActorDefs.ProfileViewDetailed | undefined) {
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

  async function projectThread(ownerDid: string, requestedUri: string, source: ThreadNode) {
    const thread = flattenThread(requestedUri, source, (postUri) =>
      stableObjectId("bluesky-post", postUri),
    );
    const intents = await loadReactionIntents(ownerDid);
    let count = 0;
    await Promise.all(
      thread.entries.map(async (entry) => {
        const bundle = normalizePost(entry.post);
        if (bundle) {
          await writePostBundle(bundle);
          await projectViewerState(ownerDid, bundle.post, entry.post?.viewer, intents);
          count += 1;
        }
        await projectRow(
          database,
          app.threadEntries,
          stableObjectId("thread-entry", `${thread.rootPostId}:${entry.postId}`),
          {
            rootPostId: thread.rootPostId,
            postId: entry.postId,
            parentPostId: bundle?.post.replyParentId ?? entry.parentPostId,
            sortOrder: entry.sortOrder,
            state: bundle ? "post" : entry.state,
            indexedAt: bundle?.post.indexedAt ?? unknownIndexedAt,
          },
        );
      }),
    );
    return { rootPostId: thread.rootPostId, count };
  }

  async function completeOperation(operation: Operation) {
    if (operation.kind === "post") {
      await database.delete(app.pendingOperations, operation.id).wait({ tier: "edge" });
      return;
    }
    try {
      await projectRow(
        database,
        app.pendingOperations,
        operation.id,
        {
          ownerDid: operation.ownerDid,
          kind: operation.kind,
          rkey: operation.rkey,
          payload: encodeOperationPayload(operation),
          state: "sent",
          error: null,
          createdAt: operation.createdAt,
        },
        "edge",
      );
    } catch (error) {
      // Cancellation or AppView confirmation may delete the intention while its PDS write is in flight.
      if (!(error instanceof Error) || !error.message.includes("row already deleted:")) throw error;
    }
  }

  async function projectPostOperation(
    operation: PostOperation,
    created: { uri: string; cid: string },
  ) {
    const bundle: PostBundle = {
      profile: undefined,
      post: {
        id: stableObjectId("bluesky-post", created.uri),
        uri: created.uri,
        cid: created.cid,
        authorDid: operation.ownerDid,
        authorProfileId: stableObjectId("bluesky-profile", operation.ownerDid),
        text: operation.payload.text,
        facetsJson: undefined,
        createdAt: operation.payload.createdAt,
        indexedAt: operation.payload.createdAt,
        replyParentId: operation.payload.reply?.parent.uri
          ? stableObjectId("bluesky-post", operation.payload.reply.parent.uri)
          : undefined,
        replyRootId: operation.payload.reply?.root.uri
          ? stableObjectId("bluesky-post", operation.payload.reply.root.uri)
          : undefined,
        replyCount: 0,
        likeCount: 0,
        repostCount: 0,
        state: "synced",
      },
      images: [],
    };
    await writePostBundle(bundle);
    await completeOperation(operation);
  }

  async function projectReactionOperation(
    operation: ReactionOperation,
    post: AppBskyFeedDefs.PostView,
    result: { uri?: string; cid?: string },
  ) {
    const bundle = normalizePost(post);
    if (!bundle) throw new Error("AppView returned an invalid subject post");
    await writePostBundle(bundle);

    const { kind, ownerDid } = operation;
    const id = stableObjectId(`bluesky-${kind}`, `${ownerDid}:${bundle.post.uri}`);
    if (kind === "like") {
      await writeLike({
        id,
        uri: result.uri ?? `at://${ownerDid}/app.bsky.feed.like/${operation.rkey}`,
        actorDid: ownerDid,
        subjectPostId: bundle.post.id,
        createdAt: operation.payload.createdAt,
        active: operation.payload.active,
      });
    } else {
      await writeRepost({
        id,
        uri: result.uri,
        cid: result.cid,
        actorDid: ownerDid,
        actorProfileId: stableObjectId("bluesky-profile", ownerDid),
        subjectPostId: bundle.post.id,
        createdAt: operation.payload.createdAt,
        active: operation.payload.active,
      });
      if (!operation.payload.active) {
        const entries = await database.all(
          app.timelineEntries.where({
            ownerDid: { eq: ownerDid },
            repostId: { eq: id },
            active: { eq: true },
          }),
        );
        await Promise.all(
          entries.map((entry) =>
            projectRow(database, app.timelineEntries, entry.id, { active: false }),
          ),
        );
      }
    }
    await completeOperation(operation);
  }

  return projection;
}
