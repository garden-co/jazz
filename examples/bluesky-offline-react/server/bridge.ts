import type { Operation, PostOperation, ReactionOperation } from "../operations.js";
import {
  deleteRecord,
  fetchPostThread,
  fetchProfile,
  fetchTimelineFeed,
  fetchViewerPosts,
  OperationError,
  putRecord,
  recordKey,
  type SessionFetcher,
} from "./bluesky.js";
import { db } from "./jazz.js";
import {
  flattenThread,
  normalizePost,
  stableObjectId,
} from "./projection-model.js";
import { createProjectionWriter } from "./projection-writer.js";

type TimelineResult = { cursor?: string; hasMore: boolean; count: number };
type TimelineJob = { ready: Promise<TimelineResult> };

const writer = createProjectionWriter(db);
const timelineJobs = new Map<string, TimelineJob>();

function timelineJobKey(ownerDid: string, cursor?: string) {
  return `${ownerDid}\n${cursor ?? "head"}`;
}

function startTimelineJob(ownerDid: string, session: SessionFetcher, cursor?: string) {
  const timeline = fetchTimelineFeed(session, cursor);
  const ready = timeline.then((response) => ({
    cursor: response.cursor,
    hasMore: Boolean(response.cursor),
    count: response.feed?.length ?? 0,
  }));
  const profileProjection = cursor
    ? Promise.resolve()
    : fetchProfile(ownerDid, session)
        .then((profile) => writer.projectProfile(profile))
        .catch((error: unknown) => console.error("Profile projection failed", error));
  const timelineProjection = timeline.then(async (response) => {
    const intents = await writer.loadReactionIntents(ownerDid);
    await writer.projectTimelinePage(ownerDid, response.feed ?? [], cursor, intents);
  }).catch((error: unknown) => console.error("Timeline projection failed", error));
  const job = { ready };
  const jobKey = timelineJobKey(ownerDid, cursor);

  timelineJobs.set(jobKey, job);
  Promise.all([timelineProjection, profileProjection]).then(() => {
    if (timelineJobs.get(jobKey) === job) timelineJobs.delete(jobKey);
  });
  return job;
}

export async function projectTimelinePage(
  ownerDid: string,
  session: SessionFetcher,
  cursor?: string,
) {
  const current = timelineJobs.get(timelineJobKey(ownerDid, cursor));
  return (current ?? startTimelineJob(ownerDid, session, cursor)).ready;
}

export async function projectThread(ownerDid: string, session: SessionFetcher, uri: string) {
  const thread = await fetchPostThread(session, uri);
  if (!thread) throw new Error("thread fetch failed");
  const flattened = flattenThread(
    uri,
    thread,
    (postUri) => stableObjectId("bluesky-post", postUri),
  );
  const intents = await writer.loadReactionIntents(ownerDid);
  return { ok: true, ...await writer.projectThread(ownerDid, flattened, intents) };
}

async function reconcilePost(did: string, session: SessionFetcher, operation: PostOperation) {
  const record = {
    $type: "app.bsky.feed.post",
    text: operation.payload.text,
    createdAt: operation.payload.createdAt,
    ...(operation.payload.reply ? { reply: operation.payload.reply } : {}),
  };
  const created = await putRecord(session, {
    repo: did,
    collection: "app.bsky.feed.post",
    rkey: operation.rkey,
    record,
  });
  const bundle = normalizePost({
    uri: created.uri,
    cid: created.cid,
    author: { did },
    record,
    indexedAt: operation.payload.createdAt,
  });
  if (!bundle) throw new OperationError("PDS returned an invalid post", 502);
  await writer.writePostBundle(bundle);
}

async function reconcileReaction(
  did: string,
  session: SessionFetcher,
  operation: ReactionOperation,
) {
  const kind = operation.kind;
  const collection = `app.bsky.feed.${kind}`;
  // Resolve the current subject again because its CID may have changed while this intention was offline.
  const [post] = await fetchViewerPosts(session, [operation.payload.subjectUri]);
  if (!post?.uri || !post.cid) throw new OperationError("subject post is unavailable", 502);

  const postId = stableObjectId("bluesky-post", post.uri);
  const viewerUri = post.viewer?.[kind];
  const wasActive = Boolean(viewerUri);
  let uri = viewerUri;
  let cid: string | undefined;

  if (operation.payload.active && !wasActive) {
    const created = await putRecord(session, {
      repo: did,
      collection,
      rkey: operation.rkey,
      record: {
        $type: collection,
        subject: { uri: post.uri, cid: post.cid },
        createdAt: operation.payload.createdAt,
      },
    });
    uri = created.uri;
    cid = created.cid;
  } else if (!operation.payload.active && wasActive) {
    const rkey = recordKey(viewerUri, did, collection);
    if (!rkey) throw new OperationError(`AppView returned an invalid ${kind} URI`, 502);
    try {
      await deleteRecord(session, { repo: did, collection, rkey });
    } catch (error) {
      if (!(error instanceof OperationError) || !error.message.includes("RecordNotFound")) throw error;
    }
    uri = undefined;
  }

  const bundle = normalizePost({
    ...post,
    likeCount: kind === "like"
      ? Math.max(0, (post.likeCount ?? 0) + Number(operation.payload.active) - Number(wasActive))
      : post.likeCount,
    repostCount: kind === "repost"
      ? Math.max(0, (post.repostCount ?? 0) + Number(operation.payload.active) - Number(wasActive))
      : post.repostCount,
  });
  if (!bundle) throw new OperationError("AppView returned an invalid subject post", 502);
  await writer.writePostBundle(bundle);

  const id = stableObjectId(`bluesky-${kind}`, `${did}:${post.uri}`);
  if (kind === "like") {
    await writer.writeLike({
      id,
      uri: uri ?? `at://${did}/${collection}/${operation.rkey}`,
      actorDid: did,
      subjectPostId: postId,
      createdAt: operation.payload.createdAt,
      active: operation.payload.active,
    });
  } else {
    await writer.writeRepost({
      id,
      uri,
      cid,
      actorDid: did,
      actorProfileId: stableObjectId("bluesky-profile", did),
      subjectPostId: postId,
      createdAt: operation.payload.createdAt,
      active: operation.payload.active,
    });
    if (!operation.payload.active) await writer.deactivateRepostTimelineEntries(did, id);
  }
}

export async function reconcileOperations(
  did: string,
  session: SessionFetcher,
  operations: Operation[],
) {
  // ATProto repository writes are ordered intentions; do not parallelise them.
  const ordered = [...operations].sort((left, right) =>
    left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id));
  for (const operation of ordered) {
    if (operation.kind === "post") await reconcilePost(did, session, operation);
    else await reconcileReaction(did, session, operation);
    await writer.completeOperation(operation);
  }
}
