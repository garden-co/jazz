import type { Operation, PostOperation, ReactionOperation } from "../shared/pending-operations.js";
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
import { projectionDb } from "./jazz.js";
import { createProjection } from "./projection.js";
import { createTimelineCoordinator, type TimelineResult } from "./timeline-coordinator.js";

type TimelineJob = { ready: Promise<TimelineResult>; freshUntil: number };

const timelineRefreshFreshnessMs = 10_000;

const projection = createProjection(projectionDb);
const timelineJobs = new Map<string, TimelineJob>();

function timelineJobKey(ownerDid: string, cursor?: string) {
  return `${ownerDid}\n${cursor ?? "head"}`;
}

function startTimelineJob(ownerDid: string, session: SessionFetcher, cursor?: string) {
  const timeline = fetchTimelineFeed(session, cursor);
  const profileProjection = cursor
    ? Promise.resolve()
    : fetchProfile(ownerDid, session)
        .then((profile) => projection.projectProfile(profile))
        .catch((error: unknown) => console.error("Profile projection failed", error));
  const ready = timeline.then(async (response) => {
    await Promise.all([
      projection.projectTimelinePage(ownerDid, response.feed ?? [], cursor),
      profileProjection,
    ]);
    return {
      cursor: response.cursor,
      hasMore: Boolean(response.cursor),
      count: response.feed?.length ?? 0,
    };
  });
  const job = { ready, freshUntil: Number.POSITIVE_INFINITY };
  const jobKey = timelineJobKey(ownerDid, cursor);

  timelineJobs.set(jobKey, job);
  const completeJob = () => {
    if (timelineJobs.get(jobKey) !== job) return;
    if (cursor) timelineJobs.delete(jobKey);
    else job.freshUntil = Date.now() + timelineRefreshFreshnessMs;
  };
  const discardJob = () => {
    if (timelineJobs.get(jobKey) === job) timelineJobs.delete(jobKey);
  };
  ready.then(completeJob, discardJob);
  return job;
}

export async function projectTimelinePage(
  ownerDid: string,
  session: SessionFetcher,
  cursor?: string,
) {
  const current = timelineJobs.get(timelineJobKey(ownerDid, cursor));
  const job =
    current && current.freshUntil > Date.now()
      ? current
      : startTimelineJob(ownerDid, session, cursor);
  return job.ready;
}

const timelineCoordinator = createTimelineCoordinator(projectTimelinePage);
export const activateTimeline = timelineCoordinator.activate;
export const projectNextTimelinePage = timelineCoordinator.loadMore;
export const deactivateTimeline = timelineCoordinator.deactivate;

export async function projectThread(ownerDid: string, session: SessionFetcher, uri: string) {
  const thread = await fetchPostThread(session, uri);
  if (!thread) throw new Error("thread fetch failed");
  return { ok: true, ...(await projection.projectThread(ownerDid, uri, thread)) };
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
  await projection.projectPostOperation(operation, created);
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
      if (!(error instanceof OperationError) || !error.message.includes("RecordNotFound"))
        throw error;
    }
    uri = undefined;
  }

  const projectedPost = {
    ...post,
    likeCount:
      kind === "like"
        ? Math.max(0, (post.likeCount ?? 0) + Number(operation.payload.active) - Number(wasActive))
        : post.likeCount,
    repostCount:
      kind === "repost"
        ? Math.max(
            0,
            (post.repostCount ?? 0) + Number(operation.payload.active) - Number(wasActive),
          )
        : post.repostCount,
  };
  await projection.projectReactionOperation(operation, projectedPost, { uri, cid });
}

export async function reconcileOperations(
  did: string,
  session: SessionFetcher,
  operations: Operation[],
) {
  // ATProto repository writes are ordered intentions; do not parallelise them.
  const ordered = [...operations].sort(
    (left, right) =>
      left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
  );
  for (const operation of ordered) {
    if (operation.kind === "post") await reconcilePost(did, session, operation);
    else await reconcileReaction(did, session, operation);
  }
}
