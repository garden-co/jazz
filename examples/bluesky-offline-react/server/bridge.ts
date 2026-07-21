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

type TimelineResult = { cursor?: string; hasMore: boolean; count: number };
type TimelineJob = { ready: Promise<TimelineResult> };
type TimelineResponse = Awaited<ReturnType<typeof fetchTimelineFeed>>;
type ProjectionLane = {
  pending?: TimelineResponse;
  running: Promise<void>;
};

const projection = createProjection(projectionDb);
const timelineJobs = new Map<string, TimelineJob>();
const projectionLanes = new Map<string, ProjectionLane>();

function timelineJobKey(ownerDid: string, cursor?: string) {
  return `${ownerDid}\n${cursor ?? "head"}`;
}

function scheduleTimelineProjection(
  ownerDid: string,
  response: TimelineResponse,
  cursor?: string,
) {
  const laneKey = timelineJobKey(ownerDid, cursor);
  const existingLane = projectionLanes.get(laneKey);
  if (existingLane) {
    // Only the newest repeated response matters once the current projection settles.
    existingLane.pending = response;
    return;
  }

  const lane: ProjectionLane = { running: Promise.resolve() };
  projectionLanes.set(laneKey, lane);
  lane.running = (async () => {
    let next: TimelineResponse | undefined = response;
    while (next) {
      const current = next;
      lane.pending = undefined;
      try {
        await projection.projectTimelinePage(ownerDid, current.feed ?? [], cursor);
      } catch (error) {
        console.error("Timeline projection failed", error);
      }
      next = lane.pending;
    }
  })().finally(() => {
    if (projectionLanes.get(laneKey) === lane) projectionLanes.delete(laneKey);
  });
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
        .then((profile) => projection.projectProfile(profile))
        .catch((error: unknown) => console.error("Profile projection failed", error));
  const timelineProjection = timeline
    .then((response) => scheduleTimelineProjection(ownerDid, response, cursor))
    .catch((error: unknown) => console.error("Timeline projection failed", error));
  const job = { ready };
  const jobKey = timelineJobKey(ownerDid, cursor);

  timelineJobs.set(jobKey, job);
  const releaseJob = () => {
    if (timelineJobs.get(jobKey) === job) timelineJobs.delete(jobKey);
  };
  // Coalesce only the AppView read. Jazz projection runs in the background and
  // must not freeze future head reads if edge acknowledgement is delayed.
  ready.then(releaseJob, releaseJob);
  Promise.all([timelineProjection, profileProjection]);
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
