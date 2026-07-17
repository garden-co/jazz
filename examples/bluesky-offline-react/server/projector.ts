import { randomUUID } from "node:crypto";
import {
  fetchPostThread,
  fetchProfile,
  fetchTimelineFeed,
  type SessionFetcher,
  type ThreadViewNode,
} from "./bluesky.js";
import { createProjectionWriter, type ProjectionWriter } from "./projection-writer.js";
import { stableObjectId, type FeedViewPost, type ProfileView } from "./timeline.js";
import { flattenThread } from "./thread-normalizer.js";

type TimelineResponse = { feed?: FeedViewPost[]; cursor?: string };

type ProjectorDependencies = {
  fetchTimelineFeed: (session: SessionFetcher, cursor?: string) => Promise<TimelineResponse>;
  fetchPostThread: (session: SessionFetcher, uri: string) => Promise<ThreadViewNode | undefined>;
  fetchProfile: (did: string, session: SessionFetcher) => Promise<(ProfileView & { indexedAt?: string }) | undefined>;
  writer: Pick<ProjectionWriter,
    "loadReactionIntents" | "projectProfile" | "projectThread" | "projectTimelinePage">;
};

export type TimelineProjectionStatus = {
  id: string;
  ownerDid: string;
  cursor?: string;
  count?: number;
  state: "fetching" | "projecting" | "completed" | "failed";
  acceptedAt: string;
  completedAt?: string;
  error?: string;
};

type TimelineJob = {
  ready: Promise<{ cursor?: string; hasMore: boolean; count: number }>;
  completion: Promise<void>;
  status: TimelineProjectionStatus;
};

export function createProjector(dependencies: ProjectorDependencies = {
  fetchTimelineFeed,
  fetchPostThread,
  fetchProfile,
  writer: createProjectionWriter(),
}) {
  const jobs = new Map<string, TimelineJob>();
  const statuses = new Map<string, TimelineProjectionStatus>();
  const completions = new Map<string, Promise<void>>();

  function timelineJobKey(ownerDid: string, cursor?: string) {
    return `${ownerDid}\n${cursor ?? "head"}`;
  }

  function startTimelineJob(ownerDid: string, session: SessionFetcher, cursor?: string) {
    const status: TimelineProjectionStatus = {
      id: randomUUID(),
      ownerDid,
      cursor,
      state: "fetching",
      acceptedAt: new Date().toISOString(),
    };
    statuses.set(ownerDid, status);

    let timeline: TimelineResponse;
    const ready = dependencies.fetchTimelineFeed(session, cursor).then((response) => {
      timeline = response;
      status.cursor = response.cursor;
      status.count = response.feed?.length ?? 0;
      if (!status.completedAt) status.state = "projecting";
      return {
        cursor: response.cursor,
        hasMore: Boolean(response.cursor),
        count: response.feed?.length ?? 0,
      };
    });

    const profileProjection = cursor
      ? Promise.resolve()
      : Promise.resolve(dependencies.fetchProfile(ownerDid, session))
          .then((profile) => dependencies.writer.projectProfile(profile));
    const timelineProjection = ready.then(async () => {
      const intents = await dependencies.writer.loadReactionIntents(ownerDid);
      await dependencies.writer.projectTimelinePage(ownerDid, timeline.feed ?? [], cursor, intents);
    });
    const completion = Promise.all([timelineProjection, profileProjection]).then(() => {
      status.state = "completed";
      status.completedAt = new Date().toISOString();
    }).catch((error: unknown) => {
      status.state = "failed";
      status.error = error instanceof Error ? error.message : String(error);
      status.completedAt = new Date().toISOString();
    });

    const job = { ready, completion, status };
    const jobKey = timelineJobKey(ownerDid, cursor);
    jobs.set(jobKey, job);
    completions.set(ownerDid, completion);
    completion.finally(() => {
      if (jobs.get(jobKey) === job) jobs.delete(jobKey);
    });
    return job;
  }

  async function projectTimelinePage(ownerDid: string, session: SessionFetcher, cursor?: string) {
    const current = jobs.get(timelineJobKey(ownerDid, cursor));
    const job = current ?? startTimelineJob(ownerDid, session, cursor);
    const result = await job.ready;
    return {
      ...result,
      projection: {
        id: job.status.id,
        state: current ? "running" as const : "accepted" as const,
      },
    };
  }

  async function projectThread(ownerDid: string, session: SessionFetcher, uri: string) {
    const thread = await dependencies.fetchPostThread(session, uri);
    if (!thread) throw new Error("thread fetch failed");
    const flattened = flattenThread(
      uri,
      thread,
      (postUri) => stableObjectId("bluesky-post", postUri),
    );
    const intents = await dependencies.writer.loadReactionIntents(ownerDid);
    return { ok: true, ...await dependencies.writer.projectThread(ownerDid, flattened, intents) };
  }

  function getTimelineProjectionStatus(ownerDid: string) {
    const status = statuses.get(ownerDid);
    return status ? { ...status } : undefined;
  }

  async function waitForTimelineProjection(ownerDid: string) {
    await completions.get(ownerDid);
  }

  return {
    getTimelineProjectionStatus,
    projectThread,
    projectTimelinePage,
    waitForTimelineProjection,
  };
}

export type Projector = ReturnType<typeof createProjector>;
