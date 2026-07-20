import {
  fetchPostThread,
  fetchProfile,
  fetchTimelineFeed,
  type SessionFetcher,
} from "./bluesky.js";
import { createProjectionWriter, type ProjectionWriter } from "./projection-writer.js";
import {
  flattenThread,
  stableObjectId,
  type FeedViewPost,
  type ProfileView,
  type ThreadViewNode,
} from "./projection-model.js";

type TimelineResponse = { feed?: FeedViewPost[]; cursor?: string };
type TimelineResult = { cursor?: string; hasMore: boolean; count: number };

type ProjectorDependencies = {
  fetchTimelineFeed: (session: SessionFetcher, cursor?: string) => Promise<TimelineResponse>;
  fetchPostThread: (session: SessionFetcher, uri: string) => Promise<ThreadViewNode | undefined>;
  fetchProfile: (did: string, session: SessionFetcher) => Promise<(ProfileView & { indexedAt?: string }) | undefined>;
  writer: Pick<ProjectionWriter,
    "loadReactionIntents" | "projectProfile" | "projectThread" | "projectTimelinePage">;
};

type TimelineJob = {
  ready: Promise<TimelineResult>;
};

export function createProjector(dependencies: ProjectorDependencies = {
  fetchTimelineFeed,
  fetchPostThread,
  fetchProfile,
  writer: createProjectionWriter(),
}, reportError: (message: string, error: unknown) => void = console.error) {
  const jobs = new Map<string, TimelineJob>();

  function timelineJobKey(ownerDid: string, cursor?: string) {
    return `${ownerDid}\n${cursor ?? "head"}`;
  }

  function startTimelineJob(ownerDid: string, session: SessionFetcher, cursor?: string) {
    const timeline = dependencies.fetchTimelineFeed(session, cursor);
    const ready = timeline.then((response) => ({
      cursor: response.cursor,
      hasMore: Boolean(response.cursor),
      count: response.feed?.length ?? 0,
    }));

    const profileProjection = cursor
      ? Promise.resolve()
      : dependencies.fetchProfile(ownerDid, session)
          .then((profile) => dependencies.writer.projectProfile(profile))
          .catch((error: unknown) => reportError("Profile projection failed", error));
    const timelineProjection = timeline.then(async (response) => {
      const intents = await dependencies.writer.loadReactionIntents(ownerDid);
      await dependencies.writer.projectTimelinePage(ownerDid, response.feed ?? [], cursor, intents);
    }).catch((error: unknown) => reportError("Timeline projection failed", error));

    const job = { ready };
    const jobKey = timelineJobKey(ownerDid, cursor);
    jobs.set(jobKey, job);
    Promise.all([timelineProjection, profileProjection]).then(() => {
      if (jobs.get(jobKey) === job) jobs.delete(jobKey);
    });
    return job;
  }

  async function projectTimelinePage(ownerDid: string, session: SessionFetcher, cursor?: string) {
    const current = jobs.get(timelineJobKey(ownerDid, cursor));
    const job = current ?? startTimelineJob(ownerDid, session, cursor);
    return job.ready;
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

  return {
    projectThread,
    projectTimelinePage,
  };
}

export type Projector = ReturnType<typeof createProjector>;
