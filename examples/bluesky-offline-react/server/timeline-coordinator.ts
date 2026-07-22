import type { SessionFetcher } from "./bluesky.js";

export type TimelineResult = { cursor?: string; hasMore: boolean; count: number };

type AccountTimeline = {
  ownerDid: string;
  session: SessionFetcher;
  cursor?: string;
  initialised: boolean;
  paginationStarted: boolean;
  headRefresh?: Promise<TimelineResult>;
  pagination?: Promise<TimelineResult>;
  timer?: ReturnType<typeof setTimeout>;
};

export function createTimelineCoordinator(
  projectPage: (
    ownerDid: string,
    session: SessionFetcher,
    cursor?: string,
  ) => Promise<TimelineResult>,
  refreshIntervalMs = 15_000,
) {
  const timelines = new Map<string, AccountTimeline>();

  function accountTimeline(ownerDid: string, session: SessionFetcher) {
    const current = timelines.get(ownerDid);
    if (current) {
      current.session = session;
      return current;
    }
    const timeline: AccountTimeline = {
      ownerDid,
      session,
      initialised: false,
      paginationStarted: false,
    };
    timelines.set(ownerDid, timeline);
    return timeline;
  }

  function scheduleHeadRefresh(timeline: AccountTimeline) {
    if (timeline.timer) clearTimeout(timeline.timer);
    timeline.timer = setTimeout(() => {
      timeline.timer = undefined;
      refreshHead(timeline)
        .catch((error: unknown) => console.error("Timeline refresh failed", error))
        .finally(() => scheduleHeadRefresh(timeline));
    }, refreshIntervalMs);
    timeline.timer.unref?.();
  }

  function refreshHead(timeline: AccountTimeline) {
    if (timeline.headRefresh) return timeline.headRefresh;
    const refresh = projectPage(timeline.ownerDid, timeline.session).then((result) => {
      timeline.initialised = true;
      if (!timeline.paginationStarted) timeline.cursor = result.cursor;
      return result;
    });
    timeline.headRefresh = refresh;
    refresh.finally(() => {
      if (timeline.headRefresh === refresh) timeline.headRefresh = undefined;
    });
    return refresh;
  }

  function activate(ownerDid: string, session: SessionFetcher) {
    const timeline = accountTimeline(ownerDid, session);
    refreshHead(timeline)
      .catch((error: unknown) => console.error("Timeline refresh failed", error))
      .finally(() => scheduleHeadRefresh(timeline));
  }

  async function loadMore(ownerDid: string, session: SessionFetcher) {
    const timeline = accountTimeline(ownerDid, session);
    if (!timeline.initialised) await refreshHead(timeline);
    if (!timeline.cursor) return { hasMore: false, count: 0 };
    if (timeline.pagination) return timeline.pagination;

    timeline.paginationStarted = true;
    const pagination = projectPage(ownerDid, session, timeline.cursor).then((result) => {
      timeline.cursor = result.cursor;
      return result;
    });
    timeline.pagination = pagination;
    pagination.finally(() => {
      if (timeline.pagination === pagination) timeline.pagination = undefined;
    });
    return pagination;
  }

  function deactivate(ownerDid: string) {
    const timeline = timelines.get(ownerDid);
    if (timeline?.timer) clearTimeout(timeline.timer);
    timelines.delete(ownerDid);
  }

  return { activate, loadMore, deactivate };
}
