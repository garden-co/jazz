// The app's live queries: where each one is fetched from, and which tables
// under which topic it depends on (the server's change events name them).
import type { Activity, ChecklistItem, Comment, Crew, Show, ShowCrew, ShowInvite, Task } from "../../shared/types.js";
import type { Live } from "./live.js";

const show = (id: string) => `show:${id}`;
const qs = (params: Record<string, string | number | undefined>) =>
  new URLSearchParams(Object.entries(params).filter(([, v]) => v !== undefined) as [string, string][]).toString();

export type ShowWithTasks = Show & { tasks: Pick<Task, "status">[] };
export type CrewMember = ShowCrew & { crew?: Crew | null };
export type ActivityEntry = Activity & { actor?: Crew | null };
export type CommentEntry = Comment & { author?: Crew | null };

export const queries = {
  shows: {
    mine: (): Live<ShowWithTasks[]> => ({
      key: ["shows"],
      path: "/shows",
      empty: [],
      // New shows come through `me`; task counts through each show's topic.
      watch: (shows) => [["me", ["shows"]], ...shows.map((s) => [show(s.id), ["shows", "tasks"]] as [string, string[]])],
    }),
    one: ({ showId }: { showId: string }): Live<Show | null> => ({
      key: ["show", showId],
      path: `/shows/${showId}`,
      empty: null,
      watch: () => [[show(showId), ["shows", "showCrew"]]],
    }),
  },
  showCrew: {
    byShow: ({ showId }: { showId: string }): Live<CrewMember[]> => ({
      key: ["show", showId, "crew"],
      path: `/shows/${showId}/crew`,
      empty: [],
      watch: () => [[show(showId), ["showCrew", "crew"]]],
    }),
  },
  showInvites: {
    byShow: ({ showId }: { showId: string }): Live<ShowInvite[]> => ({
      key: ["show", showId, "invites"],
      path: `/shows/${showId}/invites`,
      empty: [],
      watch: () => [[show(showId), ["showInvites"]]],
    }),
  },
  tasks: {
    byShow: ({ showId }: { showId: string }): Live<Task[]> => ({
      key: ["show", showId, "tasks"],
      path: `/shows/${showId}/tasks`,
      empty: [],
      watch: () => [[show(showId), ["tasks"]]],
    }),
  },
  comments: {
    byTask: ({ showId, taskId }: { showId: string; taskId: string }): Live<CommentEntry[]> => ({
      key: ["show", showId, "comments", taskId],
      path: `/tasks/${taskId}/comments`,
      empty: [],
      watch: () => [[show(showId), ["comments", "crew"]]],
    }),
  },
  activity: {
    feed: ({ showId, taskId, limit }: { showId: string; taskId?: string; limit: number }): Live<ActivityEntry[]> => ({
      key: ["show", showId, "activity", taskId, limit],
      path: `/shows/${showId}/activity?${qs({ taskId, limit })}`,
      empty: [],
      watch: () => [[show(showId), ["activity", "crew"]]],
    }),
  },
  crew: {
    mine: (): Live<Crew | null> => ({ key: ["me"], path: "/me", empty: null, watch: () => [["me", ["crew"]]] }),
  },
  checklist: {
    mine: ({ search, filter }: { search: string; filter: "all" | "open" | "done" }): Live<ChecklistItem[]> => ({
      key: ["checklist", search, filter],
      path: `/checklist?${qs({ search, filter })}`,
      empty: [],
      watch: () => [["me", ["checklist"]]],
    }),
  },
};
