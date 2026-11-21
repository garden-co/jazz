import type { Activity, ChecklistItem, Comment, Crew, Show, ShowCrew, ShowInvite, Task } from "./schema.js";

/**
 * The live queries the UI subscribes to, by name. The server builds the SQL
 * for each (server/queries.ts), scoped to the signed-in account, and seals
 * it; the browser only names the query and its arguments.
 */
export type QueryDef<Row, Result> = { name: string; args: Record<string, unknown>; one: boolean; _?: [Row, Result] };

const many =
  <Row, Args extends Record<string, unknown> = Record<string, never>>(name: string) =>
  (args?: Args): QueryDef<Row, Row[]> => ({ name, args: args ?? {}, one: false });
const one =
  <Row, Args extends Record<string, unknown> = Record<string, never>>(name: string) =>
  (args?: Args): QueryDef<Row, Row | undefined> => ({ name, args: args ?? {}, one: true });

type ShowArgs = { showId: string };

export const queries = {
  crew: { mine: one<Crew>("crew.mine") },
  shows: {
    mine: many<Show & { tasks: Pick<Task, "status">[] }>("shows.mine"),
    one: one<Show, ShowArgs>("shows.one"),
  },
  showCrew: { byShow: many<ShowCrew & { crew: Crew | null }, ShowArgs>("showCrew.byShow") },
  showInvites: { byShow: many<ShowInvite, ShowArgs>("showInvites.byShow") },
  tasks: { byShow: many<Task, ShowArgs>("tasks.byShow") },
  comments: { byTask: many<Comment & { author: Crew | null }, { taskId: string }>("comments.byTask") },
  activity: {
    feed: many<Activity & { actor: Crew | null }, { showId: string; taskId?: string; limit: number }>("activity.feed"),
  },
  checklist: {
    mine: many<ChecklistItem, { search: string; filter: "all" | "open" | "done" }>("checklist.mine"),
  },
};
