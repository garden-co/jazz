import { Effect } from "effect";
import type { JazzDb } from "jazz-tools/effect";
import {
  app,
  type ActivityKind,
  type Crew,
  type Show,
  type Task,
  type TaskStatus,
} from "../schema";

/**
 * StagePlan's writes as Effects over the Jazz service. The browser runs them
 * against its local database; the server runs `addChecklist` against the
 * requesting user's database, with the same permissions.
 */

export const STATUS_LABELS: Record<TaskStatus, string> = {
  todo: "To do",
  doing: "In progress",
  done: "Done",
  blocked: "Blocked",
};

/** The signed-in person: their account and their crew profile row. */
export type Me = { account: string; profile: Crew };
export type ShowInput = Pick<Show, "name" | "venue" | "date" | "doors">;

/** The pre-show checklist the server adds on request. */
export const STANDARD_CHECKLIST = ["Load in", "Line check", "Soundcheck", "Doors"] as const;

const activityEntry = (
  me: Me,
  task: Pick<Task, "id" | "showId">,
  kind: ActivityKind,
  detail?: string,
) => ({ showId: task.showId, taskId: task.id, actorId: me.profile.id, kind, detail });

/** Creates a show with its creator as crew chief and a first invite, in one transaction. */
export const createShow = (jazz: JazzDb, me: Me, input: ShowInput) =>
  jazz.transaction((tx) =>
    Effect.gen(function* () {
      const show = yield* tx.insert(app.shows, { ...input, chiefAccount: me.account });
      // Stable ids: a repair from another tab writes the same rows, not new ones.
      const [membershipId, inviteId] = yield* Effect.promise(() =>
        Promise.all([
          nameBasedId(`stage-plan/chief/${show.id}/${me.account}`),
          nameBasedId(`stage-plan/first-invite/${show.id}`),
        ]),
      );
      yield* tx.upsert(app.showCrew, membershipId, {
        showId: show.id,
        crewId: me.profile.id,
        account: me.account,
        role: "chief",
      });
      yield* tx.upsert(app.showInvites, inviteId, { showId: show.id, code: crypto.randomUUID() });
      return show;
    }),
  );

/** Adds a task at the end of a column and logs it. */
export const addTask = (jazz: JazzDb, me: Me, showId: string, title: string, rank: number) =>
  jazz.transaction((tx) =>
    Effect.gen(function* () {
      const task = yield* tx.insert(app.tasks, { showId, title, status: "todo", rank });
      yield* tx.insert(app.activity, activityEntry(me, task, "created", STATUS_LABELS.todo));
      return task;
    }),
  );

/** Moves a task to the end of another column; logs the move. */
export const moveTask = (jazz: JazzDb, me: Me, task: Task, status: TaskStatus, rank: number) =>
  jazz.transaction((tx) =>
    Effect.gen(function* () {
      yield* tx.update(app.tasks, task.id, { status, rank });
      if (status !== task.status)
        yield* tx.insert(app.activity, activityEntry(me, task, "moved", STATUS_LABELS[status]));
    }),
  );

/**
 * Adds the standard checklist items a show does not have yet, all in one
 * transaction: a crew member sees every item appear at once, or none.
 */
export const addChecklist = (jazz: JazzDb, me: Me, showId: string, existing: Task[]) =>
  jazz.transaction((tx) =>
    Effect.gen(function* () {
      const titles = new Set(existing.map((task) => task.title));
      let rank = rankAfter(existing.filter((task) => task.status === "todo"));
      const added: Task[] = [];
      for (const title of STANDARD_CHECKLIST) {
        if (titles.has(title)) continue;
        const task = yield* tx.insert(app.tasks, { showId, title, status: "todo", rank: rank++ });
        yield* tx.insert(app.activity, activityEntry(me, task, "created", STATUS_LABELS.todo));
        added.push(task);
      }
      return added;
    }),
  );

/** Rank that puts a task after every task in the column. */
export function rankAfter(tasks: Pick<Task, "rank">[]) {
  return tasks.reduce((max, task) => Math.max(max, task.rank), 0) + 1;
}

/** A name-based (version 5) UUID, so the same name always gives the same id. */
async function nameBasedId(name: string) {
  const namespace = Uint8Array.from("6f9d1c8e2b4a4e7d9c3a5b1e8f2d7a64".match(/../g)!, (pair) =>
    parseInt(pair, 16),
  );
  const bytes = new Uint8Array([...namespace, ...new TextEncoder().encode(name)]);
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-1", bytes)).slice(0, 16);
  hash[6] = (hash[6]! & 0x0f) | 0x50;
  hash[8] = (hash[8]! & 0x3f) | 0x80;
  const hex = [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
