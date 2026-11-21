// Who can do what in StagePlan, enforced by the server:
//
// - The crew chief (the account that created a show) manages the show, its
//   invite link and its crew.
// - Crew members (anyone with a showCrew row) see the show and can add, edit
//   and move its tasks, comment and log activity. A task is assigned to
//   nobody or to someone on the crew. The chief or a task's creator deletes it.
// - Everyone else sees nothing of the show: not the show, its tasks,
//   comments, activity or crew list. Anyone holding the current invite code
//   can join its crew.
// - Invite codes are bearer secrets: only the chief reads or rotates them.
// - Members leave on their own; the chief can remove anyone.
// - Checklist items are private to their owner.
//
// Reads put `canSeeShow` into their WHERE clause; writes call the asserts
// inside their transaction.
import { one, type Db } from "./db.js";

export class Forbidden extends Error {}

/** SQL: the show aliased `s` is visible to the account in parameter `$me`. */
export const canSeeShow = (me: string) =>
  `(s."chiefAccount" = ${me} OR EXISTS (SELECT 1 FROM "showCrew" m WHERE m."showId" = s.id AND m.account = ${me}))`;

export async function isChief(db: Db, me: string, showId: string) {
  return Boolean(await one(db, `SELECT 1 FROM shows WHERE id = $1 AND "chiefAccount" = $2`, [showId, me]));
}

export async function isCrew(db: Db, me: string, showId: string) {
  return Boolean(await one(db, `SELECT 1 FROM shows s WHERE s.id = $1 AND ${canSeeShow("$2")}`, [showId, me]));
}

export async function assertChief(db: Db, me: string, showId: string) {
  if (!(await isChief(db, me, showId))) throw new Forbidden("Only the crew chief can do that");
}

export async function assertCrew(db: Db, me: string, showId: string) {
  if (!(await isCrew(db, me, showId))) throw new Forbidden("Not on this show's crew");
}

export async function assertAssignable(db: Db, showId: string, assigneeId: string | null) {
  if (assigneeId === null) return;
  const member = await one(db, `SELECT 1 FROM "showCrew" WHERE "showId" = $1 AND "crewId" = $2`, [showId, assigneeId]);
  if (!member) throw new Forbidden("Tasks can only be assigned to the crew");
}

/** Who may subscribe to a live topic: a show's crew to `show:<id>`. */
export async function canSubscribe(db: Db, me: string, topic: string) {
  const [kind, id] = topic.split(":");
  return kind === "show" && Boolean(id) && isCrew(db, me, id);
}
