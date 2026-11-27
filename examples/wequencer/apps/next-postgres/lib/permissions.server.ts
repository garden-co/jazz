// Who can do what in Wequencer, enforced by the server:
//
// - A session is visible to its creator and its members; its members list,
//   tracks, patterns, steps, transport and presence to its members.
// - Anyone signed in creates sessions. The creator (`created_by`, immutable)
//   administers the session: adds and removes members, deletes tracks and
//   patterns. The `owner` role is only the creator's initial membership.
// - Editors and owners change tracks, patterns, pads and the transport; a
//   pad's track and pattern, and the transport's pattern, must belong to the
//   session. Viewers only read.
// - Members publish presence for their own profile.
//
// Reads put these SQL fragments in their WHERE clause; writes call the
// asserts inside their transaction.
import { one, type Db } from "@/lib/db.server";

export class Forbidden extends Error {}

/** SQL: the account in `me` has a membership in session `sessionId`. */
export const isMemberSql = (sessionId: string, me: string) =>
  `EXISTS (SELECT 1 FROM session_members sm WHERE sm.session_id = ${sessionId} AND sm.member_author = ${me})`;
/** SQL: the account may read session `s` (member or creator). */
export const canReadSql = (me: string) => `(s.created_by = ${me} OR ${isMemberSql("s.id", me)})`;

export async function assertCreator(db: Db, me: string, sessionId: string) {
  if (!(await one(db, `SELECT 1 FROM sessions WHERE id = $1 AND created_by = $2`, [sessionId, me])))
    throw new Forbidden("only the session creator can do that");
}

async function assertRole(db: Db, me: string, sessionId: string, roles: string[]) {
  const row = await one(db, `SELECT 1 FROM session_members WHERE session_id = $1 AND member_author = $2 AND role = ANY($3)`, [sessionId, me, roles]);
  if (!row) throw new Forbidden("not allowed in this session");
}
export const assertEditor = (db: Db, me: string, sessionId: string) => assertRole(db, me, sessionId, ["owner", "editor"]);
export const assertMember = (db: Db, me: string, sessionId: string) => assertRole(db, me, sessionId, ["owner", "editor", "viewer"]);

/** The track or pattern must exist and belong to the session. */
export async function assertInSession(db: Db, table: "tracks" | "patterns", id: string, sessionId: string) {
  if (!(await one(db, `SELECT 1 FROM ${table} WHERE id = $1 AND session_id = $2`, [id, sessionId])))
    throw new Forbidden(`${table} row is not in this session`);
}

/** Who may subscribe to a live topic: a session's members and creator to `session:<id>`. */
export async function canSubscribe(db: Db, me: string, topic: string) {
  const [kind, id] = topic.split(":");
  if (kind !== "session" || !id) return false;
  return Boolean(await one(db, `SELECT 1 FROM sessions s WHERE s.id = $1 AND ${canReadSql("$2")}`, [id, me]));
}
