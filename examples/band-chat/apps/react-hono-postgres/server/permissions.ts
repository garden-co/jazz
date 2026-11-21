// Who can do what in BandChat, enforced by the server:
//
// - A room, its members, messages, reactions and sketches are for its
//   members; the room's creator also sees the room itself (and its join
//   requests) without being a member.
// - Anyone signed in creates rooms; the creator renames the room and admits
//   members, and only people who asked to join (or the creator) can be
//   admitted, each with their own profile.
// - The creator removes members; a member may also leave on their own.
// - Members post messages (as their own profile), react, start sketches and
//   draw; senders delete their own messages, authors their own reactions and
//   strokes.
// - Anyone with a room link may ask to join; the requester or the creator
//   withdraws the request. Join requests are for the requester and creator.
// - Read markers are private to their reader; profiles are written by their
//   owner and visible to people who share a room or a join request with them.
//
// Reads put these SQL fragments in their WHERE clause; writes call the asserts
// inside their transaction.
import { one, type Db } from "./db";

export class Forbidden extends Error {}

/** SQL: the account in parameter `me` is a member of room `roomId`. */
export const isMemberSql = (roomId: string, me: string) =>
  `EXISTS (SELECT 1 FROM "roomMembers" rm WHERE rm."roomId" = ${roomId} AND rm."memberAuthor" = ${me})`;
/** SQL: the account may read room `r` (member or creator). */
export const canReadRoomSql = (me: string) => `(r."createdBy" = ${me} OR ${isMemberSql("r.id", me)})`;

export async function assertMember(db: Db, me: string, roomId: string) {
  if (!(await one(db, `SELECT 1 WHERE ${isMemberSql("$1", "$2")}`, [roomId, me])))
    throw new Forbidden("You are not a member of this room");
}

export async function assertCreator(db: Db, me: string, roomId: string) {
  if (!(await one(db, `SELECT 1 FROM rooms WHERE id = $1 AND "createdBy" = $2`, [roomId, me])))
    throw new Forbidden("Only the room creator can do that");
}

export async function assertMyProfile(db: Db, me: string, profileId: string) {
  if (!(await one(db, `SELECT 1 FROM profiles WHERE id = $1 AND author = $2`, [profileId, me])))
    throw new Forbidden("That is not your profile");
}

/** Who may subscribe to a live topic: a room's members and creator to `room:<id>`. */
export async function canSubscribe(db: Db, me: string, topic: string) {
  const [kind, id] = topic.split(":");
  if (kind !== "room" || !id) return false;
  return Boolean(await one(db, `SELECT 1 FROM rooms r WHERE r.id = $1 AND ${canReadRoomSql("$2")}`, [id, me]));
}
