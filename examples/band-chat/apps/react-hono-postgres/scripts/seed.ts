// Resets the database and writes the BandChat fixture: the three users
// through Better Auth's server API, everything else straight into Postgres.
// Prints the ids the harness needs.
import pg from "pg";
import { BUSY_ROOM, bandChatFixture } from "../../../../../dev/app-comparisons/fixtures/band-chat.mjs";
import { DATABASE_URL, resetDatabase } from "./db";

const fixture = bandChatFixture();
await resetDatabase();
const { auth } = await import("../server/auth");

const userIds: Record<string, string> = {};
const profileIds: Record<string, string> = {};
for (const [key, user] of Object.entries(fixture.users)) {
  const { user: created } = await auth.api.signUpEmail({ body: user });
  userIds[key] = created.id;
  profileIds[key] = crypto.randomUUID();
}
const roomIds = Object.fromEntries(fixture.rooms.map((room) => [room.key, crypto.randomUUID()]));

const db = new pg.Client({ connectionString: DATABASE_URL });
await db.connect();
const insert = (table: string, rows: object[]) =>
  db.query(`INSERT INTO "${table}" SELECT * FROM json_populate_recordset(null::"${table}", $1)`, [
    JSON.stringify(rows),
  ]);
const t0 = fixture.rooms[0]!.createdAtMs;

await db.query("BEGIN");
await insert(
  "profiles",
  Object.entries(fixture.users).map(([key, user]) => ({
    id: profileIds[key],
    author: userIds[key],
    displayName: user.name,
    createdAt: t0,
  })),
);
await insert(
  "rooms",
  fixture.rooms.map((room) => ({
    id: roomIds[room.key],
    name: room.name,
    createdBy: userIds.seed,
    createdAt: room.createdAtMs,
    // As the Jazz seed does: activity at creation, so every room reads unread.
    lastActivityAt: room.createdAtMs,
  })),
);
await insert(
  "roomMembers",
  fixture.members.map((member) => ({
    id: crypto.randomUUID(),
    roomId: roomIds[member.roomKey],
    memberAuthor: userIds[member.user],
    memberProfileId: profileIds[member.user],
  })),
);
await insert(
  "messages",
  fixture.messages.map((message) => ({
    id: crypto.randomUUID(),
    roomId: roomIds[message.roomKey],
    senderId: profileIds[message.sender],
    text: message.text,
    canvasId: null,
    createdAt: message.createdAtMs,
  })),
);
await db.query("COMMIT");
await db.end();

console.log(JSON.stringify({ busyRoomId: roomIds[BUSY_ROOM], quietRoomId: roomIds["room-1"], users: fixture.users }));
process.exit(0);
