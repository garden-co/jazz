// Resets the Instant app and writes the BandChat fixture through the admin
// SDK. Users sign up through the app's own auth (server/auth.ts); rows are
// written as the fixture's users, ending in the state the app's admission flow
// leaves (every user a member of every room, no open requests). Prints the
// ids the harness needs as JSON.
import { id } from "@instantdb/admin";
import { adminDb as db, signUp } from "./server/auth";
import { bandChatFixture, BUSY_ROOM } from "../../../../dev/app-comparisons/fixtures/band-chat.mjs";

const fixture = bandChatFixture();
const BATCH = 250;

/** Temporary apps rate-limit bursts of admin calls: wait and try again. */
async function retry<T>(call: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await call();
    } catch (error) {
      if ((error as { status?: number }).status !== 429 || attempt > 30) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

async function inBatches(chunks: any[]) {
  for (let i = 0; i < chunks.length; i += BATCH)
    await retry(() => db.transact(chunks.slice(i, i + BATCH)));
}

// Reset: delete every row the app has (users keep their ids; their
// credentials are written again below).
const namespaces = [
  "rooms",
  "roomMembers",
  "joinRequests",
  "readMarkers",
  "messages",
  "reactions",
  "canvases",
  "strokes",
  "profiles",
  "credentials",
] as const;
for (const ns of namespaces) {
  for (;;) {
    const rows = (
      (await retry(() => db.query({ [ns]: { $: { limit: 1000, fields: ["id"] } } } as any))) as any
    )[ns] as { id: string }[];
    if (rows.length === 0) break;
    await inBatches(rows.map((row) => (db.tx as any)[ns][row.id].delete()));
  }
}

const now = Date.now();
const users: Record<string, { author: string; profileId: string }> = {};
for (const [key, user] of Object.entries(fixture.users)) {
  await retry(() => signUp(user));
  const author = (await retry(() => db.auth.getUser({ email: user.email })))!.id;
  const profileId = id();
  await retry(() =>
    db.transact(
      db.tx.profiles[profileId].create({ author, displayName: user.name, createdAt: now }),
    ),
  );
  users[key] = { author, profileId };
}

const roomIds: Record<string, string> = {};
const roomChunks = [];
for (const room of fixture.rooms) {
  const roomId = (roomIds[room.key] = id());
  roomChunks.push(
    db.tx.rooms[roomId].create({
      name: room.name,
      creator: users.seed!.author,
      createdAt: room.createdAtMs,
      lastActivityAt: room.createdAtMs,
    }),
  );
}
const memberChunks = fixture.members.map((member) =>
  db.tx.roomMembers[id()]
    .create({ memberAuthor: users[member.user]!.author })
    .link({ room: roomIds[member.roomKey]!, memberProfile: users[member.user]!.profileId }),
);
await inBatches([...roomChunks, ...memberChunks]);
console.error(`  ${fixture.rooms.length} rooms, ${fixture.members.length} memberships`);

const messageChunks = fixture.messages.map((message) =>
  db.tx.messages[id()]
    .create({ text: message.text, createdAt: message.createdAtMs })
    .link({ room: roomIds[message.roomKey]!, sender: users[message.sender]!.profileId }),
);
await inBatches(messageChunks);
console.error(`  ${fixture.messages.length} messages`);

console.log(
  JSON.stringify({
    busyRoomId: roomIds[BUSY_ROOM],
    quietRoomId: roomIds["room-1"],
    users: fixture.users,
  }),
);
process.exit(0);
