// Writes the BandChat fixture as its users: each signs up through Better
// Auth, then writes through Jazz as that user (policies apply), the way the
// app would. Prints the ids the harness needs as JSON.
import { auth } from "../server/auth";
import { jazz } from "../server/jazz";
import { app } from "../jazz.generated";
import { bandChatFixture, BUSY_ROOM } from "../../../../../dev/app-comparisons/fixtures/band-chat.mjs";

const fixture = bandChatFixture();
const origin = process.env.APP_ORIGIN ?? "http://127.0.0.1:3000";
const j = await jazz();

const users: Record<string, { db: Awaited<ReturnType<typeof j.forReqUser>>; author: string; profileId: string }> = {};
for (const [key, user] of Object.entries(fixture.users)) {
  const { headers, response } = await auth.api.signUpEmail({
    body: { name: user.name, email: user.email, password: user.password },
    returnHeaders: true,
  });
  const cookie = headers.getSetCookie().map((c) => c.split(";")[0]).join("; ");
  // The app's sign-in creates the Jazz account on first use; do it here.
  await j.createAccountFor(response.user.id);
  const request = new Request(origin, { headers: { cookie } });
  const db = await j.forReqUser(request);
  const author = (await j.accountFor(request))!;
  const profile = await db.insert(app.profiles, { author, displayName: user.name }).wait({ tier: "local" });
  users[key] = { db, author, profileId: profile.id };
}

const roomIds: Record<string, string> = {};
const seed = users.seed;
const others = ["a", "b"] as const;
// The app's own flow: the creator makes the room (with their membership),
// the others ask to join, and the creator admits them.
for (const room of fixture.rooms) {
  const { value } = await seed.db.transaction((tx) => {
    const row = tx.insert(app.rooms, { name: room.name, lastActivityAt: new Date(room.createdAtMs) });
    tx.insert(app.roomMembers, { roomId: row.id, memberAuthor: seed.author, memberProfileId: seed.profileId });
    return row;
  });
  roomIds[room.key] = value.id;
}
await Promise.all(
  fixture.rooms.flatMap((room) =>
    others.map((key) =>
      users[key].db
        .insert(app.joinRequests, { roomId: roomIds[room.key], requester: users[key].author, profileId: users[key].profileId })
        .wait({ tier: "global" }),
    ),
  ),
);
const requests = await seed.db.all(app.joinRequests, { tier: "remote" });
for (const request of requests) {
  await seed.db.transaction((tx) => {
    tx.insert(app.roomMembers, { roomId: request.roomId, memberAuthor: request.requester, memberProfileId: request.profileId });
    tx.delete(app.joinRequests, request.id);
  });
}
console.error(`  admitted ${requests.length} memberships`);
let i = 0;
const pending: Promise<unknown>[] = [];
for (const message of fixture.messages) {
  const u = users[message.sender];
  pending.push(u.db.insert(app.messages, { roomId: roomIds[message.roomKey], senderId: u.profileId, text: message.text }).wait({ tier: "global" }));
  if (++i % 500 === 0) {
    await Promise.all(pending.splice(0));
    console.error(`  ${i} messages`);
  }
}
await Promise.all(pending);
await j.close();
console.log(JSON.stringify({ busyRoomId: roomIds[BUSY_ROOM], quietRoomId: roomIds["room-1"], users: fixture.users }));
