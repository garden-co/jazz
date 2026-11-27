// `pnpm seed`: resets the local deployment and writes fixtures/band-chat.mjs.
// Needs the backend running (`pnpm backend`). Like the Jazz seed, every user
// signs up through the app's auth and writes their profile; a and b ask to
// join each room and the seed user admits them, all through the app's own
// functions (and checks). Rooms with their activity time and the messages
// are written by internal functions, in bulk. Prints the harness ctx as JSON.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { ConvexHttpClient } from "convex/browser";
import { api } from "../convex/_generated/api.js";
import { bandChatFixture, BUSY_ROOM } from "../../../../../dev/app-comparisons/fixtures/band-chat.mjs";

const run = (fn, args = {}) =>
  JSON.parse(execFileSync("npx", ["convex", "run", fn, JSON.stringify(args)], { encoding: "utf8" }) || "null");
const url = readFileSync(".env.local", "utf8").match(/VITE_CONVEX_URL=(.*)/)[1].trim();
const fixture = bandChatFixture();

run("seed:reset");
const users = {};
for (const [key, user] of Object.entries(fixture.users)) {
  const client = new ConvexHttpClient(url);
  const { tokens } = await client.action(api.auth.signIn, {
    provider: "password",
    params: { flow: "signUp", email: user.email, password: user.password, name: user.name },
  });
  client.setAuth(tokens.token);
  const profileId = await client.mutation(api.profiles.create, { displayName: user.name });
  const { author } = await client.query(api.profiles.mine, {});
  users[key] = { client, author, profileId };
}
const seed = users.seed;
const rooms = run("seed:createRooms", { creator: seed.author, profileId: seed.profileId });
const others = ["a", "b"];
await Promise.all(
  Object.values(rooms).flatMap((roomId) =>
    others.map((key) => users[key].client.mutation(api.joinRequests.create, { roomId })),
  ),
);
for (const roomId of Object.values(rooms)) {
  await Promise.all(
    others.map((key) =>
      seed.client.mutation(api.members.admit, {
        roomId,
        memberAuthor: users[key].author,
        memberProfileId: users[key].profileId,
      }),
    ),
  );
}
console.error(`  admitted ${Object.keys(rooms).length * others.length} memberships`);
const profiles = Object.fromEntries(Object.entries(users).map(([key, u]) => [key, u.profileId]));
run("seed:writeMessages", { rooms, profiles });
console.log(JSON.stringify({ busyRoomId: rooms[BUSY_ROOM], quietRoomId: rooms["room-1"], users: fixture.users }));
