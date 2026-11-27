// Runs the server's write handlers and live-query SQL against the seeded
// database as different users, checking "Who can do what". Run after `pnpm seed`.
import pg from "pg";
import { runMutation } from "../server/mutators";
import { liveQueries } from "../server/queries";
import { DATABASE_URL } from "./db";

const pool = new pg.Pool({ connectionString: DATABASE_URL });
let failures = 0;
async function expect(label: string, ok: boolean, user: string, mutator: string, args: object) {
  let error: unknown;
  try {
    await runMutation(pool, user, mutator, args);
  } catch (e) {
    error = e;
  }
  const passed = ok ? !error : !!error;
  if (!passed) failures++;
  console.log(`${passed ? "ok  " : "FAIL"} ${label}${error && ok ? `: ${(error as Error).message}` : ""}`);
}
async function rows(user: string, name: string, args: object = {}) {
  const query = liveQueries[name](user, args) as unknown as { sql: string; parameters: { value: unknown }[] };
  return (await pool.query(query.sql, query.parameters.map((p) => p.value))).rows;
}
const check = (label: string, ok: boolean) => (console.log(`${ok ? "ok  " : "FAIL"} ${label}`), ok || failures++);
const known = async (user: string) => (await rows(user, "profiles.known")).map((p) => p.id as string);

const { rows: [busy] } = await pool.query(`SELECT id FROM rooms WHERE name = 'Busy room'`);
const users = Object.fromEntries((await pool.query(`SELECT u.id, u.email FROM neon_auth."user" u`)).rows.map((r) => [r.email.split("@")[0], r.id]));
const profile = Object.fromEntries((await pool.query(`SELECT id, author FROM profiles`)).rows.map((r) => [r.author, r.id]));
const { rows: [theirs] } = await pool.query(`SELECT id FROM messages WHERE "roomId" = $1 AND "senderId" = $2 LIMIT 1`, [busy.id, profile[users.b]]);
const outsider = crypto.randomUUID();
const outsiderProfile = crypto.randomUUID();

check("outsider reads no messages", ((await rows(outsider, "messages.newest", { roomId: busy.id })) as unknown[]).length === 0);
check("member reads the newest 200", ((await rows(users.a, "messages.newest", { roomId: busy.id })) as unknown[]).length === 200);
await expect("outsider makes a profile", true, outsider, "profiles.create", { id: outsiderProfile, displayName: "Outsider" });
await expect("outsider can't send", false, outsider, "messages.send", { id: crypto.randomUUID(), roomId: busy.id, senderId: outsiderProfile, text: "hi" });
await expect("outsider can't admit themselves", false, outsider, "members.admit", { id: crypto.randomUUID(), roomId: busy.id, memberAuthor: outsider, memberProfileId: outsiderProfile });
await expect("outsider can't ask with someone else's profile", false, outsider, "joinRequests.create", { id: crypto.randomUUID(), roomId: busy.id, profileId: profile[users.a] });
await expect("member can't admit someone who didn't ask", false, users.seed, "members.admit", { id: crypto.randomUUID(), roomId: busy.id, memberAuthor: outsider, memberProfileId: outsiderProfile });
await expect("outsider asks to join", true, outsider, "joinRequests.create", { id: crypto.randomUUID(), roomId: busy.id, profileId: outsiderProfile });
check("only the creator sees the request", ((await rows(users.a, "joinRequests.byRoom", { roomId: busy.id })) as unknown[]).length === 0 && ((await rows(users.seed, "joinRequests.byRoom", { roomId: busy.id })) as unknown[]).length === 1);
await expect("a non-creator can't admit", false, users.a, "members.admit", { id: crypto.randomUUID(), roomId: busy.id, memberAuthor: outsider, memberProfileId: outsiderProfile });
await expect("the creator admits", true, users.seed, "members.admit", { id: crypto.randomUUID(), roomId: busy.id, memberAuthor: outsider, memberProfileId: outsiderProfile });
await expect("the new member sends", true, outsider, "messages.send", { id: crypto.randomUUID(), roomId: busy.id, senderId: outsiderProfile, text: "hi" });
await expect("can't send as someone else", false, users.a, "messages.send", { id: crypto.randomUUID(), roomId: busy.id, senderId: profile[users.b], text: "spoof" });
await expect("can't delete someone else's message", false, users.a, "messages.delete", { id: theirs.id });
await expect("member can't rename", false, users.a, "rooms.rename", { id: busy.id, name: "mine" });
await expect("creator renames", true, users.seed, "rooms.rename", { id: busy.id, name: "Busy room" });
const { rows: [aMember] } = await pool.query(`SELECT id FROM "roomMembers" WHERE "roomId" = $1 AND "memberAuthor" = $2`, [busy.id, users.a]);
await expect("member can't remove another member", false, users.b, "members.remove", { id: aMember.id });
await expect("members react", true, users.a, "reactions.toggle", { id: crypto.randomUUID(), roomId: busy.id, messageId: theirs.id, emoji: "🎸" });
const { rows: [quiet] } = await pool.query(`SELECT id FROM rooms WHERE name = 'Room 2'`);
await expect("can't react with a mismatched room", false, users.a, "reactions.toggle", { id: crypto.randomUUID(), roomId: quiet.id, messageId: theirs.id, emoji: "🎸" });
await expect("a member moves their read marker", true, users.a, "readMarkers.mark", { id: "marker-a", roomId: busy.id, lastReadAt: 1 });
await expect("can't move someone else's read marker", false, users.b, "readMarkers.mark", { id: "marker-a", roomId: busy.id, lastReadAt: 2 });
const { rows: [outsiderMember] } = await pool.query(`SELECT id FROM "roomMembers" WHERE "roomId" = $1 AND "memberAuthor" = $2`, [busy.id, outsider]);
await expect("the creator removes a member", true, users.seed, "members.remove", { id: outsiderMember.id });
await expect("a removed member can't send", false, outsider, "messages.send", { id: crypto.randomUUID(), roomId: busy.id, senderId: outsiderProfile, text: "late" });
check("a member knows co-members' profiles", (await known(users.a)).includes(profile[users.b]));
check("an outsider knows only their own profile", (await known(outsider)).every((id) => id === outsiderProfile));
check("read markers are private", (await rows(users.b, "readMarkers.mine")).every((m) => m.reader === users.b));
check("a removed member reads no messages", (await rows(outsider, "messages.newest", { roomId: busy.id })).length === 0);
check("unread counts skip my own messages", (await rows(users.a, "messages.unread", { roomId: busy.id, myProfileId: profile[users.a] })).every((m) => m.senderId !== profile[users.a]));
await pool.end();
console.log(failures ? `${failures} FAILED` : "all permission checks passed");
process.exit(failures ? 1 : 0);
