// Runs the server-side mutators and queries against the seeded database as
// different users, checking "Who can do what". Run after `pnpm seed`.
import pg from "pg";
import { zeroNodePg } from "@rocicorp/zero/server/adapters/pg";
import { mutators } from "../mutators";
import { queries } from "../queries";
import { schema } from "../schema";
import { DATABASE_URL } from "./db";

const pool = new pg.Pool({ connectionString: DATABASE_URL });
const db = zeroNodePg(schema, pool);
let failures = 0;
type Mut = { fn: (o: { tx: never; args: never; ctx: never }) => Promise<unknown> };
async function expect(label: string, ok: boolean, user: string, mutator: Mut, args: object) {
  let error: unknown;
  try {
    await db.transaction((tx) => mutator.fn({ tx: tx as never, args: args as never, ctx: { userID: user } as never }));
  } catch (e) {
    error = e;
  }
  const passed = ok ? !error : !!error;
  if (!passed) failures++;
  console.log(`${passed ? "ok  " : "FAIL"} ${label}${error && ok ? `: ${(error as Error).message}` : ""}`);
}
async function rows(user: string, query: { fn: (o: { args: never; ctx: never }) => unknown }, args: object) {
  return db.transaction((tx) => tx.run(query.fn({ args: args as never, ctx: { userID: user } as never }) as never)) as Promise<unknown[] | unknown>;
}
const check = (label: string, ok: boolean) => (console.log(`${ok ? "ok  " : "FAIL"} ${label}`), ok || failures++);

const { rows: [busy] } = await pool.query(`SELECT id FROM rooms WHERE name = 'Busy room'`);
const users = Object.fromEntries((await pool.query(`SELECT u.id, u.email FROM auth."user" u`)).rows.map((r) => [r.email.split("@")[0], r.id]));
const profile = Object.fromEntries((await pool.query(`SELECT id, author FROM profiles`)).rows.map((r) => [r.author, r.id]));
const { rows: [theirs] } = await pool.query(`SELECT id FROM messages WHERE "roomId" = $1 AND "senderId" = $2 LIMIT 1`, [busy.id, profile[users.b]]);
const outsider = crypto.randomUUID();
const outsiderProfile = crypto.randomUUID();

check("outsider reads no messages", ((await rows(outsider, queries.messages.newest, { roomId: busy.id })) as unknown[]).length === 0);
check("member reads the newest 200", ((await rows(users.a, queries.messages.newest, { roomId: busy.id })) as unknown[]).length === 200);
await expect("outsider makes a profile", true, outsider, mutators.profiles.create, { id: outsiderProfile, displayName: "Outsider" });
await expect("outsider can't send", false, outsider, mutators.messages.send, { id: crypto.randomUUID(), roomId: busy.id, senderId: outsiderProfile, text: "hi" });
await expect("outsider can't admit themselves", false, outsider, mutators.members.admit, { id: crypto.randomUUID(), roomId: busy.id, memberAuthor: outsider, memberProfileId: outsiderProfile });
await expect("outsider can't ask with someone else's profile", false, outsider, mutators.joinRequests.create, { id: crypto.randomUUID(), roomId: busy.id, profileId: profile[users.a] });
await expect("member can't admit someone who didn't ask", false, users.seed, mutators.members.admit, { id: crypto.randomUUID(), roomId: busy.id, memberAuthor: outsider, memberProfileId: outsiderProfile });
await expect("outsider asks to join", true, outsider, mutators.joinRequests.create, { id: crypto.randomUUID(), roomId: busy.id, profileId: outsiderProfile });
check("only the creator sees the request", ((await rows(users.a, queries.joinRequests.byRoom, { roomId: busy.id })) as unknown[]).length === 0 && ((await rows(users.seed, queries.joinRequests.byRoom, { roomId: busy.id })) as unknown[]).length === 1);
await expect("a non-creator can't admit", false, users.a, mutators.members.admit, { id: crypto.randomUUID(), roomId: busy.id, memberAuthor: outsider, memberProfileId: outsiderProfile });
await expect("the creator admits", true, users.seed, mutators.members.admit, { id: crypto.randomUUID(), roomId: busy.id, memberAuthor: outsider, memberProfileId: outsiderProfile });
await expect("the new member sends", true, outsider, mutators.messages.send, { id: crypto.randomUUID(), roomId: busy.id, senderId: outsiderProfile, text: "hi" });
await expect("can't send as someone else", false, users.a, mutators.messages.send, { id: crypto.randomUUID(), roomId: busy.id, senderId: profile[users.b], text: "spoof" });
await expect("can't delete someone else's message", false, users.a, mutators.messages.delete, { id: theirs.id });
await expect("member can't rename", false, users.a, mutators.rooms.rename, { id: busy.id, name: "mine" });
await expect("creator renames", true, users.seed, mutators.rooms.rename, { id: busy.id, name: "Busy room" });
const { rows: [aMember] } = await pool.query(`SELECT id FROM "roomMembers" WHERE "roomId" = $1 AND "memberAuthor" = $2`, [busy.id, users.a]);
await expect("member can't remove another member", false, users.b, mutators.members.remove, { id: aMember.id });
await expect("members react", true, users.a, mutators.reactions.toggle, { id: crypto.randomUUID(), roomId: busy.id, messageId: theirs.id, emoji: "🎸" });
const { rows: [quiet] } = await pool.query(`SELECT id FROM rooms WHERE name = 'Room 2'`);
await expect("can't react with a mismatched room", false, users.a, mutators.reactions.toggle, { id: crypto.randomUUID(), roomId: quiet.id, messageId: theirs.id, emoji: "🎸" });
await expect("a member moves their read marker", true, users.a, mutators.readMarkers.mark, { id: "marker-a", roomId: busy.id, lastReadAt: 1 });
await expect("can't move someone else's read marker", false, users.b, mutators.readMarkers.mark, { id: "marker-a", roomId: busy.id, lastReadAt: 2 });
const { rows: [outsiderMember] } = await pool.query(`SELECT id FROM "roomMembers" WHERE "roomId" = $1 AND "memberAuthor" = $2`, [busy.id, outsider]);
await expect("the creator removes a member", true, users.seed, mutators.members.remove, { id: outsiderMember.id });
await expect("a removed member can't send", false, outsider, mutators.messages.send, { id: crypto.randomUUID(), roomId: busy.id, senderId: outsiderProfile, text: "late" });
await pool.end();
console.log(failures ? `${failures} FAILED` : "all permission checks passed");
process.exit(failures ? 1 : 0);
