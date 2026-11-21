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

const userOf = async (key: string) => (await pool.query(`SELECT id FROM auth."user" WHERE email = $1`, [`${key}@bench.invalid`])).rows[0].id as string;
const [a, b, viewer, outsider] = [await userOf("a"), await userOf("b"), await userOf("l0m3"), await userOf("l2m1")];
const labelId = async (name: string) => (await pool.query(`SELECT id FROM organizations WHERE name = $1`, [name])).rows[0].id as string;
const [l0, l1] = [await labelId("Northwind Records"), await labelId("Low Tide Records")];
const first = async (sql: string, params: unknown[]) => (await pool.query(sql, params)).rows[0];
const release = await first(`SELECT * FROM releases WHERE "organizationId" = $1 ORDER BY "releaseDate" DESC LIMIT 1`, [l0]);
const taken = await first(`SELECT "catalogNumber" FROM releases WHERE "organizationId" = $1 AND id <> $2 LIMIT 1`, [l0, release.id]);
const foreignArtist = await first(`SELECT id FROM artists WHERE "organizationId" = $1 LIMIT 1`, [l1]);
const foreignTeam = await first(`SELECT id FROM teams WHERE "organizationId" = $1 LIMIT 1`, [l1]);
const team = await first(`SELECT id FROM teams WHERE "organizationId" = $1 LIMIT 1`, [l0]);
const bMembership = await first(`SELECT id FROM memberships WHERE "organizationId" = $1 AND "userId" = $2`, [l0, b]);
const outsiderPerson = await first(`SELECT id FROM people WHERE "userId" = $1`, [outsider]);
const { organizationId: _, catalogSequence: __, ...fields } = release;
const save = { ...fields, organizationId: l0, isNew: false };
const page = { organizationId: l0, search: "", status: null, sort: "releaseDate", direction: "desc", start: null, limit: 21 };

check("emails are not in the Zero schema", !("personEmails" in (await import("../schema")).schema.tables));
check("an outsider reads no releases", ((await rows(outsider, queries.releases.page, page)) as unknown[]).length === 0);
check("a member reads the first page", ((await rows(b, queries.releases.page, page)) as unknown[]).length === 21);
check("an outsider reads no people of the label", ((await rows(outsider, queries.memberships.ofLabel, { organizationId: l0, limit: 500 })) as unknown[]).length === 0);
await expect("an outsider can't edit a release", false, outsider, mutators.releases.save, { ...save, title: "x" });
await expect("a viewer can't edit a release", false, viewer, mutators.releases.save, { ...save, title: "x" });
await expect("an editor edits a release", true, b, mutators.releases.save, { ...save, status: "released" });
await expect("catalogue numbers are unique per label", false, b, mutators.releases.save, { ...save, catalogNumber: taken.catalogNumber });
await expect("a release can't point at another label's artist", false, b, mutators.releases.save, { ...save, artistId: foreignArtist.id });
await expect("an editor can't delete a release", false, b, mutators.releases.delete, { id: release.id, organizationId: l0 });
await expect("an editor assigns a team", true, b, mutators.releaseTeams.add, { id: crypto.randomUUID(), organizationId: l0, releaseId: release.id, teamId: team.id });
await expect("an editor can't assign another label's team", false, b, mutators.releaseTeams.add, { id: crypto.randomUUID(), organizationId: l0, releaseId: release.id, teamId: foreignTeam.id });
await expect("an editor can't create a team", false, b, mutators.teams.save, { id: crypto.randomUUID(), organizationId: l0, name: "x", isNew: true });
await expect("an editor can't promote themselves", false, b, mutators.memberships.setRole, { id: bMembership.id, organizationId: l0, role: "admin" });
await expect("an editor can't add members", false, b, mutators.memberships.add, { id: crypto.randomUUID(), organizationId: l0, personId: outsiderPerson.id, userId: outsider, role: "viewer" });
await expect("an admin can't add an admin", false, a, mutators.memberships.add, { id: crypto.randomUUID(), organizationId: l0, personId: outsiderPerson.id, userId: outsider, role: "admin" });
await expect("an admin can't add someone under another account", false, a, mutators.memberships.add, { id: crypto.randomUUID(), organizationId: l0, personId: outsiderPerson.id, userId: b, role: "viewer" });
await expect("an admin can't staff another label's team", false, a, mutators.teamAssignments.add, { id: crypto.randomUUID(), organizationId: l0, teamId: foreignTeam.id, membershipId: bMembership.id, role: "member" });
await expect("an admin can't delete an artist with releases", false, a, mutators.artists.delete, { id: release.artistId, organizationId: l0 });
await expect("an editor of label 1 can't act as label 0's admin", false, a, mutators.organizations.rename, { id: l1, name: "mine" });
await expect("an admin renames their label", true, a, mutators.organizations.rename, { id: l0, name: "Northwind Records" });
const { addMember } = await import("../src/lib/members");
check("an admin adds a viewer by the route's path", (await addMember(a, { organizationId: l0, person: { id: outsiderPerson.id, userId: outsider }, role: "viewer" })) === "added");
check("adding them twice adds them once", (await addMember(a, { organizationId: l0, person: { id: outsiderPerson.id, userId: outsider }, role: "viewer" })) === "already-member");
check("the route's path answers editors 'forbidden'", (await addMember(b, { organizationId: l0, person: { id: outsiderPerson.id, userId: outsider }, role: "viewer" })) === "forbidden");
await pool.end();
console.log(failures ? `${failures} FAILED` : "all permission checks passed");
process.exit(failures ? 1 : 0);
