// Runs the server's write handlers, member routes and live-query SQL against
// the seeded database as different users, checking the README's "Roles" and
// "Admission boundary". Run after `pnpm seed` (with `--conditions
// react-server`, for the server-only modules). It writes test rows, so seed
// again before a benchmark run.
import { randomUUID } from "node:crypto";
import pg from "pg";
import { DATABASE_URL } from "./db";

const { runMutation } = await import("../src/lib/mutators.server");
const { liveQueries } = await import("../src/lib/live-queries.server");
const { addMember, findPersonByEmail } = await import("../src/lib/members");
const { ensurePersonalOrganization } = await import("../src/lib/bootstrap");
const { pool: appPool } = await import("../src/lib/db.server");

const pool = new pg.Pool({ connectionString: DATABASE_URL });
let failures = 0;
let passes = 0;
const check = (label: string, ok: boolean, detail = "") => {
  if (ok) passes++;
  else failures++;
  console.log(`${ok ? "ok  " : "FAIL"} ${label}${!ok && detail ? `: ${detail}` : ""}`);
};
async function expect(label: string, ok: boolean, user: string, mutator: string, args: object) {
  const result = await runMutation(pool, user, mutator, args);
  const error = result.kind === "rejected" ? result.error : undefined;
  check(label, ok ? !error : !!error, error ?? "accepted");
  return error;
}
async function rows(user: string, name: string, args: object = {}) {
  const query = liveQueries[name]!(user, args) as unknown as { sql: string; parameters: { value: unknown }[] };
  return (await pool.query(query.sql, query.parameters.map((p) => p.value))).rows;
}
const one = async <T = Record<string, string>>(sql: string, params: unknown[] = []) => (await pool.query(sql, params)).rows[0] as T;

const users = Object.fromEntries(
  (await pool.query(`SELECT u.id, u.email FROM neon_auth."user" u WHERE u.email LIKE '%@bench.invalid'`)).rows.map((r) => [
    r.email.split("@")[0],
    r.id,
  ]),
) as Record<string, string>;
const label0 = (await one(`SELECT id FROM organizations WHERE slug = 'label-1'`)).id!;
const label1 = (await one(`SELECT id FROM organizations WHERE slug = 'label-2'`)).id!;
const viewer = users.l0m3!; // label 0's viewer (fixture: every third member)
const outsider = randomUUID(); // signed in, no labels
const release0 = await one(`SELECT * FROM releases WHERE organization_id = $1 ORDER BY release_date DESC LIMIT 1`, [label0]);
const taken0 = await one(`SELECT catalog_number FROM releases WHERE organization_id = $1 AND id <> $2 LIMIT 1`, [label0, release0.id]);
const artist1 = (await one(`SELECT id FROM artists WHERE organization_id = $1 LIMIT 1`, [label1])).id!;
const catalogue1 = (await one(`SELECT id FROM catalogues WHERE organization_id = $1 LIMIT 1`, [label1])).id!;
const team0 = (await one(`SELECT id FROM teams WHERE organization_id = $1 ORDER BY name LIMIT 1`, [label0])).id!;
const team1 = (await one(`SELECT id FROM teams WHERE organization_id = $1 LIMIT 1`, [label1])).id!;
const membership1 = (await one(`SELECT id FROM memberships WHERE organization_id = $1 LIMIT 1`, [label1])).id!;
const membershipB = (await one(`SELECT id FROM memberships WHERE organization_id = $1 AND user_id = $2`, [label0, users.b])).id!;
const page = { organizationId: label0, sort: "releaseDate", direction: "desc", limit: 21, offset: 0 };
const saveArgs = (fields: Record<string, unknown> = {}) => ({
  id: release0.id,
  isNew: false,
  organizationId: label0,
  title: release0.title,
  artistId: release0.artist_id,
  catalogueId: release0.catalogue_id,
  catalogNumber: release0.catalog_number,
  format: release0.format,
  releaseDate: release0.release_date,
  status: release0.status,
  ...fields,
});
const newRelease = (fields: Record<string, unknown> = {}) => saveArgs({ id: randomUUID(), isNew: true, catalogNumber: `T-${randomUUID().slice(0, 8)}`, ...fields });

// ---- Reads -------------------------------------------------------------------
check("member reads the label's first page", (await rows(users.b!, "releases.page", page)).length === 21);
check("outsider reads no releases", (await rows(outsider, "releases.page", page)).length === 0);
check("non-member reads no releases of another label", (await rows(users.b!, "releases.page", { ...page, organizationId: label1 })).length === 0);
check("outsider reads no counts", (await rows(outsider, "labels.counts", { organizationId: label0 })).length === 0);
check("member reads the counts", (await rows(users.b!, "labels.counts", { organizationId: label0 }))[0]?.releases === 512);
check("member sees co-members' names", (await rows(users.b!, "memberships.byLabel", { organizationId: label0 })).every((m) => m.person?.name));
check("outsider sees nobody", (await rows(outsider, "memberships.byLabel", { organizationId: label0 })).length === 0);
check("non-member sees nobody of another label", (await rows(users.b!, "memberships.byLabel", { organizationId: label1 })).length === 0);
check("own memberships only", (await rows(users.b!, "memberships.mine")).every((m) => m.userId === users.b));
check("outsider reads no release by id", (await rows(outsider, "releases.one", { organizationId: label0, id: release0.id })).length === 0);
check(
  "no read for another label's release under your label's id",
  (await rows(users.a!, "releases.one", { organizationId: label1, id: release0.id })).length === 0,
);
const searched = await rows(users.a!, "releases.page", { ...page, search: "Summer" });
check("search is a case-insensitive contains on searchKey", searched.length > 0 && searched.every((r) => r.searchKey.includes("summer")));
check(
  "no live query reads sign-in emails",
  Object.entries(liveQueries).every(([name, build]) => {
    const args = { ...page, sort: name === "artists.page" ? "name" : "releaseDate", id: "x", artistId: "x", teamId: "x", releaseId: "x" };
    return !(build(outsider, args) as unknown as { sql: string }).sql.includes("person_emails");
  }),
);

// ---- Writes: roles -----------------------------------------------------------
await expect("editor edits a release", true, users.b!, "releases.save", saveArgs({ status: "released" }));
await expect("viewer can't edit a release", false, viewer, "releases.save", saveArgs({ status: "planning" }));
await expect("outsider can't edit a release", false, outsider, "releases.save", saveArgs({ status: "planning" }));
await expect("editor adds a release", true, users.b!, "releases.save", newRelease());
await expect("viewer can't add a release", false, viewer, "releases.save", newRelease());
await expect("editor can't delete a release", false, users.b!, "releases.delete", { id: release0.id });
await expect("editor adds an artist", true, users.b!, "artists.create", { id: randomUUID(), organizationId: label0, name: "Check Artist", genre: "Jazz", status: "active" });
await expect("viewer can't add an artist", false, viewer, "artists.create", { id: randomUUID(), organizationId: label0, name: "X", genre: "Jazz", status: "active" });
const artistWithReleases = release0.artist_id;
const deleted = await expect("admin can't delete an artist with releases", false, users.a!, "artists.delete", { id: artistWithReleases });
check("…and is told why", deleted === "Delete this artist's releases first", deleted);
await expect("editor can't create a team", false, users.b!, "teams.create", { id: randomUUID(), organizationId: label0, name: "X" });
await expect("admin creates a team", true, users.a!, "teams.create", { id: randomUUID(), organizationId: label0, name: "Check team" });
await expect("editor can't staff a team", false, users.b!, "teamAssignments.add", { id: randomUUID(), organizationId: label0, teamId: team0, membershipId: membershipB, role: "member" });
await expect("editor can't add a catalogue", false, users.b!, "catalogues.create", { id: randomUUID(), organizationId: label0, name: "X", code: "X" });
await expect("editor can't rename the label", false, users.b!, "organizations.rename", { id: label0, name: "X" });
await expect("admin of label 0 is only an editor in label 1", false, users.a!, "organizations.rename", { id: label1, name: "X" });
await expect("editor assigns a team to a release", true, users.b!, "releaseTeams.add", { id: randomUUID(), organizationId: label0, releaseId: release0.id, teamId: team0 });
await expect("viewer can't assign a team", false, viewer, "releaseTeams.add", { id: randomUUID(), organizationId: label0, releaseId: release0.id, teamId: team0 });
await expect("editor can't change roles", false, users.b!, "memberships.setRole", { id: membershipB, role: "admin" });
await expect("admin changes a role", true, users.a!, "memberships.setRole", { id: membershipB, role: "viewer" });
await expect("…and back", true, users.a!, "memberships.setRole", { id: membershipB, role: "editor" });
await expect("no write creates labels", false, users.a!, "organizations.create", { id: randomUUID(), name: "X", slug: "x" });

// ---- Writes: cross-tenant references -----------------------------------------
await expect("release → another label's artist", false, users.a!, "releases.save", newRelease({ artistId: artist1 }));
await expect("release → another label's catalogue", false, users.a!, "releases.save", newRelease({ catalogueId: catalogue1 }));
await expect(
  "release from another label under your label's id",
  false,
  users.a!,
  "releases.save",
  { ...saveArgs(), id: (await one(`SELECT id FROM releases WHERE organization_id = $1 LIMIT 1`, [label1])).id },
);
await expect("team member → another label's membership", false, users.a!, "teamAssignments.add", { id: randomUUID(), organizationId: label0, teamId: team0, membershipId: membership1, role: "member" });
await expect("team member → another label's team", false, users.a!, "teamAssignments.add", { id: randomUUID(), organizationId: label0, teamId: team1, membershipId: membershipB, role: "member" });
await expect("release team → another label's team", false, users.a!, "releaseTeams.add", { id: randomUUID(), organizationId: label0, releaseId: release0.id, teamId: team1 });

// ---- Unique catalogue numbers ------------------------------------------------
const taken = await expect("catalogue number already in use", false, users.b!, "releases.save", saveArgs({ catalogNumber: taken0.catalog_number }));
check("…and is told so", taken === `${taken0.catalog_number} is already in use`, taken);
const number = `RACE-${randomUUID().slice(0, 6)}`;
const race = await Promise.all(
  Array.from({ length: 4 }, () => runMutation(pool, users.b!, "releases.save", newRelease({ catalogNumber: number }))),
);
check("concurrent saves of one number: exactly one wins", race.filter((r) => r.kind === "committed").length === 1);

// ---- Admission ---------------------------------------------------------------
const newcomer = randomUUID();
const email = `newcomer-${newcomer.slice(0, 8)}@bench.invalid`;
const first = await ensurePersonalOrganization(newcomer, "Newcomer", email);
const again = await ensurePersonalOrganization(newcomer, "Newcomer", email);
const personal = await pool.query(`SELECT 1 FROM memberships WHERE user_id = $1`, [newcomer]);
check("bootstrap is idempotent: one personal label and membership", first.id === again.id && personal.rowCount === 1);
const person = (await findPersonByEmail(appPool, email))!;
check("server finds a signed-in person by email", person?.userId === newcomer);
check("editor can't add members", (await addMember(appPool, users.b!, { organizationId: label0, person, role: "viewer" })) === "forbidden");
check("admin can't add an admin", (await addMember(appPool, users.a!, { organizationId: label0, person, role: "admin" })) === "forbidden");
check("newcomer reads nothing of label 0 yet", (await rows(newcomer, "releases.page", page)).length === 0);
check("admin adds a viewer by email", (await addMember(appPool, users.a!, { organizationId: label0, person, role: "viewer" })) === "added");
check("a second add is a no-op", (await addMember(appPool, users.a!, { organizationId: label0, person, role: "viewer" })) === "already-member");
check("the new member reads label 0", (await rows(newcomer, "releases.page", page)).length === 21);
const membershipN = (await one(`SELECT id FROM memberships WHERE organization_id = $1 AND user_id = $2`, [label0, newcomer])).id!;
await expect("a viewer can't promote themselves", false, newcomer, "memberships.setRole", { id: membershipN, role: "admin" });
await expect("admin removes the member", true, users.a!, "memberships.remove", { id: membershipN });
check("a removed member reads nothing", (await rows(newcomer, "releases.page", page)).length === 0);

await appPool.end();
await pool.end();
console.log(failures ? `${failures} FAILED, ${passes} passed` : `all ${passes} permission checks passed`);
process.exit(failures ? 1 : 0);
