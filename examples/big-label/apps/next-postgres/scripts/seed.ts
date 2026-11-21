// Writes the BigLabel fixture (fixtures/big-label.mjs) the app's own way, as
// big-label/jazz/seed.ts does:
// - every member signs up through Better Auth and runs the bootstrap
//   (POST /api/bootstrap's ensurePersonalOrganization: their profile, sign-in
//   email and personal label);
// - each fixture label is created by the server with its admin's membership,
//   as the demo-data route does (browsers can never create labels);
// - the label's admin adds the other members as POST /api/members does
//   (server email lookup, then addMember as the admin);
// - everything else (teams, catalogues, artists, releases, assignments) goes
//   through the API's write handlers as the admin, so the role checks and the
//   tenant foreign keys apply.
// Prints the harness's ctx as JSON.
import { randomUUID } from "node:crypto";
import { bigLabelFixture, releasesNewestFirst, BENCH_LABEL, SECOND_LABEL } from "../../../../../dev/app-comparisons/fixtures/big-label.mjs";
import { resetDatabase } from "./db";

const fixture = bigLabelFixture();
await resetDatabase();
const { auth } = await import("../src/lib/auth");
const { one, pool, transaction } = await import("../src/lib/db.server");
const { ensurePersonalOrganization } = await import("../src/lib/bootstrap");
const { addMember, findPersonByEmail } = await import("../src/lib/members");
const { handlers } = await import("../src/lib/writes.server");

const users: Record<string, { userId: string; personId: string }> = {};
for (const [key, user] of Object.entries(fixture.users)) {
  const { user: created } = await auth.api.signUpEmail({ body: { name: user.name, email: user.email, password: user.password } });
  await ensurePersonalOrganization(created.id, user.name, user.email);
  const person = (await one<{ id: string }>(pool, `SELECT id FROM people WHERE "userId" = $1`, [created.id]))!;
  users[key] = { userId: created.id, personId: person.id };
}

const ids = new Map<string, string>(); // fixture key -> row id
const byLabel = <T extends { labelKey: string }>(rows: T[], labelKey: string) => rows.filter((r) => r.labelKey === labelKey);

for (const label of fixture.labels) {
  const [adminMembership, ...others] = byLabel(fixture.memberships, label.key);
  const admin = users[adminMembership.user]!;

  // The label and its first admin: the server, as /api/demo-data.
  const organizationId = randomUUID();
  ids.set(label.key, organizationId);
  await transaction(async (tx) => {
    await tx.query(`INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)`, [organizationId, label.name, label.slug]);
    await tx.query(`INSERT INTO memberships (id, "organizationId", "personId", "userId", role) VALUES ($1, $2, $3, $4, 'admin')`, [
      randomUUID(), organizationId, admin.personId, admin.userId,
    ]);
  });

  // Other members: as /api/members, the server only resolves the email.
  for (const m of others) {
    const person = await findPersonByEmail(fixture.users[m.user].email);
    const status = await addMember(admin.userId, { organizationId, person: person!, role: m.role });
    if (status !== "added") throw new Error(`adding ${m.user} to ${label.key}: ${status}`);
  }
  const membershipOf = async (key: string) =>
    (await one<{ id: string }>(pool, `SELECT id FROM memberships WHERE "organizationId" = $1 AND "userId" = $2`, [organizationId, users[key]!.userId]))!.id;

  // Everything else as the admin, through the write handlers, one transaction per label.
  await transaction(async (tx) => {
    const insert = (table: string, key: string | null, row: Record<string, unknown>) => {
      const id = randomUUID();
      if (key) ids.set(key, id);
      return handlers.insert!(tx, admin.userId, { table, id, row: { ...row, organizationId } });
    };
    for (const t of byLabel(fixture.teams, label.key)) await insert("teams", t.key, { name: t.name });
    for (const c of byLabel(fixture.catalogues, label.key)) await insert("catalogues", c.key, { name: c.name, code: c.code });
    for (const { key, labelKey: _, ...artist } of byLabel(fixture.artists, label.key)) await insert("artists", key, artist);
    for (const t of byLabel(fixture.teamMembers, label.key))
      await insert("teamAssignments", null, { teamId: ids.get(t.teamKey), membershipId: await membershipOf(t.user), role: t.role });
    for (const { key, labelKey: _, artistKey, catalogueKey, ...release } of byLabel(fixture.releases, label.key))
      await insert("releases", key, { ...release, artistId: ids.get(artistKey), catalogueId: ids.get(catalogueKey) });
    for (const rt of byLabel(fixture.releaseTeams, label.key))
      await insert("releaseTeams", null, { releaseId: ids.get(rt.releaseKey), teamId: ids.get(rt.teamKey) });
  });
}

const [newest, second] = releasesNewestFirst(fixture, BENCH_LABEL);
console.log(
  JSON.stringify({
    labelId: ids.get(BENCH_LABEL),
    secondLabelId: ids.get(SECOND_LABEL),
    renameReleaseId: ids.get(newest.key),
    statusReleaseId: ids.get(second.key),
    users: { a: fixture.users.a, b: fixture.users.b },
  }),
);
process.exit(0);
