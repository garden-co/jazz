// Writes the BigLabel fixture (fixtures/big-label.mjs) the app's own way, as
// big-label/jazz/seed.ts does:
// - every member signs up through Better Auth and runs the bootstrap route's
//   ensurePersonalOrganization (profile, sign-in email, personal label);
// - each fixture label is created with server authority, with its admin's
//   membership, as the demo-data route does (no mutator creates labels);
// - the label's admin adds the other members as POST /api/members does
//   (server email lookup, then the memberships.add mutator as the admin);
// - everything else (teams, catalogues, artists, team memberships, releases,
//   release teams) goes through the app's mutators as the admin, in one
//   server transaction per label, so their checks apply.
// Prints the harness's ctx as JSON.
import { mustGetMutator } from "@rocicorp/zero";
import { BENCH_LABEL, SECOND_LABEL, bigLabelFixture, releasesNewestFirst } from "../../../../../dev/app-comparisons/fixtures/big-label.mjs";
import { resetDatabase } from "./db";

const fixture = bigLabelFixture();
await resetDatabase();
const { auth } = await import("../src/lib/auth");
const { ensurePersonalOrganization } = await import("../src/lib/bootstrap");
const { addMember, findPersonByEmail } = await import("../src/lib/members");
const { dbProvider, pool } = await import("../src/lib/db.server");
const { mutators } = await import("../mutators");
const { zql } = await import("../schema");

const users: Record<string, { userId: string; personId: string }> = {};
for (const [key, user] of Object.entries(fixture.users)) {
  const { user: created } = await auth.api.signUpEmail({ body: { name: user.name, email: user.email, password: user.password } });
  await ensurePersonalOrganization(created.id, user.name, user.email);
  const person = (await pool.query(`SELECT id FROM people WHERE "userId" = $1`, [created.id])).rows[0];
  users[key] = { userId: created.id, personId: person.id };
}

const ids = new Map<string, string>(); // fixture key -> id
const idOf = (key: string) => {
  if (!ids.has(key)) ids.set(key, crypto.randomUUID());
  return ids.get(key)!;
};
const byLabel = <T extends { labelKey: string }>(rows: T[], labelKey: string) => rows.filter((r) => r.labelKey === labelKey);
type Fn = { fn: (o: { tx: never; ctx: never; args: never }) => Promise<unknown> };
const run = (tx: unknown, userId: string, name: string, args: object) =>
  (mustGetMutator(mutators, name) as unknown as Fn).fn({ tx: tx as never, ctx: { userID: userId } as never, args: args as never });

for (const label of fixture.labels) {
  const [adminMembership, ...others] = byLabel(fixture.memberships, label.key);
  const admin = users[adminMembership.user];
  const organizationId = idOf(label.key);

  // The label and its first admin: server authority, as /api/demo-data.
  await dbProvider.transaction(async (tx) => {
    await tx.mutate.organizations.insert({ id: organizationId, name: label.name, slug: label.slug });
    await tx.mutate.memberships.insert({ id: crypto.randomUUID(), organizationId, personId: admin.personId, userId: admin.userId, role: "admin" });
  });

  // Other members: as /api/members, the server only resolves the email.
  for (const m of others) {
    const person = await findPersonByEmail(fixture.users[m.user].email);
    const status = await addMember(admin.userId, { organizationId, person: person!, role: m.role });
    if (status !== "added") throw new Error(`adding ${m.user} to ${label.key}: ${status}`);
  }

  // Everything else as the admin, through the mutators.
  await dbProvider.transaction(async (tx) => {
    const memberships = await tx.run(zql.memberships.where("organizationId", organizationId));
    const membershipOf = (key: string) => memberships.find((row) => row.userId === users[key].userId)!.id;
    const as = (name: string, args: object) => run(tx, admin.userId, name, { organizationId, ...args });
    for (const t of byLabel(fixture.teams, label.key)) await as("teams.save", { id: idOf(t.key), name: t.name, isNew: true });
    for (const c of byLabel(fixture.catalogues, label.key))
      await as("catalogues.save", { id: idOf(c.key), name: c.name, code: c.code, isNew: true });
    for (const { key, labelKey: _, ...artist } of byLabel(fixture.artists, label.key))
      await as("artists.save", { ...artist, id: idOf(key), isNew: true });
    for (const t of byLabel(fixture.teamMembers, label.key))
      await as("teamAssignments.add", { id: idOf(t.key), teamId: idOf(t.teamKey), membershipId: membershipOf(t.user), role: t.role });
    for (const { key, labelKey: _, artistKey, catalogueKey, releaseDate, catalogSequence: __, ...release } of byLabel(fixture.releases, label.key))
      await as("releases.save", {
        ...release,
        id: idOf(key),
        isNew: true,
        artistId: idOf(artistKey),
        catalogueId: idOf(catalogueKey),
        releaseDate: new Date(releaseDate).getTime(),
      });
    for (const rt of byLabel(fixture.releaseTeams, label.key))
      await as("releaseTeams.add", { id: idOf(rt.key), releaseId: idOf(rt.releaseKey), teamId: idOf(rt.teamKey) });
  });
}
await pool.end();

const [newest, second] = releasesNewestFirst(fixture, BENCH_LABEL);
console.log(
  JSON.stringify({
    labelId: idOf(BENCH_LABEL),
    secondLabelId: idOf(SECOND_LABEL),
    renameReleaseId: idOf(newest.key),
    statusReleaseId: idOf(second.key),
    users: { a: fixture.users.a, b: fixture.users.b },
  }),
);
process.exit(0);
