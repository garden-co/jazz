// Writes the BigLabel fixture (fixtures/big-label.mjs) the app's own way:
// - every member signs up through the app's sign-up (src/lib/instant.server.ts)
//   and runs its bootstrap (POST /api/bootstrap's ensurePersonalOrganization:
//   their profile, sign-in email and personal label);
// - each fixture label is created with admin authority, with its admin's
//   membership, as the demo-data route does (browsers can never create labels);
// - the label's admin adds the other members as POST /api/members does (admin
//   email lookup, then a membership written as the label's admin);
// - everything else (teams, catalogues, artists, releases, assignments) is
//   written as the admin (`asUser`), so instant.perms.ts applies.
// Resets the app first. Prints the harness's ctx as JSON.
import { id } from "@instantdb/admin";
import { adminDb, signUp, type CallerDb } from "./src/lib/instant.server";
import { ensurePersonalOrganization } from "./src/lib/bootstrap";
import { addMember, findPersonByEmail } from "./src/lib/members";
import { membershipFields } from "./src/lib/membership";
import {
  bigLabelFixture,
  releasesNewestFirst,
  BENCH_LABEL,
  SECOND_LABEL,
} from "../../../../dev/app-comparisons/fixtures/big-label.mjs";

const fixture = bigLabelFixture();
const BATCH = 200;

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
async function inBatches(db: { transact: (chunks: any[]) => Promise<unknown> }, chunks: any[]) {
  for (let i = 0; i < chunks.length; i += BATCH)
    await retry(() => db.transact(chunks.slice(i, i + BATCH)));
}

// Reset: every row the app has (users keep their ids; credentials are written again).
const namespaces = [
  "releaseTeams",
  "releaseAssignments",
  "teamAssignments",
  "releases",
  "artists",
  "catalogues",
  "teams",
  "memberships",
  "personEmails",
  "people",
  "organizations",
  "credentials",
];
for (const ns of namespaces) {
  for (;;) {
    const result: any = await retry(() =>
      adminDb.query({ [ns]: { $: { limit: 1000, fields: ["id"] } } } as any),
    );
    const rows = result[ns] as { id: string }[];
    if (rows.length === 0) break;
    await inBatches(
      adminDb,
      rows.map((row) => (adminDb.tx as any)[ns][row.id].delete()),
    );
  }
}

type User = { db: CallerDb; userId: string; personId: string };
const users: Record<string, User> = {};
for (const [key, user] of Object.entries(fixture.users) as [
  string,
  { name: string; email: string; password: string },
][]) {
  const { token } = await retry(() => signUp(user));
  const userId = (await retry(() => adminDb.auth.getUser({ email: user.email })))!.id;
  await retry(() => ensurePersonalOrganization(userId, user.name, user.email));
  const { people } = await retry(() => adminDb.query({ people: { $: { where: { userId } } } }));
  users[key] = { db: adminDb.asUser({ token }), userId, personId: people[0]!.id };
}
console.error(`  ${Object.keys(users).length} users signed up and bootstrapped`);

const ids = new Map<string, string>(); // fixture key -> Instant id
const byLabel = <T extends { labelKey: string }>(rows: T[], labelKey: string) =>
  rows.filter((r) => r.labelKey === labelKey);

for (const label of fixture.labels) {
  const [adminMembership, ...others] = byLabel(fixture.memberships, label.key);
  const admin = users[adminMembership!.user]!;
  const organizationId = id();
  ids.set(label.key, organizationId);

  // The label and its first admin: admin authority, as /api/demo-data.
  await retry(() =>
    adminDb.transact([
      adminDb.tx.organizations[organizationId].create({ name: label.name, slug: label.slug }),
      adminDb.tx.memberships[id()]
        .create(membershipFields(organizationId, admin.userId, "admin"))
        .link({ organization: organizationId, person: admin.personId }),
    ]),
  );

  // Other members: as /api/members, the admin db only resolves the email.
  for (const m of others) {
    const person = await retry(() => findPersonByEmail(adminDb, fixture.users[m.user].email));
    const status = await retry(() =>
      addMember(admin.db, { organizationId, person: person!, role: m.role }),
    );
    if (status !== "added") throw new Error(`adding ${m.user} to ${label.key}: ${status}`);
  }
  const { memberships } = await retry(() =>
    admin.db.query({ memberships: { $: { where: { organization: organizationId } } } }),
  );
  const membershipOf = (key: string) =>
    memberships.find((row) => row.userId === users[key]!.userId)!.id;

  // Everything else as the admin, in dependency order.
  const { tx } = admin.db;
  const first: any[] = [];
  for (const t of byLabel(fixture.teams, label.key)) {
    ids.set(t.key, id());
    first.push(
      tx.teams[ids.get(t.key)!].create({ name: t.name }).link({ organization: organizationId }),
    );
  }
  for (const c of byLabel(fixture.catalogues, label.key)) {
    ids.set(c.key, id());
    first.push(
      tx.catalogues[ids.get(c.key)!]
        .create({ name: c.name, code: c.code })
        .link({ organization: organizationId }),
    );
  }
  for (const { key, labelKey: _, ...artist } of byLabel(fixture.artists, label.key)) {
    ids.set(key, id());
    first.push(tx.artists[ids.get(key)!].create(artist).link({ organization: organizationId }));
  }
  await inBatches(admin.db, first);

  const second: any[] = [];
  for (const t of byLabel(fixture.teamMembers, label.key))
    second.push(
      tx.teamAssignments[id()].create({ role: t.role }).link({
        organization: organizationId,
        team: ids.get(t.teamKey)!,
        membership: membershipOf(t.user),
      }),
    );
  for (const { key, labelKey: _, artistKey, catalogueKey, ...release } of byLabel(
    fixture.releases,
    label.key,
  )) {
    ids.set(key, id());
    second.push(
      tx.releases[ids.get(key)!]
        .create({ ...release, catalogKey: `${organizationId}/${release.catalogNumber}` })
        .link({
          organization: organizationId,
          artist: ids.get(artistKey)!,
          catalogue: ids.get(catalogueKey)!,
        }),
    );
  }
  await inBatches(admin.db, second);

  const third = byLabel(fixture.releaseTeams, label.key).map((rt) =>
    tx.releaseTeams[id()].create({}).link({
      organization: organizationId,
      release: ids.get(rt.releaseKey)!,
      team: ids.get(rt.teamKey)!,
    }),
  );
  await inBatches(admin.db, third);
  console.error(`  ${label.name}: written`);
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
