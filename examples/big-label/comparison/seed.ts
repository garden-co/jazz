// Writes the BigLabel fixture (fixtures/big-label.mjs) the app's own way:
// - every member signs up through Better Auth and runs the app's bootstrap
//   (POST /api/bootstrap's ensurePersonalOrganization: their profile, sign-in
//   email and personal label);
// - each fixture label is created with backend authority, with its admin's
//   membership, as the demo-data route does (browsers can never create labels);
// - the label's admin adds the other members as POST /api/members does
//   (backend email lookup, then a membership written as the admin);
// - everything else (teams, catalogues, artists, releases, assignments) is
//   written as the admin through forReqUser, so permissions.ts applies.
// Prints the harness's ctx as JSON.
import { auth } from "../src/lib/auth";
import { jazz } from "../src/lib/jazz.server";
import { ensurePersonalOrganization } from "../src/lib/bootstrap";
import { addMember, findPersonByEmail } from "../src/lib/members";
import { app } from "../jazz.generated";
import { bigLabelFixture, releasesNewestFirst, BENCH_LABEL, SECOND_LABEL } from "../../../dev/app-comparisons/fixtures/big-label.mjs";

const fixture = bigLabelFixture();
const origin = process.env.NEXT_PUBLIC_APP_ORIGIN!;
const j = await jazz();
type UserDb = Awaited<ReturnType<typeof j.forReqUser>>;
const users: Record<string, { db: UserDb; userId: string; personId: string }> = {};

for (const [key, user] of Object.entries(fixture.users)) {
  const { headers, response } = await auth.api.signUpEmail({
    body: { name: user.name, email: user.email, password: user.password },
    returnHeaders: true,
  });
  await j.createAccountFor(response.user.id);
  const cookie = headers.getSetCookie().map((c) => c.split(";")[0]).join("; ");
  const request = new Request(origin, { headers: { cookie } });
  const userId = (await j.accountFor(request))!;
  await ensurePersonalOrganization(userId, user.name, user.email);
  const person = (await j.db.one(app.people.where({ userId })))!;
  users[key] = { db: await j.forReqUser(request), userId, personId: person.id };
}

const ids = new Map<string, string>(); // fixture key -> Jazz id
const byLabel = <T extends { labelKey: string }>(rows: T[], labelKey: string) => rows.filter((r) => r.labelKey === labelKey);

for (const label of fixture.labels) {
  const [adminMembership, ...others] = byLabel(fixture.memberships, label.key);
  const admin = users[adminMembership.user];

  // The label and its first admin: backend authority, as /api/demo-data.
  const created = await j.db.transaction((tx) => {
    const org = tx.insert(app.organizations, { name: label.name, slug: label.slug });
    tx.insert(app.memberships, { organizationId: org.id, personId: admin.personId, userId: admin.userId, role: "admin" });
    return org.id;
  });
  await created.wait({ tier: "global" });
  const organizationId = created.value;
  ids.set(label.key, organizationId);

  // Other members: as /api/members, the backend only resolves the email.
  for (const m of others) {
    const person = await findPersonByEmail(j.db, fixture.users[m.user].email);
    const status = await addMember(admin.db, { organizationId, person: person!, role: m.role });
    if (status !== "added") throw new Error(`adding ${m.user} to ${label.key}: ${status}`);
  }
  const memberships = await admin.db.all(app.memberships.where({ organizationId }));
  const membershipOf = (key: string) => memberships.find((row) => row.userId === users[key].userId)!.id;

  // Everything else as the admin, in dependency order.
  const first = await admin.db.transaction((tx) => {
    for (const t of byLabel(fixture.teams, label.key)) ids.set(t.key, tx.insert(app.teams, { organizationId, name: t.name }).id);
    for (const c of byLabel(fixture.catalogues, label.key))
      ids.set(c.key, tx.insert(app.catalogues, { organizationId, name: c.name, code: c.code }).id);
    for (const { key, labelKey: _, ...artist } of byLabel(fixture.artists, label.key))
      ids.set(key, tx.insert(app.artists, { ...artist, organizationId }).id);
  });
  await first.wait({ tier: "global" });

  const second = await admin.db.transaction((tx) => {
    for (const t of byLabel(fixture.teamMembers, label.key))
      tx.insert(app.teamAssignments, { organizationId, teamId: ids.get(t.teamKey)!, membershipId: membershipOf(t.user), role: t.role });
    for (const { key, labelKey: _, artistKey, catalogueKey, releaseDate, ...release } of byLabel(fixture.releases, label.key))
      ids.set(
        key,
        tx.insert(app.releases, {
          ...release,
          organizationId,
          artistId: ids.get(artistKey)!,
          catalogueId: ids.get(catalogueKey)!,
          releaseDate: new Date(releaseDate),
        }).id,
      );
  });
  await second.wait({ tier: "global" });

  const third = await admin.db.transaction((tx) => {
    for (const rt of byLabel(fixture.releaseTeams, label.key))
      tx.insert(app.releaseTeams, { organizationId, releaseId: ids.get(rt.releaseKey)!, teamId: ids.get(rt.teamKey)! });
  });
  await third.wait({ tier: "global" });
}
await j.close();

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
