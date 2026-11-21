// Resets BigLabel's tables and writes the fixture (fixtures/big-label.mjs) the
// app's own way, as big-label/jazz/seed.ts does:
// - every member signs up through Managed Better Auth's HTTP API (or signs
//   in, when an earlier seed created them) and runs the app's bootstrap
//   (POST /api/bootstrap's ensurePersonalOrganization: profile, sign-in email,
//   personal label);
// - each fixture label and its admin's membership is created by the server,
//   as the demo-data route does (browsers can never create labels);
// - the label's admin adds the other members as POST /api/members does
//   (server email lookup, then addMember for the admin: admin-only, non-admin roles);
// - everything else (teams, catalogues, artists, team members, releases,
//   release teams) goes through the app's write handlers as the label's
//   admin, with their checks, in three transactions per label.
// Prints the harness's ctx as JSON.
import { randomUUID } from "node:crypto";
import { bigLabelFixture, releasesNewestFirst, BENCH_LABEL, SECOND_LABEL } from "../../../../../dev/app-comparisons/fixtures/big-label.mjs";
import { resetDatabase } from "./db";

const fixture = bigLabelFixture();
await resetDatabase();
const { pool } = await import("../src/lib/db.server");
const { ensurePersonalOrganization } = await import("../src/lib/bootstrap");
const { addMember, findPersonByEmail } = await import("../src/lib/members");
const { runMutations } = await import("../src/lib/mutators.server");

type User = { key: string; name: string; email: string; password: string };
// Managed Better Auth checks the Origin header against the branch's trusted domains.
const origin = process.env.APP_ORIGIN ?? "http://localhost:3000";
async function authUser(path: string, body: object): Promise<{ id: string } | undefined> {
  for (let attempt = 0; ; attempt++) {
    const response = await fetch(`${process.env.NEON_AUTH_BASE_URL}/${path}`, {
      method: "POST",
      headers: { "content-type": "application/json", origin },
      body: JSON.stringify(body),
    });
    if (response.status === 429 && attempt < 30) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      continue;
    }
    return response.ok ? (await response.json()).user : undefined;
  }
}

const userIds: Record<string, string> = {};
const signUps = Object.values(fixture.users as Record<string, User>);
// A few at a time: Managed Better Auth rate-limits sign-ups.
for (let i = 0; i < signUps.length; i += 4) {
  await Promise.all(
    signUps.slice(i, i + 4).map(async (user) => {
      const created =
        (await authUser("sign-up/email", { name: user.name, email: user.email, password: user.password })) ??
        (await authUser("sign-in/email", { email: user.email, password: user.password }));
      if (!created) throw new Error(`Could not sign up or sign in ${user.email}`);
      userIds[user.key] = created.id;
      await ensurePersonalOrganization(created.id, user.name, user.email);
    }),
  );
}

type Labeled = { labelKey: string };
const byLabel = <T extends Labeled>(rows: T[], labelKey: string) => rows.filter((r) => r.labelKey === labelKey);
const ids = new Map<string, string>(); // fixture key -> id
const newId = (key: string) => {
  const id = randomUUID();
  ids.set(key, id);
  return id;
};

async function seedLabel(label: { key: string; name: string; slug: string }) {
  const [adminMembership, ...others] = byLabel(fixture.memberships, label.key);
  const admin = userIds[adminMembership.user];
  const organizationId = newId(label.key);

  // The label and its first admin: the server, as /api/demo-data.
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(`INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)`, [organizationId, label.name, label.slug]);
    await client.query(
      `INSERT INTO memberships (id, organization_id, person_id, user_id, role)
       SELECT $1, $2, id, user_id, 'admin' FROM people WHERE user_id = $3`,
      [randomUUID(), organizationId, admin],
    );
    await client.query("COMMIT");
  } finally {
    client.release();
  }

  // Other members: as /api/members, the server resolves the email.
  for (const m of others) {
    const person = await findPersonByEmail(pool, fixture.users[m.user].email);
    const status = await addMember(pool, admin, { organizationId, person: person!, role: m.role });
    if (status !== "added") throw new Error(`adding ${m.user} to ${label.key}: ${status}`);
  }
  const { rows: memberships } = await pool.query<{ id: string; user_id: string }>(
    `SELECT id, user_id FROM memberships WHERE organization_id = $1`,
    [organizationId],
  );
  const membershipOf = (key: string) => memberships.find((row) => row.user_id === userIds[key])!.id;

  // Everything else as the admin, through the write handlers, in dependency order.
  const run = async (writes: { name: string; args: unknown }[]) => {
    const result = await runMutations(pool, admin, writes);
    if (result.kind === "rejected") throw new Error(`${label.key}: ${result.error}`);
  };
  await run([
    ...byLabel(fixture.teams, label.key).map((t) => ({ name: "teams.create", args: { id: newId(t.key), organizationId, name: t.name } })),
    ...byLabel(fixture.catalogues, label.key).map((c) => ({
      name: "catalogues.create",
      args: { id: newId(c.key), organizationId, name: c.name, code: c.code },
    })),
    ...byLabel(fixture.artists, label.key).map((a) => ({
      name: "artists.create",
      args: { id: newId(a.key), organizationId, name: a.name, genre: a.genre, status: a.status },
    })),
  ]);
  await run([
    ...byLabel(fixture.teamMembers, label.key).map((t) => ({
      name: "teamAssignments.add",
      args: { id: randomUUID(), organizationId, teamId: ids.get(t.teamKey)!, membershipId: membershipOf(t.user), role: t.role },
    })),
    ...byLabel(fixture.releases, label.key).map((r) => ({
      name: "releases.save",
      args: {
        id: newId(r.key),
        isNew: true,
        organizationId,
        title: r.title,
        artistId: ids.get(r.artistKey)!,
        catalogueId: ids.get(r.catalogueKey)!,
        catalogNumber: r.catalogNumber,
        format: r.format,
        releaseDate: Date.parse(r.releaseDate),
        status: r.status,
      },
    })),
  ]);
  await run(
    byLabel(fixture.releaseTeams, label.key).map((rt) => ({
      name: "releaseTeams.add",
      args: { id: randomUUID(), organizationId, releaseId: ids.get(rt.releaseKey)!, teamId: ids.get(rt.teamKey)! },
    })),
  );
}

await Promise.all(fixture.labels.map(seedLabel));
await pool.end();

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
