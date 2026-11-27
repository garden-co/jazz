// `pnpm seed`: resets the local deployment and writes fixtures/big-label.mjs
// the app's own way (needs `pnpm backend` running), like big-label/jazz/seed.ts:
// - every member signs up (Convex Auth Password) and runs the bootstrap
//   mutation (their profile, sign-in email and personal label);
// - each fixture label and its admin's membership is created by the server
//   (internal seed:createLabel), as the demo-data mutation does;
// - the admin adds the other members by email (members.addByEmail);
// - everything else is written as the label's admin through the app's
//   public mutations, under the same checks (releases through releases.save,
//   the transactional catalogue-number check).
// Prints the harness's ctx as JSON.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { ConvexHttpClient } from "convex/browser";
import { api } from "../convex/_generated/api.js";
import { bigLabelFixture, releasesNewestFirst, BENCH_LABEL, SECOND_LABEL } from "../../../../../dev/app-comparisons/fixtures/big-label.mjs";

const run = (fn, args = {}) =>
  JSON.parse(execFileSync("npx", ["convex", "run", fn, JSON.stringify(args)], { encoding: "utf8" }) || "null");
const url = readFileSync(".env.local", "utf8").match(/NEXT_PUBLIC_CONVEX_URL=(.*)/)[1].trim();
const fixture = bigLabelFixture();

/** Runs `tasks` with up to `width` in flight; retries a write that hit the local write limit. */
async function pool(tasks, width = 16) {
  const results = new Array(tasks.length);
  let next = 0;
  const retrying = async (task) => {
    for (let attempt = 0; ; attempt++) {
      try {
        return await task();
      } catch (error) {
        if (attempt >= 30 || !/Too many|OptimisticConcurrency|timed out/.test(String(error))) throw error;
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }
  };
  await Promise.all(
    Array.from({ length: width }, async () => {
      while (next < tasks.length) {
        const i = next++;
        results[i] = await retrying(tasks[i]);
      }
    }),
  );
  return results;
}

run("seed:reset");

const users = {};
await pool(
  Object.entries(fixture.users).map(([key, user]) => async () => {
    const client = new ConvexHttpClient(url);
    const { tokens } = await client.action(api.auth.signIn, {
      provider: "password",
      params: { flow: "signUp", email: user.email, password: user.password, name: user.name },
    });
    client.setAuth(tokens.token);
    await client.mutation(api.bootstrap.ensurePersonalOrganization, {});
    users[key] = { client, userId: (await client.query(api.users.viewer, {})).id };
  }),
  // Password hashing runs in the function runtime: a few at a time.
  2,
);
console.error(`  ${Object.keys(users).length} users signed up and bootstrapped`);

const ids = new Map();
const byLabel = (rows, labelKey) => rows.filter((row) => row.labelKey === labelKey);
for (const label of fixture.labels) {
  const [adminMembership, ...others] = byLabel(fixture.memberships, label.key);
  const admin = users[adminMembership.user].client;
  const organizationId = run("seed:createLabel", {
    name: label.name,
    slug: label.slug,
    adminUserId: users[adminMembership.user].userId,
  });
  ids.set(label.key, organizationId);

  for (const m of others) {
    const { status } = await admin.mutation(api.members.addByEmail, {
      organizationId,
      email: fixture.users[m.user].email,
      role: m.role,
    });
    if (status !== "added") throw new Error(`adding ${m.user} to ${label.key}: ${status}`);
  }
  const memberships = await admin.query(api.members.list, { organizationId });
  const membershipOf = (key) => memberships.find((row) => row.userId === users[key].userId)._id;
  const as = (fn, args) => () => admin.mutation(fn, { organizationId, ...args });

  const teams = byLabel(fixture.teams, label.key);
  const catalogues = byLabel(fixture.catalogues, label.key);
  const artists = byLabel(fixture.artists, label.key);
  const created = await pool([
    ...teams.map((t) => as(api.teams.create, { name: t.name })),
    ...catalogues.map((c) => as(api.catalogues.create, { name: c.name, code: c.code })),
    ...artists.map(({ name, genre, status, searchKey }) => as(api.artists.create, { name, genre, status, searchKey })),
  ]);
  [...teams, ...catalogues, ...artists].forEach((row, i) => ids.set(row.key, created[i]));

  const releases = byLabel(fixture.releases, label.key);
  const saved = await pool([
    ...byLabel(fixture.teamMembers, label.key).map((t) =>
      as(api.teams.addMember, { teamId: ids.get(t.teamKey), membershipId: membershipOf(t.user), role: t.role }),
    ),
    ...releases.map((r) =>
      as(api.releases.save, {
        title: r.title,
        artistId: ids.get(r.artistKey),
        catalogueId: ids.get(r.catalogueKey),
        catalogNumber: r.catalogNumber,
        format: r.format,
        releaseDate: Date.parse(r.releaseDate),
        status: r.status,
        searchKey: r.searchKey,
      }),
    ),
  ]);
  releases.forEach((row, i) => ids.set(row.key, saved.at(i - releases.length)));

  await pool(
    byLabel(fixture.releaseTeams, label.key).map((rt) =>
      as(api.releaseTeams.assign, { releaseId: ids.get(rt.releaseKey), teamId: ids.get(rt.teamKey) }),
    ),
  );
  console.error(`  ${label.key} written`);
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
