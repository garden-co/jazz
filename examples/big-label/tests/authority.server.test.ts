import { afterEach, describe, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { app } from "../schema.js";
import permissions from "../permissions.js";

let testApp: PolicyTestApp | undefined;
afterEach(async () => await testApp?.shutdown());

describe("BigLabel deployed tenant authority", () => {
  it("admits only the server-bootstrap identity or an existing organization admin", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const seeded = await seed(testApp);
    const admin = testApp.as({ user_id: "admin", claims: {}, authMode: "local-first" });
    const member = testApp.as({ user_id: "member", claims: {}, authMode: "local-first" });
    const outsider = testApp.as({ user_id: "outsider", claims: {}, authMode: "local-first" });
    const bootstrap = testApp.as({
      user_id: "bootstrap",
      claims: { biglabel_admin: true },
      authMode: "external",
    });

    // A server-issued claim is the only first-tenant admission path; ordinary
    // identities cannot turn a foreign organization into a readable tenant.
    await outsider.expectDenied((db) =>
      db.insert(app.memberships, {
        organizationId: seeded.foreignOrg.id,
        personId: seeded.outsider.id,
        userId: "outsider",
        role: "admin",
      }),
    );
    await outsider.expectDenied((db) =>
      db.insert(app.organizations, { name: "Forged", slug: "forged" }),
    );
    bootstrap.expectAllowed((db) =>
      db.insert(app.organizations, { name: "Bootstrap tenant", slug: "bootstrap" }),
    );

    // A legitimate admin may admit a member; that member still cannot promote
    // themselves or mutate the tenant's operational rows.
    admin.expectAllowed((db) =>
      db.insert(app.memberships, {
        organizationId: seeded.org.id,
        personId: seeded.member.id,
        userId: "member",
        role: "editor",
      }),
    );
    await member.expectDenied((db) =>
      db.update(app.memberships, seeded.memberMembership.id, { role: "admin" }),
    );
    await member.expectDenied((db) =>
      db.insert(app.releaseAssignments, {
        releaseId: seeded.release.id,
        membershipId: seeded.adminMembership.id,
        role: "owner",
      }),
    );
    // Foreign targets cannot even be resolved for a partial update (the local
    // read gate), while the admission/forgery checks above exercise the edge
    // authority receipt explicitly.
    expect(() => outsider.update(app.artists, seeded.artist.id, { status: "stolen" })).toThrow(
      /read policy denied/,
    );
    expect(() => outsider.update(app.releases, seeded.release.id, { status: "stolen" })).toThrow(
      /read policy denied/,
    );
    expect(() => outsider.update(app.teams, seeded.team.id, { name: "Stolen team" })).toThrow(
      /read policy denied/,
    );

    await expect(
      outsider.all(app.artists.where({ organizationId: seeded.org.id })),
    ).resolves.toEqual([]);
    await expect(member.all(app.artists.where({ organizationId: seeded.org.id }))).resolves.toEqual(
      [expect.objectContaining({ id: seeded.artist.id })],
    );
  }, 30_000);
});

async function seed(test: PolicyTestApp) {
  const org = await test.seed((db) =>
    db.insert(app.organizations, { name: "Owned", slug: "owned" }),
  );
  const foreignOrg = await test.seed((db) =>
    db.insert(app.organizations, { name: "Foreign", slug: "foreign" }),
  );
  const admin = await test.seed((db) => db.insert(app.people, { userId: "admin", name: "Admin" }));
  const member = await test.seed((db) =>
    db.insert(app.people, { userId: "member", name: "Member" }),
  );
  const outsider = await test.seed((db) =>
    db.insert(app.people, { userId: "outsider", name: "Outsider" }),
  );
  const adminMembership = await test.seed((db) =>
    db.insert(app.memberships, {
      organizationId: org.id,
      personId: admin.id,
      userId: "admin",
      role: "admin",
    }),
  );
  const memberMembership = await test.seed((db) =>
    db.insert(app.memberships, {
      organizationId: org.id,
      personId: member.id,
      userId: "member",
      role: "editor",
    }),
  );
  const team = await test.seed((db) =>
    db.insert(app.teams, { organizationId: org.id, name: "Operations" }),
  );
  const artist = await test.seed((db) =>
    db.insert(app.artists, {
      organizationId: org.id,
      name: "Artist",
      genre: "Jazz",
      status: "active",
    }),
  );
  const release = await test.seed((db) =>
    db.insert(app.releases, {
      organizationId: org.id,
      artistId: artist.id,
      title: "Release",
      releaseDate: new Date(),
      status: "scheduled",
    }),
  );
  return {
    org,
    foreignOrg,
    admin,
    member,
    outsider,
    adminMembership,
    memberMembership,
    team,
    artist,
    release,
  };
}
