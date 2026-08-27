import { afterEach, describe, expect, it } from "vitest";
import { userIdentity } from "jazz-tools";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { app } from "../schema.js";
import permissions from "../permissions.js";

let testApp: PolicyTestApp | undefined;
afterEach(async () => await testApp?.shutdown());

const issuer = "https://identity.big-label.test";
const authorFor = (subject: string) => userIdentity(issuer, subject);

describe("BigLabel deployed tenant authority", () => {
  it("admits client mutations only after the trusted backend bootstrap", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const seeded = await seed(testApp);
    const admin = testApp.as({
      issuer,
      user_id: "admin",
      claims: {},
      authMode: "local-first",
    });
    const member = testApp.as({
      issuer,
      user_id: "member",
      claims: {},
      authMode: "local-first",
    });
    const outsider = testApp.as({
      issuer,
      user_id: "outsider",
      claims: {},
      authMode: "local-first",
    });

    // First-tenant admission belongs only to the backend bootstrap route. No
    // client claim can create an organization, a person, or its first admin.
    await outsider.expectDenied((db) =>
      db.insert(app.people, { userId: authorFor("outsider"), name: "Forged profile" }),
    );
    await outsider.expectDenied((db) =>
      db.insert(app.memberships, {
        organizationId: seeded.foreignOrg.id,
        personId: seeded.outsider.id,
        userId: authorFor("outsider"),
        role: "admin",
      }),
    );
    await outsider.expectDenied((db) =>
      db.insert(app.organizations, { name: "Forged", slug: "forged" }),
    );
    admin.expectAllowed((db) =>
      db.update(app.people, seeded.admin.id, { name: "Updated profile" }),
    );
    await admin.expectDenied((db) => db.delete(app.people, seeded.admin.id));

    // A legitimate admin may admit a member; that member still cannot promote
    // themselves or mutate the tenant's operational rows.
    await admin
      .insert(app.memberships, {
        organizationId: seeded.org.id,
        personId: seeded.member.id,
        userId: authorFor("member"),
        role: "editor",
      })
      .wait({ tier: "edge" });
    await member.expectDenied((db) =>
      db.update(app.memberships, seeded.memberMembership.id, { role: "admin" }),
    );
    // An admin cannot join records from two tenants merely by referencing
    // them. Every relation carries/validates its tenant boundary.
    await admin.expectDenied((db) =>
      db.insert(app.releases, {
        organizationId: seeded.org.id,
        artistId: seeded.foreignArtist.id,
        title: "Cross-tenant release",
        releaseDate: new Date(),
        status: "scheduled",
      }),
    );
    await admin.expectDenied((db) =>
      db.insert(app.releaseAssignments, {
        organizationId: seeded.org.id,
        releaseId: seeded.release.id,
        membershipId: seeded.foreignMembership.id,
        role: "owner",
      }),
    );
    await member.expectDenied((db) =>
      db.insert(app.releaseAssignments, {
        organizationId: seeded.org.id,
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
  const admin = await test.seed((db) =>
    db.insert(app.people, { userId: authorFor("admin"), name: "Admin" }),
  );
  const member = await test.seed((db) =>
    db.insert(app.people, { userId: authorFor("member"), name: "Member" }),
  );
  const outsider = await test.seed((db) =>
    db.insert(app.people, { userId: authorFor("outsider"), name: "Outsider" }),
  );
  const adminMembership = await test.seed((db) =>
    db.insert(app.memberships, {
      organizationId: org.id,
      personId: admin.id,
      userId: authorFor("admin"),
      role: "admin",
    }),
  );
  const memberMembership = await test.seed((db) =>
    db.insert(app.memberships, {
      organizationId: org.id,
      personId: member.id,
      userId: authorFor("member"),
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
  const foreignArtist = await test.seed((db) =>
    db.insert(app.artists, {
      organizationId: foreignOrg.id,
      name: "Foreign artist",
      genre: "Jazz",
      status: "active",
    }),
  );
  const foreignMembership = await test.seed((db) =>
    db.insert(app.memberships, {
      organizationId: foreignOrg.id,
      personId: outsider.id,
      userId: authorFor("outsider"),
      role: "admin",
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
    foreignArtist,
    foreignMembership,
  };
}
