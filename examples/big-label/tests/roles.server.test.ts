import { afterEach, describe, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp, type TestDb } from "jazz-tools/testing";
import { app } from "../schema.js";
import permissions from "../permissions.js";

let testApp: PolicyTestApp | undefined;
afterEach(async () => await testApp?.shutdown());

const issuer = "https://identity.big-label.test";
const accounts = {
  admin: "00000000-0000-4000-8000-000000000011",
  editor: "00000000-0000-4000-8000-000000000012",
  viewer: "00000000-0000-4000-8000-000000000013",
  outsider: "00000000-0000-4000-8000-000000000014",
} as const;
type Actor = keyof typeof accounts;

const artist = (organizationId: string, name = "New artist") => ({
  organizationId,
  name,
  genre: "Jazz",
  status: "developing",
  searchKey: name.toLowerCase(),
});

describe("BigLabel roles at the Jazz edge", () => {
  it("refuses role escalation, forged assignments and writes a role can't make", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const seeded = await seed(testApp);
    const as = (actor: Actor): TestDb =>
      testApp!.as({
        issuer,
        user_id: actor,
        account_id: accounts[actor],
        claims: {},
        authMode: "local-first",
      });
    const admin = as("admin");
    const editor = as("editor");
    const viewer = as("viewer");
    const outsider = as("outsider");

    // No self-admission and no escalation: nobody but an admin writes
    // memberships, and not even an admin can insert a new admin directly.
    await editor.expectDenied((db) =>
      db.update(app.memberships, seeded.editorMembership.id, { role: "admin" }),
    );
    await viewer.expectDenied((db) =>
      db.update(app.memberships, seeded.viewerMembership.id, { role: "editor" }),
    );
    await editor.expectDenied((db) =>
      db.insert(app.memberships, {
        organizationId: seeded.org.id,
        personId: seeded.outsider.id,
        userId: accounts.outsider,
        role: "viewer",
      }),
    );
    await admin.expectDenied((db) =>
      db.insert(app.memberships, {
        organizationId: seeded.org.id,
        personId: seeded.outsider.id,
        userId: accounts.outsider,
        role: "admin",
      }),
    );
    // A membership must name the person it belongs to.
    await admin.expectDenied((db) =>
      db.insert(app.memberships, {
        organizationId: seeded.org.id,
        personId: seeded.outsider.id,
        userId: accounts.viewer,
        role: "viewer",
      }),
    );
    // Only the trusted bootstrap creates organizations.
    await admin.expectDenied((db) =>
      db.insert(app.organizations, { name: "Side label", slug: "side" }),
    );

    // Editors maintain the catalogue; viewers only read it.
    await viewer.expectDenied((db) => db.insert(app.artists, artist(seeded.org.id)));
    await viewer.expectDenied((db) =>
      db.update(app.releases, seeded.release.id, { status: "released" }),
    );
    await editor
      .insert(app.artists, artist(seeded.org.id, "Editor's artist"))
      .wait({ tier: "global" });
    await editor
      .update(app.releases, seeded.release.id, { status: "released" })
      .wait({ tier: "global" });
    await editor.expectDenied((db) => db.delete(app.artists, seeded.artist.id));
    await editor.expectDenied((db) =>
      db.insert(app.catalogues, { organizationId: seeded.org.id, name: "Bootlegs", code: "BL" }),
    );

    // Teams are admin-only; staffing a release is an editor task, within the tenant.
    await editor.expectDenied((db) =>
      db.insert(app.teams, { organizationId: seeded.org.id, name: "Shadow team" }),
    );
    await editor.expectDenied((db) =>
      db.insert(app.teamAssignments, {
        organizationId: seeded.org.id,
        teamId: seeded.team.id,
        membershipId: seeded.editorMembership.id,
        role: "lead",
      }),
    );
    await viewer.expectDenied((db) =>
      db.insert(app.releaseTeams, {
        organizationId: seeded.org.id,
        releaseId: seeded.release.id,
        teamId: seeded.team.id,
      }),
    );
    await editor
      .insert(app.releaseTeams, {
        organizationId: seeded.org.id,
        releaseId: seeded.release.id,
        teamId: seeded.team.id,
      })
      .wait({ tier: "global" });
    await editor.expectDenied((db) =>
      db.insert(app.releaseTeams, {
        organizationId: seeded.org.id,
        releaseId: seeded.release.id,
        teamId: seeded.foreignTeam.id,
      }),
    );
    await admin.expectDenied((db) =>
      db.insert(app.releases, {
        organizationId: seeded.org.id,
        artistId: seeded.artist.id,
        catalogueId: seeded.foreignCatalogue.id,
        catalogNumber: "FOREIGN-001",
        title: "Forged catalogue",
        format: "Single",
        releaseDate: new Date(),
        status: "planning",
        searchKey: "forged catalogue",
      }),
    );

    // Foreign labels stay unreadable, including their catalogues and staffing.
    const foreign = { organizationId: seeded.org.id };
    await expect(outsider.all(app.releases.where(foreign))).resolves.toEqual([]);
    await expect(outsider.all(app.catalogues.where(foreign))).resolves.toEqual([]);
    await expect(outsider.all(app.releaseTeams.where(foreign))).resolves.toEqual([]);
    await expect(outsider.all(app.teams.where(foreign))).resolves.toEqual([]);
    await expect(
      viewer.all(app.releaseTeams.where({ organizationId: seeded.org.id })),
    ).resolves.toEqual([expect.objectContaining({ teamId: seeded.team.id })]);
  }, 60_000);
});

async function seed(test: PolicyTestApp) {
  const insert = test.seed.bind(test);
  const org = await insert((db) => db.insert(app.organizations, { name: "Owned", slug: "owned" }));
  const foreignOrg = await insert((db) =>
    db.insert(app.organizations, { name: "Foreign", slug: "foreign" }),
  );
  const person = (actor: Actor) =>
    insert((db) => db.insert(app.people, { userId: accounts[actor], name: actor }));
  const admin = await person("admin");
  const editor = await person("editor");
  const viewer = await person("viewer");
  const outsider = await person("outsider");
  const membership = (organizationId: string, personId: string, actor: Actor, role: string) =>
    insert((db) =>
      db.insert(app.memberships, { organizationId, personId, userId: accounts[actor], role }),
    );
  await membership(org.id, admin.id, "admin", "admin");
  const editorMembership = await membership(org.id, editor.id, "editor", "editor");
  const viewerMembership = await membership(org.id, viewer.id, "viewer", "viewer");
  await membership(foreignOrg.id, outsider.id, "outsider", "admin");
  const team = await insert((db) =>
    db.insert(app.teams, { organizationId: org.id, name: "Release operations" }),
  );
  const foreignTeam = await insert((db) =>
    db.insert(app.teams, { organizationId: foreignOrg.id, name: "Foreign team" }),
  );
  const foreignCatalogue = await insert((db) =>
    db.insert(app.catalogues, { organizationId: foreignOrg.id, name: "Theirs", code: "TH" }),
  );
  const seededArtist = await insert((db) => db.insert(app.artists, artist(org.id, "Artist")));
  const release = await insert((db) =>
    db.insert(app.releases, {
      organizationId: org.id,
      artistId: seededArtist.id,
      catalogNumber: "OWN-001",
      title: "Release",
      format: "Album",
      releaseDate: new Date(),
      status: "scheduled",
      searchKey: "release own-001",
    }),
  );
  return {
    org,
    outsider,
    editorMembership,
    viewerMembership,
    team,
    foreignTeam,
    foreignCatalogue,
    artist: seededArtist,
    release,
  };
}
