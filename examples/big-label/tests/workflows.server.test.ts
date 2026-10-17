import { afterEach, describe, expect, it } from "vitest";
import type { Db } from "jazz-tools";
import { createPolicyTestApp, type PolicyTestApp, type TestDb } from "jazz-tools/testing";
import { app } from "../schema.js";
import permissions from "../permissions.js";
import {
  ArtistHasReleasesError,
  CatalogNumberTakenError,
  deleteArtist,
  deleteRelease,
  deleteTeam,
  nextCatalogNumber,
  removeMember,
  saveRelease,
  type ReleaseFields,
} from "../src/lib/mutations";
import { loadDemoData } from "../src/lib/demo-data";
import { addMember, findPersonByEmail } from "../src/lib/members";
import { isExclusiveConflict } from "../src/lib/write-errors";

let testApp: PolicyTestApp | undefined;
afterEach(async () => await testApp?.shutdown());

const issuer = "https://identity.big-label.test";
const accounts = {
  admin: "00000000-0000-4000-8000-000000000021",
  editor: "00000000-0000-4000-8000-000000000022",
  outsider: "00000000-0000-4000-8000-000000000023",
  newcomer: "00000000-0000-4000-8000-000000000024",
} as const;
type Actor = keyof typeof accounts;

const as = (actor: Actor): TestDb =>
  testApp!.as({
    issuer,
    user_id: actor,
    account_id: accounts[actor],
    claims: {},
    authMode: "local-first",
  });

describe("BigLabel workflows", () => {
  it("keeps catalogue numbers unique through saveRelease", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const { org, artist } = await seedLabel(testApp);
    const editor = as("editor");
    const fields = (title: string, catalogNumber: string): ReleaseFields => ({
      title,
      artistId: artist.id,
      catalogueId: null,
      catalogNumber,
      format: "Album",
      releaseDate: new Date("2026-01-01T00:00:00.000Z"),
      status: "planning",
      searchKey: title.toLowerCase(),
    });

    const firstId = crypto.randomUUID();
    await (
      await saveRelease(editor, org.id, { id: firstId, isNew: true }, fields("First", "OWN-001"))
    ).wait();

    // A second release can't take the number, and the first can keep it on edit.
    await expect(
      saveRelease(
        editor,
        org.id,
        { id: crypto.randomUUID(), isNew: true },
        fields("Second", "OWN-001"),
      ),
    ).rejects.toBeInstanceOf(CatalogNumberTakenError);
    await (
      await saveRelease(editor, org.id, { id: firstId, isNew: false }, fields("Renamed", "OWN-001"))
    ).wait();

    // Two people racing for the same number: the authority admits one.
    const race = await Promise.allSettled(
      (["admin", "editor"] as const).map(async (actor) =>
        (
          await saveRelease(
            as(actor),
            org.id,
            { id: crypto.randomUUID(), isNew: true },
            fields(`Race ${actor}`, "OWN-002"),
          )
        ).wait(),
      ),
    );
    expect(race.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1);
    await expect(
      editor.all(app.releases.where({ organizationId: org.id, catalogNumber: "OWN-002" })),
    ).resolves.toHaveLength(1);

    const holders = await editor.all(
      app.releases.where({ organizationId: org.id, catalogNumber: "OWN-001" }),
    );
    expect(holders.map((row) => row.title)).toEqual(["Renamed"]);
  }, 60_000);

  it("suggests the next catalogue number numerically, past 999", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const { org, artist } = await seedLabel(testApp);
    const catalogue = await testApp.seed((db) =>
      db.insert(app.catalogues, { organizationId: org.id, name: "Main", code: "MN" }),
    );
    const editor = as("editor");
    for (const catalogNumber of ["MN-999", "MN-1000"])
      await (
        await saveRelease(
          editor,
          org.id,
          { id: crypto.randomUUID(), isNew: true },
          {
            title: catalogNumber,
            artistId: artist.id,
            catalogueId: catalogue.id,
            catalogNumber,
            format: "Single",
            releaseDate: new Date("2026-01-01T00:00:00.000Z"),
            status: "planning",
            searchKey: catalogNumber.toLowerCase(),
          },
        )
      ).wait();
    await expect(nextCatalogNumber(editor, org.id, catalogue)).resolves.toBe("MN-1001");
  }, 60_000);

  it("deletes children with their parent, and refuses to orphan releases", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const seeded = await seedLabel(testApp);
    const { org } = seeded;
    const insert = testApp.seed.bind(testApp);
    const team = await insert((db) =>
      db.insert(app.teams, { organizationId: org.id, name: "Release operations" }),
    );
    const release = await insert((db) =>
      db.insert(app.releases, {
        organizationId: org.id,
        artistId: seeded.artist.id,
        catalogNumber: "OWN-001",
        title: "Release",
        format: "Album",
        releaseDate: new Date(),
        status: "scheduled",
        searchKey: "release",
      }),
    );
    await insert((db) =>
      db.insert(app.releaseTeams, {
        organizationId: org.id,
        releaseId: release.id,
        teamId: team.id,
      }),
    );
    await insert((db) =>
      db.insert(app.releaseAssignments, {
        organizationId: org.id,
        releaseId: release.id,
        membershipId: seeded.editorMembership.id,
        role: "owner",
      }),
    );
    await insert((db) =>
      db.insert(app.teamAssignments, {
        organizationId: org.id,
        teamId: team.id,
        membershipId: seeded.editorMembership.id,
        role: "lead",
      }),
    );
    const admin = as("admin");
    const count = async () => ({
      releaseTeams: (await admin.all(app.releaseTeams.where({ organizationId: org.id }))).length,
      releaseAssignments: (
        await admin.all(app.releaseAssignments.where({ organizationId: org.id }))
      ).length,
      teamAssignments: (await admin.all(app.teamAssignments.where({ organizationId: org.id })))
        .length,
    });

    // An artist with a release can't be deleted.
    await expect(deleteArtist(admin, org.id, seeded.artist.id)).rejects.toBeInstanceOf(
      ArtistHasReleasesError,
    );

    // Deleting the release takes its team and per-person assignments with it.
    await (await deleteRelease(admin, org.id, release.id)).wait({ tier: "global" });
    await expect(count()).resolves.toEqual({
      releaseTeams: 0,
      releaseAssignments: 0,
      teamAssignments: 1,
    });

    // Now the artist can go.
    await (await deleteArtist(admin, org.id, seeded.artist.id)).wait();
    await expect(admin.all(app.artists.where({ id: seeded.artist.id }))).resolves.toEqual([]);

    // Deleting the team removes its member assignments.
    await (await deleteTeam(admin, org.id, team.id)).wait({ tier: "global" });
    await expect(count()).resolves.toMatchObject({ teamAssignments: 0 });
    await expect(admin.all(app.teams.where({ organizationId: org.id }))).resolves.toEqual([]);
  }, 60_000);

  it("removes a member with their team and release assignments", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const seeded = await seedLabel(testApp);
    const { org } = seeded;
    const insert = testApp.seed.bind(testApp);
    const team = await insert((db) =>
      db.insert(app.teams, { organizationId: org.id, name: "Release operations" }),
    );
    const release = await insert((db) =>
      db.insert(app.releases, {
        organizationId: org.id,
        artistId: seeded.artist.id,
        catalogNumber: "OWN-001",
        title: "Release",
        format: "Album",
        releaseDate: new Date(),
        status: "scheduled",
        searchKey: "release",
      }),
    );
    const membershipId = seeded.editorMembership.id;
    await insert((db) =>
      db.insert(app.teamAssignments, {
        organizationId: org.id,
        teamId: team.id,
        membershipId,
        role: "member",
      }),
    );
    await insert((db) =>
      db.insert(app.releaseAssignments, {
        organizationId: org.id,
        releaseId: release.id,
        membershipId,
        role: "owner",
      }),
    );

    const admin = as("admin");
    await (await removeMember(admin, org.id, membershipId)).wait({ tier: "global" });
    await expect(admin.all(app.memberships.where({ id: membershipId }))).resolves.toEqual([]);
    await expect(admin.all(app.teamAssignments.where({ membershipId }))).resolves.toEqual([]);
    await expect(admin.all(app.releaseAssignments.where({ membershipId }))).resolves.toEqual([]);
  }, 60_000);

  it("loads demo data idempotently, only for the caller", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    await seedLabel(testApp);
    const backend = await backendDb(testApp);

    const first = await loadDemoData(backend, accounts.admin, "smoke");
    expect(first.created).toBe(true);
    const again = await loadDemoData(backend, accounts.admin, "smoke");
    expect(again).toEqual({ created: false, organizationIds: first.organizationIds });

    const admin = as("admin");
    for (const organizationId of first.organizationIds) {
      const mine = await admin.all(
        app.memberships.where({ organizationId, userId: accounts.admin }),
      );
      expect(mine.map((row) => row.role)).toEqual(["admin"]);
      await expect(admin.all(app.artists.where({ organizationId }))).resolves.not.toEqual([]);
      await expect(as("outsider").all(app.artists.where({ organizationId }))).resolves.toEqual([]);
    }
  }, 60_000);

  it("adds members by email as the caller, so only admins can, and only once", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const { org, outsider, newcomer } = await seedLabel(testApp);
    await testApp.seed((db) =>
      db.insert(app.personEmails, { personId: outsider.id, email: "outsider@example.com" }),
    );
    const backend = await backendDb(testApp);

    // Nobody outside the label can see the outsider, and clients can't read emails.
    await expect(as("admin").all(app.people.where({ id: outsider.id }))).resolves.toEqual([]);
    await expect(as("outsider").all(app.personEmails)).resolves.toEqual([]);

    // The backend only resolves the email.
    await expect(findPersonByEmail(backend, "nobody@example.com")).resolves.toBeNull();
    const person = await findPersonByEmail(backend, " Outsider@Example.com ");
    expect(person).toMatchObject({ id: outsider.id, name: "outsider" });

    // The membership is written as the caller, so permissions.ts decides.
    const input = { organizationId: org.id, person: person!, role: "viewer" };
    await expect(addMember(as("editor"), input)).resolves.toBe("forbidden");
    await expect(addMember(as("outsider"), input)).resolves.toBe("forbidden");
    await expect(addMember(as("admin"), { ...input, role: "admin" })).resolves.toBe("forbidden");
    await expect(addMember(as("admin"), input)).resolves.toBe("added");
    await expect(addMember(as("admin"), input)).resolves.toBe("already-member");

    // Now they share a label, the admin can see them.
    await expect(as("admin").all(app.people.where({ id: outsider.id }))).resolves.toEqual([
      expect.objectContaining({ name: "outsider" }),
    ]);

    // A double submit adds the member once. Both transactions read before
    // either commits, so one loses with a conflict, retries, and finds the
    // membership the other added.
    const conflicts: unknown[] = [];
    const twice = await Promise.all(
      [as("admin"), as("admin")].map((caller) =>
        addMember(
          caller,
          {
            organizationId: org.id,
            person: { id: newcomer.id, userId: accounts.newcomer },
            role: "editor",
          },
          { onConflict: (error) => conflicts.push(error) },
        ),
      ),
    );
    expect(twice.sort()).toEqual(["added", "already-member"]);
    expect(conflicts).toHaveLength(1);
    expect(conflicts.every(isExclusiveConflict)).toBe(true);
    await expect(
      as("admin").all(app.memberships.where({ organizationId: org.id, personId: newcomer.id })),
    ).resolves.toHaveLength(1);
  }, 60_000);
});

/**
 * The trusted server's db, as the API routes use it. PolicyTestApp hands it
 * out only inside `seed`, so this takes it from a seed of its own rather than
 * from the label's fixtures.
 */
async function backendDb(test: PolicyTestApp) {
  let backend: Db | undefined;
  await test.seed((db) => {
    backend = db;
    return db.insert(app.organizations, { name: "Unrelated label", slug: "unrelated" });
  });
  return backend!;
}

async function seedLabel(test: PolicyTestApp) {
  const insert = test.seed.bind(test);
  const org = await insert((db) => db.insert(app.organizations, { name: "Owned", slug: "owned" }));
  const person = (actor: Actor) =>
    insert((db) => db.insert(app.people, { userId: accounts[actor], name: actor }));
  const admin = await person("admin");
  const editor = await person("editor");
  const outsider = await person("outsider");
  const newcomer = await person("newcomer");
  const membership = (personId: string, actor: Actor, role: string) =>
    insert((db) =>
      db.insert(app.memberships, {
        organizationId: org.id,
        personId,
        userId: accounts[actor],
        role,
      }),
    );
  await membership(admin.id, "admin", "admin");
  const editorMembership = await membership(editor.id, "editor", "editor");
  const artist = await insert((db) =>
    db.insert(app.artists, {
      organizationId: org.id,
      name: "Artist",
      genre: "Jazz",
      status: "developing",
      searchKey: "artist",
    }),
  );
  return { org, outsider, newcomer, editorMembership, artist };
}
