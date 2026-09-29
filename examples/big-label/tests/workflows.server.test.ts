import { afterEach, describe, expect, it } from "vitest";
import type { Db } from "jazz-tools";
import { createPolicyTestApp, type PolicyTestApp, type TestDb } from "jazz-tools/testing";
import { app } from "../schema.js";
import permissions from "../permissions.js";
import { CatalogNumberTakenError, saveRelease, type ReleaseFields } from "../src/lib/mutations";
import { loadDemoData } from "../src/lib/demo-data";
import { addMemberByEmail } from "../src/lib/members";

let testApp: PolicyTestApp | undefined;
afterEach(async () => await testApp?.shutdown());

const issuer = "https://identity.big-label.test";
const accounts = {
  admin: "00000000-0000-4000-8000-000000000021",
  editor: "00000000-0000-4000-8000-000000000022",
  outsider: "00000000-0000-4000-8000-000000000023",
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

  it("loads demo data idempotently, only for the caller", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const { backend } = await seedLabel(testApp);

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

  it("adds members by email for admins only, and hides people outside the label", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const { backend, org, outsider } = await seedLabel(testApp);
    await testApp.seed((db) =>
      db.insert(app.personEmails, { personId: outsider.id, email: "outsider@example.com" }),
    );

    // Nobody outside the label can see the outsider, and clients can't read emails.
    await expect(as("admin").all(app.people.where({ id: outsider.id }))).resolves.toEqual([]);
    await expect(as("outsider").all(app.personEmails)).resolves.toEqual([]);

    const input = { organizationId: org.id, email: " Outsider@Example.com ", role: "viewer" };
    await expect(addMemberByEmail(backend, accounts.editor, input)).resolves.toEqual({
      status: "forbidden",
    });
    await expect(
      addMemberByEmail(backend, accounts.admin, { ...input, role: "admin" }),
    ).resolves.toEqual({ status: "invalid-role" });
    await expect(
      addMemberByEmail(backend, accounts.admin, { ...input, email: "nobody@example.com" }),
    ).resolves.toEqual({ status: "not-found" });
    await expect(addMemberByEmail(backend, accounts.admin, input)).resolves.toMatchObject({
      status: "added",
      name: "outsider",
    });
    await expect(addMemberByEmail(backend, accounts.admin, input)).resolves.toEqual({
      status: "already-member",
      name: "outsider",
    });

    // Now they share a label, the admin can see them.
    await expect(as("admin").all(app.people.where({ id: outsider.id }))).resolves.toEqual([
      expect.objectContaining({ name: "outsider" }),
    ]);
  }, 60_000);
});

async function seedLabel(test: PolicyTestApp) {
  let backend: Db | undefined;
  const org = await test.seed((db) => {
    backend = db;
    return db.insert(app.organizations, { name: "Owned", slug: "owned" });
  });
  const person = (actor: Actor) =>
    test.seed((db) => db.insert(app.people, { userId: accounts[actor], name: actor }));
  const admin = await person("admin");
  const editor = await person("editor");
  const outsider = await person("outsider");
  const membership = (personId: string, actor: Actor, role: string) =>
    test.seed((db) =>
      db.insert(app.memberships, {
        organizationId: org.id,
        personId,
        userId: accounts[actor],
        role,
      }),
    );
  await membership(admin.id, "admin", "admin");
  await membership(editor.id, "editor", "editor");
  const artist = await test.seed((db) =>
    db.insert(app.artists, {
      organizationId: org.id,
      name: "Artist",
      genre: "Jazz",
      status: "developing",
      searchKey: "artist",
    }),
  );
  return { backend: backend!, org, outsider, artist };
}
