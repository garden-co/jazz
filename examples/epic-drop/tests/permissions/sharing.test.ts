import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";

const alice = "00000000-0000-4000-8000-00000000000a";
const bob = "00000000-0000-4000-8000-00000000000b";
const carol = "00000000-0000-4000-8000-00000000000c";

function as(account: string) {
  return testApp.as({
    issuer: "https://epic-drop.test",
    user_id: account,
    account_id: account,
    claims: {},
    authMode: "external" as const,
  });
}

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});

afterEach(async () => {
  await testApp?.shutdown();
});

/** Alice owns Demos, with a Mixes subfolder holding one file, and two invites. */
async function seedSharedTree() {
  const demos = await testApp.seed((db) =>
    db.insert(app.folders, { name: "Demos", owner_id: alice }),
  );
  const mixes = await testApp.seed((db) =>
    db.insert(app.folders, { name: "Mixes", owner_id: alice, parent_id: demos.id }),
  );
  const file = await testApp.seed((db) =>
    db.insert(app.files, {
      folder_id: mixes.id,
      name: "take-1.wav",
      content_type: "audio/wav",
      size_bytes: 3,
      owner_id: alice,
      contents: new Uint8Array([1, 2, 3]),
    }),
  );
  await testApp.seed((db) =>
    db.insert(app.folderInvites, { folder_id: demos.id, code: "view-code", role: "viewer" }),
  );
  await testApp.seed((db) =>
    db.insert(app.folderInvites, { folder_id: demos.id, code: "edit-code", role: "editor" }),
  );
  return { demos, mixes, file };
}

async function join(account: string, folderId: string, role: "viewer" | "editor") {
  await testApp.seed((db) =>
    db.insert(app.folderMembers, {
      folder_id: folderId,
      user_id: account,
      role,
      invite_code: role === "viewer" ? "view-code" : "edit-code",
    }),
  );
}

describe("EpicDrop folder sharing", () => {
  it("keeps folders and files private to their owner by default", async () => {
    const { demos, file } = await seedSharedTree();
    await expect(as(alice).all(app.folders)).resolves.toHaveLength(2);
    await expect(as(bob).all(app.folders.where({ id: demos.id }))).resolves.toEqual([]);
    await expect(as(bob).all(app.files.where({ id: file.id }).select("name"))).resolves.toEqual([]);
  });

  it("admits a member only with a live invite for the same folder and role", async () => {
    const { demos, mixes } = await seedSharedTree();
    const bobDb = as(bob);
    const membership = (code: string, role: "viewer" | "editor", folderId = demos.id) => ({
      folder_id: folderId,
      user_id: bob,
      role,
      invite_code: code,
    });

    await bobDb.expectDenied((db) => db.insert(app.folderMembers, membership("guessed", "viewer")));
    // A viewer code cannot be upgraded to editor access.
    await bobDb.expectDenied((db) => db.insert(app.folderMembers, membership("view-code", "editor")));
    // An invite for Demos does not open another folder.
    await bobDb.expectDenied((db) =>
      db.insert(app.folderMembers, membership("view-code", "viewer", mixes.id)),
    );
    // Nobody can join on someone else's behalf.
    await bobDb.expectDenied((db) =>
      db.insert(app.folderMembers, { ...membership("view-code", "viewer"), user_id: carol }),
    );

    await bobDb
      .insert(app.folderMembers, membership("view-code", "viewer"))
      .wait({ tier: "global" });
    await expect(bobDb.all(app.folders.where({ id: demos.id }))).resolves.toHaveLength(1);
  });

  it("lets viewers read the whole subtree but change nothing", async () => {
    const { demos, mixes, file } = await seedSharedTree();
    await join(bob, demos.id, "viewer");
    const bobDb = as(bob);

    await expect(bobDb.all(app.folders.where({ id: mixes.id }))).resolves.toHaveLength(1);
    const [contents] = await bobDb.all(
      app.files.where({ id: file.id }).select({ contents: { from: 1, to: 3 } }),
    );
    expect([...contents!.contents]).toEqual([2, 3]);

    await bobDb.expectDenied((db) => db.update(app.files, file.id, { name: "renamed.wav" }));
    await bobDb.expectDenied((db) => db.delete(app.files, file.id));
    await bobDb.expectDenied((db) =>
      db.insert(app.files, {
        folder_id: mixes.id,
        name: "sneaky.txt",
        content_type: "text/plain",
        size_bytes: 1,
        owner_id: bob,
        contents: new Uint8Array([1]),
      }),
    );
    await bobDb.expectDenied((db) =>
      db.insert(app.folders, { name: "Sub", owner_id: bob, parent_id: mixes.id }),
    );
    await bobDb.expectDenied((db) => db.update(app.folders, demos.id, { name: "Mine now" }));
  });

  it("lets editors change contents without taking over ownership or sharing", async () => {
    const { demos, mixes, file } = await seedSharedTree();
    await join(bob, demos.id, "editor");
    const bobDb = as(bob);

    await bobDb
      .insert(app.files, {
        folder_id: mixes.id,
        name: "take-2.wav",
        content_type: "audio/wav",
        size_bytes: 1,
        owner_id: bob,
        contents: new Uint8Array([4]),
      })
      .wait({ tier: "global" });
    await bobDb.update(app.files, file.id, { name: "take-1-final.wav" }).wait({ tier: "global" });
    await bobDb
      .insert(app.folders, { name: "Stems", owner_id: bob, parent_id: mixes.id })
      .wait({ tier: "global" });

    // Uploads are stamped with the uploader, and ownership never moves.
    await bobDb.expectDenied((db) =>
      db.insert(app.files, {
        folder_id: mixes.id,
        name: "forged.wav",
        content_type: "audio/wav",
        size_bytes: 1,
        owner_id: alice,
        contents: new Uint8Array([5]),
      }),
    );
    await bobDb.expectDenied((db) => db.update(app.folders, demos.id, { owner_id: bob }));
    await bobDb.expectDenied((db) => db.update(app.files, file.id, { owner_id: bob }));

    // Only the owner manages access.
    await expect(bobDb.all(app.folderInvites)).resolves.toEqual([]);
    await bobDb.expectDenied((db) =>
      db.insert(app.folderInvites, { folder_id: demos.id, code: "bob-code", role: "editor" }),
    );
  });

  it("does not let an editor move shared files into a folder they cannot edit", async () => {
    const { demos, file } = await seedSharedTree();
    await join(bob, demos.id, "editor");
    const carolsFolder = await testApp.seed((db) =>
      db.insert(app.folders, { name: "Carol", owner_id: carol }),
    );
    await as(bob).expectDenied((db) =>
      db.update(app.files, file.id, { folder_id: carolsFolder.id }),
    );
  });

  it("revokes access when the owner removes a member", async () => {
    const { demos, file } = await seedSharedTree();
    await join(bob, demos.id, "viewer");
    const [membership] = await as(alice).all(app.folderMembers.where({ user_id: bob }));
    expect(membership).toBeDefined();
    await as(bob).expectDenied((db) =>
      db.update(app.folderMembers, membership!.id, { role: "editor" }),
    );

    await as(alice).delete(app.folderMembers, membership!.id).wait({ tier: "global" });
    await expect(as(bob).all(app.files.where({ id: file.id }).select("name"))).resolves.toEqual(
      [],
    );
  });
});
