import { expect, it, vi } from "vitest";
import { app } from "../../tests/fixtures/encrypted/jazz.generated.js";
import permissions from "../../tests/fixtures/encrypted/jazz.permissions.json";
import { EncryptionError } from "../encryption/index.js";
import { SerializableWriteResult } from "../runtime/client.js";
import { createJazz, keyless, memory } from "./index.js";

it("adds, lists and removes members on a backend without keys", async () => {
  const jazz = await createJazz({ app, permissions, store: memory() }).ready();
  try {
    const { db } = jazz;
    const accountId = crypto.randomUUID();
    const otherAccountId = crypto.randomUUID();
    const project = await db
      .insert(keyless(app).projects, { name: "Shared project", ownerId: accountId })
      .wait({ tier: "global" });
    const space = { table: "projects", id: project.id };
    const members = db.encryption.members;
    const table = app.__e2ee_members;
    await db.insert(table, await members.row(space, accountId)).wait({ tier: "global" });
    const other = await db
      .insert(table, await members.row(space, otherAccountId))
      .wait({ tier: "global" });

    const list = async () => db.all(table.where(await members.where(space)), { tier: "remote" });
    expect((await list()).map((row) => row.accountId)).toEqual(
      expect.arrayContaining([accountId, otherAccountId]),
    );

    using confirmation = vi.spyOn(SerializableWriteResult.prototype, "wait");
    expect(await members.remove(space, accountId)).toBe(1);
    expect(confirmation).toHaveBeenCalledOnce();
    expect(await list()).toEqual([other]);

    const keylessError = { name: "EncryptionError", code: "keyless" };
    await expect(db.encryption.devices.list()).rejects.toMatchObject(keylessError);
    await expect(db.encryption.keys.rotate(space)).rejects.toMatchObject(keylessError);
    expect(() => db.encryption.recovery.create()).toThrow(EncryptionError);
    expect(() => db.encryption.onReport(() => {})).toThrow(EncryptionError);
  } finally {
    await jazz.close();
  }
});
