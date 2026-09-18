import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createBrowserCrypto } from "./browser.js";

it.each(["removed", "retained"] as const)(
  "revalidates the newest frontier after an older child fails decryption (%s)",
  async (child) => {
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      folders: s.table({ title: s.string() }, { notes: s.reverse("notes", "folder") }),
      notes: s
        .table(
          { projectId: s.uuid(), folderId: s.uuid(), body: s.string() },
          { project: s.rel("projects", "projectId"), folder: s.rel("folders", "folderId") },
        )
        .encrypted({ space: "projectId", columns: ["body"] }),
    });
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.folders.allowRead.always();
      policy.folders.allowInsert.always();
      policy.folders.allowUpdate.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.notes.allowUpdate.always();
      policy.notes.allowDelete.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Db | undefined;
    let stop: (() => void) | undefined;
    let stopPlain: (() => void) | undefined;
    let saved: string | null = null;
    let failCell = false;
    let target = "";
    let started = false;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const adapters = await createBrowserCrypto();
      db = await createDb({
        ...(await localAccountConfig(server.appId, server.url)),
        e2ee: {
          app,
          store: {
            async read() {
              return saved;
            },
            async update(transform) {
              saved = transform(saved);
            },
          },
          crypto: {
            ...adapters,
            cellCipher: {
              ...adapters.cellCipher,
              async decrypt(key, context, envelope) {
                const decoded = new TextDecoder().decode(context);
                if (
                  failCell &&
                  decoded.includes("jazz.e2ee.cell-record.v1") &&
                  decoded.includes(target)
                ) {
                  started = true;
                  await gate;
                  const corrupt = Uint8Array.from(envelope);
                  corrupt[corrupt.length - 1] = corrupt[corrupt.length - 1]! ^ 1;
                  return adapters.cellCipher.decrypt(key, context, corrupt);
                }
                return adapters.cellCipher.decrypt(key, context, envelope);
              },
            },
          },
        },
      });
      const tx = db.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Project" });
      const folder = tx.insert(app.folders, { title: "Folder" });
      const other = tx.insert(app.folders, { title: "Other" });
      const note = tx.insert(app.notes, {
        projectId: project.id,
        folderId: folder.id,
        body: "Original",
      });
      target = note.id;
      await tx.commit().wait({ tier: "global" });
      const query = app.folders.orderBy("title", "asc").include({ notes: true });
      const expected = [
        { ...folder, notes: [note] },
        { ...other, notes: [] },
      ];
      expect(await db.all(query, { tier: "global" })).toEqual(expected);
      const snapshots: unknown[][] = [];
      let failure: Error | undefined;
      stop = db.subscribe(
        query,
        {
          onUpdate: (rows) => snapshots.push(rows),
          onError: (error) => {
            failure = error;
          },
        },
        { tier: "global" },
      );
      await expect.poll(() => failure ?? snapshots.at(-1)).toEqual(expected);
      let physical: unknown;
      stopPlain = db.subscribe(
        app.folders.orderBy("title", "asc").include({ notes: app.notes.select("id") }),
        (rows) => {
          physical = rows;
        },
        { tier: "global" },
      );
      await expect
        .poll(() => physical)
        .toEqual([
          { ...folder, notes: [{ id: note.id }] },
          { ...other, notes: [] },
        ]);
      failCell = true;
      await db.update(app.notes, note.id, { body: "Changed" }).wait({ tier: "global" });
      await expect.poll(() => started).toBe(true);
      if (child === "removed") {
        await db.delete(app.notes, note.id).wait({ tier: "global" });
        await expect
          .poll(() => physical)
          .toEqual([
            { ...folder, notes: [] },
            { ...other, notes: [] },
          ]);
      } else {
        await db.update(app.folders, other.id, { title: "Updated" }).wait({ tier: "global" });
        await expect
          .poll(() => physical)
          .toEqual([
            { ...folder, notes: [{ id: note.id }] },
            { ...other, title: "Updated", notes: [] },
          ]);
      }
      release();
      if (child === "removed") {
        await expect
          .poll(() => failure ?? snapshots.at(-1))
          .toEqual([
            { ...folder, notes: [] },
            { ...other, notes: [] },
          ]);
        await db.update(app.folders, folder.id, { title: "Still live" }).wait({ tier: "global" });
        await expect
          .poll(() => failure ?? snapshots.at(-1))
          .toEqual([
            { ...other, notes: [] },
            { ...folder, title: "Still live", notes: [] },
          ]);
        expect(failure).toBeUndefined();
      } else {
        await expect.poll(() => failure?.name).toBe("E2eeDataError");
        expect(snapshots.every((rows) => JSON.stringify(rows) === JSON.stringify(expected))).toBe(
          true,
        );
      }
    } finally {
      release();
      stop?.();
      stopPlain?.();
      await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);
