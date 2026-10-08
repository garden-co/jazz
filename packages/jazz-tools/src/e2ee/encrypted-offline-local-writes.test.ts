import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { app, permissions } from "./fixtures/encrypted-offline-fixture-schema.js";

it("updates an existing encrypted space offline and accepts the write after reconnect", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let stored: string | null = null;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    db = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      e2ee: {
        app,
        store: {
          async read() {
            return stored;
          },
          async update(transform) {
            stored = transform(stored);
          },
        },
      },
    });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Offline project" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Before disconnect" });
    await tx.commit().wait({ tier: "global" });
    const query = app.notes.where({ id: note.id });
    expect(await db.one(query, { tier: "global" })).toEqual(note);

    await db.disconnect();
    const write = db.update(app.notes, note.id, { title: "Written offline" });
    expect(write).not.toBeInstanceOf(Promise);
    const local = write.wait({ tier: "local" });
    local.catch(() => {});
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      expect(
        await Promise.race([
          local.then(() => "stored locally"),
          new Promise<string>((resolve) => {
            timer = setTimeout(() => resolve("still waiting"), 5_000);
          }),
        ]),
      ).toBe("stored locally");
    } finally {
      clearTimeout(timer);
    }
    expect(await db.one(query, { tier: "local" })).toEqual({ ...note, title: "Written offline" });

    await db.reconnect();
    await write.wait({ tier: "global" });
    expect(await db.one(query, { tier: "global" })).toEqual({ ...note, title: "Written offline" });
  } finally {
    if (db) {
      await db.reconnect();
      await db.shutdown();
    }
    await server.stop();
  }
}, 60_000);

it.each([false, true])(
  "writes offline after cold creation and verifies online cache failures (cache failure: %s)",
  async (failHistoryPersistence) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let stored: string | null = null;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let cacheFailures = 0;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const config = {
        ...(await localAccountConfig(server.appId, server.url)),
        e2ee: {
          app,
          store: {
            async read() {
              return stored;
            },
            async update(transform: (current: string | null) => string) {
              const next = transform(stored);
              if (failHistoryPersistence && JSON.parse(next).acceptedHistoryV1) {
                cacheFailures++;
                throw new Error("Offline cache storage unavailable");
              }
              stored = next;
            },
          },
        },
      };
      db = await createDb(config);
      const tx = db.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Immediate offline project" });
      const note = tx.insert(app.notes, { projectId: project.id, title: "Initial value" });
      await tx.commit().wait({ tier: "global" });
      expect(cacheFailures > 0).toBe(failHistoryPersistence);
      // No encrypted read or explain() between acceptance and disconnect.
      await db.disconnect();
      const write = db.update(app.notes, note.id, { title: "Immediate offline update" });
      expect(
        await Promise.race([
          write.wait({ tier: "local" }).then(
            () => "stored locally",
            () => "rejected",
          ),
          new Promise<string>((resolve) => {
            timer = setTimeout(() => resolve("still waiting"), 5_000);
          }),
        ]),
      ).toBe("stored locally");
      expect(await db.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual({
        ...note,
        title: "Immediate offline update",
      });
      await db.reconnect();
      await write.wait({ tier: "global" });
      if (failHistoryPersistence) {
        await db.shutdown();
        db = undefined;
        // Drop the initial in-memory bundle, forcing a verified online history read.
        cacheFailures = 0;
        db = await createDb(config);
        await expect(db.one(app.notes.where({ id: note.id }), { tier: "global" })).resolves.toEqual(
          {
            ...note,
            title: "Immediate offline update",
          },
        );
        expect(cacheFailures).toBeGreaterThan(0);
      }
    } finally {
      clearTimeout(timer);
      if (db) {
        await db.reconnect();
        await db.shutdown();
      }
      await server.stop();
    }
  },
  60_000,
);

it("stores an encrypted update locally after unexpected transport loss", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let stored: string | null = null;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    db = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      e2ee: {
        app,
        store: {
          async read() {
            return stored;
          },
          async update(transform) {
            stored = transform(stored);
          },
        },
      },
    });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Transport loss project" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Before transport loss" });
    await tx.commit().wait({ tier: "global" });
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(note);
    await server.stop();
    // Deliberately do not call disconnect(): the network, not the caller, went away.
    const write = db.update(app.notes, note.id, { title: "After transport loss" });
    expect(
      await Promise.race([
        write.wait({ tier: "local" }).then(
          () => "stored locally",
          () => "rejected",
        ),
        new Promise<string>((resolve) => {
          timer = setTimeout(() => resolve("still waiting"), 5_000);
        }),
      ]),
    ).toBe("stored locally");
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual({
      ...note,
      title: "After transport loss",
    });
  } finally {
    clearTimeout(timer);
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);
