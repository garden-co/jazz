/// <reference types="vite/client" />

/**
 * Optimistic rejection and durable restart behavior through the worker bridge.
 */

import { describe, it, expect } from "vitest";
import {
  createBrowserTestDb as createDb,
  acquireBrowserTestAccount,
  createSyncedDb,
  uniqueDbName,
} from "./support.js";

import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";

import {
  todos,
  readOnlyPermissions,
  noUpdatePermissions,
  noDeletePermissions,
  allTodos,
  publishSyncServerSchemaAndPermissions,
  publishPermissionsForServer,
  useSharedWorkerBridgeHarness,
} from "./worker-bridge-harness.js";

describe("SharedWorker bridge with IndexedDB", () => {
  const { ctx, track, untrack } = useSharedWorkerBridgeHarness();
  describe("optimistic writes are reverted on server rejection", () => {
    it("insert", async () => {
      const syncServer = await publishSyncServerSchemaAndPermissions(
        "sync-wait-edge",
        readOnlyPermissions,
      );

      const sharedLocalAuthToken = generateAuthSecret();
      const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

      const insertResult = db.insert(todos, { title: "Rejected", done: false });
      await expect(insertResult.wait({ tier: "global" })).rejects.toMatchObject({
        name: "PersistedWriteRejectedError",
        transactionId: insertResult.txId,
        code: "permission_denied",
      });

      const todosAfterRevert = await db.all(allTodos, { tier: "local" });
      expect(todosAfterRevert.length).toBe(0);
    });

    it("update", async () => {
      const syncServer = await publishSyncServerSchemaAndPermissions(
        "sync-wait-edge",
        noUpdatePermissions,
      );

      const sharedLocalAuthToken = generateAuthSecret();
      const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

      const insertResult = db.insert(todos, {
        title: "Initial task",
        done: false,
      });
      const todo = await insertResult.wait({ tier: "global" });

      const updateResult = db.update(todos, todo.id, { title: "Updated task" });
      await expect(updateResult.wait({ tier: "global" })).rejects.toMatchObject({
        name: "PersistedWriteRejectedError",
        transactionId: updateResult.txId,
        code: "permission_denied",
      });

      const todosAfterRevert = await db.all(allTodos, { tier: "local" });
      expect(todosAfterRevert).toEqual([todo]);
    });

    it("delete", async () => {
      const syncServer = await publishSyncServerSchemaAndPermissions(
        "sync-wait-edge",
        noDeletePermissions,
      );

      const sharedLocalAuthToken = generateAuthSecret();
      const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

      const insertResult = db.insert(todos, {
        title: "Initial task",
        done: false,
      });
      const todo = await insertResult.wait({ tier: "global" });

      const deleteResult = db.delete(todos, todo.id);
      await expect(deleteResult.wait({ tier: "global" })).rejects.toMatchObject({
        name: "PersistedWriteRejectedError",
        transactionId: deleteResult.txId,
        code: "permission_denied",
      });

      const todosAfterRevert = await db.all(allTodos, { tier: "local" });
      expect(todosAfterRevert).toEqual([todo]);
    });

    describe("also reverts after restart", () => {
      it("insert", async () => {
        const syncServer = await publishSyncServerSchemaAndPermissions(
          "sync-restart-revert-insert",
          readOnlyPermissions,
        );

        const dbName = uniqueDbName("sync-restart-revert-insert");
        const account = await acquireBrowserTestAccount({
          appId: syncServer.appId,
          serverUrl: syncServer.serverUrl,
          key: dbName,
        });
        const createPersistentDb = (serverUrl?: string) =>
          createDb({
            appId: syncServer.appId,
            driver: { type: "persistent" as const, dbName },
            serverUrl,
            account,
          });

        const dbBeforeRestart = track(await createPersistentDb(undefined));
        const insertResult = dbBeforeRestart.insert(todos, {
          title: "Rejected after restart",
          done: false,
        });
        await insertResult.wait({ tier: "local" });

        const todosBeforeRestart = await dbBeforeRestart.all(allTodos, {
          tier: "local",
        });
        expect(todosBeforeRestart).toEqual([insertResult.value]);

        await dbBeforeRestart.shutdown();
        untrack(dbBeforeRestart);

        const dbAfterRestart = track(await createPersistentDb(syncServer.serverUrl));
        expect(await dbAfterRestart.all(allTodos, { tier: "global" })).toEqual([]);
        await dbAfterRestart.shutdown();
        untrack(dbAfterRestart);

        // Reopen offline to prove the accepted server state crossed the public
        // runtime lifecycle boundary and was durably settled in the worker.
        const dbAfterSettlement = track(await createPersistentDb(undefined));
        expect(await dbAfterSettlement.all(allTodos, { tier: "local" })).toEqual([]);
      });

      it("update", async () => {
        const syncServer = await publishSyncServerSchemaAndPermissions(
          "sync-restart-revert-update",
        );

        const dbName = uniqueDbName("sync-restart-revert-update");
        const account = await acquireBrowserTestAccount({
          appId: syncServer.appId,
          serverUrl: syncServer.serverUrl,
          key: dbName,
        });
        const createPersistentDb = (serverUrl?: string) =>
          createDb({
            appId: syncServer.appId,
            driver: { type: "persistent" as const, dbName },
            serverUrl,
            account,
          });

        const seeder = track(await createPersistentDb(syncServer.serverUrl));
        const insertResult = seeder.insert(todos, {
          title: "Initial task",
          done: false,
        });
        const todo = insertResult.value;
        await insertResult.wait({ tier: "global" });
        await seeder.shutdown();
        untrack(seeder);

        await publishPermissionsForServer(syncServer, noUpdatePermissions);

        const dbBeforeRestart = track(await createPersistentDb(undefined));
        expect(await dbBeforeRestart.all(allTodos, { tier: "local" })).toEqual([todo]);

        const updateResult = dbBeforeRestart.update(todos, todo.id, {
          title: "Rejected update after restart",
        });
        await updateResult.wait({ tier: "local" });

        const todosBeforeRestart = await dbBeforeRestart.all(allTodos, {
          tier: "local",
        });
        expect(todosBeforeRestart).toEqual([{ ...todo, title: "Rejected update after restart" }]);

        await dbBeforeRestart.shutdown();
        untrack(dbBeforeRestart);

        const dbAfterRestart = track(await createPersistentDb(syncServer.serverUrl));
        expect(await dbAfterRestart.all(allTodos, { tier: "global" })).toEqual([todo]);
        await dbAfterRestart.shutdown();
        untrack(dbAfterRestart);

        const dbAfterSettlement = track(await createPersistentDb(undefined));
        expect(await dbAfterSettlement.all(allTodos, { tier: "local" })).toEqual([todo]);
      });

      it("delete", async () => {
        const syncServer = await publishSyncServerSchemaAndPermissions(
          "sync-restart-revert-delete",
        );

        const dbName = uniqueDbName("sync-restart-revert-delete");
        const account = await acquireBrowserTestAccount({
          appId: syncServer.appId,
          serverUrl: syncServer.serverUrl,
          key: dbName,
        });
        const createPersistentDb = (serverUrl?: string) =>
          createDb({
            appId: syncServer.appId,
            driver: { type: "persistent" as const, dbName },
            serverUrl,
            account,
          });

        const seeder = track(await createPersistentDb(syncServer.serverUrl));
        const insertResult = seeder.insert(todos, {
          title: "Initial task",
          done: false,
        });
        const todo = insertResult.value;
        await insertResult.wait({ tier: "global" });
        await seeder.shutdown();
        untrack(seeder);

        await publishPermissionsForServer(syncServer, noDeletePermissions);

        const dbBeforeRestart = track(await createPersistentDb(undefined));
        expect(await dbBeforeRestart.all(allTodos, { tier: "local" })).toEqual([todo]);

        const deleteResult = dbBeforeRestart.delete(todos, todo.id);
        await deleteResult.wait({ tier: "local" });

        const todosBeforeRestart = await dbBeforeRestart.all(allTodos, {
          tier: "local",
        });
        expect(todosBeforeRestart).toEqual([]);

        await dbBeforeRestart.shutdown();
        untrack(dbBeforeRestart);

        const dbAfterRestart = track(await createPersistentDb(syncServer.serverUrl));
        expect(await dbAfterRestart.all(allTodos, { tier: "global" })).toEqual([todo]);
        await dbAfterRestart.shutdown();
        untrack(dbAfterRestart);

        const dbAfterSettlement = track(await createPersistentDb(undefined));
        expect(await dbAfterSettlement.all(allTodos, { tier: "local" })).toEqual([todo]);
      });
    });
  });
});
