import { commands } from "@vitest/browser/context";
import { AuthSecretStorage, FileStream, Group, co, z } from "jazz-tools";
import { assert, afterAll, afterEach, describe, expect, test } from "vitest";
import { createAccountContext, startSyncServer } from "./testUtils";

const TestMAP = co.map({
  value: z.string(),
});

const CustomAccount = co
  .account({
    root: TestMAP,
    profile: co.profile(),
  })
  .withMigration((me) => {
    if (me.root === undefined) {
      me.$jazz.set("root", { value: "initial" });
    }
  });

afterAll(async () => {
  await commands.cleanup();
});

describe("Browser sync on unstable connection", () => {
  afterEach(async () => {
    await new AuthSecretStorage().clear();
  });

  test("uploads the data to the sync server even with unstable connection", async () => {
    const syncServer = await startSyncServer();
    const { contextManager } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
    });

    const bytes1MB = 1e6;

    const group = Group.create();
    group.addMember("everyone", "reader");

    const promise = FileStream.createFromBlob(
      new Blob(["1".repeat(bytes1MB)], { type: "image/png" }),
      group,
    );

    await syncServer.disconnectAllClients();

    const file = await promise;

    await syncServer.disconnectAllClients();

    await file.$jazz.waitForSync();

    contextManager.done();
    await new AuthSecretStorage().clear();

    await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
    });

    const promise2 = FileStream.loadAsBlob(file.$jazz.id);

    await syncServer.disconnectAllClients();

    const fileOnSecondAccount = await promise2;

    expect(fileOnSecondAccount?.size).toBe(bytes1MB);
  });

  test("wait for online when creating data when offline and calling waitForSync", async () => {
    const syncServer = await startSyncServer();
    const { contextManager } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
    });

    const group = Group.create();
    group.addMember("everyone", "reader");

    await syncServer.setOnline(false);

    const map = TestMAP.create({ value: "initial" }, { owner: group });

    await new Promise((resolve) => setTimeout(resolve, 10));

    await syncServer.setOnline(true);

    await map.$jazz.waitForSync();

    contextManager.done();

    await new AuthSecretStorage().clear();

    await new Promise((resolve) => {
      const req = indexedDB.deleteDatabase("jazz-storage");
      req.onsuccess = function () {
        resolve(undefined);
      };
    });

    await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
    });

    await syncServer.setOnline(false);

    const promise = TestMAP.load(map.$jazz.id);

    await new Promise((resolve) => setTimeout(resolve, 10));
    await syncServer.setOnline(true);

    const mapOnSecondAccount = await promise;

    assert(mapOnSecondAccount);

    expect(mapOnSecondAccount.value).toBe("initial");
  });

  test("load files from storage correctly when pointing to different sync servers", async () => {
    const syncServer = await startSyncServer();
    const { contextManager } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      databaseName: "shared-database",
      AccountSchema: CustomAccount,
    });

    const bytes10MB = 1e7;

    const group = Group.create();
    group.addMember("everyone", "reader");

    const file = await FileStream.createFromBlob(
      new Blob(["1".repeat(bytes10MB)], { type: "image/png" }),
      group,
    );

    const fileStream = await file.$jazz.waitForSync();

    expect(fileStream).toBeDefined();

    contextManager.done();

    const anotherSyncServer = await startSyncServer();

    await createAccountContext({
      sync: {
        peer: anotherSyncServer.url,
      },
      storage: "indexedDB",
      databaseName: "shared-database",
      AccountSchema: CustomAccount,
    });

    const fileOnSecondAccount = await FileStream.loadAsBlob(file.$jazz.id);

    expect(fileOnSecondAccount?.size).toBe(bytes10MB);
  });
});
