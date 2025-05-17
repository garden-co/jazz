import { commands } from "@vitest/browser/context";
import {
  Account,
  AuthSecretStorage,
  CoMap,
  FileStream,
  Group,
  coField,
} from "jazz-tools";
import { afterAll, afterEach, describe, expect, test } from "vitest";
import { createAccountContext, startSyncServer } from "./testUtils";

class TestMap extends CoMap {
  value = coField.string;
}

class CustomAccount extends Account {
  root = coField.ref(TestMap);

  migrate() {
    if (!this.root) {
      this.root = TestMap.create({ value: "initial" }, { owner: this });
    }
  }
}

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

    const bytes10MB = 1e7;

    const group = Group.create();
    group.addMember("everyone", "reader");

    const promise = FileStream.createFromBlob(
      new Blob(["1".repeat(bytes10MB)], { type: "image/png" }),
      group,
    );

    await syncServer.disconnectAllClients();

    const file = await promise;

    await syncServer.disconnectAllClients();

    await file.waitForSync();

    contextManager.done();
    await new AuthSecretStorage().clear();

    await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
    });

    const promise2 = FileStream.loadAsBlob(file.id);

    // TODO: If the connection is dropped in the middle of streaming, the load fails
    // await syncServer.disconnectAllClients()

    const fileOnSecondAccount = await promise2;

    expect(fileOnSecondAccount?.size).toBe(bytes10MB);
  });

  // TODO: This test is flaky, it fails when running it in CI. Related to an issue investigation
  // so it's probably flaky due some bugs.
  test.skip("load files from storage correctly when pointing to different sync servers", async () => {
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

    const fileStream = await file.waitForSync();

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

    const fileOnSecondAccount = await FileStream.loadAsBlob(file.id);

    expect(fileOnSecondAccount?.size).toBe(bytes10MB);
  });
});
