import { commands } from "@vitest/browser/context";
import { AuthSecretStorage, co, Group, z } from "jazz-tools";
import {
  afterAll,
  afterEach,
  assert,
  describe,
  expect,
  onTestFinished,
  test,
} from "vitest";
import { createAccountContext, startSyncServer } from "./testUtils";

const TestMap = co.map({ value: z.string() });

const CustomAccount = co
  .account({
    profile: co.map({ name: z.string() }),
    root: TestMap,
  })
  .withMigration((account) => {
    if (!account.root) {
      account.root = TestMap.create({ value: "initial" }, { owner: account });
    }
  });

afterAll(async () => {
  await commands.cleanup();
});

describe("Browser sync", () => {
  afterEach(async () => {
    await new AuthSecretStorage().clear();
  });

  test("syncs data between accounts through sync server", async () => {
    const syncServer = await startSyncServer();

    const { account: account1, contextManager } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
    });

    const group = Group.create(account1);
    const map = TestMap.create({ value: "test data" }, group);
    group.addMember("everyone", "reader");

    await map.waitForSync();
    contextManager.done();

    // Clearing the credentials storage so the next auth will be a new account
    await contextManager.getAuthSecretStorage().clear();

    const { account: account2 } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
    });
    // Load map in second account
    const loadedMap = await TestMap.load(map.id, {
      loadAs: account2,
    });

    expect(loadedMap).toBeDefined();
    expect(loadedMap?.value).toBe("test data");
  });

  test("loads the previous account through the sync server", async () => {
    const syncServer = await startSyncServer();

    const { account: account1, contextManager } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
    });

    const group = Group.create(account1);
    const map = TestMap.create({ value: "test data" }, group);
    group.addMember("everyone", "reader");

    await map.waitForSync();
    contextManager.done();

    const { account: account2 } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
    });

    expect(account1.id).toBe(account2.id);
  });

  test("syncs data between accounts through storage only", async () => {
    await startSyncServer();

    const { context, contextManager } = await createAccountContext({
      sync: {
        when: "never",
      },
      storage: "indexedDB",
      databaseName: "jazz-storage",
      AccountSchema: CustomAccount,
    });

    const group = Group.create(context.me);
    const map = TestMap.create({ value: "test data" }, group);
    group.addMember("everyone", "reader");

    await map.waitForSync();

    // Clearing the credentials storage so the next auth will be a new account
    await contextManager.getAuthSecretStorage().clear();

    const { account: account2 } = await createAccountContext({
      sync: {
        when: "never",
      },
      storage: "indexedDB",
      databaseName: "jazz-storage",
      AccountSchema: CustomAccount,
    });

    const loadedMap = await TestMap.load(map.id, {
      loadAs: account2,
    });

    expect(loadedMap).toBeDefined();
    expect(loadedMap?.value).toBe("test data");
  });

  test("syncs data between accounts when the the storage is shared but the sync server is not", async () => {
    const syncServer = await startSyncServer();

    const { context, contextManager } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      databaseName: "jazz-storage",
      AccountSchema: CustomAccount,
    });

    const group = Group.create(context.me);
    const map = TestMap.create({ value: "test data" }, group);
    group.addMember("everyone", "reader");

    await map.waitForSync();

    // Clearing the credentials storage so the next auth will be a new account
    await contextManager.getAuthSecretStorage().clear();

    const newSyncServer = await commands.startSyncServer();

    const { account: account2 } = await createAccountContext({
      sync: {
        peer: newSyncServer.url,
      },
      storage: "indexedDB",
      databaseName: "jazz-storage",
      AccountSchema: CustomAccount,
    });

    const loadedMap = await TestMap.load(map.id, {
      loadAs: account2,
    });

    expect(loadedMap).toBeDefined();
    expect(loadedMap?.value).toBe("test data");
  });

  test("syncs data between accounts through storage when the the connection is down", async () => {
    const syncServer = await startSyncServer();

    const { context, contextManager } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      databaseName: "jazz-storage",
      AccountSchema: CustomAccount,
    });

    const group = Group.create(context.me);
    const map = TestMap.create({ value: "test data" }, group);
    group.addMember("everyone", "reader");

    await map.waitForSync();

    await syncServer.setOnline(true);

    onTestFinished(async () => {
      await syncServer.setOnline(false);
    });

    // Clearing the credentials storage so the next auth will be a new account
    await contextManager.getAuthSecretStorage().clear();

    const { account: account2 } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      databaseName: "jazz-storage",
      AccountSchema: CustomAccount,
    });

    const loadedMap = await TestMap.load(map.id, {
      loadAs: account2,
    });

    expect(loadedMap).toBeDefined();
    expect(loadedMap?.value).toBe("test data");
  });

  test("manage to persist the account even when the node is closed immediately after creating the value", async () => {
    const syncServer = await startSyncServer();

    const { contextManager } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
      databaseName: "jazz-storage",
    });

    contextManager.done();

    const { account } = await createAccountContext({
      sync: {
        when: "never",
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
      databaseName: "jazz-storage",
    });

    expect(account).toBeDefined();
  });

  test("successfully loads a group with many account dependencies", async () => {
    const syncServer = await startSyncServer();

    // Create first account and group
    const { account: account1, contextManager: contextManager1 } =
      await createAccountContext({
        sync: {
          peer: syncServer.url,
        },
        storage: "indexedDB",
      });

    const group = Group.create(account1);
    await group.waitForSync();
    contextManager1.getAuthSecretStorage().clear();
    contextManager1.done();

    async function extendGroup() {
      // Create second account and group
      const { account, contextManager } = await createAccountContext({
        sync: {
          peer: syncServer.url,
        },
        storage: "indexedDB",
      });

      const childGroup = Group.create(account);
      const groupToExtend = await Group.load(group.id, { loadAs: account });

      assert(groupToExtend);
      childGroup.extend(groupToExtend);
      await childGroup.waitForSync();
      contextManager.getAuthSecretStorage().clear();
      contextManager.done();
    }

    await extendGroup();
    await extendGroup();
    await extendGroup();

    // Create a new account to load all groups
    const { account: loadingAccount } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
      databaseName: "jazz-storage",
    });

    await Group.load(group.id, { loadAs: loadingAccount });

    const { account: loadingRetryAccount } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: CustomAccount,
      databaseName: "jazz-storage",
    });

    const loadedGroup = await Group.load(group.id, {
      loadAs: loadingRetryAccount,
    });

    expect(loadedGroup).toBeDefined();
  });
});
