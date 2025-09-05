import { randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  Group,
  InboxSender,
  Loaded,
  co,
  z,
} from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import { afterAll, describe, expect, onTestFinished, test } from "vitest";
import { createWorkerAccount } from "../createWorkerAccount.js";
import { startSyncServer } from "../startSyncServer.js";
import { waitFor } from "./utils.js";
import { serverDefaults } from "../config.js";

const dbPath = join(tmpdir(), `test-${randomUUID()}.db`);

afterAll(() => {
  unlinkSync(dbPath);
});

async function setup<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(AccountSchema?: S) {
  const { server, port, host } = await setupSyncServer();

  const syncServer = `ws://${host}:${port}`;

  const { worker, done, waitForConnection, subscribeToConnectionChange } =
    await setupWorker(syncServer, AccountSchema);

  return {
    worker,
    done,
    syncServer,
    server,
    port,
    host,
    waitForConnection,
    subscribeToConnectionChange,
  };
}

async function setupSyncServer(
  defaultHost = serverDefaults.host,
  defaultPort = "0",
) {
  const server = await startSyncServer({
    host: defaultHost,
    port: defaultPort,
    inMemory: false,
    db: dbPath,
  });

  const port = (server.address() as { port: number }).port.toString();

  onTestFinished(() => {
    server.close();
  });

  return { server, port, host: defaultHost };
}

async function setupWorker<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(syncServer: string, AccountSchema?: S) {
  const { accountID, agentSecret } = await createWorkerAccount({
    name: "test-worker",
    peer: syncServer,
  });

  return startWorker({
    accountID: accountID,
    accountSecret: agentSecret,
    syncServer,
    AccountSchema,
  });
}

const TestMap = co.map({
  value: z.string(),
});

describe("startWorker integration", () => {
  test("worker connects to sync server successfully", async () => {
    const worker1 = await setup();
    const worker2 = await setupWorker(worker1.syncServer);

    const group = Group.create({ owner: worker1.worker });
    group.addMember("everyone", "reader");

    const map = TestMap.create(
      {
        value: "test",
      },
      { owner: group },
    );

    await map.$jazz.waitForSync();

    const mapOnWorker2 = await TestMap.load(map.$jazz.id, {
      loadAs: worker2.worker,
    });

    expect(mapOnWorker2?.value).toBe("test");

    await worker1.done();
    await worker2.done();
  });

  test("worker handles successfully the custom account migration", async () => {
    const AccountRoot = co.map({
      value: z.string(),
    });

    let shouldReloadPreviousAccount = false;

    const CustomAccount = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account) => {
        if (!account.$jazz.has("root")) {
          if (shouldReloadPreviousAccount) {
            throw new Error("Previous account not found");
          }

          shouldReloadPreviousAccount = true;

          account.$jazz.set("root", {
            value: "test",
          });
        }
      });

    const worker1 = await setup(CustomAccount);

    const { root } = await worker1.worker.$jazz.ensureLoaded({
      resolve: { root: true },
    });

    expect(root.value).toBe("test");

    await worker1.done();

    const worker2 = await startWorker({
      accountID: worker1.worker.$jazz.id,
      accountSecret:
        worker1.worker.$jazz.localNode.getCurrentAgent().agentSecret,
      syncServer: worker1.syncServer,
      AccountSchema: CustomAccount,
    });

    const { root: root2 } = await worker2.worker.$jazz.ensureLoaded({
      resolve: { root: true },
    });

    expect(root2.value).toBe("test");

    await worker2.done();
  });

  test("waits for all coValues to sync before resolving done", async () => {
    const worker1 = await setup();

    const group = Group.create({ owner: worker1.worker });
    group.addMember("everyone", "reader");

    const map = TestMap.create(
      {
        value: "test",
      },
      { owner: group },
    );

    await worker1.done();

    const worker2 = await setupWorker(worker1.syncServer);

    const mapOnWorker2 = await TestMap.load(map.$jazz.id, {
      loadAs: worker2.worker,
    });

    expect(mapOnWorker2?.value).toBe("test");

    await worker2.done();
  });

  test("reiceves the messages from the inbox", async () => {
    const worker1 = await setup();
    const worker2 = await setupWorker(worker1.syncServer);

    const group = Group.create({ owner: worker1.worker });
    const map = TestMap.create(
      {
        value: "Hello!",
      },
      { owner: group },
    );

    worker2.experimental.inbox.subscribe(TestMap, async (value) => {
      return TestMap.create(
        {
          value: value.value + " Responded from the inbox",
        },
        { owner: value.$jazz.owner },
      );
    });

    const sender = await InboxSender.load<
      Loaded<typeof TestMap>,
      Loaded<typeof TestMap>
    >(worker2.worker.$jazz.id, worker1.worker);

    const resultId = await sender.sendMessage(map);

    const result = await TestMap.load(resultId, { loadAs: worker2.worker });

    expect(result?.value).toEqual("Hello! Responded from the inbox");

    await worker1.done();
    await worker2.done();
  });

  test("worker reconnects when sync server is closed and reopened", async () => {
    const worker1 = await setup();
    const worker2 = await setupWorker(worker1.syncServer);

    const group = Group.create({ owner: worker1.worker });
    group.addMember("everyone", "reader");

    const map = TestMap.create(
      {
        value: "initial value",
      },
      { owner: group },
    );

    await map.$jazz.waitForSync();

    // Close the sync server
    worker1.server.close();

    // Create a new value while server is down
    const map2 = TestMap.create(
      {
        value: "created while offline",
      },
      { owner: group },
    );

    map.$jazz.set("value", "updated while offline");

    // Start a new sync server on the same port
    const newServer = await startSyncServer({
      host: worker1.host,
      port: worker1.port,
      inMemory: true,
      db: "",
    });

    await worker1.worker.$jazz.waitForAllCoValuesSync();

    // Verify both old and new values are synced
    const mapOnWorker2 = await TestMap.load(map.$jazz.id, {
      loadAs: worker2.worker,
    });
    const map2OnWorker2 = await TestMap.load(map2.$jazz.id, {
      loadAs: worker2.worker,
    });

    expect(mapOnWorker2?.value).toBe("updated while offline");
    expect(map2OnWorker2?.value).toBe("created while offline");

    // Cleanup
    await worker2.done();
    newServer.close();
  });

  test("waitForConnection resolves when connection is established", async () => {
    const worker1 = await setup();

    // Initially should be connected
    await worker1.waitForConnection();

    // Close the sync server
    worker1.server.close();

    // Start a new sync server on the same port
    const newServer = await startSyncServer({
      host: worker1.host,
      port: worker1.port,
      inMemory: true,
      db: "",
    });

    // Should reconnect and resolve
    await worker1.waitForConnection();

    // Cleanup
    await worker1.done();
    newServer.close();
  });

  test("subscribeToConnectionChange notifies on connection state changes", async () => {
    const worker1 = await setup();

    const connectionStates: boolean[] = [];

    // Subscribe to connection changes
    const unsubscribe = worker1.subscribeToConnectionChange((isConnected) => {
      connectionStates.push(isConnected);
    });

    await waitFor(() => {
      expect(connectionStates).toEqual([true]);
    });

    // Close the sync server
    worker1.server.close();

    await waitFor(() => {
      expect(connectionStates).toEqual([true, false]);
    });

    // Start a new sync server on the same port
    const newServer = await startSyncServer({
      host: worker1.host,
      port: worker1.port,
      inMemory: true,
      db: "",
    });

    // Wait a bit for the reconnection to be detected
    await waitFor(() => {
      expect(connectionStates).toEqual([true, false, true]);
    });

    // Cleanup
    unsubscribe();
    await worker1.done();
    newServer.close();
  });

  test("startWorker should be able to skip the inbox load", async () => {
    const UserAccount = co.account({
      root: co.map({}),
      profile: co.profile(),
    });

    const syncServer = await setupSyncServer();
    const syncServerUrl = `ws://${syncServer.host}:${syncServer.port}`;
    const { accountID, agentSecret } = await createWorkerAccount({
      name: "test-worker",
      peer: syncServerUrl,
    });

    const { worker } = await startWorker({
      AccountSchema: UserAccount,
      syncServer: syncServerUrl,
      accountID: accountID,
      accountSecret: agentSecret,
      skipInboxLoad: true,
    });

    const { profile } = await worker.$jazz.ensureLoaded({
      resolve: { profile: true },
    });

    // Set the inbox manually, so that the inbox load would fail
    profile.$jazz.set("inbox", "co_z3AUWm546svxm8xYEh7KnHqMpP2");
    profile.$jazz.set(
      "inboxInvite",
      "https://cloud.jazz.tools/inbox/co_z3AUWm546svxm8xYEh7KnHqMpP2",
    );

    await worker.$jazz.waitForAllCoValuesSync();

    let workerResult = await startWorker({
      AccountSchema: UserAccount,
      syncServer: syncServerUrl,
      accountID: accountID,
      accountSecret: agentSecret,
      skipInboxLoad: true,
    });

    // Loads successfully even with an unavailable inbox
    expect(workerResult.worker.$jazz.id).toBe(accountID);
  });
});
