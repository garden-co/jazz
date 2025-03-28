import { commands } from "@vitest/browser/context";
import { Account, AuthSecretStorage, CoMap, Group, co } from "jazz-tools";
import {
  assert,
  afterAll,
  afterEach,
  describe,
  expect,
  onTestFinished,
  test,
} from "vitest";
import { createAccountContext, startSyncServer } from "./testUtils";

class Issue extends CoMap {
  estimate = co.number;
}

afterAll(async () => {
  await commands.cleanup();
});

describe("Browser sync", () => {
  afterEach(async () => {
    await new AuthSecretStorage().clear();
  });

  test("syncs conflicts on the same map thorugh sync server", async () => {
    let syncServer = await startSyncServer(0, "sync-conflicts");

    const { context, contextManager } = await createAccountContext({
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      databaseName: "browser1",
    });

    const issue = Issue.create({ estimate: 1 }, context.me);

    await issue.waitForSync();

    // Clearing the credentials storage so the next auth will be a new account
    const credentials = await contextManager.getAuthSecretStorage().get();

    assert(credentials);

    const { account: account2 } = await createAccountContext(
      {
        sync: {
          peer: syncServer.url,
        },
        storage: "indexedDB",
        databaseName: "browser2",
      },
      {
        credentials,
      },
    );

    const loadedIssue = await Issue.load(issue.id, account2, {});

    assert(loadedIssue);

    await syncServer.close();

    loadedIssue.estimate = 2;
    await new Promise((resolve) => setTimeout(resolve, 10));
    issue.estimate = 3;

    await new Promise((resolve) => setTimeout(resolve, 10));

    syncServer = await startSyncServer(syncServer.port, "sync-conflicts");

    await issue.waitForSync();

    // Hangs up here
    await loadedIssue.waitForSync();

    expect(issue.estimate).toBe(3);
    expect(loadedIssue.estimate).toBe(3);
  });
});
