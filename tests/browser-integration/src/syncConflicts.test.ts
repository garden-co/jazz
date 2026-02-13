import { commands } from "vitest/browser";
import { AuthSecretStorage, co, z } from "jazz-tools";
import { assert, afterAll, afterEach, describe, expect, test } from "vitest";
import { createAccountContext, startSyncServer, waitFor } from "./testUtils";

const Issue = co.map({ estimate: z.number() });

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

    await issue.$jazz.waitForSync();

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

    const loadedIssue = await Issue.load(issue.$jazz.id, {
      loadAs: account2,
    });

    assert(loadedIssue);

    await syncServer.close();

    loadedIssue.$jazz.set("estimate", 2);
    await new Promise((resolve) => setTimeout(resolve, 10));
    issue.$jazz.set("estimate", 3);

    await new Promise((resolve) => setTimeout(resolve, 10));

    syncServer = await startSyncServer(syncServer.port, "sync-conflicts");

    await waitFor(() => {
      expect(issue.estimate).toBe(3);
      expect(loadedIssue.estimate).toBe(3);
    });
  });
});
