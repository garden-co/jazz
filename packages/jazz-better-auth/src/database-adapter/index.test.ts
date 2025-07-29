import { runAdapterTest } from "better-auth/adapters/test";
import { createWorkerAccount } from "jazz-run/createWorkerAccount";
import { startSyncServer } from "jazz-run/startSyncServer";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { JazzBetterAuthDatabaseAdapter } from "./index.js";

describe("JazzBetterAuthDatabaseAdapter Tests", async () => {
  let syncServer: any;
  let accountID: string;
  let accountSecret: string;
  let adapter: ReturnType<typeof JazzBetterAuthDatabaseAdapter>;

  beforeAll(async () => {
    syncServer = await startSyncServer({
      port: undefined,
      inMemory: true,
      db: "memory",
    });

    const address = syncServer.address();

    if (typeof address !== "object" || address === null) {
      throw new Error("Server address is not an object");
    }

    const workerAccount = await createWorkerAccount({
      name: "test",
      peer: `ws://localhost:${address.port}`,
    });

    accountID = workerAccount.accountID;
    accountSecret = workerAccount.agentSecret;

    adapter = JazzBetterAuthDatabaseAdapter({
      debugLogs: {
        // If your adapter config allows passing in debug logs, then pass this here.
        // isRunningAdapterTests: true, // This is our super secret flag to let us know to only log debug logs if a test fails.
      },
      syncServer: `ws://localhost:${syncServer.address().port}`,
      accountID,
      accountSecret,
    });
  });

  afterAll(async () => {
    syncServer.close();
  });

  await runAdapterTest({
    disableTests: {
      SHOULD_PREFER_GENERATE_ID_IF_PROVIDED: true,
    },
    getAdapter: async (betterAuthOptions = {}) => {
      return adapter(betterAuthOptions);
    },
  });
});
