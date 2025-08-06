import { betterAuth } from "better-auth";
import { runAdapterTest } from "better-auth/adapters/test";
import { assert, afterAll, beforeAll, describe, expect, it } from "vitest";
import { Account, Group, co, z } from "../../..";
import {
  createWorkerAccount,
  setupJazzTestSync,
  startSyncServer,
} from "../../../tools/testing.js";
import { startWorker } from "../../../worker/index.js";
import { JazzBetterAuthDatabaseAdapter } from "../index.js";

describe("JazzBetterAuthDatabaseAdapter tests", async () => {
  describe("better-auth internal tests", async () => {
    let syncServer: any;
    let accountID: string;
    let accountSecret: string;
    let adapter: ReturnType<typeof JazzBetterAuthDatabaseAdapter>;

    beforeAll(async () => {
      syncServer = await startSyncServer();
      await setupJazzTestSync({ asyncPeers: true });

      const workerAccount = await createWorkerAccount({
        name: "test",
        peer: `ws://localhost:${syncServer.port}`,
      });

      accountID = workerAccount.accountID;
      accountSecret = workerAccount.agentSecret;

      adapter = JazzBetterAuthDatabaseAdapter({
        debugLogs: {
          // If your adapter config allows passing in debug logs, then pass this here.
          isRunningAdapterTests: true, // This is our super secret flag to let us know to only log debug logs if a test fails.
        },
        syncServer: `ws://localhost:${syncServer.port}`,
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

  describe("persistence tests", async () => {
    let syncServer: any;
    let accountID: string;
    let accountSecret: string;

    beforeAll(async () => {
      syncServer = await startSyncServer();

      const workerAccount = await createWorkerAccount({
        name: "test",
        peer: `ws://localhost:${syncServer.port}`,
      });

      accountID = workerAccount.accountID;
      accountSecret = workerAccount.agentSecret;
    });

    it("data should be persisted", async () => {
      const auth = betterAuth({
        emailAndPassword: {
          enabled: true,
        },
        database: JazzBetterAuthDatabaseAdapter({
          syncServer: `ws://localhost:${syncServer.port}`,
          accountID,
          accountSecret,
        }),
      });

      const res = await auth.api.signUpEmail({
        body: {
          name: "test",
          email: "test@test.com",
          password: "123445678",
        },
      });

      expect(res.user.id).match(/^co_\w+$/);

      const log1 = await auth.api.signInEmail({
        body: {
          email: "test@test.com",
          password: "123445678",
        },
      });

      expect(log1.user.id).match(/^co_\w+$/);
    });

    it("data should be isolated by accounts", async () => {
      const auth1 = betterAuth({
        emailAndPassword: {
          enabled: true,
        },
        database: JazzBetterAuthDatabaseAdapter({
          syncServer: `ws://localhost:${syncServer.port}`,
          accountID,
          accountSecret,
        }),
      });

      await auth1.api.signUpEmail({
        body: {
          name: "test",
          email: "isolated@test.com",
          password: "123445678",
        },
      });

      const newWorker = await createWorkerAccount({
        name: "test2",
        peer: `ws://localhost:${syncServer.port}`,
      });

      const auth2 = betterAuth({
        emailAndPassword: {
          enabled: true,
        },
        database: JazzBetterAuthDatabaseAdapter({
          syncServer: `ws://localhost:${syncServer.port}`,
          accountID: newWorker.accountID,
          accountSecret: newWorker.agentSecret,
        }),
      });

      await expect(() =>
        auth2.api.signInEmail({
          body: {
            email: "isolated@test.com",
            password: "123445678",
          },
        }),
      ).rejects.toThrow("Invalid email or password");
    });

    it.skip("tables can be shared with another account", async () => {
      // Create a new account with main Worker
      const auth1 = betterAuth({
        emailAndPassword: {
          enabled: true,
        },
        database: JazzBetterAuthDatabaseAdapter({
          syncServer: `ws://localhost:${syncServer.port}`,
          accountID,
          accountSecret,
        }),
      });

      await auth1.api.signUpEmail({
        body: {
          name: "test",
          email: "shared@test.com",
          password: "123445678",
        },
      });

      // Get the owner Group from the main worker
      const { worker } = await startWorker({
        syncServer: `ws://localhost:${syncServer.port}`,
        accountID,
        accountSecret,
      });

      const DatabaseRoot = co.map({
        group: Group,
      });

      console.log("Loading DB with worker: ", accountID);
      const db = await DatabaseRoot.loadUnique("better-auth-root", accountID, {
        loadAs: worker,
        resolve: {
          group: true,
        },
      });

      assert(db);
      assert(db.group);

      // Create a new worker account
      const newWorkerAccount = await createWorkerAccount({
        name: "test2",
        peer: `ws://localhost:${syncServer.port}`,
      });

      const newWorker = await Account.load(newWorkerAccount.accountID);
      assert(newWorker);

      // Add the new worker to the group
      db.group.addMember(newWorker, "reader");

      // Try to authenticate with the authorized new worker
      const auth2 = betterAuth({
        emailAndPassword: {
          enabled: true,
        },
        database: JazzBetterAuthDatabaseAdapter({
          syncServer: `ws://localhost:${syncServer.port}`,
          accountID: newWorkerAccount.accountID,
          accountSecret: newWorkerAccount.agentSecret,
        }),
      });

      const res = await auth2.api.signInEmail({
        body: {
          email: "shared@test.com",
          password: "123445678",
        },
      });

      expect(res.user.id).match(/^co_\w+$/);
    });
  });
});
