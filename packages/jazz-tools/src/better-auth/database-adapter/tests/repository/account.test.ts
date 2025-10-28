import { beforeEach, describe, expect, it } from "vitest";
import { Account, co } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import { AccountRepository } from "../../repository/account";
import { createJazzSchema, Database } from "../../schema";
import { createWorkerAccount, startSyncServer } from "../sync-utils.js";

describe("AccountRepository", () => {
  let syncServer: any;

  let databaseSchema: Database;
  let databaseRoot: co.loaded<Database, { group: true }>;
  let worker: Account;

  beforeEach(async () => {
    syncServer = await startSyncServer();

    const workerAccount = await createWorkerAccount({
      name: "test",
      peer: `ws://localhost:${syncServer.port}`,
    });

    const JazzSchema = createJazzSchema({
      account: {
        modelName: "account",
        fields: {
          accountId: {
            type: "string",
            required: true,
          },
          providerId: {
            type: "string",
            required: true,
          },
        },
      },
      user: {
        modelName: "user",
        fields: {
          email: {
            type: "string",
            required: true,
          },
        },
      },
      session: {
        modelName: "session",
        fields: {
          userId: {
            type: "string",
            required: true,
          },
          token: {
            type: "string",
            required: true,
          },
        },
      },
    });

    const result = await startWorker({
      AccountSchema: JazzSchema.WorkerAccount,
      syncServer: `ws://localhost:${syncServer.port}`,
      accountID: workerAccount.accountID,
      accountSecret: workerAccount.agentSecret,
    });

    databaseSchema = JazzSchema.DatabaseRoot;
    databaseRoot = await JazzSchema.loadDatabase(result.worker);
    worker = result.worker;
  });

  it("should create a session repository", async () => {
    const repository = new AccountRepository(
      databaseSchema,
      databaseRoot,
      worker,
    );
    expect(repository).toBeDefined();
  });

  describe("create", () => {
    it("should create an account", async () => {
      const accountRepository = new AccountRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const account = await accountRepository.create("account", {
        accountId: "test",
        providerId: "test",
      });

      expect(account.$jazz.id).toBeDefined();
      expect(account.accountId).toBe("test");
      expect(account.providerId).toBe("test");
    });
  });

  describe("findMany", () => {
    it("should find many accounts", async () => {
      const accountRepository = new AccountRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const account = await accountRepository.create("account", {
        accountId: "test",
        providerId: "test",
      });

      const accounts = await accountRepository.findMany("account", []);

      expect(accounts.length).toBe(1);
      expect(accounts[0]?.$jazz.id).toBe(account.$jazz.id);
    });

    it("should find many accounts with where clause", async () => {
      const accountRepository = new AccountRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      await accountRepository.create("account", {
        accountId: "test",
        providerId: "test",
      });

      await accountRepository.create("account", {
        accountId: "test2",
        providerId: "test",
      });

      const accounts = await accountRepository.findMany("account", [
        {
          field: "accountId",
          operator: "eq",
          value: "test",
          connector: "AND",
        },
      ]);

      expect(accounts.length).toBe(1);
    });
  });
});
