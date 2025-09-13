import { beforeEach, describe, expect, it } from "vitest";
import { Account, co } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import { VerificationRepository } from "../../repository";
import { createJazzSchema, Database } from "../../schema";
import { createWorkerAccount, startSyncServer } from "../sync-utils.js";

describe("VerificationRepository", () => {
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
          token: {
            type: "string",
            required: true,
          },
        },
      },
      verification: {
        modelName: "verification",
        fields: {
          identifier: {
            type: "string",
            required: true,
          },
          value: {
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

  it("should create a verification entity with unique identifier", async () => {
    const verificationRepository = new VerificationRepository(
      databaseSchema,
      databaseRoot,
      worker,
    );

    const verification = await verificationRepository.create("verification", {
      identifier: "test",
      value: "test",
    });

    expect(verification.$jazz.id).toBeDefined();

    const result = await verificationRepository.findMany(
      "verification",
      [
        {
          operator: "eq",
          connector: "AND",
          field: "identifier",
          value: "test",
        },
      ],
      1,
      { field: "createdAt", direction: "desc" },
      undefined,
    );

    expect(result.length).toBe(1);
    expect(result[0]?.$jazz.id).toBe(verification.$jazz.id);
  });
});
