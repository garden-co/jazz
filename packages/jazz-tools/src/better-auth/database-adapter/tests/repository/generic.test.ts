import { beforeEach, describe, expect, it } from "vitest";
import { Account, co } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import { JazzRepository } from "../../repository/generic";
import { createJazzSchema, Database } from "../../schema";
import { createWorkerAccount, startSyncServer } from "../sync-utils.js";

describe("Generic Repository", () => {
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

  it("should throw an error on create if the model is not found", async () => {
    const repository = new JazzRepository(databaseSchema, databaseRoot, worker);

    await expect(
      repository.create("not-found", {
        name: "test",
      }),
    ).rejects.toThrow('Schema for model "not-found" not found');
  });

  it("should throw an error on find if the model is not found", async () => {
    const repository = new JazzRepository(databaseSchema, databaseRoot, worker);

    await expect(
      repository.findMany("not-found", [
        {
          field: "id",
          operator: "eq",
          value: "test",
          connector: "AND",
        },
      ]),
    ).rejects.toThrow('Schema for model "not-found" not found');
  });

  it("should create a new entity and append to the list", async () => {
    const repository = new JazzRepository(databaseSchema, databaseRoot, worker);

    const entity = await repository.create("session", {
      userId: "test",
      token: "test",
    });

    const { tables } = await databaseRoot.$jazz.ensureLoaded({
      resolve: {
        tables: {
          session: {
            $each: true,
          },
        },
      },
    });

    expect(tables.session.length).toBe(1);
    expect(tables.session[0]?.$jazz.id).toBe(entity.$jazz.id);
  });

  it("should delete an entity and remove it from the list", async () => {
    const repository = new JazzRepository(databaseSchema, databaseRoot, worker);

    const entity = await repository.create("session", {
      userId: "test",
      token: "test",
    });

    await repository.deleteValue("session", [
      {
        field: "id",
        operator: "eq",
        value: entity.$jazz.id,
        connector: "AND",
      },
    ]);

    const { tables } = await databaseRoot.$jazz.ensureLoaded({
      resolve: {
        tables: {
          session: {
            $each: true,
          },
        },
      },
    });

    expect(tables.session.length).toBe(0);

    const found = await repository.findOne("session", [
      {
        field: "id",
        operator: "eq",
        value: entity.$jazz.id,
        connector: "AND",
      },
    ]);

    expect(found).toBeNull();
  });

  it("should allow recreating an entity after deletion by identifier", async () => {
    const repository = new JazzRepository(databaseSchema, databaseRoot, worker);

    const identifier = "test";

    const entity = await repository.create(
      "session",
      {
        userId: "test",
        token: "test",
      },
      identifier,
    );

    await repository.deleteValue("session", [
      {
        field: "id",
        operator: "eq",
        value: entity.$jazz.id,
        connector: "AND",
      },
    ]);

    const entity2 = await repository.create(
      "session",
      {
        userId: "test",
        token: "test",
      },
      identifier,
    );

    expect(entity2.$jazz.id).toBe(entity.$jazz.id);
    expect(entity2.$jazz.raw.get("_deleted")).toBe(false);
  });
});
