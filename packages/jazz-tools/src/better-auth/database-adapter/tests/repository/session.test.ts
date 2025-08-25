import { beforeEach, describe, expect, it } from "vitest";
import { Account, co } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import { SessionRepository } from "../../repository/session";
import { UserRepository } from "../../repository/user";
import { createJazzSchema, Database } from "../../schema";
import { createWorkerAccount, startSyncServer } from "../sync-utils.js";

describe("SessionRepository", () => {
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

  it("should create a session repository", async () => {
    const sessionRepository = new SessionRepository(
      databaseSchema,
      databaseRoot,
      worker,
    );
  });

  describe("create", () => {
    it("should throw an error if token or userId is not provided", async () => {
      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      await expect(
        sessionRepository.create("session", {
          randomData: "random",
        }),
      ).rejects.toThrow("Token and userId are required for session creation");
    });

    it("should throw an error user does not exist", async () => {
      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      await expect(
        sessionRepository.create("session", {
          token: "test",
          userId: "test",
        }),
      ).rejects.toThrow("User not found");
    });

    it("should create a session", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );
      const user = await userRepository.create("user", {
        email: "test@test.com",
      });

      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const session = await sessionRepository.create("session", {
        token: "test",
        userId: user.id,
      });

      expect(session.token).toBe("test");
      expect(session.userId).toBe(user.id);
      expect(session.id).toBeDefined();
    });

    it("should create a session with a custom uniqueId", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );
      const user = await userRepository.create("user", {
        email: "test@test.com",
      });

      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const session = await sessionRepository.create("session", {
        token: "test",
        userId: user.id,
      });

      const sessionByToken = await sessionRepository.findByUnique("session", [
        {
          connector: "AND",
          operator: "eq",
          field: "token",
          value: "test",
        },
      ]);

      expect(sessionByToken?.id).toBe(session.id);
    });

    it("should create a session inside the user object", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );
      const user = await userRepository.create("user", {
        email: "test@test.com",
      });

      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const session = await sessionRepository.create("session", {
        token: "test",
        userId: user.id,
      });

      const { sessions } = await (
        user as unknown as co.loaded<co.Map<{ sessions: co.List<co.Map<any>> }>>
      ).ensureLoaded({
        resolve: {
          sessions: {
            $each: true,
          },
        },
      });

      expect(sessions.length).toBe(1);
      expect(sessions.at(0)?.id).toBe(session.id);

      // The generic table should be empty
      const { tables } = await databaseRoot.ensureLoaded({
        resolve: {
          tables: {
            session: {
              $each: true,
            },
          },
        },
      });

      expect(tables.session.length).toBe(0);
    });
  });
});
