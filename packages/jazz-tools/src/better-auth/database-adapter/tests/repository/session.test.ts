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
        userId: user.$jazz.id,
      });

      expect(session.token).toBe("test");
      expect(session.userId).toBe(user.$jazz.id);
      expect(session.$jazz.id).toBeDefined();
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
        userId: user.$jazz.id,
      });

      const sessionByToken = await sessionRepository.findByUnique("session", [
        {
          connector: "AND",
          operator: "eq",
          field: "token",
          value: "test",
        },
      ]);

      expect(sessionByToken?.$jazz.id).toBe(session.$jazz.id);
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
        userId: user.$jazz.id,
      });

      const { sessions } = await (
        user as unknown as co.loaded<co.Map<{ sessions: co.List<co.Map<any>> }>>
      ).$jazz.ensureLoaded({
        resolve: {
          sessions: {
            $each: true,
          },
        },
      });

      expect(sessions.length).toBe(1);
      expect(sessions.at(0)?.$jazz.id).toBe(session.$jazz.id);

      // The generic table should be empty
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
    });
  });

  describe("delete", () => {
    it("should return 0 when trying to delete by non-existent id", async () => {
      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const deleted = await sessionRepository.deleteValue("session", [
        {
          field: "id",
          operator: "eq",
          value: "does-not-exist",
          connector: "AND",
        },
      ]);

      expect(deleted).toBe(0);
    });

    it("should throw an error for unsupported where clause", async () => {
      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      await expect(
        sessionRepository.deleteValue("session", [
          { field: "random", operator: "eq", value: "x", connector: "AND" },
        ]),
      ).rejects.toThrow("Unable to find session with where:");
    });

    it("should delete a session by id", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );
      const user = await userRepository.create("user", {
        email: "delete-id@test.com",
      });

      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const session = await sessionRepository.create("session", {
        token: "token-by-id",
        userId: user.$jazz.id,
      });

      const deleted = await sessionRepository.deleteValue("session", [
        {
          field: "id",
          operator: "eq",
          value: session.$jazz.id,
          connector: "AND",
        },
      ]);

      expect(deleted).toBe(1);

      // Validate it's removed from user's sessions
      const { sessions } = await (
        user as unknown as co.loaded<co.Map<{ sessions: co.List<co.Map<any>> }>>
      ).$jazz.ensureLoaded({
        resolve: { sessions: { $each: true } },
      });
      expect(sessions.length).toBe(0);

      // Validate cannot be found anymore
      const foundAgain = await sessionRepository.findById("session", [
        {
          field: "id",
          operator: "eq",
          value: session.$jazz.id,
          connector: "AND",
        },
      ]);
      expect(foundAgain).toBeNull();
    });

    it("should delete a session by token", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );
      const user = await userRepository.create("user", {
        email: "delete-token@test.com",
      });

      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const session = await sessionRepository.create("session", {
        token: "token-by-token",
        userId: user.$jazz.id,
      });

      const deleted = await sessionRepository.deleteValue("session", [
        {
          field: "token",
          operator: "eq",
          value: "token-by-token",
          connector: "AND",
        },
      ]);

      expect(deleted).toBe(1);

      const { sessions } = await (
        user as unknown as co.loaded<co.Map<{ sessions: co.List<co.Map<any>> }>>
      ).$jazz.ensureLoaded({
        resolve: { sessions: { $each: true } },
      });
      expect(sessions.length).toBe(0);
    });

    it("should delete multiple sessions by userId with optional filters", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );
      const user = await userRepository.create("user", {
        email: "delete-many@test.com",
      });

      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const s1 = await sessionRepository.create("session", {
        token: "a1",
        userId: user.$jazz.id,
      });
      const s2 = await sessionRepository.create("session", {
        token: "a2",
        userId: user.$jazz.id,
      });
      await sessionRepository.create("session", {
        token: "b1",
        userId: user.$jazz.id,
      });

      // Delete only tokens that start with 'a'
      const deleted = await sessionRepository.deleteValue("session", [
        {
          field: "userId",
          operator: "eq",
          value: user.$jazz.id,
          connector: "AND",
        },
        {
          field: "token",
          operator: "starts_with",
          value: "a",
          connector: "AND",
        },
      ]);

      expect(deleted).toBe(2);

      const { sessions } = await (
        user as unknown as co.loaded<co.Map<{ sessions: co.List<co.Map<any>> }>>
      ).$jazz.ensureLoaded({
        resolve: { sessions: { $each: true } },
      });
      expect(sessions.length).toBe(1);
      expect(sessions.at(0)?.token).toBe("b1");

      // Verify deleted ones are gone
      const again1 = await sessionRepository.findById("session", [
        { field: "id", operator: "eq", value: s1.$jazz.id, connector: "AND" },
      ]);
      const again2 = await sessionRepository.findById("session", [
        { field: "id", operator: "eq", value: s2.$jazz.id, connector: "AND" },
      ]);
      expect(again1).toBeNull();
      expect(again2).toBeNull();
    });

    it("should return 0 when user not found for userId clause", async () => {
      const sessionRepository = new SessionRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const deleted = await sessionRepository.deleteValue("session", [
        {
          field: "userId",
          operator: "eq",
          value: "missing-user",
          connector: "AND",
        },
        { field: "token", operator: "eq", value: "anything", connector: "AND" },
      ]);

      expect(deleted).toBe(0);
    });
  });
});
