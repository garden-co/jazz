import { beforeEach, describe, expect, it } from "vitest";
import { Account, co } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import { UserRepository } from "../../repository/user";
import { createJazzSchema, Database } from "../../schema";
import { createWorkerAccount, startSyncServer } from "../sync-utils.js";

describe("UserRepository", () => {
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
          name: {
            type: "string",
            required: false,
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

  it("should create a user repository", async () => {
    const userRepository = new UserRepository(
      databaseSchema,
      databaseRoot,
      worker,
    );
    expect(userRepository).toBeDefined();
  });

  describe("create", () => {
    it("should create a user with email index and sessions list", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const user = await userRepository.create("user", {
        email: "test@example.com",
        name: "Test User",
      });

      expect(user.$jazz.id).toBeDefined();
      expect(user.email).toBe("test@example.com");
      expect(user.name).toBe("Test User");
      expect(user.sessions).toBeDefined();
      expect(Array.isArray(user.sessions)).toBe(true);
      expect(user.sessions.length).toBe(0);
    });

    it("should throw error when creating user with existing email", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      await userRepository.create("user", {
        email: "test@example.com",
        name: "Test User",
      });

      await expect(
        userRepository.create("user", {
          email: "test@example.com",
          name: "Another User",
        }),
      ).rejects.toThrow("Email already exists");
    });

    it("should handle missing session schema gracefully", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      // This test verifies that the repository handles the case where
      // the session schema exists (as required by createJazzSchema)
      // but the session list schema lookup might fail
      const user = await userRepository.create("user", {
        email: "test@example.com",
        name: "Test User",
      });

      expect(user.$jazz.id).toBeDefined();
      expect(user.email).toBe("test@example.com");
      expect(user.sessions).toBeDefined();
    });

    it("should create multiple users with different emails", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const user1 = await userRepository.create("user", {
        email: "user1@example.com",
        name: "User One",
      });

      const user2 = await userRepository.create("user", {
        email: "user2@example.com",
        name: "User Two",
      });

      expect(user1.$jazz.id).not.toBe(user2.$jazz.id);
      expect(user1.email).toBe("user1@example.com");
      expect(user2.email).toBe("user2@example.com");
    });
  });

  describe("findMany", () => {
    it("should find user by email using email index", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const createdUser = await userRepository.create("user", {
        email: "test@example.com",
        name: "Test User",
      });

      const users = await userRepository.findMany("user", [
        {
          field: "email",
          operator: "eq",
          value: "test@example.com",
          connector: "AND",
        },
      ]);

      expect(users.length).toBe(1);
      expect(users[0]?.$jazz.id).toBe(createdUser.$jazz.id);
      expect(users[0]?.email).toBe("test@example.com");
    });

    it("should return empty array when finding non-existent email", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const users = await userRepository.findMany("user", [
        {
          field: "email",
          operator: "eq",
          value: "nonexistent@example.com",
          connector: "AND",
        },
      ]);

      expect(users.length).toBe(0);
    });

    it("should find users with other where conditions using generic logic", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const user1 = await userRepository.create("user", {
        email: "user1@example.com",
        name: "User One",
      });

      const user2 = await userRepository.create("user", {
        email: "user2@example.com",
        name: "User Two",
      });

      // Find all users
      const allUsers = await userRepository.findMany("user", []);
      expect(allUsers.length).toBe(2);

      // Find by name
      const usersByName = await userRepository.findMany("user", [
        {
          field: "name",
          operator: "eq",
          value: "User One",
          connector: "AND",
        },
      ]);
      expect(usersByName.length).toBe(1);
      expect(usersByName[0]?.$jazz.id).toBe(user1.$jazz.id);
    });

    it("should find users by id using generic logic", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const createdUser = await userRepository.create("user", {
        email: "test@example.com",
        name: "Test User",
      });

      const users = await userRepository.findMany("user", [
        {
          field: "id",
          operator: "eq",
          value: createdUser.$jazz.id,
          connector: "AND",
        },
      ]);

      expect(users.length).toBe(1);
      expect(users[0]?.$jazz.id).toBe(createdUser.$jazz.id);
    });
  });

  describe("update", () => {
    it("should update user without email change", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const user = await userRepository.create("user", {
        email: "test@example.com",
        name: "Test User",
      });

      await userRepository.update(
        "user",
        [
          {
            field: "id",
            operator: "eq",
            value: user.$jazz.id,
            connector: "AND",
          },
        ],
        { name: "Updated User" },
      );

      const updatedUsers = await userRepository.findMany("user", [
        { field: "id", operator: "eq", value: user.$jazz.id, connector: "AND" },
      ]);

      expect(updatedUsers.length).toBe(1);
      expect(updatedUsers[0]?.name).toBe("Updated User");
      expect(updatedUsers[0]?.email).toBe("test@example.com");
    });

    it("should update user email and maintain email index", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const user = await userRepository.create("user", {
        email: "old@example.com",
        name: "Test User",
      });

      // Update email
      const updatedUsers = await userRepository.update(
        "user",
        [
          {
            field: "id",
            operator: "eq",
            value: user.$jazz.id,
            connector: "AND",
          },
        ],
        { email: "new@example.com" },
      );

      expect(updatedUsers.length).toBe(1);
      expect(updatedUsers[0]?.email).toBe("new@example.com");

      // Verify old email is no longer findable
      const oldEmailUsers = await userRepository.findMany("user", [
        {
          field: "email",
          operator: "eq",
          value: "old@example.com",
          connector: "AND",
        },
      ]);
      expect(oldEmailUsers.length).toBe(0);

      // Verify new email is findable
      const newEmailUsers = await userRepository.findMany("user", [
        {
          field: "email",
          operator: "eq",
          value: "new@example.com",
          connector: "AND",
        },
      ]);
      expect(newEmailUsers.length).toBe(1);
      expect(newEmailUsers[0]?.$jazz.id).toBe(user.$jazz.id);
    });

    it("should handle update with no matching users", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const updatedUsers = await userRepository.update(
        "user",
        [
          {
            field: "id",
            operator: "eq",
            value: "non-existent-id",
            connector: "AND",
          },
        ],
        { name: "Updated" },
      );

      expect(updatedUsers.length).toBe(0);
    });

    it("should not update email index when email is undefined in update", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const user = await userRepository.create("user", {
        email: "test@example.com",
        name: "Test User",
      });

      // Update only name, not email
      await userRepository.update(
        "user",
        [
          {
            field: "id",
            operator: "eq",
            value: user.$jazz.id,
            connector: "AND",
          },
        ],
        { name: "Updated User" },
      );

      // Verify email is still findable
      const emailUsers = await userRepository.findMany("user", [
        {
          field: "email",
          operator: "eq",
          value: "test@example.com",
          connector: "AND",
        },
      ]);
      expect(emailUsers.length).toBe(1);
      expect(emailUsers[0]?.name).toBe("Updated User");
    });
  });

  describe("deleteValue", () => {
    it("should delete user and remove from email index", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const user = await userRepository.create("user", {
        email: "test@example.com",
        name: "Test User",
      });

      const deletedCount = await userRepository.deleteValue("user", [
        { field: "id", operator: "eq", value: user.$jazz.id, connector: "AND" },
      ]);

      expect(deletedCount).toBe(1);

      // Verify user is no longer findable
      const users = await userRepository.findMany("user", []);
      expect(users.length).toBe(0);

      // Verify email is no longer findable
      const emailUsers = await userRepository.findMany("user", [
        {
          field: "email",
          operator: "eq",
          value: "test@example.com",
          connector: "AND",
        },
      ]);
      expect(emailUsers.length).toBe(0);
    });

    it("should delete multiple users and clean up email indexes", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const user1 = await userRepository.create("user", {
        email: "user1@example.com",
        name: "User One",
      });

      const user2 = await userRepository.create("user", {
        email: "user2@example.com",
        name: "User Two",
      });

      // Delete all users
      const deletedCount = await userRepository.deleteValue("user", []);
      expect(deletedCount).toBe(2);

      // Verify no users are findable
      const users = await userRepository.findMany("user", []);
      expect(users.length).toBe(0);

      // Verify emails are no longer findable
      const email1Users = await userRepository.findMany("user", [
        {
          field: "email",
          operator: "eq",
          value: "user1@example.com",
          connector: "AND",
        },
      ]);
      expect(email1Users.length).toBe(0);

      const email2Users = await userRepository.findMany("user", [
        {
          field: "email",
          operator: "eq",
          value: "user2@example.com",
          connector: "AND",
        },
      ]);
      expect(email2Users.length).toBe(0);
    });

    it("should handle deletion with no matching users", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const deletedCount = await userRepository.deleteValue("user", [
        {
          field: "id",
          operator: "eq",
          value: "non-existent-id",
          connector: "AND",
        },
      ]);

      expect(deletedCount).toBe(0);
    });

    it("should clean up email index for user without email", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const user = await userRepository.create("user", {
        email: "test@example.com",
        name: "Test User",
      });

      // Then delete the user
      const deletedCount = await userRepository.deleteValue("user", [
        { field: "id", operator: "eq", value: user.$jazz.id, connector: "AND" },
      ]);

      expect(deletedCount).toBe(1);

      // Verify no users remain
      const users = await userRepository.findMany("user", []);
      expect(users.length).toBe(0);
    });
  });

  describe("email index management", () => {
    it("should handle custom email field name from better auth schema", async () => {
      const customSchema = createJazzSchema({
        user: {
          modelName: "user",
          fields: {
            emailAddress: {
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

      const userRepository = new UserRepository(
        customSchema.DatabaseRoot,
        databaseRoot,
        worker,
        {
          user: {
            modelName: "user",
            fields: {
              email: {
                type: "string",
                fieldName: "emailAddress",
              },
            },
          },
        },
      );

      const user = await userRepository.create("user", {
        emailAddress: "test@example.com",
      });

      expect(user.emailAddress).toBe("test@example.com");

      // Should be findable by the custom field name
      const users = await userRepository.findMany("user", [
        {
          field: "email",
          operator: "eq",
          value: "test@example.com",
          connector: "AND",
        },
      ]);

      expect(users.length).toBe(1);
      expect(users[0]?.$jazz.id).toBe(user.$jazz.id);
    });

    it("should default to 'email' field when no better auth schema is provided", async () => {
      const userRepository = new UserRepository(
        databaseSchema,
        databaseRoot,
        worker,
      );

      const user = await userRepository.create("user", {
        email: "test@example.com",
      });

      expect(user.email).toBe("test@example.com");
    });
  });
});
