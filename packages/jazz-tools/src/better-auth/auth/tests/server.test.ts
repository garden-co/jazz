import { betterAuth } from "better-auth";
import { memoryAdapter } from "better-auth/adapters/memory";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { jazzPlugin } from "../server.js";

describe("Better Auth - Signup and Login Tests", () => {
  let auth: ReturnType<typeof betterAuth>;

  beforeEach(() => {
    // Create auth instance with in-memory database
    // @ts-expect-error
    auth = betterAuth({
      database: memoryAdapter({
        user: [],
        session: [],
        verification: [],
        account: [],
      }),
      plugins: [jazzPlugin()],
      // clientPlugins: [jazzPluginClient()],
      emailAndPassword: {
        enabled: true,
        requireEmailVerification: false, // Disable for testing
      },
      session: {
        expiresIn: 60 * 60 * 24 * 7, // 7 days
      },
    });
  });

  describe("User Registration (Signup)", () => {
    it("should successfully register a new user with email and password", async () => {
      const userData = {
        name: "test",
        email: "test@example.com",
        password: "securePassword123",
        jazzAuth: {
          accountID: "123",
          secretSeed: [1, 2, 3],
          accountSecret: "123",
          provider: "better-auth",
        },
      };

      const result = await auth.api.signUpEmail({
        body: userData,
      });

      expect(result).toBeDefined();
      expect(result).toMatchObject({
        user: {
          id: expect.any(String),
          email: userData.email,
          name: userData.name,
          image: undefined,
          emailVerified: false,
          createdAt: expect.any(Date),
          updatedAt: expect.any(Date),
        },
        jazzAuth: userData.jazzAuth,
      });

      const res = await (await auth.$context).adapter.findOne({
        model: "user",
        where: [
          {
            field: "id",
            value: result.user.id,
          },
        ],
      });

      expect(res).toMatchObject({
        id: result.user.id,
        accountID: "123",
        encryptedCredentials: expect.any(String),
      });
    });

    it("should fail to register user without account ID", async () => {
      const userData = {
        name: "test",
        email: "email@email.it",
        password: "securePassword123",
      };

      await expect(
        auth.api.signUpEmail({
          body: userData,
        }),
      ).rejects.toThrow("JazzAuth is required");
    });

    it("should have AccountID in the registration hook", async () => {
      const userData = {
        name: "test",
        email: "email@email.it",
        password: "securePassword123",
        jazzAuth: {
          accountID: "123",
          secretSeed: [1, 2, 3],
          accountSecret: "123",
          provider: "better-auth",
        },
      };

      const hookCreation = vi.fn();

      const authWithHook = betterAuth({
        database: memoryAdapter({
          user: [],
          session: [],
          verification: [],
          account: [],
        }),
        plugins: [jazzPlugin()],
        emailAndPassword: {
          enabled: true,
          requireEmailVerification: false, // Disable for testing
        },
        databaseHooks: {
          user: {
            create: {
              after: async (user) => {
                hookCreation(user);
              },
            },
          },
        },
      });

      await authWithHook.api.signUpEmail({
        // @ts-expect-error encryptedCredentials is populated by the hook
        body: userData,
      });

      expect(hookCreation).toHaveBeenCalledWith(
        expect.objectContaining({
          accountID: "123",
        }),
      );
    });
  });

  describe("User login (Signin)", () => {
    it("should successfully login a new user with email and password", async () => {
      const userData = {
        name: "test",
        email: "test@example.com",
        password: "securePassword123",
        jazzAuth: {
          accountID: "123",
          secretSeed: [1, 2, 3],
          accountSecret: "123",
          provider: "better-auth",
        },
      };

      await auth.api.signUpEmail({
        body: userData,
      });

      const result = await auth.api.signInEmail({
        body: {
          email: userData.email,
          password: userData.password,
        },
      });

      expect(result).toBeDefined();
      expect(result).toMatchObject({
        user: {
          id: expect.any(String),
          email: userData.email,
          name: userData.name,
          image: undefined,
          emailVerified: false,
          createdAt: expect.any(Date),
          updatedAt: expect.any(Date),
        },
        jazzAuth: userData.jazzAuth,
      });
    });
  });
});
