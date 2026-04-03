import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, test } from "vitest";
import { betterAuth, type BetterAuthOptions, type DBAdapter } from "better-auth";
import { createJazzContext, WasmSchema, type JazzContext } from "../backend/index.js";
import { jazzAdapter } from "./index.js";

describe("jazzAdapter", () => {
  describe("adapter methods", () => {
    let adapter: DBAdapter<BetterAuthOptions>;
    let context: JazzContext;
    let dataPath: string;

    beforeEach(async () => {
      const root = await mkdtemp(join(tmpdir(), "jazz-better-auth-adapter-"));
      dataPath = join(root, "runtime.skv");

      context = createJazzContext({
        appId: `jazz-better-auth-${randomUUID()}`,
        driver: { type: "persistent", dataPath },
      });

      adapter = jazzAdapter({
        db: () => context.db(wasmSchemaExample),
        schema: wasmSchemaExample,
      })({});
      context = context;
    });

    afterEach(async () => {
      await context.shutdown();
      await rm(dataPath, { recursive: true, force: true });
    });

    it("creates records with Jazz ids", async () => {
      const created = await adapter.create({
        model: "user",
        data: {
          name: "Alice",
          email: "alice@example.com",
          emailVerified: false,
          image: null,
        },
      });

      expect(created.id).toEqual(expect.any(String));
      expect(created.name).toBe("Alice");

      const found = await adapter.findOne({
        model: "user",
        where: [{ field: "id", operator: "eq", value: created.id, connector: "AND" }],
      });

      expect(found).toMatchObject({
        id: created.id,
        email: "alice@example.com",
        name: "Alice",
      });
    });

    it("supports findMany, count, select, sort, limit, and offset on Jazz ids", async () => {
      const createdUsers = [];
      for (const [name, email] of [
        ["user-2", "two@example.com"],
        ["user-1", "one@example.com"],
        ["user-3", "three@example.com"],
      ] as const) {
        createdUsers.push(
          await adapter.create<any>({
            model: "user",
            data: {
              name,
              email,
              emailVerified: false,
              image: null,
            },
          }),
        );
      }

      const rows = await adapter.findMany<any>({
        model: "user",
        where: [
          {
            field: "id",
            operator: "in",
            value: createdUsers.map((user) => user.id),
            connector: "AND",
          },
        ],
        select: ["id", "email"],
        sortBy: { field: "id", direction: "asc" },
        limit: 2,
        offset: 1,
      });

      expect(rows.map((row) => ({ id: row.id, email: row.email }))).toEqual(
        [...createdUsers]
          .sort((left, right) => left.id.localeCompare(right.id))
          .slice(1, 3)
          .map((row) => ({ id: row.id, email: row.email })),
      );
      expect(rows.map((row) => Object.keys(row).sort())).toEqual([
        ["email", "id"],
        ["email", "id"],
      ]);

      await expect(
        adapter.count({
          model: "user",
          where: [
            {
              field: "id",
              operator: "in",
              value: createdUsers.map((user) => user.id),
              connector: "AND",
            },
          ],
        }),
      ).resolves.toBe(3);
    });

    it("updates and deletes records using non-id filters", async () => {
      const alpha = await adapter.create<any>({
        model: "user",
        data: {
          name: "Alpha",
          email: "alpha@example.com",
          emailVerified: false,
          image: null,
        },
      });
      const beta = await adapter.create<any>({
        model: "user",
        data: {
          name: "Beta",
          email: "beta@example.com",
          emailVerified: false,
          image: null,
        },
      });
      await adapter.create<any>({
        model: "user",
        data: {
          name: "Gamma",
          email: "gamma@example.com",
          emailVerified: true,
          image: null,
        },
      });

      const updated = await adapter.update<any>({
        model: "user",
        where: [{ field: "email", operator: "eq", value: "beta@example.com", connector: "AND" }],
        update: { name: "Beta Prime" },
      });

      expect(updated).toMatchObject({
        id: beta.id,
        name: "Beta Prime",
      });

      await expect(
        adapter.updateMany({
          model: "user",
          where: [{ field: "image", operator: "eq", value: null, connector: "AND" }],
          update: { emailVerified: true },
        }),
      ).resolves.toBe(3);

      await adapter.delete({
        model: "user",
        where: [{ field: "email", operator: "eq", value: "alpha@example.com", connector: "AND" }],
      });

      await expect(
        adapter.findOne<any>({
          model: "user",
          where: [{ field: "id", operator: "eq", value: alpha.id, connector: "AND" }],
        }),
      ).resolves.toBeNull();

      await expect(
        adapter.deleteMany({
          model: "user",
          where: [
            { field: "email", operator: "eq", value: "missing@example.com", connector: "AND" },
            { field: "name", operator: "contains", value: "mm", connector: "OR" },
          ],
        }),
      ).resolves.toBe(1);

      const remaining = await adapter.findMany<any>({
        model: "user",
        limit: 100,
        offset: 0,
        sortBy: { field: "id", direction: "asc" },
      });

      expect(remaining.map((row) => row.id)).toEqual([beta.id]);
    });

    it("supports client-side-only where operators", async () => {
      const prefixUser = await adapter.create<any>({
        model: "user",
        data: {
          name: "Alice",
          email: "alice-prefix@example.com",
          emailVerified: false,
          image: null,
        },
      });
      const imageUser = await adapter.create<any>({
        model: "user",
        data: {
          name: "Bob",
          email: "bob-image@example.com",
          emailVerified: false,
          image: "https://example.com/avatar.png",
        },
      });

      const startsWith = await adapter.findMany<any>({
        model: "user",
        where: [{ field: "name", operator: "starts_with", value: "Ali", connector: "AND" }],
        limit: 10,
        offset: 0,
      });
      expect(startsWith.map((row) => row.id)).toEqual([prefixUser.id]);

      const nonNullImage = await adapter.findMany<any>({
        model: "user",
        where: [{ field: "image", operator: "ne", value: null, connector: "AND" }],
        limit: 10,
        offset: 0,
      });
      expect(nonNullImage.map((row) => row.id)).toEqual([imageUser.id]);
    });

    it("supports Better Auth fallback joins", async () => {
      const now = new Date();

      const user = await adapter.create<any>({
        model: "user",
        data: {
          name: "Join User",
          email: "join@example.com",
          emailVerified: true,
          image: null,
        },
      });

      const account = await adapter.create<any>({
        model: "account",
        data: {
          accountId: "github-account",
          providerId: "github",
          userId: user.id,
          accessToken: null,
          refreshToken: null,
          idToken: null,
          accessTokenExpiresAt: null,
          refreshTokenExpiresAt: null,
          scope: "repo",
          password: null,
          createdAt: now,
          updatedAt: now,
        },
      });

      const accounts = await adapter.findMany<any>({
        model: "account",
        where: [{ field: "providerId", operator: "eq", value: "github", connector: "AND" }],
        join: { user: true },
        limit: 10,
        offset: 0,
      });

      expect(accounts).toHaveLength(1);
      expect(accounts[0]).toMatchObject({
        id: account.id,
        providerId: "github",
        user: {
          id: user.id,
          email: "join@example.com",
        },
      });
    });

    it("accepts app-like schema sources from root schema.ts modules", async () => {
      const authSchema = { wasmSchema: wasmSchemaExample };
      const appAdapter = jazzAdapter({
        db: () => context.db(authSchema),
        schema: authSchema,
      })({});

      const created = await appAdapter.create({
        model: "user",
        data: {
          name: "Schema App User",
          email: "schema-app@example.com",
          emailVerified: false,
          image: null,
        },
      });

      expect(created.id).toEqual(expect.any(String));
      await expect(
        appAdapter.findOne({
          model: "user",
          where: [{ field: "id", operator: "eq", value: created.id, connector: "AND" }],
        }),
      ).resolves.toMatchObject({
        id: created.id,
        email: "schema-app@example.com",
      });
    });

    it("creates root schema.ts output for Better Auth CLI generation", async () => {
      const generated = await (adapter as any).createSchema({
        tables: {
          user: {
            modelName: "user",
            fields: {
              name: {
                type: "string",
                required: true,
              },
            },
          },
        },
      });

      expect(generated).toMatchObject({
        path: "./schema-better-auth/schema.ts",
        overwrite: true,
      });
      expect(generated.code).toContain('import { schema as s } from "jazz-tools";');
      expect(generated.code).toContain("export const app: s.App<AppSchema> = s.defineApp(schema);");
    });
  });

  /**
   * These adapter's calls are taken logging Better Auth's queries
   */
  describe("common user flows", async () => {
    let adapter: DBAdapter<BetterAuthOptions>;
    let context: JazzContext;
    let dataPath: string;

    beforeEach(async () => {
      const root = await mkdtemp(join(tmpdir(), "jazz-better-auth-adapter-"));
      dataPath = join(root, "runtime.skv");

      context = createJazzContext({
        appId: `jazz-better-auth-${randomUUID()}`,
        driver: { type: "persistent", dataPath },
      });

      adapter = jazzAdapter({
        db: () => context.db(wasmSchemaExample),
        schema: wasmSchemaExample,
      })({});
      context = context;
    });

    afterEach(async () => {
      await context.shutdown();
      await rm(dataPath, { recursive: true, force: true });
    });

    test("Email and Password: signup + signin + logout", async () => {
      // Signup process
      const existingUser = await adapter.findOne({
        model: "user",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "email",
            value: "test@test.com",
          },
        ],
        select: undefined,
      });
      expect(existingUser).toBeNull();

      const user = await adapter.create({
        model: "user",
        data: {
          name: "test",
          email: "test@test.com",
          emailVerified: false,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
      });

      expect(user.id).toBeDefined();

      const account = await adapter.create({
        model: "account",
        data: {
          userId: user.id,
          providerId: "credential",
          accountId: user.id,
          password: "test:test",
          createdAt: new Date(),
          updatedAt: new Date(),
        },
      });

      expect(account.id).toBeDefined();

      const session = await adapter.create({
        model: "session",
        data: {
          expiresAt: new Date(Date.now() + 1000 * 60 * 60 * 24 * 7),
          token: "Gij57x0dpEkZAtwtAsXjXxgsWOBor5SH",
          createdAt: new Date(),
          updatedAt: new Date(),
          ipAddress: "",
          userAgent:
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
          userId: user.id,
        },
      });

      expect(session.id).toBeDefined();

      // Get session
      const getSession = await adapter.findOne<{ userId: string }>({
        model: "session",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "token",
            value: "Gij57x0dpEkZAtwtAsXjXxgsWOBor5SH",
          },
        ],
        select: undefined,
      });

      expect(getSession).toEqual(session);

      const getSessionUser = await adapter.findOne({
        model: "user",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "id",
            value: getSession!.userId,
          },
        ],
        select: undefined,
      });

      expect(getSessionUser).toEqual(user);

      // Logout
      await adapter.delete({
        model: "session",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "token",
            value: "Gij57x0dpEkZAtwtAsXjXxgsWOBor5SH",
          },
        ],
      });

      const postLogoutSession = await adapter.findOne<{ userId: string }>({
        model: "session",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "token",
            value: "Gij57x0dpEkZAtwtAsXjXxgsWOBor5SH",
          },
        ],
        select: undefined,
      });

      expect(postLogoutSession).toBeNull();

      // SignIn process
      const signInUser = await adapter.findOne<{ id: string }>({
        model: "user",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "email",
            value: "test@test.com",
          },
        ],
        select: undefined,
      });

      expect(signInUser).not.toBeNull();

      const signInAccounts = await adapter.findMany({
        model: "account",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "userId",
            value: signInUser!.id,
          },
        ],
        limit: 100,
        sortBy: undefined,
        offset: undefined,
      });

      expect(signInAccounts.length).toBe(1);

      await adapter.create({
        model: "session",
        data: {
          ipAddress: "",
          userAgent:
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
          expiresAt: new Date(Date.now() + 1000 * 60 * 60 * 24 * 7),
          userId: signInUser!.id,
          token: "s2JKPEV2eN0sio9JzvtlDwddHYcZjptW",
          createdAt: new Date(),
          updatedAt: new Date(),
        },
      });
    });

    test("Social Authentication: signup + signin", async () => {
      // Verification creation before leaving to Social Provider
      await adapter.create({
        model: "verification",
        data: {
          createdAt: new Date(),
          updatedAt: new Date(),
          value:
            '{"callbackURL":"http://localhost:3000","codeVerifier":"oNjY8cSPUXUc4mU_8-wNQ1IiZGV2UzKCxjjJpPx-O3nxetLyHlViXsDLzPh_5jdgizq77mzZpnR_fTnQ52hRvBWgYA1J0Z6qrMpn-GQ0S9fgJgjmnWpwClEiKKVd2e2-","expiresAt":1755607745884}',
          identifier: "Hsj2TincfRy5e96ReAwVfrkgJUa4CAcg",
          expiresAt: new Date(Date.now() + 1000 * 60 * 60 * 24 * 7),
        },
      });

      // Once back
      const verifications = await adapter.findMany<{ id: string }>({
        model: "verification",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "identifier",
            value: "Hsj2TincfRy5e96ReAwVfrkgJUa4CAcg",
          },
        ],
        limit: 1,
        sortBy: { field: "createdAt", direction: "desc" },
        offset: undefined,
      });

      expect(verifications.length).toBe(1);

      await adapter.delete({
        model: "verification",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "id",
            value: verifications[0]!.id,
          },
        ],
      });

      const accounts = await adapter.findMany({
        model: "account",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "accountId",
            value: "account000",
          },
        ],
        limit: 100,
        sortBy: undefined,
        offset: undefined,
      });

      expect(accounts.length).toBe(0);

      const userWithSSOEmail = await adapter.findOne({
        model: "user",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "email",
            value: "test@test.com",
          },
        ],
        select: undefined,
      });

      expect(userWithSSOEmail).toBeNull();

      const user = await adapter.create({
        model: "user",
        data: {
          name: "test",
          email: "test@test.com",
          emailVerified: false,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
      });

      const account = await adapter.create({
        model: "account",
        data: {
          userId: user.id,
          providerId: "github",
          accountId: "account000",
          accessToken: "xyz",
          refreshToken: undefined,
          idToken: undefined,
          accessTokenExpiresAt: undefined,
          refreshTokenExpiresAt: undefined,
          scope: "read:user,user:email",
          createdAt: new Date(),
          updatedAt: new Date(),
        },
      });

      expect(account.id).toBeDefined();

      // Verification creation before leaving to Social Provider
      await adapter.create({
        model: "verification",
        data: {
          createdAt: new Date(),
          updatedAt: new Date(),
          value:
            '{"callbackURL":"http://localhost:3000","codeVerifier":"oNjY8cSPUXUc4mU_8-wNQ1IiZGV2UzKCxjjJpPx-O3nxetLyHlViXsDLzPh_5jdgizq77mzZpnR_fTnQ52hRvBWgYA1J0Z6qrMpn-GQ0S9fgJgjmnWpwClEiKKVd2e2-","expiresAt":1755607745884}',
          identifier: "identifier002",
          expiresAt: new Date(Date.now() + 1000 * 60 * 60 * 24 * 7),
        },
      });

      // Once back
      const verificationsSignIn = await adapter.findMany<{ id: string }>({
        model: "verification",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "identifier",
            value: "identifier002",
          },
        ],
        limit: 1,
        sortBy: { field: "createdAt", direction: "desc" },
        offset: undefined,
      });

      expect(verificationsSignIn.length).toBe(1);

      await adapter.delete({
        model: "verification",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "id",
            value: verificationsSignIn[0]!.id,
          },
        ],
      });

      const accountsSignIn = await adapter.findMany({
        model: "account",
        where: [
          {
            operator: "eq",
            connector: "AND",
            field: "accountId",
            value: "account000",
          },
        ],
        limit: 100,
        sortBy: undefined,
        offset: undefined,
      });

      expect(accountsSignIn.length).toBe(1);
    });
  });

  describe("better-auth usage", () => {
    let context: JazzContext;
    let auth: ReturnType<typeof betterAuth>;
    let dataPath: string;

    beforeEach(async () => {
      const root = await mkdtemp(join(tmpdir(), "jazz-better-auth-adapter-"));
      dataPath = join(root, "runtime.skv");

      context = createJazzContext({
        appId: `jazz-better-auth-${randomUUID()}`,
        driver: { type: "persistent", dataPath },
      });

      context = context;

      // @ts-expect-error - better-auth + plugins
      auth = betterAuth({
        baseURL: "http://localhost:3000",
        database: jazzAdapter({
          db: () => context.db(wasmSchemaExample),
          schema: wasmSchemaExample,
        }),
        emailAndPassword: {
          enabled: true,
        },
      });
    });

    afterEach(async () => {
      await context.shutdown();
      await rm(dataPath, { recursive: true, force: true });
    });

    test("Email and Password: signup + signin + logout", async () => {
      const signUpResponse = await auth.api.signUpEmail({
        body: {
          name: "test",
          email: "test@test.com",
          password: "Password123!",
        },
      });

      expect(signUpResponse.user).toEqual({
        id: expect.any(String),
        name: "test",
        email: "test@test.com",
        emailVerified: false,
        image: undefined,
        createdAt: expect.any(Date),
        updatedAt: expect.any(Date),
      });

      const signInResponse = await auth.api.signInEmail({
        body: {
          email: "test@test.com",
          password: "Password123!",
        },
      });

      expect(signInResponse.user).toEqual({
        id: expect.any(String),
        name: "test",
        email: "test@test.com",
        emailVerified: false,
        image: undefined,
        createdAt: expect.any(Date),
        updatedAt: expect.any(Date),
      });
    });
  });
});

const wasmSchemaExample: WasmSchema = {
  better_auth_user: {
    columns: [
      {
        name: "name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "email",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "emailVerified",
        column_type: {
          type: "Boolean",
        },
        nullable: false,
      },
      {
        name: "image",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "updatedAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "role",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "banned",
        column_type: {
          type: "Boolean",
        },
        nullable: true,
      },
      {
        name: "banReason",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "banExpires",
        column_type: {
          type: "Timestamp",
        },
        nullable: true,
      },
    ],
  },
  better_auth_session: {
    columns: [
      {
        name: "expiresAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "token",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "updatedAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "ipAddress",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "userAgent",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "userId",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "better_auth_user",
      },
      {
        name: "impersonatedBy",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
    ],
  },
  better_auth_account: {
    columns: [
      {
        name: "accountId",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "providerId",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "userId",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "better_auth_user",
      },
      {
        name: "accessToken",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "refreshToken",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "idToken",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "accessTokenExpiresAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: true,
      },
      {
        name: "refreshTokenExpiresAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: true,
      },
      {
        name: "scope",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "password",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "updatedAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
    ],
  },
  better_auth_verification: {
    columns: [
      {
        name: "identifier",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "value",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "expiresAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "updatedAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
    ],
  },
  better_auth_jwks: {
    columns: [
      {
        name: "publicKey",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "privateKey",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "expiresAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: true,
      },
    ],
  },
};
