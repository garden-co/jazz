import { betterAuth } from "better-auth";
import { runAdapterTest } from "better-auth/adapters/test";
import {
  assert,
  afterAll,
  beforeAll,
  describe,
  expect,
  it,
  test,
  beforeEach,
  vi,
} from "vitest";
import { Account, co, Group } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import { createWorkerAccount, startSyncServer } from "./sync-utils.js";
import { JazzBetterAuthDatabaseAdapter } from "../index.js";
import { TableItem } from "../schema.js";

describe("JazzBetterAuthDatabaseAdapter tests", async () => {
  describe("better-auth internal tests", async () => {
    let syncServer: any;
    let accountID: string;
    let accountSecret: string;
    let adapter: ReturnType<typeof JazzBetterAuthDatabaseAdapter>;

    beforeAll(async () => {
      syncServer = await startSyncServer();

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
    let syncServer: Awaited<ReturnType<typeof startSyncServer>>;
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
          debugLogs: true,
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

    it("tables can be shared with another account", async () => {
      // Create a new account as main worker
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
        tables: co.map({}),
      });

      const db = await DatabaseRoot.loadUnique("better-auth-root", accountID, {
        loadAs: worker,
        resolve: {
          group: true,
          tables: true,
        },
      });

      assert(db);
      assert(db.group);
      assert(db.tables);

      // Create a new worker account
      const newWorkerAccount = await createWorkerAccount({
        name: "test2",
        peer: `ws://localhost:${syncServer.port}`,
      });

      const newWorkerRef = await Account.load(newWorkerAccount.accountID);
      assert(newWorkerRef);

      // Add the new worker to the group
      db.group.addMember(newWorkerRef, "admin");
      // Remove the previous worker if you want to rotate the worker
      // db.group.removeMember(worker);

      await db.group.$jazz.waitForSync();

      // Start the new worker
      const { worker: newWorker } = await startWorker({
        syncServer: `ws://localhost:${syncServer.port}`,
        accountID: newWorkerAccount.accountID,
        accountSecret: newWorkerAccount.agentSecret,
      });

      // create the database root on the new worker with the same group and tables
      await DatabaseRoot.upsertUnique({
        unique: "better-auth-root",
        value: {
          group: db.group,
          tables: db.tables,
        },
        owner: newWorker,
      });

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

    it("should wait for sync before returning", async () => {
      const { localNode } = syncServer;

      const auth = betterAuth({
        database: JazzBetterAuthDatabaseAdapter({
          syncServer: `ws://localhost:${syncServer.port}`,
          accountID,
          accountSecret,
        }),
      });

      const adapter = (await auth.$context).adapter;

      const handleSyncMessageSpy = vi.spyOn(
        localNode.syncManager,
        "handleNewContent",
      );

      const user = await adapter.create({
        model: "user",
        data: {
          name: "test",
          email: "test-sync@test.com",
          password: "12345678",
        },
      });

      expect(user.id).toMatch("co_");

      expect(
        handleSyncMessageSpy.mock.calls.filter(([msg]) => msg.id === user.id),
      ).toHaveLength(3);
    });

    it("should return the new entity with date objects", async () => {
      const auth = betterAuth({
        database: JazzBetterAuthDatabaseAdapter({
          syncServer: `ws://localhost:${syncServer.port}`,
          accountID,
          accountSecret,
        }),
      });

      const date = new Date();

      await (await auth.$context).internalAdapter.createVerificationValue({
        identifier: "test",
        value: "test",
        expiresAt: date,
      });

      const verification = await (
        await auth.$context
      ).internalAdapter.findVerificationValue("test");

      expect(verification).toMatchObject({
        id: expect.any(String),
        identifier: "test",
        value: "test",
        expiresAt: date,
        createdAt: expect.any(Date),
        updatedAt: expect.any(Date),
      });
    });
  });

  /**
   * These adapter's calls are taken logging Better Auth's queries
   */
  describe("common user flows", async () => {
    let syncServer: any;
    let accountID: string;
    let accountSecret: string;

    beforeEach(async () => {
      syncServer = await startSyncServer();

      const workerAccount = await createWorkerAccount({
        name: "test",
        peer: `ws://localhost:${syncServer.port}`,
      });

      accountID = workerAccount.accountID;
      accountSecret = workerAccount.agentSecret;
    });

    test("Email and Password: signup + signin + logout", async () => {
      const adapter = JazzBetterAuthDatabaseAdapter({
        syncServer: `ws://localhost:${syncServer.port}`,
        accountID,
        accountSecret,
      })({});

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

      expect(user.id).toMatch("co_");

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

      expect(account.id).toMatch("co_");

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

      expect(session.id).toMatch("co_");

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
      const adapter = JazzBetterAuthDatabaseAdapter({
        syncServer: `ws://localhost:${syncServer.port}`,
        accountID,
        accountSecret,
      })({});

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

      expect(account.id).toMatch("co_");

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
});
