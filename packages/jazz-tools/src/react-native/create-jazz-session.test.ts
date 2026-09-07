import { beforeEach, describe, expect, it, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { makeFakeAccount } from "../react-core/test-utils.js";
import { createJazzSession as createNativeSession } from "./create-jazz-session.js";
import { createJazzSession as createExpoSession } from "../expo/create-jazz-session.js";

const mocks = vi.hoisted(() => ({
  nativeAccounts: vi.fn(),
  expoAccounts: vi.fn(),
  client: vi.fn(),
}));
vi.mock("./create-account-manager.js", () => ({ createAccountManager: mocks.nativeAccounts }));
vi.mock("../expo/create-account-manager.js", () => ({ createAccountManager: mocks.expoAccounts }));
vi.mock("./create-jazz-client.js", () => ({ createJazzClient: mocks.client }));

beforeEach(() => {
  vi.resetAllMocks();
});
for (const [host, create, prepare] of [
  ["React Native", createNativeSession, mocks.nativeAccounts],
  ["Expo", createExpoSession, mocks.expoAccounts],
] as const) {
  describe(`${host} session factory`, () => {
    it("prepares once, restores selection, and reuses host configuration after logout", async () => {
      const retained = makeFakeAccount();
      const replacement = makeFakeAccount();
      const accounts = new AccountManager(
        {
          createLocalFirst: vi.fn(() => replacement),
          restoreLocalFirst: () => replacement,
          registerJWT: async () => replacement,
          loginJWT: async () => replacement,
          linkJWT: async () => replacement,
          logout: vi.fn(),
        },
        retained,
      );
      prepare.mockResolvedValue(accounts);
      const shutdown = vi.fn(async () => {});
      mocks.client.mockResolvedValue({ shutdown });
      const config = {
        appId: "app-1",
        serverUrl: "https://jazz.example.com",
        initial: "local-first" as const,
        profile: "personal",
        env: "test",
        store: { read: async () => null, update: async () => {} },
      };
      const session = await create(config);
      expect(prepare).toHaveBeenCalledExactlyOnceWith(config);
      expect(mocks.client).toHaveBeenLastCalledWith({ ...config, account: retained });
      expect(accounts.getLoggedIn()).toBe(retained);
      expect(session.getSnapshot().account).toBe(retained);
      await session.logout();
      expect(session.getSnapshot().status).toBe("signed-out");
      expect(shutdown).toHaveBeenCalledWith({ waitForSync: true });
      await session.createLocalFirst();
      expect(prepare).toHaveBeenCalledOnce();
      expect(mocks.client).toHaveBeenLastCalledWith({ ...config, account: replacement });
      await session.close();
    });

    it("does not open a client without a retained selection or local-first policy", async () => {
      const accounts = new AccountManager({
        createLocalFirst: () => makeFakeAccount(),
        restoreLocalFirst: () => makeFakeAccount(),
        registerJWT: async () => makeFakeAccount(),
        loginJWT: async () => makeFakeAccount(),
        linkJWT: async () => makeFakeAccount(),
        logout: vi.fn(),
      });
      prepare.mockResolvedValue(accounts);
      const session = await create({
        appId: "app-1",
        serverUrl: "https://jazz.example.com",
        store: { read: async () => null, update: async () => {} },
      });
      expect(session.getSnapshot().status).toBe("signed-out");
      expect(mocks.client).not.toHaveBeenCalled();
      await session.close();
    });
  });
}
