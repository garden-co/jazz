// State-space audit repro: the #4767 first-load "signed-out" flash on the
// React Native and Expo session factories, which PR #4768 does not touch.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzClient as createNativeSession } from "./create-jazz-session.js";
import { createJazzClient as createExpoSession } from "../expo/create-jazz-session.js";

const mocks = vi.hoisted(() => ({
  nativeAccounts: vi.fn(),
  expoAccounts: vi.fn(),
  client: vi.fn(),
}));
vi.mock("./create-account-manager.js", () => ({ createAccountManager: mocks.nativeAccounts }));
vi.mock("../expo/create-account-manager.js", () => ({ createAccountManager: mocks.expoAccounts }));
vi.mock("./create-jazz-client.js", () => ({ openJazzClient: mocks.client }));

const alice = Object.freeze({
  id: "11111111-1111-4111-8111-111111111111",
  identity: { issuer: "jazz:backend", subject: "alice" },
}) as AccountHandle;

beforeEach(() => {
  vi.resetAllMocks();
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(JSON.stringify({ identity: alice.identity, account: alice.id }), {
          status: 200,
        }),
    ),
  );
});
afterEach(() => vi.unstubAllGlobals());

for (const [host, create, prepare] of [
  ["React Native", createNativeSession, mocks.nativeAccounts],
  ["Expo", createExpoSession, mocks.expoAccounts],
] as const) {
  describe(`${host}`, () => {
    it("does not report signed-out before the backend's first answer for a signed-in user", async () => {
      const accounts = new AccountManager({
        createLocalFirst: () => {
          throw new Error("unused");
        },
        loginWithBackend: async () => alice,
        logout() {},
      });
      prepare.mockResolvedValue(accounts);
      mocks.client.mockResolvedValue({ shutdown: vi.fn(async () => {}) });
      const session = await create({
        serverUrl: "https://jazz.example.com",
        store: { read: async () => null, update: async () => {} },
      });
      const first = session.getSnapshot().status;
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(session.getSnapshot().account?.identity.subject).toBe("alice");
      await session.close();
      expect(first).not.toBe("signed-out");
    });
  });
}
