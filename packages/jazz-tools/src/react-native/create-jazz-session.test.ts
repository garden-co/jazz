import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { FIRST_ANSWER_DEADLINE_MS } from "../session/follow-user.js";
import { makeFakeAccount } from "../react-core/test-utils.js";
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

// The backend's answer to "who is signed in": alice, or no one (a 401).
const backendNames = (user: AccountHandle | null) =>
  user
    ? new Response(JSON.stringify({ identity: user.identity, account: user.id }), { status: 200 })
    : new Response("unauthenticated", { status: 401 });

function signedInAsAlice() {
  return new AccountManager({
    createLocalFirst: () => {
      throw new Error("unused");
    },
    loginWithBackend: async () => alice,
    logout() {},
  });
}

beforeEach(() => {
  vi.resetAllMocks();
  // No one is signed in unless a test says otherwise.
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => backendNames(null)),
  );
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
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
          loginWithBackend: async () => replacement,
          logout: vi.fn(),
        },
        retained,
      );
      prepare.mockResolvedValue(accounts);
      const shutdown = vi.fn(async () => {});
      mocks.client.mockResolvedValue({ shutdown });
      const config = {
        scope: "app-1",
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
        loginWithBackend: async () => makeFakeAccount(),
        logout: vi.fn(),
      });
      prepare.mockResolvedValue(accounts);
      const session = await create({
        scope: "app-1",
        serverUrl: "https://jazz.example.com",
        store: { read: async () => null, update: async () => {} },
      });
      expect(session.getSnapshot().status).toBe("signed-out");
      expect(mocks.client).not.toHaveBeenCalled();
      await session.close();
    });

    it("keeps Jazz on the device with serverUrl: null", async () => {
      const guest = makeFakeAccount();
      const accounts = new AccountManager({
        createLocalFirst: vi.fn(() => guest),
        restoreLocalFirst: () => guest,
        loginWithBackend: async () => {
          throw new Error("no backend");
        },
        logout: vi.fn(),
      });
      prepare.mockResolvedValue(accounts);
      mocks.client.mockResolvedValue({ shutdown: vi.fn(async () => {}) });
      const store = { read: async () => null, update: async () => {} };
      // No `initial` needed: a device-only session always opens this device's account.
      const session = await create({ serverUrl: null, store });
      expect(session.getSnapshot().status).toBe("ready");
      expect(session.getSnapshot().account).toBe(guest);
      // The client gets no server at all.
      expect(mocks.client).toHaveBeenLastCalledWith(
        expect.objectContaining({ serverUrl: undefined, account: guest }),
      );
      await expect(session.reauthenticate()).resolves.toBeUndefined();
      expect(session.getSnapshot().status).toBe("ready");
      await session.close();
    });

    it("refuses a signed-out device-only start before preparing accounts", async () => {
      const store = { read: async () => null, update: async () => {} };
      await expect(create({ serverUrl: null, store, initial: "signed-out" })).rejects.toThrow(
        /no backend to sign in through/,
      );
      expect(prepare).not.toHaveBeenCalled();
    });

    it("refuses an appId: the app id is the backend's", async () => {
      const store = { read: async () => null, update: async () => {} };
      await expect(
        create({ appId: "app-1", serverUrl: "https://jazz.example.com", store } as never),
      ).rejects.toThrow(/takes no appId/);
      expect(prepare).not.toHaveBeenCalled();
    });

    // A returning user keeps no account on the device: the backend names
    // them. Until it has answered, the session must not say "signed-out", or
    // the app shows its sign-in screen for a moment on every start.
    it("opens a signed-in user's session without reporting signed-out first", async () => {
      vi.stubGlobal(
        "fetch",
        vi.fn(async () => backendNames(alice)),
      );
      prepare.mockResolvedValue(signedInAsAlice());
      mocks.client.mockResolvedValue({ shutdown: vi.fn(async () => {}) });
      const session = await create({
        serverUrl: "https://jazz.example.com",
        store: { read: async () => null, update: async () => {} },
      });
      const opened = session.getSnapshot();
      await session.close();
      expect(opened.status).toBe("ready");
      expect(opened.account?.identity.subject).toBe("alice");
    });

    it("opens signed out when the backend can't be reached", async () => {
      vi.stubGlobal(
        "fetch",
        vi.fn(async () => {
          throw new TypeError("Network request failed");
        }),
      );
      prepare.mockResolvedValue(signedInAsAlice());
      const session = await create({
        serverUrl: "https://jazz.example.com",
        store: { read: async () => null, update: async () => {} },
      });
      expect(session.getSnapshot().status).toBe("signed-out");
      expect(mocks.client).not.toHaveBeenCalled();
      await session.close();
    });

    it("opens an account kept on the device at once, without asking the backend first", async () => {
      // Offline: the backend never answers.
      vi.stubGlobal(
        "fetch",
        vi.fn(() => new Promise<Response>(() => {})),
      );
      const kept = makeFakeAccount();
      prepare.mockResolvedValue(
        new AccountManager(
          {
            createLocalFirst: () => kept,
            restoreLocalFirst: () => kept,
            loginWithBackend: async () => kept,
            logout: vi.fn(),
          },
          kept,
        ),
      );
      mocks.client.mockResolvedValue({ shutdown: vi.fn(async () => {}) });
      vi.useFakeTimers();
      const session = await create({
        serverUrl: "https://jazz.example.com",
        initial: "local-first",
        store: { read: async () => null, update: async () => {} },
      });
      expect(session.getSnapshot().status).toBe("ready");
      expect(session.getSnapshot().account).toBe(kept);
      await session.close();
    });

    it("opens signed out once a slow backend passes the start deadline, then follows its answer", async () => {
      let answer!: (response: Response) => void;
      vi.stubGlobal(
        "fetch",
        vi.fn(() => new Promise<Response>((resolve) => (answer = resolve))),
      );
      prepare.mockResolvedValue(signedInAsAlice());
      mocks.client.mockResolvedValue({ shutdown: vi.fn(async () => {}) });
      vi.useFakeTimers();
      let opened = false;
      const opening = create({
        serverUrl: "https://jazz.example.com",
        store: { read: async () => null, update: async () => {} },
      }).finally(() => (opened = true));
      await vi.advanceTimersByTimeAsync(FIRST_ANSWER_DEADLINE_MS - 1);
      expect(opened).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const session = await opening;
      expect(session.getSnapshot().status).toBe("signed-out");
      answer(backendNames(alice));
      await vi.waitFor(() => expect(session.getSnapshot().account?.identity.subject).toBe("alice"));
      await session.close();
    });
  });
}
