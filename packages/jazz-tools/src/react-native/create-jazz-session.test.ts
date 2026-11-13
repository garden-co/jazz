import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { makeFakeAccount } from "../react-core/test-utils.js";
import { openNativeSessionCore as createNativeSession } from "./create-jazz-session.js";
import { openNativeSessionCore as createExpoSession } from "../expo/create-jazz-session.js";
import { ownJazzSession } from "../session/jazz-session.js";

const mocks = vi.hoisted(() => ({
  nativeAccounts: vi.fn(),
  expoAccounts: vi.fn(),
  client: vi.fn(),
}));
vi.mock("./create-account-manager.js", () => ({ createAccountManager: mocks.nativeAccounts }));
vi.mock("../expo/create-account-manager.js", () => ({ createAccountManager: mocks.expoAccounts }));
vi.mock("./create-jazz-client.js", () => ({ openJazzClient: mocks.client }));

// The backend these sessions ask who is signed in (`backend-user`): no one,
// unless a test names a user.
let backendUser: { issuer: string; subject: string } | null = null;
let backendUserAnswer: Promise<void> | undefined;
// A failing backend: `backend-user` answers this code with a 500.
let backendFailure: string | undefined;
beforeEach(() => {
  vi.resetAllMocks();
  backendUser = null;
  backendUserAnswer = undefined;
  backendFailure = undefined;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string | URL | Request) => {
      const url = input instanceof Request ? input.url : String(input);
      if (!url.endsWith("/backend-user")) return new Response("not_found", { status: 404 });
      await backendUserAnswer;
      if (backendFailure) return new Response(backendFailure, { status: 500 });
      if (!backendUser) return new Response("signed_out", { status: 401 });
      return Response.json({ identity: backendUser, account: "account-of-alice" });
    }),
  );
});
afterEach(() => vi.unstubAllGlobals());
// Each host's own options: React Native takes an account `store`, Expo keeps
// accounts in its secure store and takes a `profile` instead. (Typed as React
// Native's: a call through either factory type-checks against both configs.)
const nativeStore = { store: { read: async () => null, update: async () => {} } };
const expoProfile = { profile: "personal" } as unknown as typeof nativeStore;
for (const [host, create, prepare, own] of [
  ["React Native", createNativeSession, mocks.nativeAccounts, nativeStore],
  ["Expo", createExpoSession, mocks.expoAccounts, expoProfile],
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
        initial: "device" as const,
        env: "test",
        ...own,
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
        ...own,
      });
      expect(session.getSnapshot().status).toBe("signed-out");
      expect(mocks.client).not.toHaveBeenCalled();
      await session.close();
    });

    it("opens a signed-in backend user ready, without reporting signed-out first", async () => {
      // A backend user keeps no account on the device: the backend names them.
      const alice = makeFakeAccount();
      const loginWithBackend = vi.fn(async () => alice);
      const accounts = new AccountManager({
        createLocalFirst: () => makeFakeAccount(),
        restoreLocalFirst: () => makeFakeAccount(),
        loginWithBackend,
        logout: vi.fn(),
      });
      prepare.mockResolvedValue(accounts);
      mocks.client.mockResolvedValue({ shutdown: async () => {} });
      backendUser = { issuer: "jazz:backend", subject: "alice" };
      const session = await create({
        scope: "app-1",
        serverUrl: "https://jazz.example.com",
        ...own,
      });
      expect(session.getSnapshot().status).toBe("ready");
      expect(session.getSnapshot().account).toBe(alice);
      expect(loginWithBackend).toHaveBeenCalledOnce();
      await session.close();
    });

    it("asks the backend who is signed in with the app's bearer", async () => {
      // A backend that authenticates by bearer, not cookie, answers only a
      // request that carries it.
      const alice = makeFakeAccount();
      const accounts = new AccountManager({
        createLocalFirst: () => makeFakeAccount(),
        restoreLocalFirst: () => makeFakeAccount(),
        loginWithBackend: async () => alice,
        logout: vi.fn(),
      });
      prepare.mockResolvedValue(accounts);
      mocks.client.mockResolvedValue({ shutdown: async () => {} });
      backendUser = { issuer: "jazz:backend", subject: "alice" };
      const session = await create({
        scope: "app-1",
        serverUrl: "https://jazz.example.com",
        store: { read: async () => null, update: async () => {} },
        bearer: async () => "bearer-of-alice",
      });
      const asked = vi
        .mocked(fetch)
        .mock.calls.map(([input]) => input)
        .filter((input) =>
          String(input instanceof Request ? input.url : input).endsWith("/backend-user"),
        );
      expect(asked).not.toHaveLength(0);
      for (const request of asked) {
        expect(request instanceof Request ? request.headers.get("authorization") : null).toBe(
          "Bearer bearer-of-alice",
        );
      }
      expect(session.getSnapshot().status).toBe("ready");
      await session.close();
    });

    it("opens signed out once the backend says no one is signed in, or can't be asked", async () => {
      for (const answer of ["signed-out", "offline"] as const) {
        const accounts = new AccountManager({
          createLocalFirst: () => makeFakeAccount(),
          restoreLocalFirst: () => makeFakeAccount(),
          loginWithBackend: async () => makeFakeAccount(),
          logout: vi.fn(),
        });
        prepare.mockResolvedValue(accounts);
        if (answer === "offline")
          vi.mocked(fetch).mockRejectedValueOnce(new TypeError("Network request failed"));
        const session = await create({
          scope: "app-1",
          serverUrl: "https://jazz.example.com",
          ...own,
        });
        expect(fetch).toHaveBeenCalled();
        expect(session.getSnapshot().status).toBe("signed-out");
        await session.close();
      }
    });

    it("opens an account already on the device at once, without waiting on the backend", async () => {
      const retained = makeFakeAccount();
      const accounts = new AccountManager(
        {
          createLocalFirst: () => retained,
          restoreLocalFirst: () => retained,
          loginWithBackend: async () => retained,
          logout: vi.fn(),
        },
        retained,
      );
      prepare.mockResolvedValue(accounts);
      mocks.client.mockResolvedValue({ shutdown: async () => {} });
      // The backend never answers (offline at launch): the session opens anyway.
      backendUserAnswer = new Promise(() => {});
      const session = await create({
        scope: "app-1",
        serverUrl: "https://jazz.example.com",
        initial: "device",
        ...own,
      });
      expect(session.getSnapshot().status).toBe("ready");
      expect(session.getSnapshot().account).toBe(retained);
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
      // No `initial` needed: a device-only session always opens this device's account.
      const session = await create({ serverUrl: null, ...own });
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
      await expect(create({ serverUrl: null, ...own, initial: "signed-out" })).rejects.toThrow(
        /no backend to sign in through/,
      );
      expect(prepare).not.toHaveBeenCalled();
    });

    it("refuses an appId: the app id is the backend's", async () => {
      await expect(
        create({ appId: "app-1", serverUrl: "https://jazz.example.com", ...own } as never),
      ).rejects.toThrow(/takes no appId/);
      expect(prepare).not.toHaveBeenCalled();
    });

    it("shows a failed refresh on the snapshot, keeping the signed-in user's client", async () => {
      // Adopter round 2: authenticate returned no expiresAt for a signed-in
      // user; refresh() rejected and the session showed nothing.
      const alice = { ...makeFakeAccount(), identity: { issuer: "provider", subject: "alice" } };
      const accounts = new AccountManager(
        {
          createLocalFirst: () => makeFakeAccount(),
          restoreLocalFirst: () => makeFakeAccount(),
          loginWithBackend: async () => alice as never,
          logout: vi.fn(),
        },
        alice as never,
      );
      prepare.mockResolvedValue(accounts);
      const client = { shutdown: async () => {} };
      mocks.client.mockResolvedValue(client);
      backendUser = alice.identity;
      const errors = vi.spyOn(console, "error").mockImplementation(() => {});
      const session = ownJazzSession(
        { scope: "app-1", serverUrl: "https://jazz.example.com", ...own },
        create as never,
      );
      await vi.waitFor(() => expect(session.getSnapshot().status).toBe("ready"));
      backendFailure = "authenticate_expires_at_missing";
      const failed = await session.refresh().then(
        () => undefined,
        (error: Error) => error,
      );
      expect(failed?.message).toBe("Jazz could not ask your backend who is signed in");
      expect((failed?.cause as Error | undefined)?.message).toMatch(
        /^authenticate_expires_at_missing: /,
      );
      expect(session.getSnapshot()).toMatchObject({
        status: "ready",
        client,
        user: { identity: { subject: "alice" } },
        error: failed,
        recovery: "action",
      });
      // Retrying asks the backend again; once it answers, the error clears.
      backendFailure = undefined;
      await session.retry();
      expect(session.getSnapshot().error).toBeUndefined();
      await session.close();
      errors.mockRestore();
    });

    it("refuses a missing config and unknown options before preparing accounts", async () => {
      await expect((create as (config?: unknown) => Promise<unknown>)()).rejects.toThrow(
        /createJazzClient needs a config object.*got nothing\. Options: .*nativeRelay.*serverUrl/,
      );
      await expect(
        create({ serverUrl: "https://jazz.example.com", ...own, guest: true } as never),
      ).rejects.toThrow(
        /has no option "guest": guests are device accounts: use `initial: "device"`/,
      );
      expect(prepare).not.toHaveBeenCalled();
    });
  });
}
