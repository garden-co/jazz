import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => {
  const account = {
    id: "local-account",
    identity: { issuer: "urn:jazz:local-first", subject: "local-account" },
  };
  const createLocalFirst = vi.fn(() => account);
  const getLoggedIn = vi.fn<() => typeof account | undefined>(() => undefined);
  const subscribe = vi.fn(() => () => {});
  const loginJWT = vi.fn(async () => account);
  const getSession = vi.fn<() => Promise<{ data: { session: { id: string } } | null }>>(
    async () => ({ data: null }),
  );
  const createAccountManager = vi.fn(async () => ({
    createLocalFirst,
    getLoggedIn,
    subscribe,
    loginJWT,
  }));
  const createDb = vi.fn(async () => ({ shutdown: vi.fn() }));
  const mountApp = vi.fn(() => ({ setDb: vi.fn(), destroy: vi.fn() }));
  return {
    account,
    createLocalFirst,
    getLoggedIn,
    subscribe,
    loginJWT,
    getSession,
    createAccountManager,
    createDb,
    mountApp,
  };
});

vi.mock("jazz-tools", () => ({
  createAccountManager: mocks.createAccountManager,
  createDb: mocks.createDb,
}));
vi.mock("./app.js", () => ({ mountApp: mocks.mountApp }));
vi.mock("./auth-client.js", () => ({
  authClient: { getSession: mocks.getSession },
}));

beforeEach(() => {
  vi.resetModules();
  mocks.createAccountManager.mockClear();
  mocks.createLocalFirst.mockClear().mockImplementation(() => {
    mocks.getLoggedIn.mockReturnValue(mocks.account);
    return mocks.account;
  });
  mocks.getLoggedIn.mockClear().mockReturnValue(undefined);
  mocks.loginJWT.mockClear().mockResolvedValue(mocks.account);
  mocks.getSession.mockClear().mockResolvedValue({ data: null });
  mocks.createDb.mockClear();
  mocks.mountApp.mockClear();
  vi.stubEnv("VITE_JAZZ_APP_ID", "test-app");
  vi.stubEnv("VITE_JAZZ_SERVER_URL", "https://sync.test");
  vi.stubGlobal("document", { getElementById: vi.fn(() => ({})) });
});

afterEach(() => vi.unstubAllGlobals());

describe("hybrid account bootstrap", () => {
  it("founds a local-first account and opens the context with its opaque handle", async () => {
    await import("./main.js");
    await vi.waitFor(() => expect(mocks.mountApp).toHaveBeenCalledOnce());
    expect(mocks.createLocalFirst).toHaveBeenCalledOnce();
    expect(mocks.createDb).toHaveBeenCalledWith({
      appId: "test-app",
      serverUrl: "https://sync.test",
      account: mocks.account,
    });
  });

  it("keeps a retained local-first account open when an unlinked provider session cannot log in", async () => {
    mocks.getLoggedIn.mockReturnValue(mocks.account);
    mocks.getSession.mockResolvedValue({ data: { session: { id: "provider-session" } } });
    mocks.loginJWT.mockRejectedValue(new Error("account is not linked"));

    await import("./main.js");
    await vi.waitFor(() => expect(mocks.mountApp).toHaveBeenCalledOnce());

    expect(mocks.loginJWT).toHaveBeenCalledOnce();
    expect(mocks.createLocalFirst).not.toHaveBeenCalled();
    expect(mocks.createDb).toHaveBeenCalledWith({
      appId: "test-app",
      serverUrl: "https://sync.test",
      account: mocks.account,
    });
    expect((mocks.mountApp.mock.calls[0] as unknown[] | undefined)?.[3]).toBeInstanceOf(Error);
  });
});
