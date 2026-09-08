import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => {
  const account = {
    id: "local-account",
    identity: { issuer: "urn:jazz:local-first", subject: "local-account" },
  };
  const db = {};
  const session = {
    getSnapshot: vi.fn(() => ({ status: "ready", account, client: { db } })),
    subscribe: vi.fn(() => () => {}),
    loginJWT: vi.fn(async () => {}),
    close: vi.fn(async () => {}),
  };
  return {
    account,
    db,
    session,
    createJazzSession: vi.fn(async () => session),
    getSession: vi.fn<() => Promise<{ data: { session: { id: string } } | null }>>(async () => ({
      data: null,
    })),
    mountApp: vi.fn(() => ({ setDb: vi.fn(), destroy: vi.fn() })),
  };
});
vi.mock("jazz-tools/client", () => ({ createJazzSession: mocks.createJazzSession }));
vi.mock("./app.js", () => ({ mountApp: mocks.mountApp }));
vi.mock("./auth-client.js", () => ({ authClient: { getSession: mocks.getSession } }));

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
  mocks.session.loginJWT.mockResolvedValue(undefined);
  mocks.getSession.mockResolvedValue({ data: null });
  vi.stubEnv("VITE_JAZZ_APP_ID", "test-app");
  vi.stubEnv("VITE_JAZZ_SERVER_URL", "https://sync.test");
  vi.stubGlobal("document", { getElementById: vi.fn(() => ({})) });
  vi.stubGlobal("window", { addEventListener: vi.fn() });
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

describe("hybrid account bootstrap", () => {
  it("requests a local-first session with one configuration and mounts its selected client", async () => {
    await import("./main.js");
    await vi.waitFor(() => expect(mocks.mountApp).toHaveBeenCalledOnce());
    expect(mocks.createJazzSession).toHaveBeenCalledWith({
      appId: "test-app",
      serverUrl: "https://sync.test",
      initial: "local-first",
    });
    expect(mocks.session.loginJWT).not.toHaveBeenCalled();
    expect(mocks.mountApp).toHaveBeenCalledWith(
      expect.anything(),
      mocks.db,
      mocks.session,
      undefined,
    );
  });
  it("restores provider login and retains local data with an explicit link error when login fails", async () => {
    mocks.getSession.mockResolvedValue({ data: { session: { id: "provider-session" } } });
    mocks.session.loginJWT.mockRejectedValue(new Error("account is not linked"));
    await import("./main.js");
    await vi.waitFor(() => expect(mocks.mountApp).toHaveBeenCalledOnce());
    expect(mocks.session.loginJWT).toHaveBeenCalledWith({ getToken: expect.any(Function) });
    expect(mocks.mountApp).toHaveBeenCalledWith(
      expect.anything(),
      mocks.db,
      mocks.session,
      expect.objectContaining({ message: "account is not linked" }),
    );
    expect(mocks.session.close).not.toHaveBeenCalled();
  });
});
