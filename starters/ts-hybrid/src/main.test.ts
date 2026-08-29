import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type Session = {
  isPending: boolean;
  data: { session: object; user: { name: string } } | null;
};

const mocks = vi.hoisted(() => {
  let session: Session = { isPending: false, data: null };
  const subscribers = new Set<(next: Session) => unknown>();
  const createDb = vi.fn();
  const getOrCreateSecret = vi.fn();
  const fetchToken = vi.fn();
  const setDb = vi.fn();
  const mountApp = vi.fn(() => ({ setDb, destroy: vi.fn() }));
  const useSession = {
    get: vi.fn(() => session),
    subscribe: vi.fn((listener: (next: Session) => unknown) => {
      subscribers.add(listener);
      return () => subscribers.delete(listener);
    }),
  };

  return {
    createDb,
    getOrCreateSecret,
    fetchToken,
    mountApp,
    setDb,
    useSession,
    emit(next: Session) {
      session = next;
      return [...subscribers].map((listener) => listener(next));
    },
    reset() {
      session = { isPending: false, data: null };
      subscribers.clear();
      createDb.mockReset();
      getOrCreateSecret.mockReset();
      fetchToken.mockReset();
      setDb.mockReset();
      mountApp.mockReset().mockImplementation(() => ({ setDb, destroy: vi.fn() }));
      useSession.get.mockClear();
      useSession.subscribe.mockClear();
    },
  };
});

vi.mock("jazz-tools", () => ({
  BrowserAuthSecretStore: { getOrCreateSecret: mocks.getOrCreateSecret },
  createDb: mocks.createDb,
}));
vi.mock("./auth-client.js", () => ({
  authClient: { useSession: mocks.useSession, $fetch: mocks.fetchToken },
}));
vi.mock("./app.js", () => ({ mountApp: mocks.mountApp }));

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function signedIn(name: string): Session {
  return { isPending: false, data: { session: {}, user: { name } } };
}

function dbHandle() {
  const authListeners = new Set<(state: { error?: string }) => void>();
  return {
    onAuthChanged: vi.fn((listener: (state: { error?: string }) => void) => {
      authListeners.add(listener);
      return () => authListeners.delete(listener);
    }),
    updateAuthToken: vi.fn(),
    shutdown: vi.fn(async () => {}),
    emitAuth(state: { error?: string }) {
      for (const listener of authListeners) listener(state);
    },
  };
}

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

beforeEach(() => {
  vi.resetModules();
  mocks.reset();
});

describe("hybrid auth Db ownership", () => {
  it("keeps the newest auth generation active and closes an older sign-in Db", async () => {
    const initialDb = dbHandle();
    const logoutDb = dbHandle();
    const newerSignInDb = dbHandle();
    const olderSignInDb = dbHandle();
    const olderDbOpen = deferred<typeof olderSignInDb>();
    let localOpenCount = 0;

    mocks.getOrCreateSecret.mockResolvedValue("local-secret");
    mocks.fetchToken
      .mockResolvedValueOnce({ data: { token: "older-token" }, error: null })
      .mockResolvedValueOnce({ data: { token: "newer-token" }, error: null });
    mocks.createDb.mockImplementation((config: { jwtToken?: string }) => {
      if (config.jwtToken === "older-token") return olderDbOpen.promise;
      if (config.jwtToken === "newer-token") return Promise.resolve(newerSignInDb);
      localOpenCount += 1;
      return Promise.resolve(localOpenCount === 1 ? initialDb : logoutDb);
    });

    vi.stubEnv("VITE_JAZZ_APP_ID", "test-app");
    vi.stubEnv("VITE_JAZZ_SERVER_URL", "https://sync.test");
    vi.stubGlobal("document", { getElementById: vi.fn(() => ({})) });
    // Import after installing boot-time environment and DOM state; the module starts boot on load.
    await import("./main.js");
    await vi.waitFor(() => expect(mocks.mountApp).toHaveBeenCalledWith({}, initialDb));

    const olderSignIn = mocks.emit(signedIn("older"))[0] as Promise<void>;
    await vi.waitFor(() =>
      expect(mocks.createDb).toHaveBeenCalledWith(
        expect.objectContaining({ jwtToken: "older-token" }),
      ),
    );

    await Promise.all(mocks.emit({ isPending: false, data: null }));
    await vi.waitFor(() => expect(mocks.setDb).toHaveBeenCalledWith(logoutDb));
    await Promise.all(mocks.emit(signedIn("newer")));
    olderDbOpen.resolve(olderSignInDb);
    await olderSignIn;
    await vi.waitFor(() => expect(mocks.setDb).toHaveBeenCalledWith(newerSignInDb));

    expect(mocks.setDb.mock.calls.map(([db]) => db)).toEqual([logoutDb, newerSignInDb]);
    expect(initialDb.shutdown).toHaveBeenCalledOnce();
    expect(logoutDb.shutdown).toHaveBeenCalledOnce();
    expect(olderSignInDb.shutdown).toHaveBeenCalledOnce();
    expect(newerSignInDb.shutdown).not.toHaveBeenCalled();
  });

  it("refreshes only the current Db when an older session's request settles", async () => {
    const initialDb = dbHandle();
    const signedInDb = dbHandle();
    const staleRefresh = deferred<{ data: { token: string } | null; error: null }>();
    const currentRefresh = deferred<{ data: { token: string } | null; error: null }>();
    let localOpenCount = 0;

    mocks.getOrCreateSecret.mockResolvedValue("local-secret");
    mocks.fetchToken
      .mockImplementationOnce(() => staleRefresh.promise)
      .mockResolvedValueOnce({ data: { token: "sign-in-token" }, error: null })
      .mockImplementationOnce(() => currentRefresh.promise);
    mocks.createDb.mockImplementation((config: { jwtToken?: string }) => {
      if (config.jwtToken === "sign-in-token") return Promise.resolve(signedInDb);
      localOpenCount += 1;
      return Promise.resolve(initialDb);
    });

    vi.stubEnv("VITE_JAZZ_APP_ID", "test-app");
    vi.stubEnv("VITE_JAZZ_SERVER_URL", "https://sync.test");
    vi.stubGlobal("document", { getElementById: vi.fn(() => ({})) });
    await import("./main.js");
    await vi.waitFor(() => expect(mocks.mountApp).toHaveBeenCalledWith({}, initialDb));

    initialDb.emitAuth({ error: "expired" });
    await vi.waitFor(() => expect(mocks.fetchToken).toHaveBeenCalledTimes(1));
    await Promise.all(mocks.emit(signedIn("signed-in")));
    await vi.waitFor(() => expect(mocks.setDb).toHaveBeenCalledWith(signedInDb));

    staleRefresh.resolve({ data: { token: "stale-token" }, error: null });
    await Promise.resolve();
    expect(signedInDb.updateAuthToken).not.toHaveBeenCalled();

    signedInDb.emitAuth({ error: "expired" });
    await vi.waitFor(() => expect(mocks.fetchToken).toHaveBeenCalledTimes(3));
    currentRefresh.resolve({ data: { token: "current-token" }, error: null });
    await vi.waitFor(() =>
      expect(signedInDb.updateAuthToken).toHaveBeenCalledWith("current-token"),
    );
    expect(initialDb.updateAuthToken).not.toHaveBeenCalled();
  });

  it("reconciles a session change that happens while the initial Db is opening", async () => {
    const initialDb = dbHandle();
    const signedInDb = dbHandle();
    const initialOpen = deferred<typeof initialDb>();

    mocks.getOrCreateSecret.mockResolvedValue("local-secret");
    mocks.fetchToken.mockResolvedValue({ data: { token: "sign-in-token" }, error: null });
    mocks.createDb.mockImplementation((config: { jwtToken?: string }) => {
      return config.jwtToken === "sign-in-token"
        ? Promise.resolve(signedInDb)
        : initialOpen.promise;
    });

    vi.stubEnv("VITE_JAZZ_APP_ID", "test-app");
    vi.stubEnv("VITE_JAZZ_SERVER_URL", "https://sync.test");
    vi.stubGlobal("document", { getElementById: vi.fn(() => ({})) });
    await import("./main.js");
    await vi.waitFor(() => expect(mocks.createDb).toHaveBeenCalledOnce());

    // No session listener exists until the initial Db is ready. The final
    // current-session read must still observe this sign-in.
    mocks.emit(signedIn("signed-in"));
    initialOpen.resolve(initialDb);

    await vi.waitFor(() => expect(mocks.mountApp).toHaveBeenCalledWith({}, initialDb));
    await vi.waitFor(() => expect(mocks.setDb).toHaveBeenCalledWith(signedInDb));
    expect(initialDb.shutdown).toHaveBeenCalledOnce();
  });
});
