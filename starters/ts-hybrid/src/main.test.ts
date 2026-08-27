import { afterEach, describe, expect, it, vi } from "vitest";

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
  return {
    onAuthChanged: vi.fn(),
    updateAuthToken: vi.fn(),
    shutdown: vi.fn(async () => {}),
  };
}

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
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
    await Promise.all(mocks.emit(signedIn("newer")));
    olderDbOpen.resolve(olderSignInDb);
    await olderSignIn;

    expect(mocks.setDb.mock.calls.map(([db]) => db)).toEqual([logoutDb, newerSignInDb]);
    expect(initialDb.shutdown).toHaveBeenCalledOnce();
    expect(logoutDb.shutdown).toHaveBeenCalledOnce();
    expect(olderSignInDb.shutdown).toHaveBeenCalledOnce();
    expect(newerSignInDb.shutdown).not.toHaveBeenCalled();
  });
});
