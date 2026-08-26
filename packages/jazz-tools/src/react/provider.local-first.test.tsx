import { cleanup, fireEvent, render, waitFor } from "@testing-library/react";
import { indexedDB as fakeIndexedDb } from "fake-indexeddb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AuthState } from "../runtime/auth-state.js";
import { BrowserAuthSecretStore } from "../runtime/auth-secret-store.js";
import type { Session } from "../runtime/context.js";

const mock = vi.hoisted(() => ({
  createJazzClient: vi.fn(),
}));

vi.mock("./create-jazz-client.js", () => ({
  createJazzClient: mock.createJazzClient,
}));

import { JazzProvider, useDb, useSession } from "./provider.js";
import { useLocalFirstAuth } from "./use-local-first-auth.js";

const SECRET = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const SESSION: Session = {
  user_id: "local-user",
  claims: {},
  issuer: "urn:jazz:local-first",
  authMode: "local-first",
};

function makeStorage(options: { failLegacyRemoval?: boolean } = {}) {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => {
      if (options.failLegacyRemoval && key === "jazz-auth-secret") {
        throw new Error("legacy removal failed");
      }
      values.delete(key);
    },
    clear: () => values.clear(),
  };
}

function makeLockManager() {
  let queue = Promise.resolve();
  const stats = { active: 0, maxActive: 0 };
  return {
    locks: {
      request: <T,>(_: string, __: unknown, callback: () => Promise<T>) => {
        const result = queue.then(async () => {
          stats.active += 1;
          stats.maxActive = Math.max(stats.maxActive, stats.active);
          try {
            return await callback();
          } finally {
            stats.active -= 1;
          }
        });
        queue = result.then(
          () => undefined,
          () => undefined,
        );
        return result;
      },
    },
    stats,
  };
}

function makeClient() {
  const state: AuthState = { authMode: "local-first", session: SESSION };
  return {
    db: {
      getAuthState: () => state,
      onAuthChanged: () => () => {},
      updateAuthToken: () => {},
    },
    session: SESSION,
    shutdown: async () => {},
  };
}

function IdentityProbe({
  appId,
  replacementSecret,
}: {
  appId?: string;
  replacementSecret?: string;
}) {
  const { secret, login } = useLocalFirstAuth();
  const { secret: scopedSecret } = useLocalFirstAuth(appId ? { appId } : {});
  const session = useSession();
  const db = useDb();

  return (
    <output
      data-testid={`identity-${appId ?? "default"}`}
      data-session-user={session?.user ?? ""}
      data-db-user={db.getAuthState().session?.user_id}
      data-scoped-secret={scopedSecret ?? ""}
      data-secret={secret ?? ""}
    >
      {secret}:{scopedSecret}:{session?.user_id}
      {replacementSecret ? (
        <button onClick={() => void login(replacementSecret)} type="button">
          Replace identity
        </button>
      ) : null}
    </output>
  );
}

describe("JazzProvider local-first auth", () => {
  beforeEach(async () => {
    await new Promise<void>((resolve, reject) => {
      const request = fakeIndexedDb.deleteDatabase("jazz-auth-secret-migration");
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
    // Node's experimental global localStorage masks Vitest's browser storage
    // and resolves to undefined unless --localstorage-file is configured.
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: makeStorage(),
    });
    Object.defineProperty(globalThis.navigator, "locks", {
      configurable: true,
      value: makeLockManager().locks,
    });
    Object.defineProperty(globalThis, "indexedDB", { configurable: true, value: fakeIndexedDb });
    await BrowserAuthSecretStore.clearSecret();
    localStorage.setItem("jazz-auth-secret:react-local-first", SECRET);
    mock.createJazzClient.mockReset();
    mock.createJazzClient.mockResolvedValue(makeClient());
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    vi.restoreAllMocks();
  });

  it("uses the existing app-scoped hook identity for the client and descendants", async () => {
    const view = render(
      <JazzProvider
        config={{ appId: "react-local-first", serverUrl: "https://jazz.example.com" }}
        auth="local-first"
        autoAttachDevTools={false}
        fallback={<p>Loading...</p>}
      >
        <IdentityProbe appId="react-local-first" />
      </JazzProvider>,
    );

    await waitFor(() => {
      const identity = view.getByTestId("identity-react-local-first");
      expect(identity.dataset.secret).toBe(SECRET);
      expect(identity.dataset.scopedSecret).toBe(SECRET);
      expect(identity.dataset.dbUser).toBe(SESSION.user_id);
      expect(identity.dataset.sessionUser).toBe('["urn:jazz:local-first","local-user"]');
    });

    expect(mock.createJazzClient).toHaveBeenCalledWith(expect.objectContaining({ secret: SECRET }));
  });

  it("migrates the legacy provider identity once without sharing it with a second app", async () => {
    const legacySecret = "legacy-provider-secret";
    localStorage.setItem("jazz-auth-secret", legacySecret);

    const view = render(
      <JazzProvider
        config={{ appId: "migrated-alpha", serverUrl: "https://jazz.example.com" }}
        auth="local-first"
        autoAttachDevTools={false}
      >
        <IdentityProbe appId="migrated-alpha" replacementSecret="migrated-replacement" />
      </JazzProvider>,
    );

    await waitFor(() => {
      expect(view.getByTestId("identity-migrated-alpha").dataset.secret).toBe(legacySecret);
      expect(view.getByTestId("identity-migrated-alpha").dataset.scopedSecret).toBe(legacySecret);
    });
    expect(localStorage.getItem("jazz-auth-secret:migrated-alpha")).toBe(legacySecret);
    expect(localStorage.getItem("jazz-auth-secret")).toBeNull();

    fireEvent.click(view.getByRole("button", { name: "Replace identity" }));
    await waitFor(() => {
      expect(view.getByTestId("identity-migrated-alpha").dataset.secret).toBe(
        "migrated-replacement",
      );
    });
    expect(localStorage.getItem("jazz-auth-secret:migrated-alpha")).toBe("migrated-replacement");
    expect(localStorage.getItem("jazz-auth-secret")).toBeNull();

    view.unmount();
    const betaView = render(
      <JazzProvider
        config={{ appId: "migrated-beta", serverUrl: "https://jazz.example.com" }}
        auth="local-first"
        autoAttachDevTools={false}
      >
        <IdentityProbe appId="migrated-beta" />
      </JazzProvider>,
    );

    await waitFor(() => {
      expect(betaView.getByTestId("identity-migrated-beta").dataset.secret).not.toBe(legacySecret);
    });
    expect(localStorage.getItem("jazz-auth-secret:migrated-beta")).not.toBe(legacySecret);
  });

  it("does not let a second app claim a legacy secret after its removal fails", async () => {
    const legacySecret = "legacy-removal-failure-secret";
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: makeStorage({ failLegacyRemoval: true }),
    });
    localStorage.setItem("jazz-auth-secret", legacySecret);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    const view = render(
      <>
        <JazzProvider
          config={{ appId: "failed-migration-alpha", serverUrl: "https://jazz.example.com" }}
          auth="local-first"
          autoAttachDevTools={false}
          fallback={<p data-testid="alpha-fallback">Loading...</p>}
        >
          <IdentityProbe appId="failed-migration-alpha" />
        </JazzProvider>
        <JazzProvider
          config={{ appId: "failed-migration-beta", serverUrl: "https://jazz.example.com" }}
          auth="local-first"
          autoAttachDevTools={false}
        >
          <IdentityProbe appId="failed-migration-beta" />
        </JazzProvider>
      </>,
    );

    await waitFor(() => {
      expect(view.getByTestId("identity-failed-migration-alpha").dataset.secret).toBe(legacySecret);
      expect(view.getByTestId("identity-failed-migration-beta").dataset.secret).not.toBe(
        legacySecret,
      );
    });

    expect(localStorage.getItem("jazz-auth-secret:failed-migration-alpha")).toBe(legacySecret);
    expect(localStorage.getItem("jazz-auth-secret")).toBe(legacySecret);
    expect(localStorage.getItem("jazz-auth-secret:failed-migration-beta")).not.toBe(legacySecret);
    expect(warn).toHaveBeenCalledWith(
      "Jazz could not remove the legacy local-first secret",
      expect.any(Error),
    );
  });

  it("keeps a cleanup-failed Web Locks migration exclusive when the next app uses IndexedDB", async () => {
    const legacySecret = "cross-mechanism-legacy-secret";
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: makeStorage({ failLegacyRemoval: true }),
    });
    localStorage.setItem("jazz-auth-secret", legacySecret);
    vi.spyOn(console, "warn").mockImplementation(() => {});

    const alpha = render(
      <JazzProvider
        config={{ appId: "cross-mechanism-alpha", serverUrl: "https://jazz.example.com" }}
        auth="local-first"
        autoAttachDevTools={false}
      >
        <IdentityProbe appId="cross-mechanism-alpha" />
      </JazzProvider>,
    );
    await waitFor(() => {
      expect(alpha.getByTestId("identity-cross-mechanism-alpha").dataset.secret).toBe(legacySecret);
    });

    Object.defineProperty(globalThis.navigator, "locks", { configurable: true, value: undefined });
    const beta = render(
      <JazzProvider
        config={{ appId: "cross-mechanism-beta", serverUrl: "https://jazz.example.com" }}
        auth="local-first"
        autoAttachDevTools={false}
      >
        <IdentityProbe appId="cross-mechanism-beta" />
      </JazzProvider>,
    );
    await waitFor(() => {
      expect(beta.getByTestId("identity-cross-mechanism-beta").dataset.secret).not.toBe(
        legacySecret,
      );
    });
  });

  it("fails closed without Web Locks or IndexedDB instead of replacing the legacy identity", async () => {
    const legacySecret = "uncoordinated-legacy-secret";
    Object.defineProperty(globalThis.navigator, "locks", { configurable: true, value: undefined });
    Object.defineProperty(globalThis, "indexedDB", { configurable: true, value: undefined });
    localStorage.setItem("jazz-auth-secret", legacySecret);

    const view = render(
      <JazzProvider
        config={{ appId: "uncoordinated-app", serverUrl: "https://jazz.example.com" }}
        auth="local-first"
        autoAttachDevTools={false}
        fallback={<p data-testid="uncoordinated-fallback">Loading...</p>}
      >
        <IdentityProbe appId="uncoordinated-app" />
      </JazzProvider>,
    );

    await waitFor(() => expect(view.getByTestId("uncoordinated-fallback")).toBeTruthy());
    expect(mock.createJazzClient).not.toHaveBeenCalled();
    expect(localStorage.getItem("jazz-auth-secret:uncoordinated-app")).toBeNull();
    expect(localStorage.getItem("jazz-auth-secret")).toBe(legacySecret);
  });

  it("serializes concurrent app migrations through the origin-wide lock", async () => {
    const legacySecret = "concurrent-legacy-secret";
    const lockManager = makeLockManager();
    Object.defineProperty(globalThis.navigator, "locks", {
      configurable: true,
      value: lockManager.locks,
    });
    localStorage.setItem("jazz-auth-secret", legacySecret);

    const view = render(
      <>
        <JazzProvider
          config={{ appId: "concurrent-alpha", serverUrl: "https://jazz.example.com" }}
          auth="local-first"
          autoAttachDevTools={false}
        >
          <IdentityProbe appId="concurrent-alpha" />
        </JazzProvider>
        <JazzProvider
          config={{ appId: "concurrent-beta", serverUrl: "https://jazz.example.com" }}
          auth="local-first"
          autoAttachDevTools={false}
        >
          <IdentityProbe appId="concurrent-beta" />
        </JazzProvider>
      </>,
    );

    await waitFor(() => {
      expect(view.getByTestId("identity-concurrent-alpha").dataset.secret).toBe(legacySecret);
      expect(view.getByTestId("identity-concurrent-beta").dataset.secret).not.toBe(legacySecret);
    });
    expect(lockManager.stats.maxActive).toBe(1);
  });

  it("uses IndexedDB ownership to migrate safely without Web Locks", async () => {
    const legacySecret = "unlocked-legacy-secret";
    Object.defineProperty(globalThis.navigator, "locks", {
      configurable: true,
      value: undefined,
    });
    localStorage.setItem("jazz-auth-secret", legacySecret);

    const view = render(
      <JazzProvider
        config={{ appId: "unlocked-app", serverUrl: "https://jazz.example.com" }}
        auth="local-first"
        autoAttachDevTools={false}
      >
        <IdentityProbe appId="unlocked-app" />
      </JazzProvider>,
    );

    await waitFor(() => {
      expect(view.getByTestId("identity-unlocked-app").dataset.secret).toBe(legacySecret);
    });
    expect(localStorage.getItem("jazz-auth-secret")).toBeNull();
  });

  it("falls back to IndexedDB when Web Locks throws or rejects", async () => {
    for (const [appId, locks] of [
      [
        "throwing-locks",
        {
          request: () => {
            throw new Error("unsupported");
          },
        },
      ],
      ["rejecting-locks", { request: () => Promise.reject(new Error("unavailable")) }],
    ] as const) {
      await new Promise<void>((resolve, reject) => {
        const request = fakeIndexedDb.deleteDatabase("jazz-auth-secret-migration");
        request.onsuccess = () => resolve();
        request.onerror = () => reject(request.error);
      });
      localStorage.setItem("jazz-auth-secret", `${appId}-legacy`);
      Object.defineProperty(globalThis.navigator, "locks", { configurable: true, value: locks });
      const view = render(
        <JazzProvider
          config={{ appId, serverUrl: "https://jazz.example.com" }}
          auth="local-first"
          autoAttachDevTools={false}
        >
          <IdentityProbe appId={appId} />
        </JazzProvider>,
      );
      await waitFor(() => {
        expect(view.getByTestId(`identity-${appId}`).dataset.secret).toBe(`${appId}-legacy`);
      });
      view.unmount();
    }
  });

  it("isolates provider-owned secrets for different appIds on one origin", async () => {
    const alphaSecret = "alpha-secret";
    const betaSecret = "beta-secret";
    localStorage.setItem("jazz-auth-secret:alpha", alphaSecret);
    localStorage.setItem("jazz-auth-secret:beta", betaSecret);

    const view = render(
      <>
        <JazzProvider
          config={{ appId: "alpha", serverUrl: "https://jazz.example.com" }}
          auth="local-first"
          autoAttachDevTools={false}
        >
          <IdentityProbe appId="alpha" replacementSecret="alpha-replacement" />
        </JazzProvider>
        <JazzProvider
          config={{ appId: "beta", serverUrl: "https://jazz.example.com" }}
          auth="local-first"
          autoAttachDevTools={false}
        >
          <IdentityProbe appId="beta" />
        </JazzProvider>
      </>,
    );

    await waitFor(() => {
      expect(view.getByTestId("identity-alpha").dataset.secret).toBe(alphaSecret);
      expect(view.getByTestId("identity-alpha").dataset.scopedSecret).toBe(alphaSecret);
      expect(view.getByTestId("identity-beta").dataset.secret).toBe(betaSecret);
      expect(view.getByTestId("identity-beta").dataset.scopedSecret).toBe(betaSecret);
    });

    expect(mock.createJazzClient).toHaveBeenCalledWith(
      expect.objectContaining({ secret: alphaSecret }),
    );
    expect(mock.createJazzClient).toHaveBeenCalledWith(
      expect.objectContaining({ secret: betaSecret }),
    );

    fireEvent.click(view.getByRole("button", { name: "Replace identity" }));

    await waitFor(() => {
      expect(view.getByTestId("identity-alpha").dataset.secret).toBe("alpha-replacement");
      expect(view.getByTestId("identity-alpha").dataset.scopedSecret).toBe("alpha-replacement");
      expect(view.getByTestId("identity-beta").dataset.secret).toBe(betaSecret);
      expect(view.getByTestId("identity-beta").dataset.scopedSecret).toBe(betaSecret);
    });

    await waitFor(() => {
      expect(mock.createJazzClient).toHaveBeenCalledWith(
        expect.objectContaining({ secret: "alpha-replacement" }),
      );
    });
  });

  it("reloads the selected store when a mounted provider changes appId", async () => {
    const alphaSecret = "rerender-alpha-secret";
    const betaSecret = "rerender-beta-secret";
    localStorage.setItem("jazz-auth-secret:rerender-alpha", alphaSecret);
    localStorage.setItem("jazz-auth-secret:rerender-beta", betaSecret);

    const view = render(
      <JazzProvider
        config={{ appId: "rerender-alpha", serverUrl: "https://jazz.example.com" }}
        auth="local-first"
        autoAttachDevTools={false}
      >
        <IdentityProbe appId="rerender-alpha" />
      </JazzProvider>,
    );

    await waitFor(() => {
      expect(view.getByTestId("identity-rerender-alpha").dataset.secret).toBe(alphaSecret);
    });
    mock.createJazzClient.mockClear();

    view.rerender(
      <JazzProvider
        config={{ appId: "rerender-beta", serverUrl: "https://jazz.example.com" }}
        auth="local-first"
        autoAttachDevTools={false}
      >
        <IdentityProbe appId="rerender-beta" />
      </JazzProvider>,
    );

    await waitFor(() => {
      expect(view.getByTestId("identity-rerender-beta").dataset.secret).toBe(betaSecret);
      expect(mock.createJazzClient).toHaveBeenCalledWith(
        expect.objectContaining({ secret: betaSecret }),
      );
    });
    expect(mock.createJazzClient).not.toHaveBeenCalledWith(
      expect.objectContaining({ secret: alphaSecret }),
    );
  });
});
