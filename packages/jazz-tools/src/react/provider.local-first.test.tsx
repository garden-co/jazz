import { cleanup, render, waitFor } from "@testing-library/react";
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

vi.mock("jazz-tools/_dev/schema-hash", () => ({}), { virtual: true });

import { JazzProvider, useDb, useSession } from "./provider.js";
import { useLocalFirstAuth } from "./use-local-first-auth.js";

const SECRET = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const SESSION: Session = {
  user_id: "local-user",
  claims: {},
  authMode: "local-first",
};

function makeStorage() {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
    clear: () => values.clear(),
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

function IdentityProbe() {
  const { secret } = useLocalFirstAuth();
  const session = useSession();
  const db = useDb();

  return (
    <output data-testid="identity" data-db-user={db.getAuthState().session?.user_id}>
      {secret}:{session?.user_id}
    </output>
  );
}

describe("JazzProvider local-first auth", () => {
  beforeEach(async () => {
    // Node's experimental global localStorage masks Vitest's browser storage
    // and resolves to undefined unless --localstorage-file is configured.
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: makeStorage(),
    });
    await BrowserAuthSecretStore.clearSecret();
    localStorage.setItem("jazz-auth-secret", SECRET);
    mock.createJazzClient.mockReset();
    mock.createJazzClient.mockResolvedValue(makeClient());
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
  });

  it("creates a client whose local-first session and descendants share the stored identity", async () => {
    const view = render(
      <JazzProvider
        config={{ appId: "react-local-first", serverUrl: "https://jazz.example.com" }}
        auth="local-first"
        autoAttachDevTools={false}
        fallback={<p>Loading...</p>}
      >
        <IdentityProbe />
      </JazzProvider>,
    );

    await waitFor(() => {
      const identity = view.getByTestId("identity");
      expect(identity.textContent).toBe(`${SECRET}:${SESSION.user_id}`);
      expect(identity.dataset.dbUser).toBe(SESSION.user_id);
    });

    expect(mock.createJazzClient).toHaveBeenCalledWith(expect.objectContaining({ secret: SECRET }));
  });
});
