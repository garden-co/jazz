import { cleanup, render, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AuthState } from "../runtime/auth-state.js";
import {
  authSecretStorageKey,
  BrowserAuthSecretStore,
  generateAuthSecret,
} from "../runtime/auth-secret-store.js";
import type { PublicSession, Session } from "../runtime/context.js";

const mock = vi.hoisted(() => ({ createJazzClient: vi.fn() }));
vi.mock("./create-jazz-client.js", () => ({ createJazzClient: mock.createJazzClient }));

import { JazzProvider } from "./provider.js";
import { useLocalFirstAuth } from "./use-local-first-auth.js";

const SESSION: Session = {
  user_id: "local-user",
  claims: {},
  issuer: "urn:jazz:local-first",
  authMode: "local-first",
};
const PUBLIC_SESSION: PublicSession = {
  user: '["urn:jazz:local-first","local-user"]',
  claims: { iss: SESSION.issuer, sub: SESSION.user_id },
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
  const state: AuthState = { authMode: "local-first", session: PUBLIC_SESSION };
  return {
    db: { getAuthState: () => state, onAuthChanged: () => () => {}, updateAuthToken: () => {} },
    session: PUBLIC_SESSION,
    shutdown: async () => {},
  };
}

function IdentityProbe({ appId }: { appId: string }) {
  const inherited = useLocalFirstAuth();
  const scoped = useLocalFirstAuth({ appId });
  return (
    <output
      data-testid="identity"
      data-inherited={inherited.secret ?? ""}
      data-scoped={scoped.secret ?? ""}
    />
  );
}

describe("JazzProvider local-first auth", () => {
  beforeEach(() => {
    Object.defineProperty(globalThis, "localStorage", { configurable: true, value: makeStorage() });
    mock.createJazzClient.mockReset();
    mock.createJazzClient.mockResolvedValue(makeClient());
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("uses one canonical app-scoped identity for the provider and descendants", async () => {
    const appId = "react-local-first";
    const secret = generateAuthSecret();
    localStorage.setItem(authSecretStorageKey({ appId }), secret);

    const view = render(
      <JazzProvider config={{ appId, serverUrl: "https://jazz.example.com" }} auth="local-first">
        <IdentityProbe appId={appId} />
      </JazzProvider>,
    );

    await waitFor(() => {
      expect(view.getByTestId("identity").dataset.inherited).toBe(secret);
      expect(view.getByTestId("identity").dataset.scoped).toBe(secret);
      expect(mock.createJazzClient).toHaveBeenCalledWith(
        expect.objectContaining({ appId, secret }),
      );
    });
  });

  it("does not read or claim a pre-format legacy key", async () => {
    const appId = "fresh-app";
    localStorage.setItem("jazz-auth-secret", generateAuthSecret());
    const store = BrowserAuthSecretStore.getDefault({ appId });

    const secret = await store.getOrCreateSecret();
    expect(secret).not.toBe(localStorage.getItem("jazz-auth-secret"));
    expect(localStorage.getItem(authSecretStorageKey({ appId }))).toBe(secret);
  });
});
