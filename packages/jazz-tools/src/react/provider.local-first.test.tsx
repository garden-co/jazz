import { act, cleanup, render, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { BrowserAuthSecretStore } from "../runtime/auth-secret-store.js";

const mock = vi.hoisted(() => ({
  config: null as Record<string, unknown> | null,
}));

vi.mock("../react-core/provider.js", () => ({
  JazzProvider: ({ children, config }: { children: ReactNode; config: object }) => {
    mock.config = config as Record<string, unknown>;
    return <>{children}</>;
  },
  useDb: () => ({}),
  useJazzClient: () => ({ db: {} }),
  useSession: () => null,
}));

vi.mock("./create-jazz-client.js", () => ({
  createJazzClient: vi.fn(),
}));

vi.mock("jazz-tools/_dev/schema-hash", () => ({}), { virtual: true });

import { JazzProvider } from "./provider.js";
import { useLocalFirstAuth } from "./use-local-first-auth.js";

const SECRET = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

function makeStorage() {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
    clear: () => values.clear(),
  };
}

function AuthProbe() {
  const { secret } = useLocalFirstAuth();
  return <output data-testid="secret">{secret}</output>;
}

describe("JazzProvider local-first auth", () => {
  beforeEach(async () => {
    mock.config = null;
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: makeStorage(),
    });
    await BrowserAuthSecretStore.clearSecret();
    localStorage.setItem("jazz-auth-secret", SECRET);
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
  });

  it("creates the client from the shared local-first secret and exposes it to descendants", async () => {
    let view!: ReturnType<typeof render>;

    await act(async () => {
      view = render(
        <JazzProvider
          config={{ appId: "react-local-first", serverUrl: "https://jazz.example.com" }}
          auth="local-first"
          autoAttachDevTools={false}
          fallback={<p>Loading...</p>}
        >
          <AuthProbe />
        </JazzProvider>,
      );
      await Promise.resolve();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(mock.config?.secret).toBe(SECRET);
      expect(view.getByTestId("secret").textContent).toBe(SECRET);
    });
  });
});
