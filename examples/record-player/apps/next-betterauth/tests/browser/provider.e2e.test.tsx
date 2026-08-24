import { afterEach, describe, expect, it, vi } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";

const auth = vi.hoisted(() => ({
  session: {
    data: { session: { id: "better-auth-session" } },
    isPending: false,
  },
  token: vi.fn(async () => "jazz-jwt"),
}));

vi.mock("../../src/lib/auth-client", () => ({
  authClient: {
    useSession: () => auth.session,
    signIn: { email: vi.fn() },
    signUp: { email: vi.fn() },
  },
  getJwtFromBetterAuth: auth.token,
}));

vi.mock("jazz-tools/react", () => ({
  JazzProvider: ({
    children,
    config,
  }: {
    children: React.ReactNode;
    config: { jwtToken: string };
  }) => <div data-jazz-jwt={config.jwtToken}>{children}</div>,
  useDb: () => ({ insert: vi.fn() }),
  useAll: () => ({ data: [] }),
}));

import { RecordPlayerClient } from "../../app/record-player-client";
import { RecordPlayerProvider } from "../../app/record-player-provider";

async function waitFor(check: () => boolean, message: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (check()) return;
    await act(async () => new Promise((resolve) => setTimeout(resolve, 20)));
  }
  throw new Error(message);
}

describe("RecordPlayer Better Auth bridge", () => {
  let root: Root | undefined;
  let container: HTMLDivElement | undefined;

  afterEach(async () => {
    if (root) await act(async () => root?.unmount());
    container?.remove();
    root = undefined;
    container = undefined;
    auth.session = { data: { session: { id: "better-auth-session" } }, isPending: false };
    auth.token.mockClear();
    vi.unstubAllGlobals();
  });

  it("bootstraps an authenticated account before mounting the Jazz query surface", async () => {
    const fetch = vi.fn(async () => new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", fetch);
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);

    await act(async () => {
      root?.render(
        <RecordPlayerProvider>
          <RecordPlayerClient />
        </RecordPlayerProvider>,
      );
    });

    await waitFor(
      () => container?.querySelector("[data-jazz-jwt='jazz-jwt']") !== null,
      "expected the authenticated Jazz provider to mount",
    );
    expect(fetch).toHaveBeenCalledWith("/api/bootstrap", {
      method: "POST",
      credentials: "same-origin",
    });
    expect(container.querySelector("button")?.textContent).toBe("Create playlist");
  });

  it("does not mount the Jazz query surface before Better Auth has a session", async () => {
    auth.session = { data: null, isPending: false };
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);

    await act(async () => {
      root?.render(
        <RecordPlayerProvider>
          <RecordPlayerClient />
        </RecordPlayerProvider>,
      );
    });

    expect(container.querySelector("[data-jazz-jwt]")).toBeNull();
    expect(container.textContent).toContain("Sign in to RecordPlayer");
  });
});
