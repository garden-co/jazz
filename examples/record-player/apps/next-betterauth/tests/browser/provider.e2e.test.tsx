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

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
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

  it("keeps Jazz and its query surface unmounted until trusted bootstrap succeeds", async () => {
    const bootstrap = deferred<Response>();
    const fetch = vi.fn(() => bootstrap.promise);
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

    await waitFor(() => fetch.mock.calls.length === 1, "expected trusted bootstrap to start");
    expect(container.querySelector("[data-jazz-jwt]")).toBeNull();
    expect(container.querySelector("button")).toBeNull();
    expect(container.textContent).toContain("Preparing your RecordPlayer");

    await act(async () => {
      bootstrap.resolve(new Response(null, { status: 200 }));
      await bootstrap.promise;
    });

    await waitFor(
      () => container?.querySelector("[data-jazz-jwt='jazz-jwt']") !== null,
      "expected Jazz to mount after trusted bootstrap",
    );
    expect(container.querySelector("button")?.textContent).toBe("Create playlist");
  });

  it("shows a bootstrap error without mounting Jazz after trusted bootstrap rejects", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new Error("trusted bootstrap rejected");
      }),
    );
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
      () => container?.querySelector("[role='alert']") !== null,
      "expected trusted bootstrap failure to be rendered",
    );
    expect(container.querySelector("[data-jazz-jwt]")).toBeNull();
    expect(container.querySelector("button")).toBeNull();
    expect(container.textContent).toContain("could not establish its trusted session");
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
