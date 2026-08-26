import { afterEach, describe, expect, it, vi } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";

const auth = vi.hoisted(() => ({
  session: {
    data: { session: { id: "better-auth-session" }, user: { id: "better-auth-user" } },
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

function click(element: Element): void {
  element.dispatchEvent(new MouseEvent("click", { bubbles: true }));
}

describe("RecordPlayer Better Auth bridge", () => {
  let root: Root | undefined;
  let container: HTMLDivElement | undefined;

  afterEach(async () => {
    if (root) await act(async () => root?.unmount());
    container?.remove();
    root = undefined;
    container = undefined;
    auth.session = {
      data: { session: { id: "better-auth-session" }, user: { id: "better-auth-user" } },
      isPending: false,
    };
    auth.token.mockClear();
    vi.unstubAllGlobals();
  });

  it("mounts the Jazz query surface after Better Auth supplies a token", async () => {
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
    expect(container.querySelector("button")?.textContent).toBe("Create playlist");
  });

  it("keeps Jazz and its query surface unmounted until Better Auth supplies a token", async () => {
    const token = deferred<string>();
    auth.token.mockImplementationOnce(() => token.promise);
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

    await waitFor(() => auth.token.mock.calls.length === 1, "expected token request to start");
    expect(container.querySelector("[data-jazz-jwt]")).toBeNull();
    expect(container.querySelector("button")).toBeNull();
    expect(container.textContent).toContain("Connecting RecordPlayer");

    await act(async () => {
      token.resolve("jazz-jwt");
      await token.promise;
    });

    await waitFor(
      () => container?.querySelector("[data-jazz-jwt='jazz-jwt']") !== null,
      "expected Jazz to mount after token acquisition",
    );
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

  it("surfaces token failures and retries without mounting Jazz", async () => {
    auth.token
      .mockRejectedValueOnce(new Error("token endpoint unavailable"))
      .mockResolvedValueOnce("retried-jazz-jwt");
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
      "expected token error",
    );
    expect(container.querySelector("[data-jazz-jwt]")).toBeNull();
    expect(container.textContent).toContain("token endpoint unavailable");

    await act(async () => click(container!.querySelector("button")!));
    await waitFor(
      () => container?.querySelector("[data-jazz-jwt='retried-jazz-jwt']") !== null,
      "expected retry to mount Jazz",
    );
    expect(auth.token).toHaveBeenCalledTimes(2);
  });

  it("does not spin forever when Better Auth returns no token", async () => {
    auth.token.mockResolvedValueOnce(null);
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
    await act(async () => {
      root?.render(<RecordPlayerProvider>ready</RecordPlayerProvider>);
    });
    await waitFor(
      () => container?.querySelector("[role='alert']") !== null,
      "expected null-token error",
    );
    expect(container.textContent).toContain("did not provide a Jazz session token");
  });
});
