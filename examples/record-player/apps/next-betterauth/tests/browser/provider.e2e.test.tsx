import { afterEach, describe, expect, it, vi } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";

const auth = vi.hoisted(() => ({
  session: {
    data: { session: { id: "better-auth-session" }, user: { id: "better-auth-user" } },
    isPending: false,
  },
  token: vi.fn(
    async () =>
      `header.${btoa(JSON.stringify({ iss: "https://auth.example", sub: "better-auth-user" }))}.signature`,
  ),
  signIn: vi.fn(),
  accounts: undefined as { logout(): void } | undefined,
  prepareError: undefined as Error | undefined,
}));

vi.mock("../../src/lib/auth-client", () => ({
  authClient: {
    useSession: () => auth.session,
    signIn: { email: auth.signIn },
    signUp: { email: vi.fn() },
  },
  getJwtFromBetterAuth: auth.token,
}));

vi.mock("../../src/lib/accounts", async () => {
  const { createAccountManagerWithRuntime } =
    await import("../../../../../../packages/jazz-tools/src/accounts/enrollment.js");
  const { accountRegistryUrl } =
    await import("../../../../../../packages/jazz-tools/src/accounts/context.js");
  return {
    prepareAccounts: async (appId: string, serverUrl: string) => {
      if (auth.prepareError) throw auth.prepareError;
      return (auth.accounts ??= createAccountManagerWithRuntime({
        registry: accountRegistryUrl(
          serverUrl || "https://core.example",
          appId || "record-player-test",
        ),
        localFirst: {
          create: () => {
            throw new Error("not used");
          },
        },
        fetch: (async () =>
          new Response(
            JSON.stringify({
              account: "00000000-0000-4000-8000-000000000001",
              identity: { issuer: "https://auth.example", subject: "better-auth-user" },
            }),
          ) as Response) as typeof fetch,
      }));
    },
  };
});

vi.mock("jazz-tools/react", () => ({
  createJazzClient: async ({ account }: { account: { id: string } }) => ({
    account,
    shutdown: vi.fn(),
  }),
  JazzClientProvider: ({
    children,
    client,
  }: {
    children: React.ReactNode;
    client: { account: { id: string } };
  }) => <div data-jazz-account={client.account.id}>{children}</div>,
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
    auth.session = {
      data: { session: { id: "better-auth-session" }, user: { id: "better-auth-user" } },
      isPending: false,
    };
    auth.token.mockClear();
    auth.signIn.mockReset();
    auth.prepareError = undefined;
    auth.accounts?.logout();
    auth.accounts = undefined;
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
      () =>
        container?.querySelector("[data-jazz-account='00000000-0000-4000-8000-000000000001']") !==
        null,
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
    expect(container.querySelector("[data-jazz-account]")).toBeNull();
    expect(container.querySelector("button")).toBeNull();
    expect(container.textContent).toContain("Connecting RecordPlayer");

    await act(async () => {
      token.resolve(
        `header.${btoa(JSON.stringify({ iss: "https://auth.example", sub: "better-auth-user" }))}.signature`,
      );
      await token.promise;
    });

    await waitFor(
      () =>
        container?.querySelector("[data-jazz-account='00000000-0000-4000-8000-000000000001']") !==
        null,
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

    expect(container.querySelector("[data-jazz-account]")).toBeNull();
    expect(container.textContent).toContain("Sign in to RecordPlayer");
  });

  it("surfaces a missing token and retries rather than connecting forever", async () => {
    auth.token
      .mockImplementationOnce(async () => null)
      .mockImplementationOnce(
        async () =>
          `header.${btoa(JSON.stringify({ iss: "https://auth.example", sub: "better-auth-user" }))}.signature`,
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
      () =>
        container?.textContent?.includes("Better Auth did not provide a Jazz session token.") ??
        false,
      "expected a missing-token error instead of a permanent connecting state",
    );
    await act(async () => {
      container?.querySelector("button")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    });
    await waitFor(
      () =>
        container?.querySelector("[data-jazz-account='00000000-0000-4000-8000-000000000001']") !==
        null,
      "expected retry to mount Jazz after a token becomes available",
    );
  });

  it("surfaces an account-manager startup failure and retries it", async () => {
    auth.prepareError = new Error("registry unavailable");
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
    await act(async () => root?.render(<RecordPlayerProvider>rooms</RecordPlayerProvider>));
    await waitFor(
      () => container?.textContent?.includes("registry unavailable") ?? false,
      "expected startup error",
    );
    auth.prepareError = undefined;
    await act(async () =>
      container?.querySelector("button")?.dispatchEvent(new MouseEvent("click", { bubbles: true })),
    );
    await waitFor(
      () => container?.querySelector("[data-jazz-account]") !== null,
      "expected startup retry to connect",
    );
  });

  it("clears sign-in pending state when Better Auth rejects", async () => {
    auth.session = { data: null, isPending: false };
    auth.signIn.mockRejectedValueOnce(new Error("auth offline"));
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
    await act(async () => root?.render(<RecordPlayerProvider>rooms</RecordPlayerProvider>));
    const signIn = container.querySelector("button") as HTMLButtonElement;
    await act(async () => signIn.dispatchEvent(new MouseEvent("click", { bubbles: true })));
    await waitFor(
      () => container?.textContent?.includes("auth offline") ?? false,
      "expected auth rejection",
    );
    expect(signIn.disabled).toBe(false);
  });
});
