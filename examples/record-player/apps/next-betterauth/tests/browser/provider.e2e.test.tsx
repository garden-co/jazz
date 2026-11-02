import { afterEach, describe, expect, it, vi } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";

// A Better Auth client with no session: the real JazzProvider and its Better
// Auth bridge run against it. Signing in is through the backend, so a
// signed-in listener is covered by the live app and the SDK's own tests.
const auth = vi.hoisted(() => ({
  signIn: vi.fn(),
  signUp: vi.fn(),
}));

vi.mock("../../src/lib/auth-client", () => {
  const state = { data: null, isPending: false };
  return {
    authClient: {
      $store: { atoms: { session: { get: () => state, subscribe: () => () => {} } } },
      signOut: async () => ({}),
      signIn: { email: auth.signIn },
      signUp: { email: auth.signUp },
    },
  };
});

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

const frame = (content: React.ReactNode, signedIn: boolean) => (
  <div data-signed-in={String(signedIn)}>{content}</div>
);

describe("RecordPlayer sign-in", () => {
  let root: Root | undefined;
  let container: HTMLDivElement | undefined;

  afterEach(async () => {
    if (root) await act(async () => root?.unmount());
    container?.remove();
    root = undefined;
    container = undefined;
    auth.signIn.mockReset();
    auth.signUp.mockReset();
  });

  async function render() {
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
    await act(async () => {
      root?.render(
        <RecordPlayerProvider frame={frame}>
          <RecordPlayerClient />
        </RecordPlayerProvider>,
      );
    });
    await waitFor(
      () => container?.textContent?.includes("Sign in to RecordPlayer") ?? false,
      "expected the sign-in form",
    );
    return container;
  }

  it("shows sign-in, not the library, before Better Auth has a session", async () => {
    const view = await render();
    expect(view.querySelector("[data-testid='record-player']")).toBeNull();
    expect(view.querySelector("[data-signed-in='false']")).not.toBeNull();
  });

  it("signs up and in only through Better Auth: Jazz asks the backend afterwards", async () => {
    auth.signUp.mockResolvedValueOnce({ data: {} });
    const view = await render();
    const create = [...view.querySelectorAll("button")].find(
      (button) => button.textContent === "Create account",
    )!;
    await act(async () => create.dispatchEvent(new MouseEvent("click", { bubbles: true })));
    expect(auth.signUp).toHaveBeenCalledWith(
      expect.objectContaining({ email: "listener@example.com" }),
    );
    expect(auth.signIn).not.toHaveBeenCalled();
  });

  it("clears sign-in pending state when Better Auth rejects", async () => {
    auth.signIn.mockRejectedValueOnce(new Error("auth offline"));
    const view = await render();
    const signIn = view.querySelector("button[type='submit']") as HTMLButtonElement;
    await act(async () => signIn.dispatchEvent(new MouseEvent("click", { bubbles: true })));
    await waitFor(
      () => view.textContent?.includes("auth offline") ?? false,
      "expected auth rejection",
    );
    expect(signIn.disabled).toBe(false);
  });
});
