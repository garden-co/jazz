// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";
import type { createJazzSession } from "jazz-tools/client";
type JazzSession = Awaited<ReturnType<typeof createJazzSession>>;

type Session = {
  isPending: boolean;
  data: {
    session: { id: string };
    user: { id: string; name: string };
  } | null;
};

const session: Session = {
  isPending: false,
  data: null,
};
let sessionSubscriber: ((next: Session) => void) | undefined;

vi.mock("./auth-client.js", () => ({
  authClient: {
    getSession: vi.fn(async () => ({ data: session.data })),
    signIn: { email: vi.fn(async () => ({})) },
    signUp: { email: vi.fn(async () => ({})) },
    signOut: vi.fn(),
    useSession: {
      get: () => session,
      subscribe(listener: (next: typeof session) => void) {
        sessionSubscriber = listener;
        listener(session);
        return () => {
          if (sessionSubscriber === listener) sessionSubscriber = undefined;
        };
      },
    },
  },
}));

vi.mock("./todo-widget.js", () => ({
  mountTodoWidget: vi.fn(() => () => {}),
}));

import { mountApp } from "./app.js";
import { mountTodoWidget } from "./todo-widget.js";

describe("mountApp", () => {
  beforeEach(() => {
    session.isPending = false;
    session.data = null;
    sessionSubscriber = undefined;
    vi.clearAllMocks();
  });

  it("renders the real sign-in form when session subscription notifies synchronously", () => {
    const root = document.createElement("div");
    const lifecycle = {
      getSnapshot: () => ({}),
      loginJWT: vi.fn(async () => {}),
      logout: vi.fn(async () => {}),
    } as unknown as JazzSession;

    const app = mountApp(root, lifecycle);

    expect(root.querySelector("form")).not.toBeNull();
    app.destroy();
  });

  it("keeps the initial todo subscription when Better Auth replays the same session", () => {
    session.data = {
      session: { id: "session-1" },
      user: { id: "user-1", name: "Ada" },
    };
    const root = document.createElement("div");
    const lifecycle = {
      getSnapshot: () => ({ client: { db: {} } }),
      loginJWT: vi.fn(async () => {}),
      logout: vi.fn(async () => {}),
    } as unknown as JazzSession;

    const app = mountApp(root, lifecycle);

    expect(mountTodoWidget).toHaveBeenCalledTimes(1);
    app.destroy();
  });

  it("renders a later same-session profile update", () => {
    session.data = {
      session: { id: "session-1" },
      user: { id: "user-1", name: "Ada" },
    };
    const root = document.createElement("div");
    const lifecycle = {
      getSnapshot: () => ({ client: { db: {} } }),
      loginJWT: vi.fn(async () => {}),
      logout: vi.fn(async () => {}),
    } as unknown as JazzSession;

    const app = mountApp(root, lifecycle);
    session.data = {
      session: { id: "session-1" },
      user: { id: "user-1", name: "Grace" },
    };
    sessionSubscriber?.(session);

    expect(root.textContent).toContain("Hello, Grace");
    expect(mountTodoWidget).toHaveBeenCalledTimes(2);
    app.destroy();
  });
});

describe("session auth intent", () => {
  it.each([false, true])(
    "delegates explicit signup=%s to the matching session command",
    async (signup) => {
      session.isPending = false;
      session.data = null;
      const root = document.createElement("div");
      const loginJWT = vi.fn(async () => {});
      const registerJWT = vi.fn(async () => {});
      const jazz = { getSnapshot: () => ({}), loginJWT, registerJWT } as unknown as JazzSession;
      const app = mountApp(root, jazz);
      if (signup) root.querySelector<HTMLButtonElement>('[data-action="toggle"]')!.click();
      const form = root.querySelector("form")!;
      (form.elements.namedItem("email") as HTMLInputElement).value = "member@example.com";
      (form.elements.namedItem("password") as HTMLInputElement).value = "password";
      if (signup) (form.elements.namedItem("name") as HTMLInputElement).value = "Member";
      form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
      await vi.waitFor(() => expect(signup ? registerJWT : loginJWT).toHaveBeenCalledOnce());
      expect(signup ? loginJWT : registerJWT).not.toHaveBeenCalled();
      expect((signup ? registerJWT : loginJWT).mock.calls[0]).toEqual([
        { getToken: expect.any(Function) },
      ]);
      app.destroy();
    },
  );

  it("reconciles provider logout through the session and detaches the old data view", async () => {
    session.data = { session: { id: "session-1" }, user: { id: "user-1", name: "Ada" } };
    const root = document.createElement("div");
    const logout = vi.fn(async () => {});
    const jazz = { getSnapshot: () => ({ client: { db: {} } }), logout } as unknown as JazzSession;
    const app = mountApp(root, jazz);
    session.data = null;
    sessionSubscriber?.(session);
    await vi.waitFor(() => expect(logout).toHaveBeenCalledOnce());
    expect(root.querySelector("form")).not.toBeNull();
    expect(root.textContent).not.toContain("Hello, Ada");
    app.destroy();
  });
});
