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
    getSession: vi.fn(),
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
