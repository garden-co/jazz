// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";
import type { JazzLifecycle } from "./jazz-lifecycle.js";

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

vi.mock("./auth-client.js", () => ({
  authClient: {
    getSession: vi.fn(),
    signOut: vi.fn(),
    useSession: {
      get: () => session,
      subscribe(listener: (next: typeof session) => void) {
        listener(session);
        return () => {};
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
    vi.clearAllMocks();
  });

  it("renders the real sign-in form when session subscription notifies synchronously", () => {
    const root = document.createElement("div");
    const lifecycle = {
      getClient: () => null,
      transition: vi.fn(),
    } as unknown as JazzLifecycle;

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
      getClient: () => ({}),
      transition: vi.fn(),
    } as unknown as JazzLifecycle;

    const app = mountApp(root, lifecycle);

    expect(mountTodoWidget).toHaveBeenCalledTimes(1);
    app.destroy();
  });
});
