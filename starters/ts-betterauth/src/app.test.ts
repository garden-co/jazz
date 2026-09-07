// @vitest-environment jsdom

import { describe, expect, it, vi } from "vitest";
import type { JazzLifecycle } from "./jazz-lifecycle.js";

const session = {
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

describe("mountApp", () => {
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
});
