// @vitest-environment jsdom
import { beforeEach, expect, it, vi } from "vitest";
import type { createJazzSession } from "jazz-tools/client";
type JazzSession = Awaited<ReturnType<typeof createJazzSession>>;
const fixture = vi.hoisted(() => {
  type State = {
    isPending: boolean;
    data: null | { session: { id: string }; user: { id: string; name: string } };
  };
  let state: State = { isPending: false, data: null };
  const listeners = new Set<(state: State) => void>();
  const atom = {
    get: () => state,
    subscribe(listener: (state: State) => void) {
      listeners.add(listener);
      listener(state);
      return () => listeners.delete(listener);
    },
  };
  const update = (next: State) => {
    state = next;
    for (const listener of listeners) listener(state);
  };
  const signIn = vi.fn(async () => {
    update({
      isPending: false,
      data: { session: { id: "session-1" }, user: { id: "user-1", name: "Ada" } },
    });
    return {};
  });
  const signOut = vi.fn(async () => {
    update({ isPending: false, data: null });
    return {};
  });
  return { atom, update, signIn, signOut, listeners };
});
vi.mock("./auth-client.js", () => ({
  authClient: {
    $store: { atoms: { session: fixture.atom } },
    $fetch: async () => ({ data: { token: "jwt" } }),
    useSession: fixture.atom,
    signIn: { email: fixture.signIn },
    signUp: { email: fixture.signIn },
    signOut: fixture.signOut,
  },
}));
vi.mock("./todo-widget.js", () => ({ mountTodoWidget: vi.fn(() => () => {}) }));
import { mountApp } from "./app.js";
import { mountTodoWidget } from "./todo-widget.js";
beforeEach(() => {
  fixture.listeners.clear();
  fixture.update({ isPending: false, data: null });
  vi.clearAllMocks();
});
function setup() {
  let current = { status: "signed-out", client: undefined as unknown };
  const listeners = new Set<() => void>();
  const loginOrRegisterJWT = vi.fn(async () => {
    current = { status: "ready", client: { db: {} } };
    for (const listener of listeners) listener();
  });
  const logout = vi.fn(async () => {
    current = { status: "signed-out", client: undefined };
    for (const listener of listeners) listener();
  });
  const jazz = {
    getSnapshot: () => current,
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    loginOrRegisterJWT,
    logout,
  } as unknown as JazzSession;
  const root = document.createElement("div");
  const app = mountApp(root, jazz);
  const unsubscribe = jazz.subscribe(() => app.setDb(jazz.getSnapshot().client?.db ?? null));
  return {
    root,
    loginOrRegisterJWT,
    logout,
    destroy() {
      unsubscribe();
      app.destroy();
    },
  };
}
it("renders the real sign-in form after initial signed-out hydration", async () => {
  const app = setup();
  await vi.waitFor(() => expect(app.root.querySelector("form")).not.toBeNull());
  app.destroy();
});
it("mounts the initial todo subscription once and renders later profile updates", async () => {
  await fixture.signIn();
  const app = setup();
  await vi.waitFor(() => expect(app.root.textContent).toContain("Hello, Ada"));
  expect(mountTodoWidget).toHaveBeenCalledTimes(1);
  fixture.update({
    ...fixture.atom.get(),
    data: { session: { id: "session-1" }, user: { id: "user-1", name: "Grace" } },
  });
  expect(app.root.textContent).toContain("Hello, Grace");
  expect(app.loginOrRegisterJWT).toHaveBeenCalledTimes(1);
  app.destroy();
});
it.each([false, true])(
  "signup=%s only authenticates with provider; shared connection admits identity",
  async (signup) => {
    const app = setup();
    await vi.waitFor(() => expect(app.root.querySelector("form")).not.toBeNull());
    if (signup) app.root.querySelector<HTMLButtonElement>('[data-action="toggle"]')!.click();
    const form = app.root.querySelector("form")!;
    (form.elements.namedItem("email") as HTMLInputElement).value = "member@example.com";
    (form.elements.namedItem("password") as HTMLInputElement).value = "password";
    if (signup) (form.elements.namedItem("name") as HTMLInputElement).value = "Member";
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await vi.waitFor(() => expect(app.loginOrRegisterJWT).toHaveBeenCalledOnce());
    app.destroy();
  },
);
it("provider logout detaches the old data view", async () => {
  await fixture.signIn();
  const app = setup();
  await vi.waitFor(() => expect(app.root.textContent).toContain("Hello, Ada"));
  fixture.update({ isPending: false, data: null });
  await vi.waitFor(() => expect(app.logout).toHaveBeenCalledOnce());
  await vi.waitFor(() => expect(app.root.querySelector("form")).not.toBeNull());
  expect(app.root.textContent).not.toContain("Hello, Ada");
  app.destroy();
});
it("provider signout failure remains visible and Retry repeats signout", async () => {
  await fixture.signIn();
  const app = setup();
  await vi.waitFor(() => expect(app.root.textContent).toContain("Hello, Ada"));
  fixture.signOut.mockRejectedValueOnce(new Error("provider signout unavailable"));
  app.root.querySelector<HTMLButtonElement>('[data-action="signout"]')!.click();
  await vi.waitFor(() => expect(app.root.textContent).toContain("provider signout unavailable"));
  app.root.querySelector<HTMLButtonElement>('[data-action="retry"]')!.click();
  await vi.waitFor(() => expect(app.root.querySelector("form")).not.toBeNull());
  expect(fixture.signOut).toHaveBeenCalledTimes(2);
  app.destroy();
});
