// @vitest-environment jsdom
import { beforeEach, expect, it, vi } from "vitest";
import {
  AccountManager,
  type AccountHandle,
} from "../../../packages/jazz-tools/dist/accounts/state.js";
import { createJazzSessionOwner } from "../../../packages/jazz-tools/dist/session/state.js";
import { betterAuth, createJazzAppOwner } from "../../../packages/jazz-tools/dist/session/app.js";
import type { Db } from "jazz-tools";
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
import { authClient } from "./auth-client.js";
import { mountTodoWidget } from "./todo-widget.js";
beforeEach(() => {
  fixture.listeners.clear();
  fixture.update({ isPending: false, data: null });
  vi.clearAllMocks();
});
function setup(options: { startupFailure?: boolean } = {}) {
  const events: string[] = [];
  const account = {
    id: "account-1",
    identity: { issuer: "provider", subject: "user-1" },
  } as AccountHandle;
  const loginOrRegisterJWT = vi.fn(async () => account);
  const logout = vi.fn(() => {
    events.push("logout");
  });
  const accounts = new AccountManager({
    createLocalFirst: () => account,
    restoreLocalFirst: () => account,
    registerJWT: loginOrRegisterJWT,
    loginJWT: loginOrRegisterJWT,
    loginOrRegisterJWT,
    linkJWT: async () => account,
    logout,
  });
  let startupFailure = options.startupFailure;
  const jazz = createJazzAppOwner({ auth: betterAuth(authClient) }, async () => {
    if (startupFailure) {
      startupFailure = false;
      throw new Error("storage unavailable");
    }
    return createJazzSessionOwner({
      accounts,
      async openClient() {
        return {
          db: {} as Db,
          async shutdown() {
            events.push("flush");
          },
        };
      },
    });
  });
  vi.mocked(mountTodoWidget).mockImplementation(() => () => {
    events.push("detach");
  });
  const root = document.createElement("div");
  const app = mountApp(root, jazz);
  return {
    root,
    jazz,
    events,
    loginOrRegisterJWT,
    logout,
    async destroy() {
      app.destroy();
      await jazz.dispose();
    },
  };
}
it("renders the real sign-in form after initial signed-out hydration", async () => {
  const app = setup();
  await vi.waitFor(() => expect(app.root.querySelector("form")).not.toBeNull());
  await app.destroy();
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
  await app.destroy();
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
    await app.destroy();
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
  await app.destroy();
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
  await app.destroy();
});

it("keeps private content hidden during provider hydration", async () => {
  fixture.update({ isPending: true, data: null });
  const app = setup();
  await app.jazz.start();
  expect(app.root.textContent).toContain("Loading");
  expect(app.root.querySelector("form")).toBeNull();
  expect(mountTodoWidget).not.toHaveBeenCalled();
  fixture.update({ isPending: false, data: null });
  await vi.waitFor(() => expect(app.root.querySelector("form")).not.toBeNull());
  await app.destroy();
});
it("shows startup errors and retries initialization", async () => {
  const app = setup({ startupFailure: true });
  await vi.waitFor(() => expect(app.root.textContent).toContain("storage unavailable"));
  expect(mountTodoWidget).not.toHaveBeenCalled();
  app.root.querySelector<HTMLButtonElement>('[data-action="retry"]')!.click();
  await vi.waitFor(() => expect(app.root.querySelector("form")).not.toBeNull());
  await app.destroy();
});
it("detaches the rendered subscription before flushing and releases observers on destroy", async () => {
  await fixture.signIn();
  const app = setup();
  await vi.waitFor(() => expect(app.root.textContent).toContain("Hello, Ada"));
  app.root.querySelector<HTMLButtonElement>('[data-action="signout"]')!.click();
  await vi.waitFor(() => expect(app.root.querySelector("form")).not.toBeNull());
  expect(app.events.indexOf("detach")).toBeGreaterThanOrEqual(0);
  expect(app.events.indexOf("flush")).toBeGreaterThan(app.events.indexOf("detach"));
  await app.destroy();
  expect(fixture.listeners.size).toBe(0);
  expect(app.root.childElementCount).toBe(0);
});
