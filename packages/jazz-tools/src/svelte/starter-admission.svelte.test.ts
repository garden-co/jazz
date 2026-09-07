import { it, expect, vi } from "vitest";
import { mount, unmount, tick, createRawSnippet } from "svelte";
import { writable } from "svelte/store";
import { AccountManager } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
const controls = vi.hoisted(() => ({
  owner: undefined as any,
  auth: undefined as any,
  current: undefined as any,
}));
vi.mock("jazz-tools/svelte", async () => ({
  JazzSessionProvider: (await import("./JazzSessionProvider.svelte")).default,
  createJazzSession: async () => controls.owner,
}));
vi.mock("$lib/auth-client", () => ({
  authClient: {
    useSession: () => controls.auth,
    getSession: async () => ({ data: controls.current }),
  },
  getToken: async () => "token",
}));
vi.mock("$lib/accounts", () => ({ credential: async () => "token" }));
vi.mock("$lib/auth-actions", () => ({ setAuthActions: vi.fn() }));
vi.mock("../dev-tools/auto-attach.js", () => ({ startInspectorOnce: vi.fn() }));
import AppProvider from "../../../../starters/sveltekit-betterauth/src/lib/JazzClientProvider.svelte";
const settle = async () => {
  for (let i = 0; i < 30; i++) {
    await tick();
    await Promise.resolve();
  }
};
it("B's startup retry cannot admit A after failed B login and failed A reopen", async () => {
  controls.current = { session: { id: "session-a" }, user: { id: "principal-a" } };
  controls.auth = writable({ data: controls.current });
  let failLogin = false,
    failOpen = false;
  const handles = {
    "principal-a": { id: "account-a", identity: { issuer: "provider", subject: "principal-a" } },
    "principal-b": { id: "account-b", identity: { issuer: "provider", subject: "principal-b" } },
  };
  const accounts = new AccountManager({
    createLocalFirst: () => {
      throw new Error("unexpected");
    },
    restoreLocalFirst: () => {
      throw new Error("unexpected");
    },
    logout: () => {},
    registerJWT: async () => {
      throw new Error("unexpected");
    },
    linkJWT: async () => {
      throw new Error("unexpected");
    },
    loginJWT: async () => {
      if (failLogin) throw new Error("B login rejected");
      return handles[controls.current.user.id as keyof typeof handles] as never;
    },
  });
  const owner = await createJazzSessionOwner({
    accounts,
    openClient: async () => {
      if (failOpen) {
        failOpen = false;
        throw new Error("A reopen failed");
      }
      return attachSubscriptionStore(
        { db: { onAuthChanged: () => () => {} }, session: null, shutdown: async () => {} },
        {} as never,
      );
    },
  });
  controls.owner = owner;
  const target = document.createElement("div");
  const component = mount(AppProvider, {
    target,
    props: { children: createRawSnippet(() => ({ render: () => "<p>PRIVATE DATA</p>" })) },
  });
  await settle();
  expect(target.textContent).toContain("PRIVATE DATA");
  failLogin = true;
  failOpen = true;
  controls.current = { session: { id: "session-b" }, user: { id: "principal-b" } };
  controls.auth.set({ data: controls.current });
  await settle();
  expect(owner.getSnapshot().status).toBe("error");
  expect(owner.getSnapshot().account?.identity.subject).toBe("principal-a");
  expect(target.textContent).not.toContain("PRIVATE DATA");
  target.querySelector<HTMLButtonElement>("button")!.click();
  await settle();
  const observed = target.textContent;
  await unmount(component);
  await settle();
  expect(observed).not.toContain("PRIVATE DATA");
});
