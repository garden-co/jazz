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
  actions: undefined as any,
  read: undefined as undefined | (() => Promise<{ data: any; error?: { message: string } }>),
}));
vi.mock("jazz-tools/svelte", async () => ({
  JazzSessionProvider: (await import("./JazzSessionProvider.svelte")).default,
  createJazzSession: async () => controls.owner,
}));
vi.mock("$lib/auth-client", () => ({
  authClient: {
    useSession: () => controls.auth,
    getSession: async () => (controls.read ? controls.read() : { data: controls.current }),
    signOut: async () => {
      controls.current = null;
      return {};
    },
  },
  getToken: async () => "token",
}));
vi.mock("$lib/accounts", () => ({ credential: async () => "token" }));
vi.mock("$lib/auth-actions", () => ({
  setAuthActions: (actions: unknown) => {
    controls.actions = actions;
  },
}));
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

async function setupNotifications() {
  controls.read = undefined;
  const events: string[] = [];
  let releaseLogin: (() => void) | undefined;
  let delayedSubject: string | undefined;
  controls.current = { session: { id: "session-a" }, user: { id: "principal-a" } };
  controls.auth = writable({ data: controls.current });
  const accounts = new AccountManager({
    createLocalFirst: () => {
      throw new Error("unexpected local-first");
    },
    restoreLocalFirst: () => {
      throw new Error("unexpected restore");
    },
    registerJWT: async () => {
      throw new Error("unexpected registration");
    },
    linkJWT: async () => {
      throw new Error("unexpected link");
    },
    logout: () => {
      events.push("logout");
    },
    loginJWT: async () => {
      const subject = controls.current.user.id;
      events.push(`login:${subject}`);
      if (subject === delayedSubject)
        await new Promise<void>((resolve) => {
          releaseLogin = resolve;
        });
      return { id: subject, identity: { issuer: "provider", subject } } as never;
    },
  });
  const owner = await createJazzSessionOwner({
    accounts,
    openClient: async () =>
      attachSubscriptionStore(
        { db: { onAuthChanged: () => () => {} }, session: null, shutdown: async () => {} },
        {} as never,
      ),
  });
  controls.owner = owner;
  const target = document.createElement("div");
  const component = mount(AppProvider, {
    target,
    props: {
      children: createRawSnippet(() => ({ render: () => "<p>PRIVATE DATA</p>" })),
    },
  });
  await settle();
  return {
    owner,
    target,
    events,
    component,
    delay(subject: string) {
      delayedSubject = subject;
    },
    release() {
      releaseLogin!();
    },
  };
}

it("logout then explicit login tolerates delayed provider-store notifications without extra transitions", async () => {
  const { owner, target, events, component } = await setupNotifications();
  expect(events).toEqual(["login:principal-a"]);
  const logout = controls.actions.signOut();
  await settle();
  await logout;
  expect(owner.getSnapshot().status).toBe("signed-out");
  // The store still advertises A even though the authoritative provider is null.
  expect(events).toEqual(["login:principal-a", "logout"]);
  controls.auth.set({ data: null });
  await settle();
  const login = controls.actions.authenticate(false, async () => {
    controls.current = { session: { id: "session-b" }, user: { id: "principal-b" } };
    return {};
  });
  await settle();
  await login;
  expect(owner.getSnapshot().status).toBe("ready");
  expect(owner.getSnapshot().account?.identity.subject).toBe("principal-b");
  expect(events).toEqual(["login:principal-a", "logout", "login:principal-b"]);
  expect(target.textContent).not.toContain("PRIVATE DATA");
  controls.auth.set({ data: controls.current });
  await settle();
  expect(target.textContent).toContain("PRIVATE DATA");
  // A genuinely newer null notification still logs out the selected account.
  controls.current = null;
  controls.auth.set({ data: null });
  await settle();
  expect(owner.getSnapshot().status).toBe("signed-out");
  expect(events).toEqual(["login:principal-a", "logout", "login:principal-b", "logout"]);
  await unmount(component);
  await settle();
});

it("coalesces provider changes during reconciliation and processes the latest account after the pending login", async () => {
  const { owner, target, events, component, delay, release } = await setupNotifications();
  delay("principal-b");
  controls.current = { session: { id: "session-b" }, user: { id: "principal-b" } };
  controls.auth.set({ data: controls.current });
  await settle();
  expect(events).toEqual(["login:principal-a", "login:principal-b"]);
  controls.current = { session: { id: "session-c" }, user: { id: "principal-c" } };
  controls.auth.set({ data: controls.current });
  await settle();
  expect(target.textContent).not.toContain("PRIVATE DATA");
  release();
  await settle();
  expect(events).toEqual(["login:principal-a", "login:principal-b", "login:principal-c"]);
  expect(owner.getSnapshot().account?.identity.subject).toBe("principal-c");
  expect(target.textContent).toContain("PRIVATE DATA");
  expect(target.textContent).not.toContain("already pending");
  await unmount(component);
  await settle();
});

it("does not enroll after an authoritative provider read finishes following unmount", async () => {
  const { owner, events, component } = await setupNotifications();
  let resolve!: (value: { data: any }) => void;
  controls.read = () =>
    new Promise((yes) => {
      resolve = yes;
    });
  const next = { session: { id: "session-b" }, user: { id: "principal-b" } };
  controls.current = next;
  controls.auth.set({ data: next });
  await settle();
  await unmount(component);
  resolve({ data: next });
  await settle();
  expect(owner.getSnapshot().status).toBe("closed");
  expect(events).toEqual(["login:principal-a"]);
  controls.read = undefined;
});

it("keeps a successful provider request with a failed session read actionable", async () => {
  const { target, events, component } = await setupNotifications();
  const failure = new Error("provider session read unavailable");
  const login = controls.actions.authenticate(false, async () => {
    controls.current = { session: { id: "session-b" }, user: { id: "principal-b" } };
    controls.auth.set({ data: controls.current });
    controls.read = async () => ({ data: null, error: { message: failure.message } });
    return {};
  });
  await expect(login).rejects.toThrow(failure.message);
  await settle();
  expect(target.textContent).toContain(failure.message);
  expect(target.querySelector("button")).not.toBeNull();
  expect(events).toEqual(["login:principal-a"]);
  controls.read = undefined;
  target.querySelector<HTMLButtonElement>("button")!.click();
  await settle();
  expect(target.textContent).toContain("PRIVATE DATA");
  expect(events).toEqual(["login:principal-a", "login:principal-b"]);
  await unmount(component);
  await settle();
});

it("does not let failed signup recovery suppress a genuinely newer provider account", async () => {
  const { owner, target, events, component } = await setupNotifications();
  const signup = controls.actions.authenticate(true, async () => {
    controls.current = { session: { id: "session-b" }, user: { id: "principal-b" } };
    controls.auth.set({ data: controls.current });
    return {};
  });
  await expect(signup).rejects.toThrow("unexpected registration");
  await settle();
  expect(target.textContent).not.toContain("PRIVATE DATA");
  controls.current = { session: { id: "session-c" }, user: { id: "principal-c" } };
  controls.auth.set({ data: controls.current });
  await settle();
  expect(events).toEqual(["login:principal-a", "login:principal-c"]);
  expect(owner.getSnapshot().account?.identity.subject).toBe("principal-c");
  expect(target.textContent).toContain("PRIVATE DATA");
  await unmount(component);
  await settle();
});

it("does not turn a resolved provider read error into logout during reconciliation", async () => {
  const { owner, target, events, component } = await setupNotifications();
  controls.read = async () => ({ data: null, error: { message: "provider unavailable" } });
  controls.auth.set({ data: null });
  await settle();
  expect(events).toEqual(["login:principal-a"]);
  expect(owner.getSnapshot().account?.identity.subject).toBe("principal-a");
  expect(target.textContent).not.toContain("PRIVATE DATA");
  controls.read = undefined;
  await unmount(component);
  await settle();
});
