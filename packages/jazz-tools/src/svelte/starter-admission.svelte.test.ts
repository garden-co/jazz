import { afterEach, it, expect, vi } from "vitest";
import { mount, unmount, tick, createRawSnippet } from "svelte";
import { writable, get } from "svelte/store";
import { AccountManager } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import type { JWTAuth } from "../accounts/enrollment.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";

type ProviderSession = { session: { id: string }; user: { id: string } } | null;
type ProviderRead = { data: ProviderSession; error?: { message: string } };
const controls = vi.hoisted(() => ({
  owner: undefined as any,
  auth: undefined as any,
  current: null as ProviderSession,
  actions: undefined as { signOut(): Promise<void> } | undefined,
  read: undefined as undefined | (() => Promise<ProviderRead>),
  tokenRead: undefined as undefined | (() => Promise<{ data: { token: string } }>),
}));
vi.mock("jazz-tools/svelte", async () => ({
  JazzSessionProvider: (await import("./JazzSessionProvider.svelte")).default,
  createJazzSession: async () => controls.owner,
  connectBetterAuth: (await import("../session/better-auth.js")).connectBetterAuth,
}));
vi.mock("$lib/auth-client", () => ({
  authClient: {
    $store: {
      atoms: {
        get session() {
          return controls.auth;
        },
      },
    },
    $fetch: async () =>
      controls.tokenRead ? controls.tokenRead() : { data: { token: controls.current!.user.id } },
    signOut: async () => {
      // Better Auth's atom can publish its null snapshot after signOut resolves.
      controls.current = null;
      return {};
    },
  },
}));
vi.mock("$lib/auth-actions", () => ({
  setAuthActions: (actions: typeof controls.actions) => {
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
const identity = (letter: string): ProviderSession => ({
  session: { id: `session-${letter}` },
  user: { id: `principal-${letter}` },
});
let dispose: (() => Promise<void>) | undefined;
afterEach(async () => {
  await dispose?.();
  dispose = undefined;
});

async function setupNotifications(read?: typeof controls.read) {
  controls.read = read;
  controls.tokenRead = undefined;
  controls.current = identity("a");
  const state = writable({
    data: controls.current,
    isPending: false,
    error: undefined as ProviderRead["error"],
    refetch: refresh,
  });
  async function refresh() {
    const result = controls.read ? await controls.read() : { data: controls.current };
    state.set({ ...result, isPending: false, error: result.error, refetch: refresh });
  }
  controls.auth = {
    get: () => get(state),
    subscribe: state.subscribe,
    set: (next: ProviderRead) =>
      state.set({ ...next, isPending: false, error: next.error, refetch: refresh }),
  };
  if (read) await refresh();
  const events: string[] = [];
  let releaseLogin: (() => void) | undefined;
  let delayedSubject: string | undefined;
  let rejectedSubject: string | undefined;
  let failOpen = false;
  const unused = async () => {
    throw new Error("unexpected non-atomic admission");
  };
  const accounts = new AccountManager({
    createLocalFirst: () => {
      throw new Error("unexpected local-first");
    },
    restoreLocalFirst: () => {
      throw new Error("unexpected restore");
    },
    registerJWT: unused,
    loginJWT: unused,
    linkJWT: unused,
    logout: () => {
      events.push("logout");
    },
    loginOrRegisterJWT: async (auth: JWTAuth) => {
      const subject = typeof auth === "string" ? auth : await auth.getToken();
      events.push(`login:${subject}`);
      if (subject === rejectedSubject) throw new Error(`${subject} admission rejected`);
      if (subject === delayedSubject)
        await new Promise<void>((resolve) => {
          releaseLogin = resolve;
        });
      return { id: `account-${subject}`, identity: { issuer: "provider", subject } } as never;
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
    props: {
      // A signed-out route renders a login form; private content needs a client.
      // A stale reopened A client would still expose this marker if admitted for B.
      children: createRawSnippet(() => ({
        render: () => (owner.getSnapshot().client ? "<p>PRIVATE DATA</p>" : "<p>SIGN IN</p>"),
      })),
    },
  });
  let mounted = true;
  const stop = async () => {
    if (mounted) {
      mounted = false;
      await unmount(component);
    }
    await settle();
  };
  dispose = stop;
  await settle();
  return {
    owner,
    target,
    events,
    stop,
    refresh,
    publish(letter: string | null) {
      controls.current = letter === null ? null : identity(letter);
      controls.auth.set({ data: controls.current });
    },
    delay(subject: string) {
      delayedSubject = subject;
    },
    release() {
      releaseLogin!();
    },
    reject(subject: string) {
      rejectedSubject = subject;
    },
    failReopen() {
      failOpen = true;
    },
  };
}

it("B's startup retry cannot admit A after failed B login and failed A reopen", async () => {
  const { owner, target, publish, reject, failReopen } = await setupNotifications();
  expect(target.textContent).toContain("PRIVATE DATA");
  reject("principal-b");
  failReopen();
  publish("b");
  await settle();
  expect(owner.getSnapshot().status).toBe("error");
  expect(owner.getSnapshot().account?.identity.subject).toBe("principal-a");
  expect(target.textContent).not.toContain("PRIVATE DATA");
  target.querySelector<HTMLButtonElement>("button")!.click();
  await settle();
  expect(target.textContent).not.toContain("PRIVATE DATA");
});

it("logout then provider login tolerates delayed atom notifications without extra transitions", async () => {
  const { owner, target, events, publish } = await setupNotifications();
  expect(events).toEqual(["login:principal-a"]);
  const logout = controls.actions!.signOut();
  await settle();
  await logout;
  expect(owner.getSnapshot().status).toBe("signed-out");
  expect(events).toEqual(["login:principal-a", "logout"]);
  controls.auth.set({ data: null });
  await settle();
  // Forms now call the provider directly. Jazz waits for the provider atom.
  controls.current = identity("b");
  await settle();
  expect(owner.getSnapshot().status).toBe("signed-out");
  expect(target.textContent).not.toContain("PRIVATE DATA");
  controls.auth.set({ data: controls.current });
  await settle();
  expect(owner.getSnapshot().account?.identity.subject).toBe("principal-b");
  expect(events).toEqual(["login:principal-a", "logout", "login:principal-b"]);
  expect(target.textContent).toContain("PRIVATE DATA");
  publish(null);
  await settle();
  expect(owner.getSnapshot().status).toBe("signed-out");
  expect(events).toEqual(["login:principal-a", "logout", "login:principal-b", "logout"]);
});

it("coalesces provider changes during reconciliation and processes the latest account after the pending login", async () => {
  const { owner, target, events, publish, delay, release } = await setupNotifications();
  delay("principal-b");
  publish("b");
  await settle();
  expect(events).toEqual(["login:principal-a", "login:principal-b"]);
  publish("c");
  await settle();
  expect(target.textContent).not.toContain("PRIVATE DATA");
  release();
  await settle();
  expect(events).toEqual(["login:principal-a", "login:principal-b", "login:principal-c"]);
  expect(owner.getSnapshot().account?.identity.subject).toBe("principal-c");
  expect(target.textContent).toContain("PRIVATE DATA");
  expect(target.textContent).not.toContain("already pending");
});

it("does not enroll after a provider token read finishes following unmount", async () => {
  const { owner, events, publish, stop } = await setupNotifications();
  let resolve!: (value: { data: { token: string } }) => void;
  controls.tokenRead = () =>
    new Promise((yes) => {
      resolve = yes;
    });
  publish("b");
  await settle();
  await stop();
  resolve({ data: { token: "principal-b" } });
  await settle();
  expect(owner.getSnapshot().status).toBe("closed");
  expect(events).toEqual(["login:principal-a"]);
});

it("keeps a successful provider request with a failed session read actionable", async () => {
  const { target, events, refresh } = await setupNotifications();
  controls.current = identity("b");
  controls.read = async () => ({
    data: null,
    error: { message: "provider session read unavailable" },
  });
  await refresh();
  await settle();
  expect(target.textContent).toContain("provider session read unavailable");
  expect(target.querySelector("button")).not.toBeNull();
  expect(events).toEqual(["login:principal-a"]);
  controls.read = undefined;
  target.querySelector<HTMLButtonElement>("button")!.click();
  await settle();
  expect(target.textContent).toContain("PRIVATE DATA");
  expect(events).toEqual(["login:principal-a", "login:principal-b"]);
});

it("does not let failed signup recovery suppress a genuinely newer provider account", async () => {
  const { owner, target, events, publish, reject } = await setupNotifications();
  reject("principal-b");
  publish("b");
  await settle();
  expect(target.textContent).toContain("principal-b admission rejected");
  expect(target.textContent).not.toContain("PRIVATE DATA");
  publish("c");
  await settle();
  expect(events).toEqual(["login:principal-a", "login:principal-b", "login:principal-c"]);
  expect(owner.getSnapshot().account?.identity.subject).toBe("principal-c");
  expect(target.textContent).toContain("PRIVATE DATA");
});

it("does not turn a resolved provider read error into logout during reconciliation", async () => {
  const { owner, target, events, refresh } = await setupNotifications();
  controls.read = async () => ({ data: null, error: { message: "provider unavailable" } });
  await refresh();
  await settle();
  expect(events).toEqual(["login:principal-a"]);
  expect(owner.getSnapshot().account?.identity.subject).toBe("principal-a");
  expect(target.textContent).not.toContain("PRIVATE DATA");
});

it("retains the prepared session and retries an initial provider read failure", async () => {
  const { owner, target, events } = await setupNotifications(async () => ({
    data: null,
    error: { message: "initial provider read unavailable" },
  }));
  expect(target.textContent).toContain("initial provider read unavailable");
  expect(events).toEqual([]);
  expect(owner.getSnapshot().status).toBe("signed-out");
  controls.read = undefined;
  target.querySelector<HTMLButtonElement>("button")!.click();
  await settle();
  expect(events).toEqual(["login:principal-a"]);
  expect(owner.getSnapshot().status).toBe("ready");
  expect(target.textContent).toContain("PRIVATE DATA");
});
