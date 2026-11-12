import { afterEach, expect, it, vi } from "vitest";
import { mount, unmount, tick, createRawSnippet, getAllContexts } from "svelte";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import JazzProvider from "./JazzProvider.svelte";
import type { JazzSessionState } from "./session-state.js";
import AuthStatus from "../../tests/svelte/AuthStatus.svelte";
import UserApp from "../../tests/svelte/UserApp.svelte";
const factory = vi.hoisted(() => vi.fn());
vi.mock("../session/create-jazz-session.js", () => ({ openBrowserSessionCore: factory }));
const settle = async () => {
  for (let i = 0; i < 30; i++) {
    await tick();
    await Promise.resolve();
  }
};
let component: ReturnType<typeof mount> | undefined;
afterEach(async () => {
  if (component) await unmount(component);
  component = undefined;
  await settle();
  factory.mockReset();
});
it("owns startup retry and keeps the hook context across fallback and ready children", async () => {
  const events: string[] = [];
  const handle = { id: "local", identity: { issuer: "test", subject: "local" } } as AccountHandle;
  let failLogout = false;
  const accounts = new AccountManager(
    {
      createLocalFirst: () => handle,
      logout: () => {
        if (failLogout) throw new Error("logout failed");
      },
    } as any,
    handle,
  );
  const owner = await createJazzSessionOwner({
    accounts,
    openClient: async () =>
      attachSubscriptionStore(
        {
          db: { onAuthChanged: () => () => {} },
          session: null,
          shutdown: async () => {
            events.push("shutdown");
          },
        },
        {} as never,
      ) as any,
  });
  factory.mockRejectedValueOnce(new Error("startup failed")).mockResolvedValue(owner);
  const target = document.createElement("div");
  let observedAuth!: JazzSessionState;
  const status = createRawSnippet(() => ({
    render: () => "<div></div>",
    setup: (element) => {
      const nested = mount(AuthStatus, {
        target: element,
        context: getAllContexts(),
        props: {
          onAuth: (auth: JazzSessionState) => {
            observedAuth = auth;
          },
        },
      });
      return () => {
        events.push("detached");
        void unmount(nested);
      };
    },
  }));
  component = mount(JazzProvider, {
    target,
    props: {
      config: { scope: "test" },
      children: status,
      signedOut: createRawSnippet(() => ({ render: () => "<p>SIGN IN</p>" })),
    },
  });
  await settle();
  expect(target.textContent).toContain("startup failed");
  expect(events).toEqual([]);
  target.querySelector("button")!.click();
  await settle();
  expect(factory).toHaveBeenCalledTimes(2);
  expect(factory.mock.calls[1][0].initial).toBeUndefined();
  expect(target.textContent).toContain("ready");
  target.querySelector("button")!.click();
  await settle();
  expect(target.textContent).toBe("SIGN IN");
  expect(events).toEqual(["detached", "shutdown"]);
  const { createDeviceAccount } = observedAuth;
  await createDeviceAccount();
  await settle();
  expect(target.textContent).toContain("ready");
  failLogout = true;
  await expect(observedAuth.signOut()).rejects.toThrow("logout failed");
  await settle();
  await unmount(component);
  component = undefined;
  await settle();
  expect(owner.getSnapshot().status).toBe("closed");
});
it("renders custom startup loading and error snippets with the provider retry", async () => {
  let reject!: (cause: Error) => void;
  factory.mockImplementation(
    () =>
      new Promise((_resolve, no) => {
        reject = no;
      }),
  );
  const target = document.createElement("div");
  let retry!: () => Promise<void>;
  component = mount(JazzProvider, {
    target,
    props: {
      config: { scope: "test" },
      children: createRawSnippet(() => ({ render: () => "<p>PRIVATE</p>" })),
      loading: createRawSnippet(() => ({ render: () => "<p>CUSTOM LOADING</p>" })),
      error: createRawSnippet((error, action) => {
        retry = action();
        return { render: () => `<p>CUSTOM ${error().message}</p>` };
      }),
    },
  });
  await settle();
  expect(target.textContent).toBe("CUSTOM LOADING");
  reject(new Error("failure"));
  await settle();
  expect(target.textContent).toBe("CUSTOM failure");
  void retry().catch(() => {});
  await settle();
  expect(factory).toHaveBeenCalledTimes(2);
  expect(target.textContent).toBe("CUSTOM LOADING");
  reject(new Error("again"));
  await settle();
});

it("follows the backend on refresh and never mounts private children signed out", async () => {
  const events: string[] = [];
  // Who the app's backend names (null: no one).
  let backendUser: string | null = null;
  const handle = (id: string) =>
    ({ id, identity: { issuer: "test", subject: id } }) as AccountHandle;
  const accounts = new AccountManager({
    logout: () => {},
    loginWithBackend: async () => handle(backendUser!),
  } as any);
  const owner = await createJazzSessionOwner({
    accounts,
    openClient: async () =>
      attachSubscriptionStore(
        {
          db: { onAuthChanged: () => () => {} },
          session: null,
          shutdown: async () => {
            events.push("shutdown");
          },
        },
        {} as never,
      ) as any,
    // A stand-in for the browser's backend watch.
    decorate: (session) => ({
      async reauthenticate() {
        const current = session.getSnapshot().account?.identity.subject;
        if (current === backendUser) return;
        if (session.getSnapshot().status === "ready") await session.logout();
        if (backendUser !== null) await session.loginWithBackend();
      },
    }),
  });
  factory.mockResolvedValue(owner);
  let auth!: JazzSessionState;
  let mounts = 0;
  const target = document.createElement("div");
  component = mount(UserApp, {
    target,
    props: {
      onAuth: (value: JazzSessionState) => (auth = value),
      children: createRawSnippet(() => ({
        render: () => {
          mounts++;
          return "<p>PRIVATE</p>";
        },
        setup: () => () => {
          events.push("detached");
        },
      })),
    },
  });
  await settle();
  expect(target.textContent).toContain("SIGN IN");
  expect(factory).toHaveBeenCalledWith(expect.objectContaining({ initial: "signed-out" }));
  expect(mounts).toBe(0);
  backendUser = "a";
  await auth.refresh();
  await settle();
  expect(target.textContent).toBe("PRIVATE");
  backendUser = "b";
  await auth.refresh();
  await settle();
  expect(target.textContent).toBe("PRIVATE");
  expect(owner.getSnapshot().account?.identity.subject).toBe("b");
  expect(events.slice(0, 2)).toEqual(["detached", "shutdown"]);
  backendUser = null;
  await auth.refresh();
  await settle();
  expect(target.textContent).toContain("SIGN IN");
});
