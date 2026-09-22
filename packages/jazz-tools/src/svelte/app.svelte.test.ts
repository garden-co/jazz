import { afterEach, expect, it, vi } from "vitest";
import { mount, unmount, tick, createRawSnippet, getAllContexts } from "svelte";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import JazzProvider from "./JazzProvider.svelte";
import type { JazzAuthState } from "./auth-state.js";
import { writable } from "svelte/store";
import { jwtAuth, type JazzAuth } from "../session/app.js";
import JwtApp from "../../tests/svelte/JwtApp.svelte";
import AuthStatus from "../../tests/svelte/AuthStatus.svelte";
const factory = vi.hoisted(() => vi.fn());
vi.mock("../session/create-jazz-session.js", () => ({ createJazzSession: factory }));
vi.mock("../dev-tools/auto-attach.js", () => ({ startInspectorOnce: vi.fn() }));
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
  let observedAuth!: JazzAuthState;
  const status = createRawSnippet(() => ({
    render: () => "<div></div>",
    setup: (element) => {
      const nested = mount(AuthStatus, {
        target: element,
        context: getAllContexts(),
        props: {
          onAuth: (auth: JazzAuthState) => {
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
      appId: "test",
      autoAttachDevTools: false,
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
  expect(factory.mock.calls[1][0].initial).toBe("local-first");
  expect(target.textContent).toContain("ready");
  target.querySelector("button")!.click();
  await settle();
  expect(target.textContent).toBe("SIGN IN");
  expect(events).toEqual(["detached", "shutdown"]);
  const actions = observedAuth.sessionActions;
  await actions.createLocalFirst();
  await settle();
  expect(target.textContent).toContain("ready");
  expect(observedAuth.sessionActions).toBe(actions);
  failLogout = true;
  await expect(observedAuth.logout()).resolves.toBeUndefined();
  await settle();
  expect(target.textContent).toContain("logout failed");
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
      appId: "test",
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

it("updates JWT descriptors reactively and never mounts private children while pending", async () => {
  const events: string[] = [];
  const handle = (id: string) =>
    ({ id, identity: { issuer: "test", subject: id } }) as AccountHandle;
  const accounts = new AccountManager({
    logout: () => {},
    loginOrRegisterJWT: async (auth: any) => handle(await auth.getToken()),
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
  });
  factory.mockResolvedValue(owner);
  const descriptor = (key: string | null, isPending = false) =>
    jwtAuth({ key, isPending, getToken: async () => key!, logout: async () => {} });
  const auth = writable(descriptor(null));
  const seen: boolean[] = [];
  let mounts = 0;
  const target = document.createElement("div");
  component = mount(JwtApp, {
    target,
    props: {
      auth,
      observe: (value: JazzAuth) => {
        seen.push(value.kind === "jwt" && value.isPending === true);
        return "";
      },
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
  expect(target.textContent).toBe("SIGN IN");
  expect(mounts).toBe(0);
  auth.set(descriptor("a"));
  await settle();
  expect(target.textContent).toBe("PRIVATE");
  auth.set(descriptor("a", true));
  await settle();
  expect(target.textContent).toBe("WAIT");
  expect(seen).not.toContain(true);
  auth.set(descriptor("b"));
  await settle();
  expect(target.textContent).toBe("PRIVATE");
  expect(owner.getSnapshot().account?.identity.subject).toBe("b");
  expect(events.slice(0, 2)).toEqual(["detached", "shutdown"]);
});
