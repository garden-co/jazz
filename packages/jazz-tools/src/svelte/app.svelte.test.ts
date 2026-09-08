import { afterEach, expect, it, vi } from "vitest";
import { mount, unmount, tick, createRawSnippet, getAllContexts } from "svelte";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import JazzProvider from "./JazzProvider.svelte";
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
  const accounts = new AccountManager(
    { createLocalFirst: () => handle, logout: () => {} } as any,
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
  const status = createRawSnippet(() => ({
    render: () => "<div></div>",
    setup: (element) => {
      const nested = mount(AuthStatus, { target: element, context: getAllContexts() });
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
