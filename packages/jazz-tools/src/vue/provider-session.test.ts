// @vitest-environment jsdom
import {
  createApp,
  createSSRApp,
  defineComponent,
  h,
  isReadonly,
  nextTick,
  onUnmounted,
  shallowRef,
} from "vue";
import { renderToString } from "@vue/server-renderer";
import { describe, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { ownJazzSession, startJazzSession, type JazzSession } from "../session/jazz-session.js";
import { JazzProvider, useJazzSession, type UseJazzSession } from "./session.js";
import { GracefulShutdownSyncError } from "../runtime/graceful-shutdown-error.js";
import type { JazzClient } from "./create-jazz-client.js";
const mocks = vi.hoisted(() => ({ factory: vi.fn() }));
vi.mock("../session/create-jazz-session.js", () => ({ openBrowserSessionCore: mocks.factory }));
const flush = async () => {
  for (let i = 0; i < 20; i++) {
    await Promise.resolve();
    await nextTick();
  }
};
const account = Object.freeze({
  id: "local",
  identity: { issuer: "test", subject: "local" },
}) as AccountHandle;
async function fixture(fail: false | "unknown" | "graceful" = false) {
  const events: string[] = [];
  const accounts = new AccountManager(
    {
      createLocalFirst: () => account,
      loginWithBackend: async () => account,
    },
    account,
  );
  const client: JazzClient = {
    db: { onAuthChanged: () => () => events.push("auth-cleanup") } as never,
    session: null,
    shutdown: vi.fn(async () => {
      events.push("shutdown");
      if (fail) {
        const error = new Error("flush failed");
        const failure = fail === "graceful" ? new GracefulShutdownSyncError(error) : error;
        fail = false;
        throw failure;
      }
    }),
  };
  const session = await createJazzSessionOwner({ accounts, openClient: async () => client });
  return { events, session };
}
describe("Vue JazzProvider", () => {
  it("is inert in SSR and supplies default loading", async () => {
    mocks.factory.mockReset();
    const app = createSSRApp({ render: () => h(JazzProvider, { config: { scope: "test" } }) });
    expect(await renderToString(app)).toContain("Loading Jazz");
    expect(mocks.factory).not.toHaveBeenCalled();
  });
  it("renders nothing once its session is closed, or the closed slot", async () => {
    for (const closed of [undefined, () => h("p", "Session closed")]) {
      const { session: owner } = await fixture();
      const session = ownJazzSession({}, async () => owner, { start: false });
      await startJazzSession(session as JazzSession<unknown>);
      const target = document.createElement("div");
      const app = createApp({
        render: () =>
          h(
            JazzProvider,
            { session: session as never },
            {
              default: () => h("p", "data"),
              loading: () => h("p", "Loading"),
              ...(closed ? { closed } : {}),
            },
          ),
      });
      app.mount(target);
      await flush();
      expect(target.textContent).toBe("data");
      await session.close();
      await flush();
      expect(session.getSnapshot().status).toBe("closed");
      // Before: the loading slot, forever.
      expect(target.textContent).toBe(closed ? "Session closed" : "");
      app.unmount();
    }
  });
  it("passes its config to the session as is", async () => {
    const { session } = await fixture();
    mocks.factory.mockReset().mockResolvedValue(session);
    const config = { scope: "test", clearOnSignOut: true, linkDeviceAccountOnSignUp: false };
    const app = createApp({ render: () => h(JazzProvider, { config }) });
    app.mount(document.createElement("div"));
    await flush();
    expect(mocks.factory).toHaveBeenCalledExactlyOnceWith(config);
    app.unmount();
  });

  it("refuses config props spread on the provider, and a config with an account", () => {
    mocks.factory.mockReset();
    for (const props of [{ scope: "test" }, { config: { scope: "test", account: {} } }]) {
      const errors: unknown[] = [];
      const app = createApp({ render: () => h(JazzProvider, props as never) });
      app.config.errorHandler = (error) => {
        errors.push(error);
      };
      app.config.warnHandler = () => {};
      app.mount(document.createElement("div"));
      expect(String(errors[0])).toMatch(/JazzProvider/);
      app.unmount();
    }
    expect(mocks.factory).not.toHaveBeenCalled();
  });

  it("refuses the removed auto-attach-dev-tools attribute instead of passing it to the client", () => {
    mocks.factory.mockReset();
    for (const attrs of [{ autoAttachDevTools: false }, { "auto-attach-dev-tools": false }]) {
      const errors: unknown[] = [];
      const app = createApp({
        render: () => h(JazzProvider, { config: { scope: "test" }, ...attrs }),
      });
      app.config.errorHandler = (error) => {
        errors.push(error);
      };
      app.config.warnHandler = () => {};
      app.mount(document.createElement("div"));
      expect(String(errors[0])).toMatch(/JazzProvider takes no autoAttachDevTools.*mountInspector/);
      app.unmount();
    }
    expect(mocks.factory).not.toHaveBeenCalled();
  });

  it("retries startup failures with the default error button and starts signed out by default", async () => {
    const { session } = await fixture();
    mocks.factory
      .mockReset()
      .mockRejectedValueOnce(new Error("startup failed"))
      .mockResolvedValue(session);
    const node = document.createElement("div");
    const app = createApp({
      render: () =>
        h(JazzProvider, { config: { scope: "test" } }, { default: () => h("p", "active") }),
    });
    app.mount(node);
    await flush();
    expect(node.textContent).toContain("startup failed");
    node.querySelector("button")!.click();
    await flush();
    expect(node.textContent).toBe("active");
    expect(mocks.factory.mock.calls[0][0].initial).toBeUndefined();
    app.unmount();
    await flush();
  });
  it("keeps captured runtime configuration", async () => {
    const { session } = await fixture();
    mocks.factory.mockReset().mockResolvedValue(session);
    const version = shallowRef(0);
    const node = document.createElement("div");
    const app = createApp({
      render: () =>
        h(JazzProvider, {
          config: { scope: `test-${version.value}`, initial: "signed-out" },
        }),
    });
    app.mount(node);
    await flush();
    version.value++;
    await flush();
    expect(mocks.factory).toHaveBeenCalledTimes(1);
    expect(mocks.factory.mock.calls[0][0].scope).toBe("test-0");
    expect(mocks.factory.mock.calls[0][0].initial).toBe("signed-out");
    app.unmount();
    await flush();
  });
  it.each(["unknown", "graceful"] as const)(
    "detaches consumers before a %s sign-out failure, then signs out",
    async (failure) => {
      const { session, events } = await fixture(failure);
      mocks.factory.mockReset().mockResolvedValue(session);
      let auth!: UseJazzSession;
      const Child = defineComponent({
        setup() {
          auth = useJazzSession();
          expect(isReadonly(auth.snapshot)).toBe(true);
          onUnmounted(() => events.push("child-cleanup"));
          return () => h("p", "active");
        },
      });
      const node = document.createElement("div");
      const app = createApp({
        render: () =>
          h(
            JazzProvider,
            { config: { scope: "test" } },
            { default: () => h(Child), signedOut: () => h("p", "signed out") },
          ),
      });
      app.mount(node);
      await flush();
      const logged = vi.spyOn(console, "error").mockImplementation(() => {});
      if (failure === "graceful") {
        // The client couldn't sync but still works: ready, with the error.
        await expect(auth.signOut()).rejects.toThrow();
        await flush();
        expect(auth.snapshot.value.status).toBe("ready");
        expect(auth.snapshot.value.error?.message).toContain("could not synchronize");
        await auth.retry();
      } else {
        // The user asked to leave and teardown began: signed out, logged.
        await auth.signOut();
      }
      logged.mockRestore();
      await flush();
      expect(events.indexOf("child-cleanup")).toBeLessThan(events.indexOf("shutdown"));
      expect(node.textContent).toBe("signed out");
      app.unmount();
      await flush();
    },
  );
});

it("follows the backend on refresh: signed out, signed in, signed out", async () => {
  mocks.factory.mockReset();
  // Who the app's backend names (null: no one).
  let backendUser: string | null = null;
  const handle = (id: string) =>
    ({ id, identity: { issuer: "test", subject: id } }) as AccountHandle;
  const session = await createJazzSessionOwner({
    accounts: new AccountManager({
      logout: () => {},
      loginWithBackend: async () => handle(backendUser!),
    } as never),
    openClient: async () =>
      ({
        db: { onAuthChanged: () => () => {} } as never,
        session: null,
        shutdown: async () => {},
      }) as JazzClient,
    // A stand-in for the browser's backend watch.
    decorate: (owned) => ({
      async reauthenticate() {
        if (backendUser === null) await owned.logout();
        else await owned.loginWithBackend();
      },
    }),
  });
  mocks.factory.mockResolvedValue(session);
  let auth!: UseJazzSession;
  const SignedOut = defineComponent({
    setup() {
      auth = useJazzSession();
      return () => h("p", "SIGN IN");
    },
  });
  const node = document.createElement("div");
  const app = createApp({
    render: () =>
      h(
        JazzProvider,
        { config: { initial: "signed-out" } },
        {
          default: () => h("p", "PRIVATE"),
          signedOut: () => h(SignedOut),
          loading: () => h("p", "WAIT"),
        },
      ),
  });
  app.mount(node);
  await flush();
  expect(node.textContent).toBe("SIGN IN");
  backendUser = "a";
  await auth.refresh();
  await flush();
  expect(node.textContent).toBe("PRIVATE");
  expect(auth.snapshot.value.user).toMatchObject({ accountId: "a", identity: { subject: "a" } });
  backendUser = null;
  await auth.refresh();
  await flush();
  expect(node.textContent).toBe("SIGN IN");
  app.unmount();
});
