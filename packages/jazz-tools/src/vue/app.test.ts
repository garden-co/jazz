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
import { JazzProvider, useJazzAuth, type UseJazzAuth } from "./app.js";
import { GracefulShutdownSyncError } from "../runtime/graceful-shutdown-error.js";
import type { JazzClient } from "./create-jazz-client.js";
const mocks = vi.hoisted(() => ({ factory: vi.fn() }));
vi.mock("../session/create-jazz-session.js", () => ({ createJazzSession: mocks.factory }));
vi.mock("../dev-tools/auto-attach.js", () => ({ startInspectorOnce: vi.fn() }));
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
      registerJWT: async () => account,
      loginJWT: async () => account,
      loginOrRegisterJWT: async () => account,
      linkJWT: async () => account,
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
describe("Vue ergonomic app", () => {
  it("is inert in SSR and supplies default loading", async () => {
    mocks.factory.mockReset();
    const app = createSSRApp({ render: () => h(JazzProvider, { appId: "test" }) });
    expect(await renderToString(app)).toContain("Loading Jazz");
    expect(mocks.factory).not.toHaveBeenCalled();
  });
  it("retries startup failures with the default error button and defaults to local-first", async () => {
    const { session } = await fixture();
    mocks.factory
      .mockReset()
      .mockRejectedValueOnce(new Error("startup failed"))
      .mockResolvedValue(session);
    const node = document.createElement("div");
    const app = createApp({
      render: () =>
        h(
          JazzProvider,
          { appId: "test", autoAttachDevTools: false },
          { default: () => h("p", "active") },
        ),
    });
    app.mount(node);
    await flush();
    expect(node.textContent).toContain("startup failed");
    node.querySelector("button")!.click();
    await flush();
    expect(node.textContent).toBe("active");
    expect(mocks.factory.mock.calls[0][0].initial).toBe("local-first");
    app.unmount();
    await flush();
  });
  it("updates inline auth in place and keeps captured runtime configuration", async () => {
    const { session } = await fixture();
    mocks.factory.mockReset().mockResolvedValue(session);
    const version = shallowRef(0);
    const getToken = vi.fn(async () => "token");
    const logout = vi.fn();
    const node = document.createElement("div");
    const app = createApp({
      render: () =>
        h(JazzProvider, {
          appId: `test-${version.value}`,
          autoAttachDevTools: false,
          auth: { kind: "jwt", key: null, isPending: true, getToken, logout },
        }),
    });
    app.mount(node);
    await flush();
    version.value++;
    await flush();
    expect(mocks.factory).toHaveBeenCalledTimes(1);
    expect(mocks.factory.mock.calls[0][0].appId).toBe("test-0");
    expect(mocks.factory.mock.calls[0][0].initial).toBeUndefined();
    expect(getToken).not.toHaveBeenCalled();
    expect(node.textContent).toContain("Loading Jazz");
    app.unmount();
    await flush();
  });
  it.each(["unknown", "graceful"] as const)(
    "detaches consumers before %s logout failure and retries",
    async (failure) => {
      const { session, events } = await fixture(failure);
      mocks.factory.mockReset().mockResolvedValue(session);
      let auth!: UseJazzAuth;
      const Child = defineComponent({
        setup() {
          auth = useJazzAuth();
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
            { appId: "test", autoAttachDevTools: false },
            { default: () => h(Child), signedOut: () => h("p", "signed out") },
          ),
      });
      app.mount(node);
      await flush();
      await expect(auth.logout()).resolves.toBeUndefined();
      expect(auth.snapshot.value.status).toBe("error");
      const errorMessage = failure === "graceful" ? "could not synchronize" : "flush failed";
      expect(auth.snapshot.value.error?.message).toContain(errorMessage);
      await flush();
      expect(events.indexOf("child-cleanup")).toBeLessThan(events.indexOf("shutdown"));
      expect(node.textContent).toContain(errorMessage);
      await auth.retry();
      await flush();
      expect(node.textContent).toBe("signed out");
      app.unmount();
      await flush();
    },
  );
});
