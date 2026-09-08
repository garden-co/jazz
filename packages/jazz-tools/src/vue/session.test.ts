// @vitest-environment jsdom
import { createApp, defineComponent, h, nextTick, onUnmounted } from "vue";
import { describe, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { JazzSessionProvider, useJazzSession } from "./session.js";
import { useJazzClient } from "./provider.js";
import type { JazzClient } from "../web/create-jazz-client.js";
vi.mock("../web/create-jazz-client.js", () => ({ createJazzClient: vi.fn() }));
vi.mock("../dev-tools/auto-attach.js", () => ({ startInspectorOnce: vi.fn() }));
const account = Object.freeze({
  id: "local",
  identity: { issuer: "test", subject: "local" },
}) as AccountHandle;
async function fixture() {
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
    db: {
      onAuthChanged: () => () => {
        events.push("auth-cleanup");
      },
    } as never,
    session: null,
    shutdown: vi.fn(async () => {
      events.push("shutdown");
    }),
  };
  const session = await createJazzSessionOwner({ accounts, openClient: async () => client });
  return { events, client, session };
}
const flush = async () => {
  for (let i = 0; i < 8; i++) {
    await Promise.resolve();
    await nextTick();
  }
};
describe("Vue session provider", () => {
  it("detaches active children before shutdown and supplies fallback commands", async () => {
    const { events, client, session } = await fixture();
    let fallbackStatus: string | undefined;
    const Child = defineComponent({
      setup() {
        expect(useJazzClient()).toBe(client);
        expect(useJazzSession().snapshot.value.account).toBe(account);
        onUnmounted(() => {
          events.push("child-cleanup");
        });
        return () => h("p", "active");
      },
    });
    const Fallback = defineComponent({
      setup() {
        const state = useJazzSession();
        expect(state.logout).toBe(session.logout);
        return () => {
          fallbackStatus = state.snapshot.value.status;
          return h("p", fallbackStatus);
        };
      },
    });
    const node = document.createElement("div");
    const app = createApp({
      render: () =>
        h(
          JazzSessionProvider,
          { session, autoAttachDevTools: false },
          { default: () => h(Child), fallback: () => h(Fallback) },
        ),
    });
    app.mount(node);
    await flush();
    expect(node.textContent).toBe("active");
    await session.logout();
    await flush();
    expect(fallbackStatus).toBe("signed-out");
    expect(events).toContain("child-cleanup");
    expect(events).toContain("auth-cleanup");
    expect(events.indexOf("child-cleanup")).toBeLessThan(events.indexOf("shutdown"));
    expect(events.indexOf("auth-cleanup")).toBeLessThan(events.indexOf("shutdown"));
    app.unmount();
    expect(session.getSnapshot().status).toBe("signed-out");
    await session.close();
  });
  it("unmount releases an in-flight detach without closing the caller session", async () => {
    const { session, client } = await fixture();
    const app = createApp({
      render: () =>
        h(
          JazzSessionProvider,
          { session, autoAttachDevTools: false },
          { default: () => h("p", "active") },
        ),
    });
    app.mount(document.createElement("div"));
    await flush();
    const logout = session.logout();
    app.unmount();
    expect(client.shutdown).not.toHaveBeenCalled();
    await logout;
    expect(session.getSnapshot().status).toBe("signed-out");
    await session.close();
  });
});
