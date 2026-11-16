// @vitest-environment jsdom
import { createApp, defineComponent, h, nextTick, onUnmounted } from "vue";
import { describe, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { JazzProvider, useJazzSession } from "./session.js";
import { ownJazzSession, startJazzSession, type JazzSession } from "../session/jazz-session.js";
import { JazzClientProvider, useJazzClient } from "./provider.js";
import type { JazzClient } from "../web/create-jazz-client.js";
vi.mock("../web/create-jazz-client.js", () => ({ openJazzClient: vi.fn() }));
const account = Object.freeze({
  id: "local",
  identity: { issuer: "test", subject: "local" },
}) as AccountHandle;
async function fixture() {
  const events: string[] = [];
  const accounts = new AccountManager(
    {
      createLocalFirst: () => account,
      loginWithBackend: async () => account,
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
  const core = await createJazzSessionOwner({ accounts, openClient: async () => client });
  const session = ownJazzSession({}, async () => core, { start: false });
  await startJazzSession(session as JazzSession<unknown>);
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
        expect(useJazzSession().snapshot.value.user?.accountId).toBe(account.id);
        onUnmounted(() => {
          events.push("child-cleanup");
        });
        return () => h("p", "active");
      },
    });
    const Fallback = defineComponent({
      setup() {
        const state = useJazzSession();
        expect(state.signOut).toBe(session.signOut);
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
          JazzProvider,
          { session },
          {
            default: () => h(Child),
            signedOut: () => h(Fallback),
            loading: () => h(Fallback),
          },
        ),
    });
    app.mount(node);
    await flush();
    expect(node.textContent).toBe("active");
    await session.signOut();
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
      render: () => h(JazzProvider, { session }, { default: () => h("p", "active") }),
    });
    app.mount(document.createElement("div"));
    await flush();
    const logout = session.signOut();
    app.unmount();
    expect(client.shutdown).not.toHaveBeenCalled();
    await logout;
    expect(session.getSnapshot().status).toBe("signed-out");
    await session.close();
  });
  it("a nested <JazzClientProvider client> reads its own client, not the session", async () => {
    const { session } = await fixture();
    const inner = {
      db: { onAuthChanged: () => () => {} } as never,
      session: {
        user: "inner-account",
        identity: { issuer: "https://issuer.example", subject: "inner" },
        claims: {},
        authMode: "external",
      },
      shutdown: vi.fn(async () => {}),
    } as unknown as JazzClient;
    let outerUser: string | undefined;
    let nested: ReturnType<typeof useJazzSession> | undefined;
    const Outer = defineComponent({
      setup() {
        outerUser = useJazzSession().snapshot.value.user?.accountId;
        return () => h(JazzClientProvider, { client: inner }, { default: () => h(Inner) });
      },
    });
    const Inner = defineComponent({
      setup() {
        nested = useJazzSession();
        return () => h("p", "inner");
      },
    });
    const app = createApp({
      render: () => h(JazzProvider, { session }, { default: () => h(Outer) }),
    });
    app.mount(document.createElement("div"));
    await flush();
    expect(outerUser).toBe(account.id);
    expect(nested!.snapshot.value.client).toBe(inner);
    expect(nested!.snapshot.value.user?.accountId).toBe("inner-account");
    await expect(nested!.signOut()).rejects.toThrow(/outside a Jazz session/);
    expect(session.getSnapshot().status).toBe("ready");
    app.unmount();
  });
});
