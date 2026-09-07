import { onCleanup } from "solid-js";
import { render } from "solid-js/web";
import { describe, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { JazzSessionProvider, useJazzSession } from "./session.js";
import { useJazzClient } from "./provider.js";
import { attachSubscriptionStore, getSubscriptionStore } from "../subscription-store-internal.js";
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
      linkJWT: async () => account,
    },
    account,
  );
  const client: JazzClient = attachSubscriptionStore(
    {
      db: {
        getAuthState: () => ({ session: null }),
        onAuthChanged: () => () => {
          events.push("auth-cleanup");
        },
      } as never,
      session: null,
      shutdown: vi.fn(async () => {
        events.push("shutdown");
      }),
    },
    {} as never,
  );
  const session = await createJazzSessionOwner({ accounts, openClient: async () => client });
  return { events, client, session };
}
const flush = async () => {
  for (let i = 0; i < 12; i++) await Promise.resolve();
};
describe("Solid session provider", () => {
  it("disposes descendants before shutdown and provides fallback commands", async () => {
    const { session, client, events } = await fixture();
    const Child = () => {
      const active = useJazzClient();
      expect(active.db).toBe(client.db);
      expect(getSubscriptionStore(active)).toBe(getSubscriptionStore(client));
      expect(useJazzSession().snapshot().account).toBe(account);
      onCleanup(() => {
        events.push("child-cleanup");
      });
      return <p>active</p>;
    };
    const Fallback = () => {
      const state = useJazzSession();
      expect(state.logout).toBe(session.logout);
      return <p>{state.snapshot().status}</p>;
    };
    const node = document.createElement("div");
    const dispose = render(
      () => (
        <JazzSessionProvider session={session} fallback={<Fallback />} autoAttachDevTools={false}>
          <Child />
        </JazzSessionProvider>
      ),
      node,
    );
    await flush();
    expect(node.textContent).toBe("active");
    await session.logout();
    await flush();
    expect(node.textContent).toBe("signed-out");
    expect(events).toContain("child-cleanup");
    expect(events).toContain("auth-cleanup");
    expect(events.indexOf("child-cleanup")).toBeLessThan(events.indexOf("shutdown"));
    expect(events.indexOf("auth-cleanup")).toBeLessThan(events.indexOf("shutdown"));
    dispose();
    expect(session.getSnapshot().status).toBe("signed-out");
    await session.close();
  });
  it("unmount releases consumer without closing caller session", async () => {
    const { session, client } = await fixture();
    const dispose = render(
      () => (
        <JazzSessionProvider session={session} autoAttachDevTools={false}>
          <p>active</p>
        </JazzSessionProvider>
      ),
      document.createElement("div"),
    );
    await flush();
    const logout = session.logout();
    dispose();
    expect(client.shutdown).not.toHaveBeenCalled();
    await logout;
    expect(session.getSnapshot().status).toBe("signed-out");
    await session.close();
  });
});
