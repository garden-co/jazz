import { describe, expect, it, vi } from "vitest";
import { mount, unmount, tick, createRawSnippet, getAllContexts } from "svelte";
import JazzSessionProvider from "./JazzSessionProvider.svelte";
import { createJazzSessionOwner } from "../session/state.js";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import type { JWTAuth } from "../accounts/enrollment.js";
import { attachSubscriptionStore, type SubscriptionStore } from "../subscription-store-internal.js";
import type { JazzClient } from "./create-jazz-client.js";
import SessionStatus from "../../tests/svelte/SessionStatus.svelte";
import { sessionState } from "./session-state.js";

async function setup(events: string[]) {
  const handle = (id: string) =>
    ({ id, identity: { issuer: "test", subject: id } }) as AccountHandle;
  const old = handle("old"),
    next = handle("next");
  const accounts = new AccountManager<JWTAuth>(
    {
      createLocalFirst: () => old,
      restoreLocalFirst: () => old,
      logout: () => {},
      registerJWT: async () => next,
      loginJWT: async () => next,
      loginOrRegisterJWT: async () => next,
      linkJWT: async () => {
        events.push("link");
        return next;
      },
    },
    old,
  );
  const clients: JazzClient[] = [];
  const session = await createJazzSessionOwner({
    accounts,
    openClient: async (account) => {
      const client = attachSubscriptionStore(
        {
          db: { onAuthChanged: () => () => {} },
          session: null,
          shutdown: vi.fn(async () => {
            events.push(`shutdown:${account.id}`);
          }),
        },
        {} as SubscriptionStore,
      ) as unknown as JazzClient;
      clients.push(client);
      return client;
    },
  });
  return { session, clients };
}
const settle = async () => {
  for (let i = 0; i < 8; i++) {
    await tick();
    await Promise.resolve();
  }
};

describe("Svelte Jazz session", () => {
  it("provides startup status and retry to a configured fallback", async () => {
    const events: string[] = [];
    const { session } = await setup(events);
    const factory = await import("../session/create-jazz-session.js");
    const create = vi
      .spyOn(factory, "createJazzSession")
      .mockRejectedValueOnce(new Error("startup failed"))
      .mockResolvedValueOnce(session);
    const target = document.createElement("div");
    const component = mount(JazzSessionProvider, {
      target,
      props: {
        config: { appId: "test", serverUrl: "http://localhost:1", initial: "local-first" },
        autoAttachDevTools: false,
        children: createRawSnippet(() => ({ render: () => "<p>Ready</p>" })),
        fallback: createRawSnippet(() => ({
          render: () => "<div></div>",
          setup: (element) => {
            const status = mount(SessionStatus, { target: element, context: getAllContexts() });
            return () => {
              void unmount(status);
            };
          },
        })),
      },
    });
    await settle();
    expect(target.textContent).toContain("error");
    (target.querySelector("button") as HTMLButtonElement).click();
    await settle();
    expect(create).toHaveBeenCalledTimes(2);
    expect(target.textContent).toBe("Ready");
    await unmount(component);
    await settle();
    create.mockRestore();
  });
  it("detaches the rendered subtree before syncing and linking", async () => {
    const events: string[] = [];
    const { session, clients } = await setup(events);
    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(JazzSessionProvider, {
      target,
      props: {
        session,
        autoAttachDevTools: false,
        children: createRawSnippet(() => ({
          render: () => "<p>Ready</p>",
          setup: () => () => {
            events.push("detach");
          },
        })),
        fallback: createRawSnippet(() => ({ render: () => "<p>Waiting</p>" })),
      },
    });
    await settle();
    expect(target.textContent).toBe("Ready");
    await session.linkJWT("token");
    await settle();
    expect(events.slice(0, 3)).toEqual(["detach", "shutdown:old", "link"]);
    expect(target.textContent).toBe("Ready");
    await unmount(component);
    await settle();
    expect(clients[1]!.shutdown).not.toHaveBeenCalled();
    await session.close();
    target.remove();
  });

  it("releases the transition barrier when the provider unmounts", async () => {
    const { session } = await setup([]);
    const target = document.createElement("div");
    const component = mount(JazzSessionProvider, {
      target,
      props: {
        session,
        autoAttachDevTools: false,
        children: createRawSnippet(() => ({ render: () => "<p>Ready</p>" })),
      },
    });
    await settle();
    const transition = session.logout();
    await unmount(component);
    await transition;
    expect(session.getSnapshot().status).toBe("signed-out");
    await session.close();
  });

  it("store subscriptions expose snapshots and bound commands", async () => {
    const { session } = await setup([]);
    const state = sessionState(session);
    const statuses: string[] = [];
    const unsubscribe = state.subscribe((value) => statuses.push(value.status));
    const { logout } = state;
    await logout();
    expect(statuses[0]).toBe("ready");
    expect(statuses).toContain("transitioning");
    expect(statuses.at(-1)).toBe("signed-out");
    unsubscribe();
    await session.close();
  });
});
