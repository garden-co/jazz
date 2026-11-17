import { describe, expect, it, vi } from "vitest";
import { mount, unmount, tick, createRawSnippet, getAllContexts } from "svelte";
import JazzProvider from "./JazzProvider.svelte";
import { ownJazzSession, startJazzSession, type JazzSession } from "../session/jazz-session.js";
import { createJazzSessionOwner, replaceJazzSessionClient } from "../session/state.js";
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
      linkWithBackend: async () => {
        events.push("link");
        return next;
      },
    },
    old,
  );
  const clients: JazzClient[] = [];
  const core = await createJazzSessionOwner({
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
  const session = ownJazzSession({}, async () => core, { start: false });
  await startJazzSession(session as JazzSession<unknown>);
  return { core, session, clients };
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
    const { core } = await setup(events);
    const factory = await import("../session/create-jazz-session.js");
    const create = vi
      .spyOn(factory, "openBrowserSessionCore")
      .mockRejectedValueOnce(new Error("startup failed"))
      .mockResolvedValueOnce(core);
    const target = document.createElement("div");
    const component = mount(JazzProvider, {
      target,
      props: {
        config: { scope: "test", serverUrl: "http://localhost:1", initial: "device" },
        children: createRawSnippet(() => ({ render: () => "<p>Ready</p>" })),
        error: createRawSnippet(() => ({
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
    const component = mount(JazzProvider, {
      target,
      props: {
        session,
        children: createRawSnippet(() => ({
          render: () => "<p>Ready</p>",
          setup: () => () => {
            events.push("detach");
          },
        })),
        loading: createRawSnippet(() => ({ render: () => "<p>Waiting</p>" })),
      },
    });
    await settle();
    expect(target.textContent).toBe("Ready");
    await session.linkDeviceAccount();
    await settle();
    expect(events.slice(0, 3)).toEqual(["detach", "shutdown:old", "link"]);
    expect(target.textContent).toBe("Ready");
    await unmount(component);
    await settle();
    expect(clients[1]!.shutdown).not.toHaveBeenCalled();
    await session.close();
    target.remove();
  });

  it("keeps the app mounted when the session upgrades its client in place (#5459)", async () => {
    const account = {
      id: "guest",
      identity: { issuer: "test", subject: "guest" },
    } as AccountHandle;
    const accounts = new AccountManager<JWTAuth>(
      {
        createLocalFirst: () => account,
        restoreLocalFirst: () => account,
        logout: () => {},
        linkWithBackend: async () => account,
      },
      account,
    );
    let localFirst = false;
    const client = attachSubscriptionStore(
      {
        db: { onAuthChanged: () => () => {}, isLocalFirst: () => localFirst },
        session: null,
        shutdown: vi.fn(async () => {}),
      },
      {} as SubscriptionStore,
    ) as unknown as JazzClient;
    const core = await createJazzSessionOwner({ accounts, openClient: async () => client });
    const session = ownJazzSession({}, async () => core, { start: false });
    await startJazzSession(session as JazzSession<unknown>);
    let mounts = 0;
    let unmounts = 0;
    const target = document.createElement("div");
    const component = mount(JazzProvider, {
      target,
      props: {
        session,
        children: createRawSnippet(() => ({
          render: () => "<p>App</p>",
          setup: () => {
            mounts++;
            return () => {
              unmounts++;
            };
          },
        })),
      },
    });
    await settle();
    expect(target.textContent).toBe("App");
    expect(session.getSnapshot()).toMatchObject({ client, localFirst: false });

    // The thin client turns local-first in place: same client, new snapshot.
    await replaceJazzSessionClient(
      core,
      async () => client,
      () => {
        localFirst = true;
      },
    );
    await settle();
    expect(session.getSnapshot()).toMatchObject({ client, localFirst: true });
    expect(target.textContent).toBe("App");
    expect({ mounts, unmounts }).toEqual({ mounts: 1, unmounts: 0 });

    await unmount(component);
    await session.close();
  });

  it("releases the transition barrier when the provider unmounts", async () => {
    const { session } = await setup([]);
    const target = document.createElement("div");
    const component = mount(JazzProvider, {
      target,
      props: {
        session,
        children: createRawSnippet(() => ({ render: () => "<p>Ready</p>" })),
      },
    });
    await settle();
    const transition = session.signOut();
    await unmount(component);
    await transition;
    expect(session.getSnapshot().status).toBe("signed-out");
    await session.close();
  });

  it("renders nothing once the session is closed, or the closed snippet", async () => {
    for (const closed of [
      undefined,
      createRawSnippet(() => ({ render: () => "<p>Session closed</p>" })),
    ]) {
      const { session } = await setup([]);
      const target = document.createElement("div");
      const component = mount(JazzProvider, {
        target,
        props: {
          session,
          children: createRawSnippet(() => ({ render: () => "<p>Ready</p>" })),
          ...(closed ? { closed } : {}),
        },
      });
      await settle();
      expect(target.textContent).toBe("Ready");
      await session.close();
      await settle();
      expect(session.getSnapshot().status).toBe("closed");
      // Before: the loading view, forever.
      expect(target.textContent).toBe(closed ? "Session closed" : "");
      await unmount(component);
    }
  });

  it("store subscriptions expose snapshots and bound commands", async () => {
    const { session } = await setup([]);
    const state = sessionState(session);
    const statuses: string[] = [];
    const unsubscribe = state.subscribe((value) => statuses.push(value.status));
    const { signOut } = state;
    await signOut();
    expect(statuses[0]).toBe("ready");
    expect(statuses).toContain("transitioning");
    expect(statuses.at(-1)).toBe("signed-out");
    unsubscribe();
    await session.close();
  });
});
