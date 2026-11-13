import React, { StrictMode, useEffect, useSyncExternalStore } from "react";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { renderToString } from "react-dom/server";
import { afterEach, describe, expect, it, onTestFinished, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { JazzSessionProvider, useJazzSession, type UseJazzSessionResult } from "./session.js";
import { ownJazzSession, startJazzSession, type JazzSession } from "../session/jazz-session.js";
import type { SessionCore } from "../session/state.js";
import type { CoreJazzClient } from "./provider.js";
import { useJazzClient } from "./provider.js";
import { makeFakeAccount, makeFakeClient } from "./test-utils.js";

afterEach(cleanup);
async function setup() {
  const account = makeFakeAccount();
  const enrollment = {
    createLocalFirst: () => makeFakeAccount(),
    restoreLocalFirst: () => makeFakeAccount(),
    loginWithBackend: vi.fn(async () => makeFakeAccount()),
    linkWithBackend: vi.fn(async () => makeFakeAccount()),
    logout: vi.fn(),
  };
  const accounts = new AccountManager(enrollment, account);
  const clients: ReturnType<typeof makeFakeClient>[] = [];
  const openClient = vi.fn(async () => {
    const client = makeFakeClient({ authMode: "local-first", userId: "test", claims: {} });
    client.shutdown = vi.fn(async () => {});
    clients.push(client);
    return client;
  });
  const core = await createJazzSessionOwner({ accounts, openClient });
  const session = ownJazzSession({}, async () => core, { start: false });
  await startJazzSession(session as JazzSession<unknown>);
  return { core, session, clients, enrollment, openClient };
}
/** The provider with one view for every status but ready, as the old `fallback`. */
function Provider({
  fallback,
  children,
  ...source
}: { fallback?: React.ReactNode; children: React.ReactNode } & (
  | { session: JazzSession<CoreJazzClient>; config?: never; openCore?: never }
  | {
      config: object;
      openCore: (config: object) => Promise<SessionCore<CoreJazzClient>>;
      session?: never;
    }
)) {
  const noCore = async (): Promise<never> => {
    throw new Error("unused");
  };
  return (
    <JazzSessionProvider
      {...(source.session ? { session: source.session } : { config: source.config! })}
      openCore={source.openCore ?? noCore}
      loading={fallback}
      signedOut={fallback}
      error={fallback}
    >
      {children}
    </JazzSessionProvider>
  );
}
let latest: UseJazzSessionResult;
function Status() {
  latest = useJazzSession();
  return (
    <span>
      {latest.status}:{latest.error?.message}
    </span>
  );
}

describe("JazzSessionProvider", () => {
  it("detaches passive client consumers before shutdown, then exposes signed-out commands", async () => {
    const { session, clients, enrollment } = await setup();
    let subscribed = 0;
    let effectMounted = false;
    const subscribe = () => {
      subscribed++;
      return () => {
        subscribed--;
      };
    };
    function QueryConsumer() {
      useJazzClient();
      useSyncExternalStore(subscribe, () => 1);
      useEffect(() => {
        effectMounted = true;
        return () => {
          effectMounted = false;
        };
      }, []);
      return <Status />;
    }
    vi.mocked(clients[0]!.shutdown).mockImplementation(async () => {
      expect(subscribed).toBe(0);
      expect(effectMounted).toBe(false);
      expect(enrollment.logout).not.toHaveBeenCalled();
    });
    const view = render(
      <StrictMode>
        <Provider session={session} fallback={<Status />}>
          <QueryConsumer />
        </Provider>
      </StrictMode>,
    );
    expect(subscribed).toBe(1);
    let operation!: Promise<void>;
    act(() => {
      operation = latest.signOut();
    });
    await act(async () => {
      await operation;
    });
    expect(view.container.textContent).toBe("signed-out:");
    expect(enrollment.logout).toHaveBeenCalledOnce();
    const { createDeviceAccount } = latest;
    act(() => {
      operation = createDeviceAccount();
    });
    await act(async () => {
      await operation;
    });
    expect(view.container.textContent).toBe("ready:");
    expect(subscribed).toBe(1);
    view.unmount();
    await session.close();
  });

  it("releases an unmounted consumer's pending barrier without closing an external session", async () => {
    const { session, clients } = await setup();
    const view = render(
      <Provider session={session}>
        <Status />
      </Provider>,
    );
    let operation!: Promise<void>;
    act(() => {
      operation = session.signOut();
      view.unmount();
    });
    await operation;
    expect(session.getSnapshot().status).toBe("signed-out");
    expect(clients[0]!.shutdown).toHaveBeenCalledOnce();
    await session.createDeviceAccount();
    expect(session.getSnapshot().status).toBe("ready");
    await session.close();
  });

  it("does not dispose an externally owned ready client on unmount", async () => {
    const { session, clients } = await setup();
    const view = render(
      <StrictMode>
        <Provider session={session}>
          <Status />
        </Provider>
      </StrictMode>,
    );
    view.unmount();
    await Promise.resolve();
    expect(clients[0]!.shutdown).not.toHaveBeenCalled();
    await session.close();
    expect(clients[0]!.shutdown).toHaveBeenCalledOnce();
  });

  it("shows a recoverable transition error and permits a later operation", async () => {
    const { session, enrollment } = await setup();
    enrollment.linkWithBackend.mockRejectedValueOnce(new Error("link rejected"));
    const view = render(
      <Provider session={session} fallback={<Status />}>
        <Status />
      </Provider>,
    );
    let operation!: Promise<void>;
    act(() => {
      operation = latest.linkDeviceAccount();
    });
    await act(async () => {
      await expect(operation).rejects.toThrow("link rejected");
    });
    expect(view.container.textContent).toBe("ready:link rejected");
    act(() => {
      operation = latest.signOut();
    });
    await act(async () => {
      await operation;
    });
    expect(view.container.textContent).toBe("signed-out:");
    view.unmount();
    await session.close();
  });

  it("owns one factory result in StrictMode and closes it on real unmount", async () => {
    const { core, clients } = await setup();
    const factory = vi.fn(async () => core);
    const view = render(
      <StrictMode>
        <Provider config={{}} openCore={factory} fallback={<Status />}>
          <Status />
        </Provider>
      </StrictMode>,
    );
    await waitFor(() => expect(view.container.textContent).toBe("ready:"));
    expect(factory).toHaveBeenCalledOnce();
    expect(clients[0]!.shutdown).not.toHaveBeenCalled();
    view.unmount();
    await waitFor(() => expect(core.getSnapshot().status).toBe("closed"));
    expect(clients[0]!.shutdown).toHaveBeenCalledOnce();
  });

  it("exposes initialization errors and retries from the fallback", async () => {
    // A fallback may render nothing, so the failure is also logged.
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    onTestFinished(() => consoleError.mockRestore());
    const { core } = await setup();
    const factory = vi
      .fn()
      .mockRejectedValueOnce(new Error("startup failed"))
      .mockResolvedValueOnce(core);
    const view = render(
      <Provider config={{}} openCore={factory} fallback={<Status />}>
        <Status />
      </Provider>,
    );
    await waitFor(() => expect(view.container.textContent).toBe("error:startup failed"));
    expect(consoleError).toHaveBeenCalledWith(
      "Jazz could not start:",
      expect.objectContaining({ message: "startup failed" }),
    );
    await act(async () => {
      await latest.retry();
    });
    expect(factory).toHaveBeenCalledTimes(2);
    expect(view.container.textContent).toBe("ready:");
    view.unmount();
    await waitFor(() => expect(core.getSnapshot().status).toBe("closed"));
  });

  it("disposes a factory result that arrives after unmount", async () => {
    const { core, clients } = await setup();
    let resolve!: (value: typeof core) => void;
    const factory = () =>
      new Promise<typeof core>((yes) => {
        resolve = yes;
      });
    const view = render(
      <Provider config={{}} openCore={factory}>
        <Status />
      </Provider>,
    );
    await act(async () => {
      await Promise.resolve();
    });
    view.unmount();
    await new Promise((yes) => setTimeout(yes, 5));
    resolve(core);
    await waitFor(() => expect(clients[0]!.shutdown).toHaveBeenCalledOnce());
    expect(core.getSnapshot().status).toBe("closed");
  });

  it("does not initialize during SSR and makes the fallback hook available", () => {
    const factory = vi.fn();
    expect(
      renderToString(
        <Provider config={{}} openCore={factory} fallback={<Status />}>
          <Status />
        </Provider>,
      ),
    ).toContain("starting");
    expect(factory).not.toHaveBeenCalled();
  });
});

it("Suspense hide and reveal does not leak a detach lease", async () => {
  const { session } = await setup();
  const never = new Promise<void>(() => {});
  function Child({ suspended }: { suspended: boolean }) {
    useJazzClient();
    if (suspended) throw never;
    return <Status />;
  }
  const tree = (suspended: boolean) => (
    <React.Suspense fallback={<p>Suspended</p>}>
      <Provider session={session} fallback={<Status />}>
        <Child suspended={suspended} />
      </Provider>
    </React.Suspense>
  );
  const view = render(tree(false));
  view.rerender(tree(true));
  expect(view.container.textContent).toContain("Suspended");
  view.rerender(tree(false));
  expect(view.container.textContent).toBe("ready:");
  let operation!: Promise<void>;
  act(() => {
    operation = session.signOut();
  });
  await act(async () => {
    for (let i = 0; i < 20; i++) await Promise.resolve();
  });
  await waitFor(() => expect(session.getSnapshot().status).toBe("signed-out"), { timeout: 200 });
  await operation;
  view.unmount();
  await session.close();
});

it("replaces external sessions and releases both views on unmount", async () => {
  const first = await setup();
  const second = await setup();
  const view = render(
    <Provider session={first.session}>
      <Status />
    </Provider>,
  );
  view.rerender(
    <Provider session={second.session}>
      <Status />
    </Provider>,
  );
  await first.session.close();
  expect(first.clients[0]!.shutdown).toHaveBeenCalledOnce();
  expect(second.clients[0]!.shutdown).not.toHaveBeenCalled();
  view.unmount();
  await second.session.close();
  expect(second.clients[0]!.shutdown).toHaveBeenCalledOnce();
});
