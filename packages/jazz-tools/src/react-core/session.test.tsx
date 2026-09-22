import React, { StrictMode, useEffect, useSyncExternalStore } from "react";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { renderToString } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import {
  JazzSessionProvider,
  ConfiguredJazzSessionProvider,
  useJazzSession,
  useJazzSessionOwner,
  type UseJazzSessionResult,
} from "./session.js";
import { useJazzClient } from "./provider.js";
import { makeFakeAccount, makeFakeClient } from "./test-utils.js";

afterEach(cleanup);
async function setup() {
  const account = makeFakeAccount();
  const enrollment = {
    createLocalFirst: () => makeFakeAccount(),
    restoreLocalFirst: () => makeFakeAccount(),
    registerJWT: vi.fn(async () => makeFakeAccount()),
    loginJWT: vi.fn(async () => makeFakeAccount()),
    loginOrRegisterJWT: vi.fn(async () => makeFakeAccount()),
    linkJWT: vi.fn(async () => makeFakeAccount()),
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
  const session = await createJazzSessionOwner({ accounts, openClient });
  return { session, clients, enrollment, openClient };
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
        <JazzSessionProvider session={session} fallback={<Status />}>
          <QueryConsumer />
        </JazzSessionProvider>
      </StrictMode>,
    );
    expect(subscribed).toBe(1);
    let operation!: Promise<void>;
    act(() => {
      operation = latest.logout();
    });
    await act(async () => {
      await operation;
    });
    expect(view.container.textContent).toBe("signed-out:");
    expect(enrollment.logout).toHaveBeenCalledOnce();
    const { createLocalFirst } = latest;
    act(() => {
      operation = createLocalFirst();
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
      <JazzSessionProvider session={session}>
        <Status />
      </JazzSessionProvider>,
    );
    let operation!: Promise<void>;
    act(() => {
      operation = session.logout();
      view.unmount();
    });
    await operation;
    expect(session.getSnapshot().status).toBe("signed-out");
    expect(clients[0]!.shutdown).toHaveBeenCalledOnce();
    await session.createLocalFirst();
    expect(session.getSnapshot().status).toBe("ready");
    await session.close();
  });

  it("does not dispose an externally owned ready client on unmount", async () => {
    const { session, clients } = await setup();
    const view = render(
      <StrictMode>
        <JazzSessionProvider session={session}>
          <Status />
        </JazzSessionProvider>
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
    enrollment.linkJWT.mockRejectedValueOnce(new Error("link rejected"));
    const view = render(
      <JazzSessionProvider session={session} fallback={<Status />}>
        <Status />
      </JazzSessionProvider>,
    );
    let operation!: Promise<void>;
    act(() => {
      operation = latest.linkJWT("token");
    });
    await act(async () => {
      await expect(operation).rejects.toThrow("link rejected");
    });
    expect(view.container.textContent).toBe("ready:link rejected");
    act(() => {
      operation = latest.logout();
    });
    await act(async () => {
      await operation;
    });
    expect(view.container.textContent).toBe("signed-out:");
    view.unmount();
    await session.close();
  });

  it("owns one factory result in StrictMode and closes it on real unmount", async () => {
    const { session, clients } = await setup();
    const factory = vi.fn(async () => session);
    const view = render(
      <StrictMode>
        <ConfiguredJazzSessionProvider
          config={{}}
          createJazzSession={factory}
          fallback={<Status />}
        >
          <Status />
        </ConfiguredJazzSessionProvider>
      </StrictMode>,
    );
    await waitFor(() => expect(view.container.textContent).toBe("ready:"));
    expect(factory).toHaveBeenCalledOnce();
    expect(clients[0]!.shutdown).not.toHaveBeenCalled();
    view.unmount();
    await waitFor(() => expect(session.getSnapshot().status).toBe("closed"));
    expect(clients[0]!.shutdown).toHaveBeenCalledOnce();
  });

  it("exposes initialization errors and retries from the fallback", async () => {
    const { session } = await setup();
    const factory = vi
      .fn()
      .mockRejectedValueOnce(new Error("startup failed"))
      .mockResolvedValueOnce(session);
    const view = render(
      <ConfiguredJazzSessionProvider config={{}} createJazzSession={factory} fallback={<Status />}>
        <Status />
      </ConfiguredJazzSessionProvider>,
    );
    await waitFor(() => expect(view.container.textContent).toBe("error:startup failed"));
    await act(async () => {
      await latest.retry();
    });
    expect(factory).toHaveBeenCalledTimes(2);
    expect(view.container.textContent).toBe("ready:");
    view.unmount();
    await waitFor(() => expect(session.getSnapshot().status).toBe("closed"));
  });

  it("disposes a factory result that arrives after unmount", async () => {
    const { session, clients } = await setup();
    let resolve!: (value: typeof session) => void;
    const factory = () =>
      new Promise<typeof session>((yes) => {
        resolve = yes;
      });
    const view = render(
      <ConfiguredJazzSessionProvider config={{}} createJazzSession={factory}>
        <Status />
      </ConfiguredJazzSessionProvider>,
    );
    await act(async () => {
      await Promise.resolve();
    });
    view.unmount();
    await new Promise((yes) => setTimeout(yes, 5));
    resolve(session);
    await waitFor(() => expect(clients[0]!.shutdown).toHaveBeenCalledOnce());
    expect(session.getSnapshot().status).toBe("closed");
  });

  it("does not initialize during SSR and makes the fallback hook available", () => {
    const factory = vi.fn();
    expect(
      renderToString(
        <ConfiguredJazzSessionProvider
          config={{}}
          createJazzSession={factory}
          fallback={<Status />}
        >
          <Status />
        </ConfiguredJazzSessionProvider>,
      ),
    ).toContain("transitioning");
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
      <JazzSessionProvider session={session} fallback={<Status />}>
        <Child suspended={suspended} />
      </JazzSessionProvider>
    </React.Suspense>
  );
  const view = render(tree(false));
  view.rerender(tree(true));
  expect(view.container.textContent).toContain("Suspended");
  view.rerender(tree(false));
  expect(view.container.textContent).toBe("ready:");
  let operation!: Promise<void>;
  act(() => {
    operation = session.logout();
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
    <JazzSessionProvider session={first.session}>
      <Status />
    </JazzSessionProvider>,
  );
  view.rerender(
    <JazzSessionProvider session={second.session}>
      <Status />
    </JazzSessionProvider>,
  );
  await first.session.close();
  expect(first.clients[0]!.shutdown).toHaveBeenCalledOnce();
  expect(second.clients[0]!.shutdown).not.toHaveBeenCalled();
  view.unmount();
  await second.session.close();
  expect(second.clients[0]!.shutdown).toHaveBeenCalledOnce();
});

it("the owner hook keeps auth coordination mounted across ready and fallback views", async () => {
  const { session } = await setup();
  const factory = vi.fn(async () => session);
  const mounted = vi.fn();
  const unmounted = vi.fn();
  function Coordinator() {
    const owner = useJazzSessionOwner({}, factory);
    useEffect(() => {
      mounted();
      return unmounted;
    }, []);
    return owner.session ? (
      <JazzSessionProvider session={owner.session} fallback={<Status />}>
        <Status />
      </JazzSessionProvider>
    ) : null;
  }
  const view = render(<Coordinator />);
  await waitFor(() => expect(view.container.textContent).toBe("ready:"));
  let operation!: Promise<void>;
  act(() => {
    operation = session.logout();
  });
  await act(async () => {
    await operation;
  });
  expect(view.container.textContent).toBe("signed-out:");
  expect(mounted).toHaveBeenCalledOnce();
  expect(unmounted).not.toHaveBeenCalled();
  expect(factory).toHaveBeenCalledOnce();
  view.unmount();
  await waitFor(() => expect(session.getSnapshot().status).toBe("closed"));
  expect(unmounted).toHaveBeenCalledOnce();
});
