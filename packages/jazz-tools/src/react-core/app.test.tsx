import React, { StrictMode, Suspense, useEffect, useSyncExternalStore } from "react";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { renderToString } from "react-dom/server";
import { afterEach, expect, it, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { betterAuth, jwtAuth, type JazzAuth } from "../session/app.js";
import { ConfiguredJazzAppProvider, useJazzAuth, type JazzAuthState } from "./app.js";
import { useJazzClient } from "./provider.js";
import { makeFakeAccount, makeFakeClient } from "./test-utils.js";

afterEach(cleanup);
function fixture() {
  const enrollment = {
    createLocalFirst: () => makeFakeAccount(),
    restoreLocalFirst: () => makeFakeAccount(),
    registerJWT: vi.fn(async () => makeFakeAccount()),
    loginJWT: vi.fn(async () => makeFakeAccount()),
    loginOrRegisterJWT: vi.fn(async () => makeFakeAccount()),
    linkJWT: vi.fn(async () => makeFakeAccount()),
    logout: vi.fn(),
  };
  const client = makeFakeClient({ authMode: "local-first", userId: "test", claims: {} });
  client.shutdown = vi.fn(async () => {});
  const factory = vi.fn(() =>
    createJazzSessionOwner({
      accounts: new AccountManager(enrollment, makeFakeAccount()),
      openClient: async () => client,
    }),
  );
  return { factory, client, enrollment };
}
let current: JazzAuthState;
function Status() {
  current = useJazzAuth();
  return <p>{current.status}</p>;
}
it("does not create a host during SSR or an abandoned Suspense render", async () => {
  const { factory } = fixture();
  const tree = (
    <ConfiguredJazzAppProvider config={{}} createJazzSession={factory} loading={<Status />}>
      <p>data</p>
    </ConfiguredJazzAppProvider>
  );
  expect(renderToString(tree)).toContain("starting");
  expect(factory).not.toHaveBeenCalled();
  const pending = new Promise<void>(() => {});
  function Suspend(): never {
    throw pending;
  }
  const view = render(
    <Suspense fallback="suspended">
      {tree}
      <Suspend />
    </Suspense>,
  );
  expect(view.container.textContent).toBe("suspended");
  expect(factory).not.toHaveBeenCalled();
});
it("retains one StrictMode owner and detaches passive queries before logout", async () => {
  const { factory, client, enrollment } = fixture();
  let subscriptions = 0;
  let effects = 0;
  const subscribe = () => {
    subscriptions++;
    return () => {
      subscriptions--;
    };
  };
  function Query() {
    useJazzClient();
    useSyncExternalStore(subscribe, () => 1);
    useEffect(() => {
      effects++;
      return () => {
        effects--;
      };
    }, []);
    return <Status />;
  }
  vi.mocked(client.shutdown).mockImplementation(async () => {
    expect(subscriptions).toBe(0);
    expect(effects).toBe(0);
  });
  const view = render(
    <StrictMode>
      <ConfiguredJazzAppProvider
        config={{}}
        createJazzSession={factory}
        loading={<Status />}
        signedOut={<Status />}
        error={<Status />}
      >
        <Query />
      </ConfiguredJazzAppProvider>
    </StrictMode>,
  );
  await waitFor(() => expect(view.container.textContent).toBe("ready"));
  expect(factory).toHaveBeenCalledOnce();
  let logout!: Promise<void>;
  act(() => {
    logout = current.logout();
  });
  await act(async () => {
    await logout;
  });
  expect(view.container.textContent).toBe("signed-out");
  expect(enrollment.logout).toHaveBeenCalledOnce();
  view.unmount();
  await new Promise((resolve) => setTimeout(resolve, 10));
});
it("keeps auth fallback mounted to acknowledge provider-driven detach", async () => {
  const { factory, client } = fixture();
  const getToken = async () => "token";
  const makeTree = (key: string | null) => (
    <ConfiguredJazzAppProvider
      config={{}}
      createJazzSession={factory}
      auth={jwtAuth({ key, getToken, logout: async () => {} })}
      loading={<Status />}
      signedOut={<Status />}
      error={<Status />}
    >
      <Status />
    </ConfiguredJazzAppProvider>
  );
  const view = render(makeTree("user-a"));
  await waitFor(() => expect(view.container.textContent).toBe("ready"));
  view.rerender(makeTree(null));
  await waitFor(() => expect(view.container.textContent).toBe("signed-out"));
  expect(client.shutdown).toHaveBeenCalled();
});
it("exposes safe retry in custom initialization error UI", async () => {
  const { factory } = fixture();
  const create = vi.fn().mockRejectedValueOnce(new Error("offline")).mockImplementation(factory);
  const view = render(
    <ConfiguredJazzAppProvider
      config={{}}
      createJazzSession={create}
      loading={<Status />}
      error={(state) => (
        <button onClick={() => void state.retry()}>Retry {state.error?.message}</button>
      )}
    >
      <Status />
    </ConfiguredJazzAppProvider>,
  );
  await waitFor(() => expect(view.container.textContent).toBe("Retry offline"));
  await act(async () => {
    view.getByRole("button").click();
  });
  await waitFor(() => expect(view.container.textContent).toBe("ready"));
});
it("retains its mounted owner while Suspense hides and restores the data tree", async () => {
  const { factory, client } = fixture();
  const pending = new Promise<void>(() => {});
  function Data({ hidden }: { hidden: boolean }) {
    useJazzClient();
    if (hidden) throw pending;
    return <Status />;
  }
  const tree = (hidden: boolean) => (
    <Suspense fallback={<p>suspended</p>}>
      <ConfiguredJazzAppProvider config={{}} createJazzSession={factory} loading={<Status />}>
        <Data hidden={hidden} />
      </ConfiguredJazzAppProvider>
    </Suspense>
  );
  const view = render(tree(false));
  await waitFor(() => expect(view.getByText("ready")).toBeDefined());
  view.rerender(tree(true));
  await new Promise((resolve) => setTimeout(resolve, 10));
  expect(view.getByText("suspended")).toBeDefined();
  expect(client.shutdown).not.toHaveBeenCalled();
  view.rerender(tree(false));
  expect(view.getByText("ready")).toBeDefined();
  expect(factory).toHaveBeenCalledOnce();
  view.unmount();
  await waitFor(() => expect(client.shutdown).toHaveBeenCalledOnce());
});

it("new pending auth never commits ready children", async () => {
  const { factory } = fixture();
  const commits: string[] = [];
  function Data({ pending }: { pending: boolean }) {
    const state = useJazzAuth();
    React.useLayoutEffect(() => {
      commits.push(`${pending}:${state.status}`);
    });
    return <Status />;
  }
  const tree = (pending: boolean) => (
    <ConfiguredJazzAppProvider
      config={{}}
      createJazzSession={factory}
      auth={jwtAuth({
        key: "a",
        isPending: pending,
        getToken: async () => "a",
        logout: async () => {},
      })}
      loading={<Status />}
    >
      <Data pending={pending} />
    </ConfiguredJazzAppProvider>
  );
  const view = render(tree(false));
  await waitFor(() => expect(view.container.textContent).toBe("ready"));
  commits.length = 0;
  view.rerender(tree(true));
  expect(commits).not.toContain("true:ready");
});

it("waits for passive queries while outer Suspense cannot commit", async () => {
  const { factory, client } = fixture();
  let active = 0;
  const pending = new Promise<void>(() => {});
  function Query() {
    useJazzClient();
    useEffect(() => {
      active++;
      return () => {
        active--;
      };
    }, []);
    return <Status />;
  }
  function Block({ blocked }: { blocked: boolean }): null {
    if (blocked) throw pending;
    return null;
  }
  const tree = (blocked: boolean) => (
    <Suspense fallback="suspended">
      <ConfiguredJazzAppProvider config={{}} createJazzSession={factory} loading={<Status />}>
        <Query />
      </ConfiguredJazzAppProvider>
      <Block blocked={blocked} />
    </Suspense>
  );
  const view = render(tree(false));
  await waitFor(() => expect(view.getByText("ready")).toBeDefined());
  view.rerender(tree(true));
  expect(active).toBe(1);
  let logout!: Promise<void>;
  act(() => {
    logout = current.logout();
  });
  await act(async () => {
    for (let i = 0; i < 60; i++) await Promise.resolve();
  });
  expect(active).toBe(1);
  expect(client.shutdown).not.toHaveBeenCalled();
  view.unmount();
  await logout;
  expect(active).toBe(0);
});

it.each([
  ["session key", { key: "b" }],
  ["signed out", { key: null }],
  ["provider error", { key: "a", error: new Error("provider unavailable") }],
] as const)(
  "masks old hook identity and data before committing a changed %s",
  async (_name, next) => {
    const { factory } = fixture();
    const commits: string[] = [];
    const fallbackCommits: string[] = [];
    function Data({ revision }: { revision: number }) {
      const state = useJazzAuth();
      React.useLayoutEffect(() => {
        commits.push(`${revision}:${state.status}`);
      });
      return <Status />;
    }
    function Fallback({ revision }: { revision: number }) {
      const state = useJazzAuth();
      React.useLayoutEffect(() => {
        fallbackCommits.push(`${revision}:${state.status}:${!!state.client}:${!!state.account}`);
      });
      return <Status />;
    }
    const auth = (options: typeof next | { key: string }) =>
      jwtAuth({ ...options, getToken: async () => "token", logout: async () => {} });
    const tree = (revision: number, descriptor: JazzAuth) => (
      <ConfiguredJazzAppProvider
        config={{}}
        createJazzSession={factory}
        auth={descriptor}
        loading={<Fallback revision={revision} />}
        error={<Fallback revision={revision} />}
        signedOut={<Fallback revision={revision} />}
      >
        <Data revision={revision} />
      </ConfiguredJazzAppProvider>
    );
    const view = render(tree(0, auth({ key: "a" })));
    await waitFor(() => expect(view.container.textContent).toBe("ready"));
    commits.length = 0;
    fallbackCommits.length = 0;
    view.rerender(tree(1, auth(next)));
    expect(commits).not.toContain("1:ready");
    expect(fallbackCommits[0]).toBe(`1:${"error" in next ? "error" : "transitioning"}:false:false`);
  },
);
it("masks ready children when replacing a BetterAuth client", async () => {
  const { factory } = fixture();
  const commits: string[] = [];
  const makeClient = (isPending: boolean) => ({
    $store: {
      atoms: {
        session: {
          get: () => ({ data: { session: { id: "session" }, user: { id: "user" } }, isPending }),
          subscribe: () => () => {},
        },
      },
    },
    $fetch: async () => ({ data: { token: "token" } }),
    signOut: async () => ({}),
  });
  function Data({ revision }: { revision: number }) {
    const state = useJazzAuth();
    React.useLayoutEffect(() => {
      commits.push(`${revision}:${state.status}`);
    });
    return <Status />;
  }
  const tree = (revision: number, auth: JazzAuth) => (
    <ConfiguredJazzAppProvider
      config={{}}
      createJazzSession={factory}
      auth={auth}
      loading={<Status />}
    >
      <Data revision={revision} />
    </ConfiguredJazzAppProvider>
  );
  const view = render(tree(0, betterAuth(makeClient(false))));
  await waitFor(() => expect(view.container.textContent).toBe("ready"));
  commits.length = 0;
  view.rerender(tree(1, betterAuth(makeClient(true))));
  expect(commits).not.toContain("1:ready");
  expect(view.container.textContent).toBe("transitioning");
});
