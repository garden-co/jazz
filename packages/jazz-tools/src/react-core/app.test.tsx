import React, { StrictMode, Suspense, useEffect, useSyncExternalStore } from "react";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { renderToString } from "react-dom/server";
import { afterEach, expect, it, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { jwtAuth } from "../session/app.js";
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
