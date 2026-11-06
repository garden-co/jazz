import { act, cleanup, render, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { makeFakeAccount, makeFakeClient } from "../react-core/test-utils.js";
import { useJazzAuth } from "../react-core/app.js";
import { useJazzSession } from "../react-core/session.js";
const host = vi.hoisted(() => ({ create: vi.fn() }));
vi.mock("../session/create-jazz-session.js", () => ({ createJazzClient: host.create }));
import { JazzProvider } from "./provider.js";
afterEach(() => {
  cleanup();
  host.create.mockReset();
});
it("starts signed out by default and supplies built-in loading and safe retry UI", async () => {
  host.create.mockRejectedValue(new Error("unavailable"));
  const view = render(
    <JazzProvider scope="app" serverUrl="https://sync.example.test" autoAttachDevTools={false}>
      <p>data</p>
    </JazzProvider>,
  );
  expect(view.getByRole("status").textContent).toBe("Loading…");
  await waitFor(() => expect(view.getByRole("alert")).toBeDefined());
  expect(host.create.mock.calls[0][0].initial).toBeUndefined();
  await act(async () => {
    view.getByRole("button", { name: "Try again" }).click();
  });
  expect(host.create).toHaveBeenCalledTimes(2);
});
it("provides logout in the data tree without an application auth context", async () => {
  const account = makeFakeAccount();
  const accounts = new AccountManager(
    {
      createLocalFirst: () => account,
      restoreLocalFirst: () => account,
      loginWithBackend: async () => account,
      logout() {},
    },
    account,
  );
  host.create.mockImplementation(() =>
    createJazzSessionOwner({
      accounts,
      openClient: async () =>
        makeFakeClient({ authMode: "local-first", userId: "test", claims: {} }),
    }),
  );
  function Data() {
    const { logout } = useJazzAuth();
    return <button onClick={() => void logout()}>Sign out</button>;
  }
  function SignedOut() {
    const { status } = useJazzAuth();
    return <p>{status}</p>;
  }
  const view = render(
    <JazzProvider
      scope="app"
      serverUrl="https://sync.example.test"
      autoAttachDevTools={false}
      signedOut={<SignedOut />}
    >
      <Data />
    </JazzProvider>,
  );
  await waitFor(() => expect(view.getByText("Sign out")).toBeDefined());
  await act(async () => {
    view.getByText("Sign out").click();
  });
  await waitFor(() => expect(view.getByText("signed-out")).toBeDefined());
});

it("starts signed out with `initial`, then follows the backend's user on reauthenticate", async () => {
  const account = makeFakeAccount();
  const cloned: unknown[] = [];
  const shutdowns: unknown[] = [];
  const backend = { signedIn: false };
  host.create.mockImplementation((config) =>
    createJazzSessionOwner({
      accounts: new AccountManager({
        createLocalFirst: () => account,
        restoreLocalFirst: () => account,
        loginWithBackend: async () => account,
        logout() {},
      }),
      initial: config.initial,
      openClient: async () => {
        // Options cross the worker boundary only once an account is enrolled.
        cloned.push(structuredClone(config));
        const client = makeFakeClient({ authMode: "external", userId: "one", claims: {} });
        const shutdown = client.shutdown.bind(client);
        return Object.assign(client, {
          shutdown: async (options?: { waitForSync?: boolean }) => {
            shutdowns.push(options);
            await shutdown();
          },
        });
      },
      decorate: (session) => ({
        async reauthenticate() {
          if (backend.signedIn) await session.loginWithBackend();
          else await session.logout();
        },
      }),
    }),
  );
  let auth!: ReturnType<typeof useJazzAuth>;
  function SignedOut() {
    auth = useJazzAuth();
    return <p>signed out</p>;
  }
  function Data() {
    auth = useJazzAuth();
    return <p>private data</p>;
  }
  const view = render(
    <JazzProvider
      scope="app"
      serverUrl="https://sync.example.test"
      initial="signed-out"
      autoAttachDevTools={false}
      signedOut={<SignedOut />}
    >
      <Data />
    </JazzProvider>,
  );
  await waitFor(() => expect(view.getByText("signed out")).toBeDefined());
  expect(cloned).toEqual([]);
  backend.signedIn = true;
  await act(async () => {
    await auth.reauthenticate();
  });
  await waitFor(() => expect(view.getByText("private data")).toBeDefined());
  expect(cloned).toEqual([
    { scope: "app", serverUrl: "https://sync.example.test", initial: "signed-out" },
  ]);
  backend.signedIn = false;
  await act(async () => {
    await auth.reauthenticate();
  });
  await waitFor(() => expect(view.getByText("signed out")).toBeDefined());
  expect(shutdowns).toEqual([{ waitForSync: true }]);
});
it("refuses a `fallback` prop by name instead of handing it to the client config", () => {
  const props = { fallback: <p>Loading…</p> } as object;
  vi.spyOn(console, "error").mockImplementation(() => {});
  expect(() =>
    render(
      <JazzProvider
        scope="app"
        serverUrl="https://sync.example.test"
        autoAttachDevTools={false}
        {...props}
      >
        <p>data</p>
      </JazzProvider>,
    ),
  ).toThrow(/has no `fallback` prop.*`loading`|`loading`.*no `fallback` prop/s);
  expect(host.create).not.toHaveBeenCalled();
});
it("points useJazzSession under <JazzProvider> to useJazzAuth", async () => {
  host.create.mockReturnValue(new Promise(() => {}));
  function Status() {
    useJazzSession();
    return null;
  }
  vi.spyOn(console, "error").mockImplementation(() => {});
  expect(() =>
    render(
      <JazzProvider
        scope="app"
        serverUrl="https://sync.example.test"
        autoAttachDevTools={false}
        loading={<Status />}
      >
        <p>data</p>
      </JazzProvider>,
    ),
  ).toThrow(/Under <JazzProvider>, use useJazzAuth\(\)/);
});
