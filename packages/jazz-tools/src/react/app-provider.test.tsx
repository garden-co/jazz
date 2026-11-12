import { createElement } from "react";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { makeFakeAccount, makeFakeClient } from "../react-core/test-utils.js";
import { useJazzSession } from "../react-core/session.js";
const host = vi.hoisted(() => ({ create: vi.fn() }));
vi.mock("../session/create-jazz-session.js", () => ({ openBrowserSessionCore: host.create }));
import { JazzProvider } from "./provider.js";
afterEach(() => {
  cleanup();
  host.create.mockReset();
});
it("starts signed out by default and supplies built-in loading and safe retry UI", async () => {
  host.create.mockRejectedValue(new Error("unavailable"));
  const view = render(
    <JazzProvider config={{ scope: "app", serverUrl: "https://sync.example.test" }}>
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
it("provides signOut in the data tree without an application auth context", async () => {
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
    const { signOut } = useJazzSession();
    return <button onClick={() => void signOut()}>Sign out</button>;
  }
  function SignedOut() {
    const { status } = useJazzSession();
    return <p>{status}</p>;
  }
  const view = render(
    <JazzProvider
      config={{ scope: "app", serverUrl: "https://sync.example.test" }}
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

it("starts signed out with `initial`, then follows the backend's user on refresh", async () => {
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
      initial: config.initial === "device" ? "local-first" : config.initial,
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
  let auth!: ReturnType<typeof useJazzSession>;
  function SignedOut() {
    auth = useJazzSession();
    return <p>signed out</p>;
  }
  function Data() {
    auth = useJazzSession();
    return <p>private data</p>;
  }
  const view = render(
    <JazzProvider
      config={{ scope: "app", serverUrl: "https://sync.example.test", initial: "signed-out" }}
      signedOut={<SignedOut />}
    >
      <Data />
    </JazzProvider>,
  );
  await waitFor(() => expect(view.getByText("signed out")).toBeDefined());
  expect(cloned).toEqual([]);
  backend.signedIn = true;
  await act(async () => {
    await auth.refresh();
  });
  await waitFor(() => expect(view.getByText("private data")).toBeDefined());
  expect(cloned).toEqual([
    { scope: "app", serverUrl: "https://sync.example.test", initial: "signed-out" },
  ]);
  backend.signedIn = false;
  await act(async () => {
    await auth.refresh();
  });
  await waitFor(() => expect(view.getByText("signed out")).toBeDefined());
  expect(shutdowns).toEqual([{ waitForSync: true }]);
});
it("refuses the removed provider shapes by name instead of handing them to the client config", () => {
  vi.spyOn(console, "error").mockImplementation(() => {});
  const fallback = { fallback: <p>Loading…</p> } as object;
  expect(() =>
    render(
      <JazzProvider config={{ serverUrl: "https://sync.example.test" }} {...fallback}>
        <p>data</p>
      </JazzProvider>,
    ),
  ).toThrow(/has no `fallback`.*`loading`/s);
  const spread = { scope: "app", serverUrl: "https://sync.example.test" } as object;
  expect(() => render(createElement(JazzProvider, spread as never))).toThrow(
    /takes its configuration as `config=/,
  );
  const withAccount = { config: { serverUrl: "/jazz", account: {} } } as object;
  expect(() => render(createElement(JazzProvider, withAccount as never))).toThrow(
    /takes no `account`.*JazzClientProvider/s,
  );
  expect(host.create).not.toHaveBeenCalled();
});
it("offers the session in its loading view", async () => {
  host.create.mockReturnValue(new Promise(() => {}));
  let seen: string | undefined;
  function Status() {
    seen = useJazzSession().status;
    return null;
  }
  render(
    <JazzProvider config={{ serverUrl: "https://sync.example.test" }} loading={<Status />}>
      <p>data</p>
    </JazzProvider>,
  );
  await waitFor(() => expect(seen).toBe("starting"));
});
