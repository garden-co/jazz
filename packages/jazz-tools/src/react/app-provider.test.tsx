import React from "react";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { makeFakeAccount, makeFakeClient } from "../react-core/test-utils.js";
import { useJazzAuth } from "../react-core/app.js";
const host = vi.hoisted(() => ({ create: vi.fn() }));
vi.mock("../session/create-jazz-session.js", () => ({ createJazzSession: host.create }));
import { JazzProvider } from "./provider.js";
afterEach(() => {
  cleanup();
  host.create.mockReset();
});
it("supplies local-first by default and built-in loading and safe retry UI", async () => {
  host.create.mockRejectedValue(new Error("unavailable"));
  const view = render(
    <JazzProvider appId="app" serverUrl="https://sync.example.test" autoAttachDevTools={false}>
      <p>data</p>
    </JazzProvider>,
  );
  expect(view.getByRole("status").textContent).toBe("Loading…");
  await waitFor(() => expect(view.getByRole("alert")).toBeDefined());
  expect(host.create).toHaveBeenCalledWith(expect.objectContaining({ initial: "local-first" }));
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
      registerJWT: async () => account,
      loginJWT: async () => account,
      loginOrRegisterJWT: async () => account,
      linkJWT: async () => account,
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
      appId="app"
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
