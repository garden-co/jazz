import React from "react";
import { cleanup, fireEvent, render, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { createJazzSessionOwner, type JazzSession } from "../session/state.js";
import { makeFakeAccount, makeFakeClient } from "./test-utils.js";

import { ConfiguredJazzAppProvider } from "./app.js";
function TestAppProvider(
  props: Pick<
    React.ComponentProps<typeof ConfiguredJazzAppProvider>,
    "children" | "signedOut" | "loading" | "error"
  >,
) {
  return (
    <ConfiguredJazzAppProvider
      {...props}
      config={{}}
      createJazzSession={async () => fixture.session!}
    />
  );
}
const fixture = vi.hoisted(() => ({
  session: undefined as JazzSession<ReturnType<typeof makeFakeClient>> | undefined,
  stored: null as { token: string; username: string } | null,
  token: null as string | null,
  signOutCalls: 0,
  failSignOut: false,
  mounts: 0,
}));
vi.mock("jazz-tools/react", async () => ({
  ...(await import("../react/index.js")),
  useJazzSessionOwner: () => ({ session: fixture.session }),
  JazzProvider: ({
    children,
    signedOut,
    loading,
    error,
  }: React.PropsWithChildren<{
    signedOut: React.ReactNode;
    loading: React.ReactNode;
    error: React.ReactNode;
  }>) => (
    <TestAppProvider children={children} signedOut={signedOut} loading={loading} error={error} />
  ),
}));
vi.mock("../../../../examples/auth-simple-chat/src/api.js", () => ({
  requestSignIn: async () => ({ token: "provider-token", username: "member" }),
  requestSignUp: async () => ({ token: "provider-token", username: "member" }),
}));
vi.mock("../../../../examples/auth-simple-chat/src/auth-storage.js", () => ({
  readStoredAuthSession: () => fixture.stored,
  writeStoredAuthSession: (_app: string, value: typeof fixture.stored) => {
    fixture.stored = value;
  },
  clearStoredAuthSession: () => {
    fixture.signOutCalls++;
    if (fixture.failSignOut) {
      fixture.failSignOut = false;
      throw new Error("provider signout unavailable");
    }
    fixture.stored = null;
  },
}));
vi.mock("../../../../examples/auth-betterauth-chat/src/lib/auth-client", () => ({
  getJwtFromBetterAuth: async () => fixture.token,
  authClient: {
    signIn: { email: async () => ({}) },
    signUp: {
      email: async () => {
        fixture.token = "provider-token";
        return {};
      },
    },
    signOut: async () => {
      fixture.signOutCalls++;
      if (fixture.failSignOut) {
        fixture.failSignOut = false;
        return { error: { message: "provider signout unavailable" } };
      }
      fixture.token = null;
      return {};
    },
  },
}));
function AuthCard({
  onSignUp,
  onSignOut,
}: {
  onSignUp(email: string, password: string): Promise<void>;
  onSignOut(): Promise<void>;
}) {
  React.useEffect(() => {
    fixture.mounts++;
  }, []);
  return (
    <>
      <button onClick={() => void onSignUp("member@example.com", "password").catch(() => {})}>
        Sign up
      </button>
      <button onClick={() => void onSignOut().catch(() => {})}>Sign out</button>
    </>
  );
}
vi.mock("../../../../examples/auth-simple-chat/src/AuthCard.js", () => ({ AuthCard }));
vi.mock("../../../../examples/auth-betterauth-chat/src/AuthCard", () => ({ AuthCard }));
vi.mock("../../../../examples/auth-simple-chat/src/ChatPanel.js", () => ({
  ChatPanel: () => null,
}));
vi.mock("../../../../examples/auth-betterauth-chat/src/ChatPanel", () => ({
  ChatPanel: () => null,
}));
// Exercise the actual app entry points without pulling their separately checked
// source trees into the SDK TypeScript project's rootDir.
const customEntry = new URL("../../../../examples/auth-simple-chat/src/App.tsx", import.meta.url)
  .pathname;
const betterAuthEntry = new URL(
  "../../../../examples/auth-betterauth-chat/app/page.tsx",
  import.meta.url,
).pathname;
const { App: CustomJWTApp }: { App: React.ComponentType } = await import(customEntry);
const { default: BetterAuthApp }: { default: React.ComponentType } = await import(betterAuthEntry);

beforeEach(() => {
  fixture.stored = null;
  fixture.token = null;
  fixture.signOutCalls = 0;
  fixture.failSignOut = false;
  fixture.mounts = 0;
});
afterEach(async () => {
  cleanup();
  await fixture.session?.close();
  fixture.session = undefined;
});
async function setup() {
  const account = makeFakeAccount();
  const enroll = vi.fn(async () => account);
  const link = vi.fn(async () => account);
  const createLocal = vi.fn(() => account);
  const accounts = new AccountManager(
    {
      createLocalFirst: createLocal,
      restoreLocalFirst: () => account,
      registerJWT: enroll,
      loginJWT: enroll,
      loginOrRegisterJWT: enroll,
      linkJWT: link,
      logout() {},
    },
    account,
  );
  fixture.session = await createJazzSessionOwner({
    accounts,
    async openClient() {
      return makeFakeClient({ authMode: "local-first", userId: "guest", claims: {} });
    },
  });
  return { enroll, link, createLocal };
}
for (const [name, App] of [
  ["custom JWT", CustomJWTApp],
  ["Better Auth", BetterAuthApp],
] as const) {
  describe(`${name} actual app with JazzProvider`, () => {
    it("retries failed signup linking after ready/fallback remount without enrolling", async () => {
      const { enroll, link } = await setup();
      link.mockRejectedValueOnce(new Error("link unavailable"));
      const view = render(<App />);
      fireEvent.click(await view.findByText("Sign up"));
      await waitFor(() => expect(view.getByText("link unavailable")).toBeDefined());
      fireEvent.click(view.getByText("Retry"));
      await waitFor(() => expect(link).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(view.queryByRole("alert")).toBeNull());
      expect(fixture.mounts).toBeGreaterThanOrEqual(2);
      expect(enroll).not.toHaveBeenCalled();
    });
    it("retries provider signout after Jazz detaches without logging in again", async () => {
      const { enroll, link, createLocal } = await setup();
      const view = render(<App />);
      fireEvent.click(await view.findByText("Sign up"));
      await waitFor(() => expect(link).toHaveBeenCalledTimes(1));
      await waitFor(() => expect(fixture.session?.getSnapshot().status).toBe("ready"));
      fixture.failSignOut = true;
      fireEvent.click(view.getByText("Sign out"));
      await waitFor(() => expect(view.getByText("provider signout unavailable")).toBeDefined());
      expect(fixture.session?.getSnapshot().status).toBe("signed-out");
      expect(view.queryByText("Sign out")).toBeNull();
      fireEvent.click(view.getByText("Retry"));
      await waitFor(() => expect(fixture.signOutCalls).toBe(2));
      await waitFor(() => expect(view.getByText("Sign out")).toBeDefined());
      expect(createLocal).toHaveBeenCalledTimes(1);
      expect(enroll).not.toHaveBeenCalled();
    });
  });
}
