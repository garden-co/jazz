import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, expect, it, vi } from "vitest";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const controls = vi.hoisted(() => ({
  events: [] as string[],
  session: { session: { id: "session-a" }, user: { id: "principal-a" } } as {
    session: { id: string };
    user: { id: string };
  } | null,
  signOutError: undefined as string | undefined,
  loginError: undefined as Error | undefined,
  shutdownError: undefined as Error | undefined,
}));

vi.mock("@/src/lib/auth-client", () => ({
  authClient: {
    useSession: () => ({ data: controls.session, isPending: false }),
    signOut: async () => {
      controls.events.push("provider-signout");
      return controls.signOutError ? { error: { message: controls.signOutError } } : {};
    },
  },
  getJwtFromBetterAuth: async () => "token",
}));

vi.mock("jazz-tools/react", async () => {
  const core = await import("../../../../../../packages/jazz-tools/src/react-core/session.js");
  const { createJazzSessionOwner } =
    await import("../../../../../../packages/jazz-tools/src/session/state.js");
  const { AccountManager } =
    await import("../../../../../../packages/jazz-tools/src/accounts/state.js");
  return {
    useJazzSessionOwner: (config: object) =>
      core.useJazzSessionOwner(config, async () => {
        const select = async () => {
          const subject = controls.session!.user.id;
          controls.events.push(`login:${subject}`);
          if (controls.loginError) throw controls.loginError;
          return { id: subject, identity: { subject } } as never;
        };
        const accounts = new AccountManager({
          createLocalFirst: () => {
            throw new Error("unexpected local-first enrollment");
          },
          restoreLocalFirst: () => {
            throw new Error("unexpected restore");
          },
          loginJWT: select,
          registerJWT: select,
          linkJWT: select,
          logout: () => {
            controls.events.push("logout");
          },
        });
        return createJazzSessionOwner({
          accounts,
          openClient: async (account) => ({
            db: {
              getAuthState: () => ({ authMode: "external" as const, session: null }),
              onAuthChanged: () => () => {},
            },
            shutdown: async ({ waitForSync }: { waitForSync?: boolean } = {}) => {
              controls.events.push(`shutdown:${account.identity.subject}:${waitForSync}`);
              if (waitForSync && controls.shutdownError) throw controls.shutdownError;
            },
          }),
        });
      }),
    JazzSessionProvider: ({
      children,
      session,
      fallback,
    }: React.PropsWithChildren<{
      session: import("jazz-tools/react").JazzSession<any>;
      fallback: React.ReactNode;
    }>) => (
      <core.JazzSessionProvider session={session} fallback={fallback}>
        <section data-account={session.getSnapshot().account?.id}>{children}</section>
      </core.JazzSessionProvider>
    ),
  };
});

import { JazzProvider, useBandChatLifecycle } from "../../components/jazz-provider";

async function waitFor(check: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (check()) return;
    await act(async () => new Promise((resolve) => setTimeout(resolve, 10)));
  }
  throw new Error("timed out waiting for lifecycle transition");
}

afterEach(() => {
  controls.events.splice(0);
  controls.session = { session: { id: "session-a" }, user: { id: "principal-a" } };
  controls.signOutError = undefined;
  controls.loginError = undefined;
  controls.shutdownError = undefined;
});

it("does not render A for B and syncs A before replacing its account", async () => {
  const element = document.createElement("div");
  document.body.append(element);
  const root = createRoot(element);
  await act(async () => root.render(<JazzProvider>rooms</JazzProvider>));
  await waitFor(() => element.querySelector("[data-account='principal-a']") !== null);

  controls.session = { session: { id: "session-b" }, user: { id: "principal-b" } };
  await act(async () => root.render(<JazzProvider>rooms</JazzProvider>));
  expect(element.querySelector("[data-account='principal-a']")).toBeNull();
  await waitFor(() => element.querySelector("[data-account='principal-b']") !== null);
  expect(controls.events).toEqual([
    "login:principal-a",
    "shutdown:principal-a:true",
    "login:principal-b",
  ]);
  await act(async () => root.unmount());
  await new Promise((resolve) => setTimeout(resolve, 10));
  element.remove();
});

it("revalidates a new Better Auth session even when the principal is unchanged", async () => {
  const element = document.createElement("div");
  const root = createRoot(element);
  await act(async () => root.render(<JazzProvider>rooms</JazzProvider>));
  await waitFor(() => element.textContent === "rooms");
  controls.session = { session: { id: "new-session-a" }, user: { id: "principal-a" } };
  await act(async () => root.render(<JazzProvider>rooms</JazzProvider>));
  await waitFor(
    () => controls.events.filter((event) => event === "login:principal-a").length === 2,
  );
  expect(controls.events).toEqual([
    "login:principal-a",
    "shutdown:principal-a:true",
    "login:principal-a",
  ]);
  await act(async () => root.unmount());
  await new Promise((resolve) => setTimeout(resolve, 10));
});

it("keeps old-account data hidden if replacement authentication fails and permits retry", async () => {
  const element = document.createElement("div");
  const root = createRoot(element);
  await act(async () => root.render(<JazzProvider>private rooms</JazzProvider>));
  await waitFor(() => element.textContent === "private rooms");
  controls.loginError = new Error("replacement login rejected");
  controls.session = { session: { id: "session-b" }, user: { id: "principal-b" } };
  await act(async () => root.render(<JazzProvider>private rooms</JazzProvider>));
  await waitFor(() => element.textContent!.includes("replacement login rejected"));
  expect(element.textContent).not.toContain("private rooms");
  controls.loginError = undefined;
  await act(async () => element.querySelector("button")!.click());
  await waitFor(() => element.textContent === "private rooms");
  expect(element.querySelector("[data-account='principal-b']")).not.toBeNull();
  await act(async () => root.unmount());
  await new Promise((resolve) => setTimeout(resolve, 10));
});

it("does not revoke provider credentials when Jazz cannot finish graceful shutdown", async () => {
  const element = document.createElement("div");
  const root = createRoot(element);
  function SignOut() {
    const actions = useBandChatLifecycle();
    return <button onClick={() => void actions.signOut()}>Sign out</button>;
  }
  await act(async () =>
    root.render(
      <JazzProvider>
        <SignOut />
      </JazzProvider>,
    ),
  );
  await waitFor(() => element.querySelector("button") !== null);
  controls.shutdownError = new Error("sync unavailable");
  await act(async () => element.querySelector("button")!.click());
  await waitFor(() => element.textContent!.includes("sync unavailable"));
  expect(controls.events).not.toContain("provider-signout");
  expect(controls.events).not.toContain("logout");
  controls.shutdownError = undefined;
  await act(async () => root.unmount());
  await new Promise((resolve) => setTimeout(resolve, 10));
});

it("keeps failed provider signout visible after Jazz logout and offers explicit recovery", async () => {
  const element = document.createElement("div");
  const root = createRoot(element);
  function SignOut() {
    const actions = useBandChatLifecycle();
    return <button onClick={() => void actions.signOut()}>Sign out</button>;
  }
  await act(async () =>
    root.render(
      <JazzProvider>
        <SignOut />
      </JazzProvider>,
    ),
  );
  await waitFor(() => element.querySelector("button") !== null);
  controls.signOutError = "provider signout unavailable";
  await act(async () => element.querySelector("button")!.click());
  await waitFor(() => element.textContent!.includes("provider signout unavailable"));
  expect(controls.events).toEqual([
    "login:principal-a",
    "shutdown:principal-a:true",
    "logout",
    "provider-signout",
  ]);
  expect(element.querySelector("button")!.textContent).toBe("Retry");
  controls.signOutError = undefined;
  await act(async () => element.querySelector("button")!.click());
  await waitFor(() => element.querySelector("button")!.textContent === "Sign out");
  expect(controls.events.at(-1)).toBe("login:principal-a");
  await act(async () => root.unmount());
  await new Promise((resolve) => setTimeout(resolve, 10));
});
