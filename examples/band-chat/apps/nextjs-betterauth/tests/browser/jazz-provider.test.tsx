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
  selected: undefined as { id: string; identity: { subject: string } } | undefined,
}));

vi.mock("@/src/lib/auth-client", () => ({
  authClient: { useSession: () => ({ data: controls.session, isPending: false }) },
  getJwtFromBetterAuth: async () => "token",
}));

vi.mock("@/src/lib/accounts", () => ({
  prepareAccounts: async () => ({
    getLoggedIn: () => controls.selected,
    logout: () => {
      controls.events.push("logout");
      controls.selected = undefined;
    },
    loginJWT: async () => {
      const subject = controls.session!.user.id;
      controls.events.push(`login:${subject}`);
      controls.selected = { id: subject, identity: { subject } };
    },
    registerJWT: vi.fn(),
  }),
}));

vi.mock("jazz-tools/react", () => ({
  createJazzClient: async ({
    account,
  }: {
    account: { id: string; identity: { subject: string } };
  }) => ({
    account,
    shutdown: async ({ waitForSync }: { waitForSync?: boolean } = {}) =>
      controls.events.push(`shutdown:${account.identity.subject}:${waitForSync}`),
  }),
  JazzClientProvider: ({
    children,
    client,
  }: {
    children: React.ReactNode;
    client: { account: { id: string } };
  }) => <section data-account={client.account.id}>{children}</section>,
}));

import { JazzProvider } from "../../components/jazz-provider";

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
  controls.selected = undefined;
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
    "logout",
    "login:principal-b",
  ]);
  await act(async () => root.unmount());
  element.remove();
});
