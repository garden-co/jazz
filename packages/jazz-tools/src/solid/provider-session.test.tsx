import { onCleanup } from "solid-js";
import { render } from "solid-js/web";
import { afterEach, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import { JazzProvider, useJazzSession } from "./session.js";
import { useJazzClient } from "./provider.js";
const factory = vi.hoisted(() => vi.fn());
vi.mock("../session/create-jazz-session.js", () => ({ openBrowserSessionCore: factory }));
vi.mock("../web/create-jazz-client.js", () => ({ openJazzClient: vi.fn() }));
const flush = async () => {
  for (let i = 0; i < 40; i++) await Promise.resolve();
};
let dispose: (() => void) | undefined;
afterEach(async () => {
  dispose?.();
  dispose = undefined;
  await flush();
  factory.mockReset();
  document.body.replaceChildren();
});
// Who the app's backend names (null: no one).
const backend: { user: string | null } = { user: null };
async function setup() {
  const events: string[] = [];
  const handle = (id: string) =>
    ({ id, identity: { issuer: "test", subject: id } }) as AccountHandle;
  let failLogout = false;
  const accounts = new AccountManager({
    createLocalFirst: () => handle("local"),
    logout: () => {
      if (failLogout) throw new Error("logout failed");
    },
    // The backend names whoever the test's provider says is signed in.
    loginWithBackend: async () => handle(backend.user!),
  });
  const owner = await createJazzSessionOwner({
    accounts,
    openClient: async (account) =>
      attachSubscriptionStore(
        {
          db: { getAuthState: () => ({ session: null }), onAuthChanged: () => () => {} },
          session: null,
          shutdown: async () => {
            events.push(`shutdown:${account.id}`);
          },
        },
        {} as never,
      ) as any,
    // A stand-in for the browser's backend watch.
    decorate: (session) => ({
      async reauthenticate() {
        if (backend.user === null) await session.logout();
        else if (session.getSnapshot().account?.identity.subject !== backend.user) {
          if (session.getSnapshot().status === "ready") await session.logout();
          await session.loginWithBackend();
        }
      },
    }),
  });
  factory.mockResolvedValue(owner);
  return {
    owner,
    events,
    failLogout: () => {
      failLogout = true;
    },
  };
}
it("retries startup through the default UI and exposes unified auth to children", async () => {
  const { owner, events, failLogout } = await setup();
  await owner.createLocalFirst();
  factory.mockRejectedValueOnce(new Error("startup unavailable"));
  let auth: ReturnType<typeof useJazzSession>;
  const Child = () => {
    auth = useJazzSession();
    expect(auth.snapshot().status).toBe("ready");
    expect(useJazzClient().db).toBe(owner.getSnapshot().client!.db);
    onCleanup(() => events.push("detached"));
    return <p>PRIVATE</p>;
  };
  const node = document.createElement("div");
  document.body.append(node);
  dispose = render(
    () => (
      <JazzProvider config={{ scope: "test" }}>
        <Child />
      </JazzProvider>
    ),
    node,
  );
  await flush();
  expect(node.textContent).toContain("startup unavailable");
  expect(node.textContent).not.toContain("PRIVATE");
  node.querySelector("button")!.click();
  await flush();
  expect(factory).toHaveBeenCalledTimes(2);
  expect(factory.mock.calls[1][0].initial).toBeUndefined();
  expect(node.textContent).toContain("PRIVATE");
  await auth!.signOut();
  expect(events).toEqual(["detached", "shutdown:local"]);
  expect(node.textContent).not.toContain("PRIVATE");
  const { createDeviceAccount } = auth!;
  await createDeviceAccount();
  await flush();
  expect(node.textContent).toContain("PRIVATE");
  failLogout();
  await expect(auth!.signOut()).rejects.toThrow("logout failed");
  await flush();
  expect(auth!.snapshot().error?.message).toBe("logout failed");
});
it("follows the backend on refresh and releases children before shutdown", async () => {
  const { owner, events } = await setup();
  backend.user = null;
  let mounts = 0;
  const Child = () => {
    mounts++;
    onCleanup(() => events.push("detached"));
    return <p>PRIVATE</p>;
  };
  let auth!: ReturnType<typeof useJazzSession>;
  const SignedOut = () => {
    auth = useJazzSession();
    return <p>{auth.snapshot().status}</p>;
  };
  const node = document.createElement("div");
  document.body.append(node);
  dispose = render(
    () => (
      <JazzProvider
        config={{ scope: "test", initial: "signed-out" }}
        signedOut={<SignedOut />}
        loading={<p>WAIT</p>}
      >
        <Child />
      </JazzProvider>
    ),
    node,
  );
  await flush();
  expect(node.textContent).toBe("signed-out");
  expect(factory).toHaveBeenCalledWith(expect.objectContaining({ initial: "signed-out" }));
  expect(mounts).toBe(0);
  backend.user = "a";
  await auth.refresh();
  await flush();
  expect(node.textContent).toBe("PRIVATE");
  expect(auth.snapshot().user).toMatchObject({
    accountId: "a",
    identity: { issuer: "test", subject: "a" },
  });
  backend.user = "b";
  await auth.refresh();
  await flush();
  expect(owner.getSnapshot().account?.identity.subject).toBe("b");
  expect(node.textContent).toBe("PRIVATE");
  expect(events.indexOf("detached")).toBeLessThan(events.indexOf("shutdown:a"));
  dispose();
  dispose = undefined;
  await flush();
  expect(owner.getSnapshot().status).toBe("closed");
  expect(events.slice(-2)).toEqual(["detached", "shutdown:b"]);
});
it("refuses the removed autoAttachDevTools prop instead of passing it to the client", () => {
  const node = document.createElement("div");
  document.body.append(node);
  const props = { autoAttachDevTools: false } as Record<string, unknown>;
  expect(() =>
    render(
      () => (
        <JazzProvider config={{ scope: "test" }} {...props}>
          <p>ready</p>
        </JazzProvider>
      ),
      node,
    ),
  ).toThrow(/JazzProvider takes no autoAttachDevTools.*mountInspector/);
  expect(factory).not.toHaveBeenCalled();
});
