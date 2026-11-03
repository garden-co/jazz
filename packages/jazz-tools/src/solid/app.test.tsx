import { onCleanup } from "solid-js";
import { render } from "solid-js/web";
import { afterEach, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import { JazzProvider, useJazzAuth } from "./app.js";
import { useJazzClient } from "./provider.js";
const factory = vi.hoisted(() => vi.fn());
vi.mock("../session/create-jazz-session.js", () => ({ createJazzSession: factory }));
vi.mock("../web/create-jazz-client.js", () => ({ createJazzClient: vi.fn() }));
vi.mock("../dev-tools/auto-attach.js", () => ({ startInspectorOnce: vi.fn() }));
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
  let auth: ReturnType<typeof useJazzAuth>;
  const Child = () => {
    auth = useJazzAuth();
    expect(auth.snapshot().status).toBe("ready");
    expect(useJazzClient().db).toBe(owner.getSnapshot().client!.db);
    onCleanup(() => events.push("detached"));
    return <p>PRIVATE</p>;
  };
  const node = document.createElement("div");
  document.body.append(node);
  dispose = render(
    () => (
      <JazzProvider appId="test" autoAttachDevTools={false}>
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
  expect(factory.mock.calls[1][0].initial).toBe("local-first");
  expect(node.textContent).toContain("PRIVATE");
  await auth!.logout();
  expect(events).toEqual(["detached", "shutdown:local"]);
  expect(node.textContent).not.toContain("PRIVATE");
  const actions = auth!.sessionActions;
  await actions.createLocalFirst();
  await flush();
  expect(node.textContent).toContain("PRIVATE");
  expect(auth!.sessionActions).toBe(actions);
  failLogout();
  await expect(auth!.logout()).resolves.toBeUndefined();
  await flush();
  expect(node.textContent).toContain("logout failed");
});
it("follows the backend on reauthenticate and releases children before shutdown", async () => {
  const { owner, events } = await setup();
  backend.user = null;
  let mounts = 0;
  const Child = () => {
    mounts++;
    onCleanup(() => events.push("detached"));
    return <p>PRIVATE</p>;
  };
  let auth!: ReturnType<typeof useJazzAuth>;
  const SignedOut = () => {
    auth = useJazzAuth();
    return <p>{auth.snapshot().status}</p>;
  };
  const node = document.createElement("div");
  document.body.append(node);
  dispose = render(
    () => (
      <JazzProvider
        appId="test"
        initial="signed-out"
        signedOut={<SignedOut />}
        loading={<p>WAIT</p>}
        autoAttachDevTools={false}
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
  await auth.reauthenticate();
  await flush();
  expect(node.textContent).toBe("PRIVATE");
  expect(auth.snapshot().user).toMatchObject({ id: "a", issuer: "test" });
  backend.user = "b";
  await auth.reauthenticate();
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
