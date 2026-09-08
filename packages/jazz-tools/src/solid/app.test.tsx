import { createSignal, onCleanup } from "solid-js";
import { render } from "solid-js/web";
import { afterEach, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";
import { jwtAuth } from "../session/app.js";
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
    registerJWT: async () => handle("unused"),
    loginJWT: async () => handle("unused"),
    loginOrRegisterJWT: async (auth: any) => handle(await auth.getToken()),
    linkJWT: async () => handle("unused"),
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
it("reacts to JWT getters, conceals children during provider pending, and releases before shutdown", async () => {
  const { owner, events } = await setup();
  const [key, setKey] = createSignal<string | null>(null);
  const [pending, setPending] = createSignal(false);
  let mounts = 0;
  const Child = () => {
    mounts++;
    onCleanup(() => events.push("detached"));
    return <p>PRIVATE</p>;
  };
  const SignedOut = () => <p>{useJazzAuth().snapshot().status}</p>;
  const node = document.createElement("div");
  document.body.append(node);
  dispose = render(
    () => (
      <JazzProvider
        appId="test"
        auth={jwtAuth({
          key: key(),
          isPending: pending(),
          getToken: async () => key()!,
          logout: async () => {
            setKey(null);
          },
        })}
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
  expect(mounts).toBe(0);
  setKey("a");
  await flush();
  expect(node.textContent).toBe("PRIVATE");
  setPending(true);
  await flush();
  expect(node.textContent).toBe("WAIT");
  setKey("b");
  setPending(false);
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
