import { expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle, type AccountIdentity } from "../accounts/state.js";
import { AccountAuthError } from "../accounts/registry-client.js";
import { connectAuthProvider } from "./auth-provider.js";
import { watchBackendUser } from "./backend-watch.js";
import { createJazzSessionOwner } from "./state.js";

const handle = (issuer: string, subject: string) =>
  Object.freeze({ id: subject, identity: { issuer, subject } }) as AccountHandle;
const guest = handle("urn:jazz:local-first", "guest");
const user = (subject: string) => handle("jazz:app:test", subject);
const settle = async () => {
  for (let i = 0; i < 20; i++) await Promise.resolve();
};

async function setup(initial: AccountHandle | undefined, noBackend = false) {
  let backendUser: AccountHandle | null = null;
  const loginWithBackend = vi.fn(async () => {
    if (!backendUser) throw new Error("signed_out");
    return backendUser;
  });
  const accounts = new AccountManager(
    {
      createLocalFirst: () => guest,
      restoreLocalFirst: () => guest,
      loginWithBackend,
      logout() {},
    },
    initial,
  );
  const session = await createJazzSessionOwner({
    accounts,
    openClient: async () => ({ shutdown: async () => {} }),
  });
  let focus: (() => void) | undefined;
  const posts = vi.fn();
  let otherTab: (() => void) | undefined;
  const probe = vi.fn(async (): Promise<AccountIdentity | null> => {
    if (noBackend) throw new AccountAuthError("not_found", 404);
    return backendUser?.identity ?? null;
  });
  const stop = watchBackendUser(session, probe, {
    onFocus(check) {
      focus = check;
      return () => (focus = undefined);
    },
    crossTab: {
      post: posts,
      listen(check) {
        otherTab = check;
        return () => (otherTab = undefined);
      },
    },
  });
  return {
    session,
    probe,
    posts,
    loginWithBackend,
    stop,
    signIn: (next: AccountHandle | null) => (backendUser = next),
    focus: () => focus?.(),
    otherTab: () => otherTab?.(),
    hasListeners: () => !!focus || !!otherTab,
  };
}

it("signs in whoever the backend names on start, then follows focus and other tabs", async () => {
  const t = await setup(undefined);
  await settle();
  expect(t.probe).toHaveBeenCalledOnce();
  expect(t.session.getSnapshot().status).toBe("signed-out");

  t.signIn(user("alice"));
  t.focus();
  await settle();
  expect(t.session.getSnapshot().account?.identity.subject).toBe("alice");
  expect(t.posts).toHaveBeenCalledOnce();

  // Same user on the next check: nothing to do.
  t.focus();
  await settle();
  expect(t.loginWithBackend).toHaveBeenCalledOnce();

  t.signIn(user("bob"));
  t.otherTab();
  await settle();
  expect(t.session.getSnapshot().account?.identity.subject).toBe("bob");

  t.signIn(null);
  t.focus();
  await settle();
  expect(t.session.getSnapshot().status).toBe("signed-out");
  expect(t.posts).toHaveBeenCalledTimes(3);
  t.stop();
  expect(t.hasListeners()).toBe(false);
  await t.session.close();
});

it("keeps a local-first guest when the backend says no one is signed in", async () => {
  const t = await setup(guest);
  await settle();
  expect(t.probe).toHaveBeenCalledOnce();
  expect(t.session.getSnapshot()).toMatchObject({ status: "ready", account: guest });
  t.signIn(user("alice"));
  t.focus();
  await settle();
  expect(t.session.getSnapshot().account?.identity.subject).toBe("alice");
  t.stop();
  await t.session.close();
});

it("stays quiet while a `user` prop or setUser drives the session", async () => {
  const t = await setup(undefined);
  await settle();
  const connection = connectAuthProvider(t.session);
  t.signIn(user("alice"));
  t.focus();
  t.otherTab();
  await settle();
  expect(t.probe).toHaveBeenCalledOnce();
  expect(t.loginWithBackend).not.toHaveBeenCalled();
  connection.dispose();
  t.stop();
  await t.session.close();
});

it("stops when there is no backend to ask", async () => {
  const t = await setup(undefined, true);
  await settle();
  expect(t.hasListeners()).toBe(false);
  await t.session.close();
});
