import { describe, expect, it } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "./state.js";
import { connectAuthProvider } from "./auth-provider.js";
import { GracefulShutdownSyncError } from "../runtime/graceful-shutdown-error.js";
const tick = async () => {
  for (let i = 0; i < 60; i++) await Promise.resolve();
};
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
async function setup() {
  const events: string[] = [];
  let shutdownFailure = false;
  let admissionFailure = false;
  const handles = new Map<string, AccountHandle>();
  const account = (id: string) => {
    let result = handles.get(id);
    if (!result) {
      result = { id, identity: { issuer: "provider", subject: id } } as AccountHandle;
      handles.set(id, result);
    }
    return result;
  };
  // Who the app's backend says is signed in; a test can hold the answer back.
  const backend: { user: string; next?: Promise<string> } = { user: "one" };
  const signIn = async () => {
    const pending = backend.next;
    backend.next = undefined;
    const id = await (pending ?? backend.user);
    events.push(`admit:${id}`);
    if (admissionFailure) throw new Error("registry offline");
    return account(id);
  };
  const accounts = new AccountManager({
    createLocalFirst: () => account("local"),
    restoreLocalFirst: () => account("local"),
    logout() {
      events.push("logout");
    },
    loginWithBackend: signIn,
    async linkWithBackend() {
      events.push("link");
      return account("local");
    },
  });
  const session = await createJazzSessionOwner({
    accounts,
    async openClient(selected) {
      events.push(`open:${selected.id}`);
      return {
        async shutdown() {
          events.push(`flush:${selected.id}`);
          if (shutdownFailure) throw new GracefulShutdownSyncError(new Error("offline"));
        },
      };
    },
  });
  return {
    session,
    events,
    backend,
    failShutdown(value: boolean) {
      shutdownFailure = value;
    },
    failAdmission(value: boolean) {
      admissionFailure = value;
    },
  };
}

describe("provider connection with real Jazz session lifecycle", () => {
  it("waits for hydration and coalesces repeated identity and refresh events", async () => {
    const { session, events } = await setup();
    const auth = connectAuthProvider(session);
    auth.update({ key: null, isPending: true });
    await tick();
    expect(events).toEqual([]);
    auth.update({ key: "issuer:one" });
    await tick();
    auth.update({ key: "issuer:one" });
    auth.update({ key: "issuer:one" });
    await tick();
    expect(events).toEqual(["admit:one", "open:one"]);
    expect(auth.getSnapshot()).toMatchObject({ ready: true, isPending: false });
    auth.dispose();
    await session.close();
  });
  it("ends signed out when the provider signs out during a backend sign-in", async () => {
    const { session, backend } = await setup();
    const answer = deferred<string>();
    backend.next = answer.promise;
    const auth = connectAuthProvider(session);
    auth.update({ key: "one" });
    await tick();
    auth.update({ key: null });
    answer.resolve("one");
    await tick();
    expect(session.getSnapshot().status).toBe("signed-out");
    expect(auth.getSnapshot().ready).toBe(true);
    auth.dispose();
    await session.close();
  });
  it("explicit signout supersedes a pending backend sign-in", async () => {
    const { session, events, backend } = await setup();
    const answer = deferred<string>();
    backend.next = answer.promise;
    const auth = connectAuthProvider(session);
    auth.update({ key: "one" });
    await tick();
    const leaving = auth.logout(async () => {
      events.push("revoke");
      auth.update({ key: null });
    });
    answer.resolve("one");
    await leaving;
    expect(events.slice(-2)).toEqual(["logout", "revoke"]);
    expect(session.getSnapshot().status).toBe("signed-out");
    expect(auth.getSnapshot().ready).toBe(true);
    auth.dispose();
    await session.close();
  });
  it("failed flush retains provider credentials and retries logout without reenrollment", async () => {
    const { session, events, failShutdown } = await setup();
    const auth = connectAuthProvider(session);
    auth.update({ key: "one" });
    await tick();
    failShutdown(true);
    await expect(
      auth.logout(async () => {
        events.push("revoke");
        auth.update({ key: null });
      }),
    ).rejects.toThrow();
    expect(events).not.toContain("revoke");
    auth.update({ key: "one" });
    await tick();
    expect(auth.getSnapshot().error).toBeDefined();
    failShutdown(false);
    await auth.retry();
    expect(events.filter((e) => e.startsWith("admit:"))).toEqual(["admit:one"]);
    expect(events.at(-1)).toBe("revoke");
    auth.dispose();
    await session.close();
  });
  it("exposes enrollment failures and retries the same identity", async () => {
    const { session, failAdmission } = await setup();
    const auth = connectAuthProvider(session);
    failAdmission(true);
    auth.update({ key: "one" });
    await tick();
    expect(auth.getSnapshot()).toMatchObject({
      ready: false,
      isPending: false,
      error: expect.any(Error),
    });
    failAdmission(false);
    await auth.retry();
    expect(auth.getSnapshot()).toMatchObject({ ready: true, error: undefined });
    auth.dispose();
    await session.close();
  });
  it("does not reenroll stale provider state after successful signout", async () => {
    const { session, events } = await setup();
    const auth = connectAuthProvider(session);
    auth.update({ key: "one" });
    await tick();
    await auth.logout(async () => {});
    auth.update({ key: "one" });
    await tick();
    expect(events.filter((e) => e.startsWith("admit:"))).toEqual(["admit:one"]);
    expect(auth.getSnapshot().ready).toBe(false);
    auth.update({ key: null });
    await tick();
    expect(auth.getSnapshot().ready).toBe(true);
    auth.dispose();
    await session.close();
  });
  it("disposal releases connection ownership, not the session, and ignores queued work", async () => {
    const { session, events, backend } = await setup();
    const auth = connectAuthProvider(session);
    expect(() => connectAuthProvider(session)).toThrow("already");
    auth.update({ key: "one" });
    auth.dispose();
    await tick();
    expect(events).toEqual([]);
    backend.user = "two";
    const next = connectAuthProvider(session);
    next.update({ key: "two" });
    await tick();
    expect(session.getSnapshot().account?.id).toBe("two");
    next.dispose();
    expect(session.getSnapshot().status).toBe("ready");
    await session.close();
  });
  it("manual hybrid linking can precede any automatic enrollment", async () => {
    const { session, events } = await setup();
    await session.createLocalFirst();
    await session.linkWithBackend();
    expect(events).toContain("link");
    expect(events.some((event) => event.startsWith("admit:"))).toBe(false);
    expect(session.getSnapshot().account?.id).toBe("local");
    await session.close();
  });
});

it("admits a newer provider session observed while explicit signout finishes", async () => {
  const { session, events, backend } = await setup();
  const auth = connectAuthProvider(session);
  auth.update({ key: "one" });
  await tick();
  const done = deferred<void>();
  const leaving = auth.logout(() => done.promise);
  await tick();
  backend.user = "two";
  auth.update({ key: "two" });
  done.resolve();
  await leaving;
  await tick();
  expect(events.filter((event) => event.startsWith("admit:"))).toEqual(["admit:one", "admit:two"]);
  expect(auth.getSnapshot()).toMatchObject({ ready: true, key: "two" });
  auth.dispose();
  await session.close();
});

it("Better Auth retries a failed initial provider fetch before admitting Jazz", async () => {
  const { connectBetterAuth } = await import("./better-auth.js");
  const { session, events } = await setup();
  let notify = () => {};
  let failed = true;
  const state = () => ({
    data: failed ? null : { session: { id: "session-one" }, user: { id: "one" } },
    isPending: false,
    error: failed ? { message: "provider offline" } : null,
    async refetch() {
      failed = false;
      notify();
    },
  });
  const auth = connectBetterAuth(session, {
    $store: {
      atoms: {
        session: {
          get: state,
          subscribe(listener) {
            notify = () => listener(state());
            notify();
            return () => {
              notify = () => {};
            };
          },
        },
      },
    },
    async signOut() {
      return {};
    },
  });
  await tick();
  expect(events).toEqual([]);
  expect(auth.getSnapshot().error?.message).toBe("provider offline");
  await auth.retry();
  expect(auth.getSnapshot().ready).toBe(true);
  auth.dispose();
  await session.close();
});

it("signs in again after an A to B to A provider transition during a sign-in", async () => {
  const { session, events, backend } = await setup();
  const stale = deferred<string>();
  backend.next = stale.promise;
  backend.user = "fresh-A";
  const auth = connectAuthProvider(session);
  auth.update({ key: "A" });
  await tick();
  auth.update({ key: "B" });
  auth.update({ key: "A" });
  stale.resolve("stale-A");
  await tick();
  // The sign-in that started before the transition doesn't count: the
  // backend is asked again, and its answer now is the one that sticks.
  expect(events.filter((event) => event.startsWith("admit:")).at(-1)).toBe("admit:fresh-A");
  expect(session.getSnapshot().account?.id).toBe("fresh-A");
  expect(auth.getSnapshot()).toMatchObject({ ready: true, key: "A" });
  auth.dispose();
  await session.close();
});

it("concurrent retries retain the active attempt across unchanged provider notifications", async () => {
  const { session, events, failAdmission } = await setup();
  const auth = connectAuthProvider(session);
  failAdmission(true);
  auth.update({ key: "one" });
  await tick();
  expect(auth.getSnapshot().error?.message).toBe("registry offline");
  failAdmission(false);
  await Promise.all([auth.retry(), auth.retry()]);
  await tick();
  expect(auth.getSnapshot().ready).toBe(true);
  expect(events).toEqual(["admit:one", "admit:one", "open:one"]);
  auth.update({ key: "one" });
  await tick();
  expect(events).toEqual(["admit:one", "admit:one", "open:one"]);
  auth.dispose();
  await session.close();
});
