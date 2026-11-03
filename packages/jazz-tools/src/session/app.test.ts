import { describe, expect, it } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "./state.js";
import { createJazzAppOwner } from "./app.js";
import { watchBackendUser } from "./backend-watch.js";
import { GracefulShutdownSyncError } from "../runtime/graceful-shutdown-error.js";
const tick = async () => {
  for (let i = 0; i < 60; i++) await Promise.resolve();
};
// Lets the backend watch's start check (a timer) run, then settles.
const settle = async () => {
  await new Promise((resolve) => setTimeout(resolve, 0));
  await tick();
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
  let unknownShutdownFailure = false;
  let admissionFailure = false;
  let linkFailure = false;
  let openFailure = false;
  const handles = new Map<string, AccountHandle>();
  const account = (id: string) => {
    let result = handles.get(id);
    if (!result) {
      result = (
        id === "local"
          ? { id, identity: { issuer: "urn:jazz:local-first", subject: id } }
          : { id, identity: { issuer: "provider", subject: id }, claims: { role: `role-${id}` } }
      ) as AccountHandle;
      handles.set(id, result);
    }
    return result;
  };
  // Who the app's backend says is signed in (null: no one).
  const backend: { user: string | null } = { user: null };
  const signIn = async () => {
    const id = backend.user;
    if (id === null) throw new Error("signed_out");
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
      if (linkFailure) throw new Error("link unavailable");
      return account("local");
    },
  });
  const session = await createJazzSessionOwner({
    accounts,
    async openClient(selected) {
      events.push(`open:${selected.id}`);
      if (openFailure) throw new Error("open unavailable");
      return {
        async shutdown() {
          events.push(`flush:${selected.id}`);
          if (unknownShutdownFailure) throw new Error("unknown shutdown failure");
          if (shutdownFailure) throw new GracefulShutdownSyncError(new Error("offline"));
        },
      };
    },
    // Follow the backend as browsers do, without focus or cross-tab events.
    decorate: (owned) => {
      const watch = watchBackendUser(
        owned,
        async () => (backend.user === null ? null : { issuer: "provider", subject: backend.user }),
        { onFocus: () => () => {} },
        { confirmMs: 0 },
      );
      const close = owned.close;
      return {
        reauthenticate: () => watch.reauthenticate(),
        close() {
          watch.stop();
          return close();
        },
      };
    },
  });
  return {
    session,
    events,
    backend,
    failUnknownShutdown(value: boolean) {
      unknownShutdownFailure = value;
    },
    failShutdown(value: boolean) {
      shutdownFailure = value;
    },
    failLink(value: boolean) {
      linkFailure = value;
    },
    failOpen(value: boolean) {
      openFailure = value;
    },
    failAdmission(value: boolean) {
      admissionFailure = value;
    },
  };
}

describe("shared Jazz application lifecycle", () => {
  it("is inert for SSR and retries asynchronous host failures", async () => {
    const f = await setup();
    let calls = 0;
    const app = createJazzAppOwner(
      {},
      async () => {
        if (++calls === 1) throw new Error("host unavailable");
        return f.session;
      },
      { start: false },
    );
    await tick();
    expect(calls).toBe(0);
    expect(app.getSnapshot().status).toBe("starting");
    await expect(app.start()).rejects.toThrow("host unavailable");
    expect(app.getSnapshot().status).toBe("error");
    await app.retry();
    expect(app.getSnapshot().status).toBe("signed-out");
    expect(calls).toBe(2);
    await app.dispose();
  });
  it("signs in the backend's user on start and exposes what its answer carried", async () => {
    const f = await setup();
    f.backend.user = "one";
    const app = createJazzAppOwner({}, async () => f.session);
    await settle();
    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      user: { id: "one", issuer: "provider", claims: { role: "role-one" } },
    });
    expect(f.events).toEqual(["admit:one", "open:one"]);
    await app.dispose();
  });
  it("follows the backend's answer when the app reauthenticates", async () => {
    const f = await setup();
    const app = createJazzAppOwner({}, async () => f.session);
    await settle();
    expect(app.getSnapshot()).toMatchObject({ status: "signed-out", user: null });
    f.backend.user = "one";
    await app.reauthenticate();
    expect(app.getSnapshot()).toMatchObject({ status: "ready", user: { id: "one" } });
    // Asking again with the same answer keeps the client.
    const client = app.getSnapshot().client;
    await Promise.all([app.reauthenticate(), app.reauthenticate()]);
    expect(app.getSnapshot().client).toBe(client);
    expect(f.events.filter((event) => event.startsWith("admit"))).toEqual(["admit:one"]);
    // The app signed out: no one is signed in, at once.
    f.backend.user = null;
    await app.reauthenticate();
    expect(app.getSnapshot()).toMatchObject({ status: "signed-out", user: null });
    expect(f.events.slice(-2)).toEqual(["flush:one", "logout"]);
    await app.dispose();
  });
  it("keeps a local-first guest when the backend names no one", async () => {
    const f = await setup();
    const app = createJazzAppOwner({}, async () => f.session);
    await settle();
    await app.sessionActions.createLocalFirst();
    await app.reauthenticate();
    expect(app.getSnapshot()).toMatchObject({ status: "ready", user: null });
    expect(app.getSnapshot().account?.id).toBe("local");
    await app.dispose();
  });
  it("refuses reauthenticate for a session that does not follow a backend", async () => {
    const f = await setup();
    const { reauthenticate: _, ...plain } = f.session;
    const app = createJazzAppOwner({}, async () => plain);
    await settle();
    await expect(app.reauthenticate()).rejects.toThrow("reauthenticate is not supported");
    await app.dispose();
  });
  it("waits for committed detachment before flushing on account switch", async () => {
    const f = await setup();
    f.backend.user = "one";
    const app = createJazzAppOwner({}, async () => f.session);
    await settle();
    const consumer = app.attachConsumer();
    const ready = app.getSnapshot();
    expect(ready.status).toBe("ready");
    f.backend.user = "two";
    const switching = app.reauthenticate();
    await tick();
    expect(app.getSnapshot().client).toBeUndefined();
    expect(f.events).not.toContain("flush:one");
    consumer.acknowledge(ready);
    await tick();
    expect(f.events).not.toContain("flush:one");
    // Switching users signs "one" out first, then signs "two" in: the
    // consumer acknowledges each step it has detached from.
    for (let step = 0; step < 4 && app.getSnapshot().status !== "ready"; step++) {
      consumer.acknowledge(app.getSnapshot());
      await tick();
    }
    expect(f.events.indexOf("flush:one")).toBeLessThan(f.events.indexOf("open:two"));
    await switching;
    expect(app.getSnapshot()).toMatchObject({ status: "ready", user: { id: "two" } });
    expect(f.events).toContain("open:two");
    consumer.release();
    await app.dispose();
  });
  it("waits for a mounted consumer to detach during explicit disposal", async () => {
    const f = await setup();
    await f.session.createLocalFirst();
    const app = createJazzAppOwner({}, async () => f.session);
    await tick();
    const consumer = app.attachConsumer();
    const ready = app.getSnapshot();
    const closing = app.dispose();
    await tick();
    expect(app.getSnapshot().client).toBeUndefined();
    expect(f.events).not.toContain("flush:local");
    consumer.acknowledge(ready);
    await tick();
    expect(f.events).not.toContain("flush:local");
    consumer.acknowledge(app.getSnapshot());
    await closing;
    expect(f.events).toContain("flush:local");
    consumer.release();
  });
  it("keeps the account after a flush failure and retries logout", async () => {
    const f = await setup();
    f.backend.user = "one";
    const app = createJazzAppOwner({}, async () => f.session);
    await settle();
    f.failShutdown(true);
    await expect(app.logout()).rejects.toThrow();
    expect(app.getSnapshot().status).toBe("error");
    expect(app.getSnapshot().client).toBeUndefined();
    f.failShutdown(false);
    await app.retry();
    expect(f.events.slice(-2)).toEqual(["flush:one", "logout"]);
    expect(app.getSnapshot().status).toBe("signed-out");
    await app.dispose();
  });
  it("signs in again once a failed admission succeeds on reauthenticate", async () => {
    const f = await setup();
    const app = createJazzAppOwner({}, async () => f.session);
    await settle();
    f.failAdmission(true);
    f.backend.user = "three";
    await app.reauthenticate();
    expect(app.getSnapshot().status).toBe("error");
    expect(app.getSnapshot().client).toBeUndefined();
    f.failAdmission(false);
    await app.reauthenticate();
    expect(app.getSnapshot()).toMatchObject({ status: "ready", user: { id: "three" } });
    await app.dispose();
  });
  it("closes late host results and exposes shutdown failures without publishing a client", async () => {
    const f = await setup();
    await f.session.createLocalFirst();
    const host = deferred<typeof f.session>();
    const app = createJazzAppOwner({}, () => host.promise);
    await tick();
    const closed = app.dispose();
    host.resolve(f.session);
    await closed;
    expect(f.events).toContain("flush:local");
    expect(app.getSnapshot().client).toBeUndefined();
    const broken = createJazzAppOwner({}, async () => ({
      ...f.session,
      close: async () => {
        throw new Error("close failed");
      },
    }));
    await tick();
    await expect(broken.dispose()).rejects.toThrow("close failed");
    expect(broken.getSnapshot()).toMatchObject({
      status: "error",
      error: { message: "close failed" },
    });
  });
  it("allows explicit local-first linking flows without a competing provider owner", async () => {
    const f = await setup();
    const app = createJazzAppOwner({}, async () => f.session);
    const consumer = app.attachConsumer();
    await tick();
    const creating = app.sessionActions.createLocalFirst();
    await tick();
    expect(app.getSnapshot().status).toBe("transitioning");
    consumer.acknowledge(app.getSnapshot());
    await creating;
    expect(app.getSnapshot().status).toBe("ready");
    const linking = app.sessionActions.linkWithBackend();
    await tick();
    consumer.acknowledge(app.getSnapshot());
    await linking;
    expect(f.events).toContain("link");
    expect(app.getSnapshot().status).toBe("ready");
    consumer.release();
    await app.dispose();
  });
  it("repeats failed manual linking but only reopens after successful linking", async () => {
    const f = await setup();
    const app = createJazzAppOwner({}, async () => f.session);
    await tick();
    await app.sessionActions.createLocalFirst();
    f.failLink(true);
    await expect(app.sessionActions.linkWithBackend()).rejects.toThrow("link unavailable");
    expect(app.getSnapshot()).toMatchObject({ status: "error", recovery: "action" });
    f.failLink(false);
    await Promise.all([app.retry(), app.retry()]);
    expect(f.events.filter((event) => event === "link")).toHaveLength(2);
    f.failOpen(true);
    await expect(app.sessionActions.linkWithBackend()).rejects.toThrow("open unavailable");
    expect(app.getSnapshot()).toMatchObject({ status: "error", recovery: "session" });
    f.failOpen(false);
    await app.retry();
    expect(f.events.filter((event) => event === "link")).toHaveLength(3);
    expect(app.getSnapshot().status).toBe("ready");
    await app.dispose();
  });
  it("retries failed unmanaged logout instead of reopening the old account", async () => {
    const f = await setup();
    const app = createJazzAppOwner({}, async () => f.session);
    await tick();
    await app.sessionActions.createLocalFirst();
    f.failShutdown(true);
    await expect(app.logout()).rejects.toThrow();
    expect(app.getSnapshot()).toMatchObject({ status: "error", recovery: "action" });
    f.failShutdown(false);
    await app.retry();
    expect(app.getSnapshot().status).toBe("signed-out");
    await app.dispose();
  });
  it("repeats logout after an unknown shutdown failure without reopening the account", async () => {
    const f = await setup();
    const app = createJazzAppOwner({}, async () => f.session);
    await tick();
    await app.sessionActions.createLocalFirst();
    f.failUnknownShutdown(true);
    await expect(app.logout()).rejects.toThrow("unknown shutdown failure");
    expect(f.session.getSnapshot()).toMatchObject({ status: "error", recovery: "action" });
    expect(app.getSnapshot()).toMatchObject({ status: "error", recovery: "action" });
    f.failUnknownShutdown(false);
    await app.retry();
    expect(app.getSnapshot().status).toBe("signed-out");
    expect(f.events.filter((event) => event === "open:local")).toHaveLength(1);
    await app.dispose();
  });
});
it("rejected overlapping action does not erase failed action retry", async () => {
  const f = await setup();
  const app = createJazzAppOwner({}, async () => f.session);
  await tick();
  await app.sessionActions.createLocalFirst();
  f.failLink(true);
  const first = app.sessionActions.linkWithBackend();
  const second = app.sessionActions.linkWithBackend();
  await expect(second).rejects.toThrow("already pending");
  await expect(first).rejects.toThrow("link unavailable");
  expect(app.getSnapshot().recovery).toBe("action");
  f.failLink(false);
  await app.retry();
  expect(f.events.filter((e) => e === "link")).toHaveLength(2);
  await app.dispose();
});
it("rejected manual action during logout preserves logout retry", async () => {
  const f = await setup();
  const app = createJazzAppOwner({}, async () => f.session);
  await tick();
  await app.sessionActions.createLocalFirst();
  f.failShutdown(true);
  const logout = app.logout();
  await expect(app.sessionActions.linkWithBackend()).rejects.toThrow("already pending");
  await expect(logout).rejects.toThrow();
  f.failShutdown(false);
  await app.retry();
  expect(app.getSnapshot().status).toBe("signed-out");
  expect(f.events).not.toContain("link");
  await app.dispose();
});
