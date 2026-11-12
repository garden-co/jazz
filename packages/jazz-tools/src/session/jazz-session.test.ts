import { describe, expect, it } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "./state.js";
import {
  attachJazzSessionConsumer,
  coreInitial,
  createStartedJazzSession,
  ownJazzSession,
  startJazzSession,
  type JazzSession,
} from "./jazz-session.js";
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
  let probeFailure = false;
  let focus: (() => void) | undefined;
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
        async () => {
          if (probeFailure) throw new Error("backend unavailable");
          return backend.user === null ? null : { issuer: "provider", subject: backend.user };
        },
        {
          onFocus(check) {
            focus = check;
            return () => (focus = undefined);
          },
        },
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
    failProbe(value: boolean) {
      probeFailure = value;
    },
    focus: () => focus?.(),
    failAdmission(value: boolean) {
      admissionFailure = value;
    },
  };
}

describe("JazzSession lifecycle", () => {
  it("is inert for SSR and retries asynchronous host failures", async () => {
    const f = await setup();
    let calls = 0;
    const app = ownJazzSession(
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
    await expect(startJazzSession(app as JazzSession<unknown>)).rejects.toThrow("host unavailable");
    expect(app.getSnapshot().status).toBe("error");
    await app.retry();
    expect(app.getSnapshot().status).toBe("signed-out");
    expect(calls).toBe(2);
    await app.close();
  });
  it("signs in the backend's user on start and exposes what its answer carried", async () => {
    const f = await setup();
    f.backend.user = "one";
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      user: {
        accountId: "one",
        kind: "backend",
        identity: { issuer: "provider", subject: "one" },
        claims: { role: "role-one" },
      },
    });
    expect(f.events).toEqual(["admit:one", "open:one"]);
    await app.close();
  });
  it("follows the backend's answer when the app refreshes", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    expect(app.getSnapshot()).toMatchObject({ status: "signed-out", user: null });
    f.backend.user = "one";
    await app.refresh();
    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      user: { identity: { subject: "one" } },
    });
    // Asking again with the same answer keeps the client.
    const client = app.getSnapshot().client;
    await Promise.all([app.refresh(), app.refresh()]);
    expect(app.getSnapshot().client).toBe(client);
    expect(f.events.filter((event) => event.startsWith("admit"))).toEqual(["admit:one"]);
    // The app signed out: no one is signed in, at once.
    f.backend.user = null;
    await app.refresh();
    expect(app.getSnapshot()).toMatchObject({ status: "signed-out", user: null });
    expect(f.events.slice(-2)).toEqual(["flush:one", "logout"]);
    await app.close();
  });
  it("keeps a device account when the backend names no one", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    await app.createDeviceAccount();
    await app.refresh();
    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      user: {
        accountId: "local",
        kind: "device",
        identity: { issuer: "urn:jazz:local-first", subject: "local" },
        claims: {},
      },
    });
    await app.close();
  });
  it("refreshes a session that doesn't follow a backend by signing in its user", async () => {
    const f = await setup();
    const { reauthenticate: _, ...plain } = f.session;
    const app = ownJazzSession({}, async () => plain);
    await settle();
    f.backend.user = "one";
    await app.refresh();
    expect(app.getSnapshot()).toMatchObject({ status: "ready", user: { accountId: "one" } });
    await app.close();
  });
  it("waits for committed detachment before flushing on account switch", async () => {
    const f = await setup();
    f.backend.user = "one";
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    const consumer = attachJazzSessionConsumer(app);
    const ready = app.getSnapshot();
    expect(ready.status).toBe("ready");
    f.backend.user = "two";
    const switching = app.refresh();
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
    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      user: { identity: { subject: "two" } },
    });
    expect(f.events).toContain("open:two");
    consumer.release();
    await app.close();
  });
  it("waits for a mounted consumer to detach during explicit disposal", async () => {
    const f = await setup();
    await f.session.createLocalFirst();
    const app = ownJazzSession({}, async () => f.session);
    await tick();
    const consumer = attachJazzSessionConsumer(app);
    const ready = app.getSnapshot();
    const closing = app.close();
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
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    f.failShutdown(true);
    const client = app.getSnapshot().client;
    await expect(app.signOut()).rejects.toThrow();
    // The client is still usable: the session stays ready, with the error.
    expect(app.getSnapshot()).toMatchObject({ status: "ready", client, recovery: "action" });
    expect(app.getSnapshot().error).toBeDefined();
    f.failShutdown(false);
    await app.retry();
    expect(f.events.slice(-2)).toEqual(["flush:one", "logout"]);
    expect(app.getSnapshot().status).toBe("signed-out");
    await app.close();
  });
  it("signs in again once a failed admission succeeds on refresh", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    f.failAdmission(true);
    f.backend.user = "three";
    await expect(app.refresh()).rejects.toThrow("registry offline");
    expect(app.getSnapshot()).toMatchObject({
      status: "signed-out",
      error: { message: "registry offline" },
    });
    expect(app.getSnapshot().client).toBeUndefined();
    f.failAdmission(false);
    await app.refresh();
    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      user: { identity: { subject: "three" } },
    });
    await app.close();
  });
  it("keeps a ready client when the backend can't be asked on refresh", async () => {
    const f = await setup();
    f.backend.user = "one";
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      user: { identity: { subject: "one" } },
    });
    const client = app.getSnapshot().client;
    f.failProbe(true);
    await expect(app.refresh()).rejects.toThrow("Jazz could not ask your backend who is signed in");
    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      client,
      user: { identity: { subject: "one" } },
    });
    await app.close();
  });
  it("retries a failed refresh by asking the backend, and clears it once a check follows", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    expect(app.getSnapshot().status).toBe("signed-out");
    // The app signed in, but the backend can't be asked: show the failure.
    f.backend.user = "one";
    f.failProbe(true);
    await expect(app.refresh()).rejects.toThrow();
    expect(app.getSnapshot()).toMatchObject({ status: "signed-out", recovery: "action" });
    expect(app.getSnapshot().error).toBeDefined();
    // Retrying asks the backend again, which now answers.
    f.failProbe(false);
    await app.retry();
    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      user: { identity: { subject: "one" } },
    });

    // A later check that follows the backend clears a refresh's failure.
    f.backend.user = "two";
    f.failProbe(true);
    await expect(app.refresh()).rejects.toThrow();
    expect(app.getSnapshot().status).toBe("ready");
    f.backend.user = null;
    await f.session.logout();
    f.backend.user = "two";
    await expect(app.refresh()).rejects.toThrow();
    expect(app.getSnapshot()).toMatchObject({ status: "signed-out", recovery: "action" });
    f.failProbe(false);
    f.focus();
    await settle();
    expect(app.getSnapshot()).toMatchObject({
      status: "ready",
      user: { identity: { subject: "two" } },
    });
    await app.close();
  });
  it("retries a refresh whose sign-in failed by asking the backend, not reopening", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    f.backend.user = "one";
    f.failOpen(true);
    await expect(app.refresh()).rejects.toThrow("open unavailable");
    expect(app.getSnapshot().status).toBe("error");
    // Signed out meanwhile: retrying follows the backend instead of reopening "one".
    f.failOpen(false);
    f.backend.user = null;
    f.events.length = 0;
    await app.retry();
    expect(app.getSnapshot().status).toBe("signed-out");
    expect(f.events).not.toContain("open:one");
    await app.close();
  });
  it("closes late host results and exposes shutdown failures without publishing a client", async () => {
    const f = await setup();
    await f.session.createLocalFirst();
    const host = deferred<typeof f.session>();
    const app = ownJazzSession({}, () => host.promise);
    await tick();
    const closed = app.close();
    host.resolve(f.session);
    await closed;
    expect(f.events).toContain("flush:local");
    expect(app.getSnapshot().client).toBeUndefined();
    const broken = ownJazzSession({}, async () => ({
      ...f.session,
      close: async () => {
        throw new Error("close failed");
      },
    }));
    await tick();
    await expect(broken.close()).rejects.toThrow("close failed");
    expect(broken.getSnapshot()).toMatchObject({
      status: "closed",
      error: { message: "close failed" },
    });
  });
  it("allows explicit local-first linking flows without a competing provider owner", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    const consumer = attachJazzSessionConsumer(app);
    await tick();
    const creating = app.createDeviceAccount();
    await tick();
    expect(app.getSnapshot().status).toBe("transitioning");
    consumer.acknowledge(app.getSnapshot());
    await creating;
    expect(app.getSnapshot().status).toBe("ready");
    const linking = app.linkDeviceAccount();
    await tick();
    consumer.acknowledge(app.getSnapshot());
    await linking;
    expect(f.events).toContain("link");
    expect(app.getSnapshot().status).toBe("ready");
    consumer.release();
    await app.close();
  });
  it("repeats failed manual linking but only reopens after successful linking", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    await tick();
    await app.createDeviceAccount();
    f.failLink(true);
    await expect(app.linkDeviceAccount()).rejects.toThrow("link unavailable");
    // The device account's client is still usable: ready, with the error.
    expect(app.getSnapshot()).toMatchObject({ status: "ready", recovery: "action" });
    expect(app.getSnapshot().client).toBeDefined();
    f.failLink(false);
    await Promise.all([app.retry(), app.retry()]);
    expect(f.events.filter((event) => event === "link")).toHaveLength(2);
    f.failOpen(true);
    await expect(app.linkDeviceAccount()).rejects.toThrow("open unavailable");
    expect(app.getSnapshot()).toMatchObject({ status: "error", recovery: "session" });
    f.failOpen(false);
    await app.retry();
    expect(f.events.filter((event) => event === "link")).toHaveLength(3);
    expect(app.getSnapshot().status).toBe("ready");
    await app.close();
  });
  it("retries failed unmanaged logout instead of reopening the old account", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    await tick();
    await app.createDeviceAccount();
    f.failShutdown(true);
    await expect(app.signOut()).rejects.toThrow();
    expect(app.getSnapshot()).toMatchObject({ status: "ready", recovery: "action" });
    f.failShutdown(false);
    await app.retry();
    expect(app.getSnapshot().status).toBe("signed-out");
    await app.close();
  });
  it("repeats logout after an unknown shutdown failure without reopening the account", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    await tick();
    await app.createDeviceAccount();
    f.failUnknownShutdown(true);
    await expect(app.signOut()).rejects.toThrow("unknown shutdown failure");
    expect(f.session.getSnapshot()).toMatchObject({ status: "error", recovery: "action" });
    expect(app.getSnapshot()).toMatchObject({ status: "error", recovery: "action" });
    f.failUnknownShutdown(false);
    await app.retry();
    expect(app.getSnapshot().status).toBe("signed-out");
    expect(f.events.filter((event) => event === "open:local")).toHaveLength(1);
    await app.close();
  });
});
it("rejected overlapping action does not erase failed action retry", async () => {
  const f = await setup();
  const app = ownJazzSession({}, async () => f.session);
  await tick();
  await app.createDeviceAccount();
  f.failLink(true);
  const first = app.linkDeviceAccount();
  const second = app.linkDeviceAccount();
  await expect(second).rejects.toThrow("already pending");
  await expect(first).rejects.toThrow("link unavailable");
  expect(app.getSnapshot().recovery).toBe("action");
  f.failLink(false);
  await app.retry();
  expect(f.events.filter((e) => e === "link")).toHaveLength(2);
  await app.close();
});
it("rejected manual action during logout preserves logout retry", async () => {
  const f = await setup();
  const app = ownJazzSession({}, async () => f.session);
  await tick();
  await app.createDeviceAccount();
  f.failShutdown(true);
  const logout = app.signOut();
  await expect(app.linkDeviceAccount()).rejects.toThrow("already pending");
  await expect(logout).rejects.toThrow();
  f.failShutdown(false);
  await app.retry();
  expect(app.getSnapshot().status).toBe("signed-out");
  expect(f.events).not.toContain("link");
  await app.close();
});

describe("JazzSession surface", () => {
  it("names the running operation in the device-account terms", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    const consumer = attachJazzSessionConsumer(app);
    await tick();
    const creating = app.createDeviceAccount();
    await tick();
    expect(app.getSnapshot()).toMatchObject({
      status: "transitioning",
      pending: "createDeviceAccount",
    });
    consumer.acknowledge(app.getSnapshot());
    await creating;
    expect(app.getSnapshot().pending).toBeUndefined();
    consumer.release();
    await app.close();
  });
  it("exports a key only for a device account", async () => {
    const f = await setup();
    f.backend.user = "one";
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    expect(() => app.exportDeviceKey()).toThrow("isn't signed in as a device account");
    await app.close();
    const g = await setup();
    const device = ownJazzSession({}, async () => g.session);
    await settle();
    await device.createDeviceAccount();
    // This fake device account has no key material: the export reached the
    // account's own credentials (real keys: create-jazz-session.device-only).
    expect(() => device.exportDeviceKey()).toThrow("local_first_recovery_unavailable");
    await device.close();
  });
  it("refuses operations once closed", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session);
    await settle();
    await app.close();
    expect(app.getSnapshot().status).toBe("closed");
    await expect(app.createDeviceAccount()).rejects.toThrow("closed");
    await expect(app.retry()).rejects.toThrow("closed");
  });
});

describe("createStartedJazzSession", () => {
  it("resolves once started", async () => {
    const f = await setup();
    f.backend.user = "one";
    const session = await createStartedJazzSession({}, async () => f.session);
    expect(session.getSnapshot().status).not.toBe("starting");
    await session.close();
  });
  it("rejects, and closes the session, when startup fails", async () => {
    await expect(
      createStartedJazzSession({}, async () => {
        throw new Error("host unavailable");
      }),
    ).rejects.toThrow("host unavailable");
  });
});

describe("coreInitial", () => {
  it("maps the device-account names and refuses removed or unknown ones", () => {
    expect(coreInitial(undefined)).toBeUndefined();
    expect(coreInitial("signed-out")).toBe("signed-out");
    expect(coreInitial("device")).toBe("local-first");
    expect(() => coreInitial("local-first")).toThrow('initial: "device"');
    expect(() => coreInitial("guest")).toThrow('expected "device" or "signed-out"');
  });
});

describe("session.db", () => {
  it("is the ready client's Db, and names the status when there is none", async () => {
    const f = await setup();
    const app = ownJazzSession({}, async () => f.session, { start: false });
    expect(() => app.db).toThrow("there is no Db while the session is starting");
    await startJazzSession(app as JazzSession<unknown>);
    expect(() => app.db).toThrow(/while the session is signed-out: sign in first/);
    await app.createDeviceAccount();
    const client = app.getSnapshot().client as { db?: unknown } | undefined;
    // The fake clients here have no db: a real one returns it.
    expect(() => app.db).toThrow("while the session is ready");
    expect(client).toBeDefined();
    await app.close();
    expect(() => app.db).toThrow("while the session is closed");
  });
});
