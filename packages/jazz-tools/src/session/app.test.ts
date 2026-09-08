import { describe, expect, it } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "./state.js";
import { betterAuth, createJazzAppOwner, jwtAuth } from "./app.js";
import type { BetterAuthClient } from "./better-auth.js";
import type { JWTAuth } from "../accounts/enrollment.js";
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
  let unknownShutdownFailure = false;
  let admissionFailure = false;
  let linkFailure = false;
  let openFailure = false;
  const handles = new Map<string, AccountHandle>();
  const account = (id: string) => {
    let result = handles.get(id);
    if (!result) {
      result = { id, identity: { issuer: "provider", subject: id } } as AccountHandle;
      handles.set(id, result);
    }
    return result;
  };
  const token = async (auth: JWTAuth) => (typeof auth === "string" ? auth : auth.getToken());
  const enroll = async (auth: JWTAuth) => {
    const id = await token(auth);
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
    registerJWT: enroll,
    loginJWT: enroll,
    loginOrRegisterJWT: enroll,
    async linkJWT(_account: AccountHandle, auth: JWTAuth) {
      events.push("link");
      if (linkFailure) throw new Error("link unavailable");
      await token(auth);
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
  });
  return {
    session,
    events,
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
    app.updateAuth(undefined);
    expect(app.getSnapshot().status).toBe("error");
    await app.retry();
    expect(app.getSnapshot().status).toBe("signed-out");
    expect(calls).toBe(2);
    await app.dispose();
  });
  it("hides restored clients until hydration and coalesces inline descriptor refresh", async () => {
    const f = await setup();
    await f.session.createLocalFirst();
    const app = createJazzAppOwner(
      {
        auth: jwtAuth({
          logout: async () => {},
          key: null,
          isPending: true,
          getToken: async () => "one",
        }),
      },
      async () => f.session,
    );
    await tick();
    expect(app.getSnapshot().client).toBeUndefined();
    app.updateAuth(jwtAuth({ logout: async () => {}, key: "one", getToken: async () => "one" }));
    await tick();
    expect(app.getSnapshot().status).toBe("ready");
    const client = app.getSnapshot().client;
    expect(client).toBeDefined();
    app.updateAuth(
      jwtAuth({ logout: async () => {}, key: "one", getToken: async () => "refreshed" }),
    );
    await tick();
    expect(app.getSnapshot().client).toBe(client);
    expect(f.events.filter((e) => e.startsWith("admit"))).toEqual(["admit:one"]);
    await app.dispose();
  });
  it("waits for committed detachment before flushing on account switch", async () => {
    const f = await setup();
    const app = createJazzAppOwner(
      { auth: jwtAuth({ logout: async () => {}, key: "one", getToken: async () => "one" }) },
      async () => f.session,
    );
    await tick();
    const consumer = app.attachConsumer();
    const ready = app.getSnapshot();
    app.updateAuth(jwtAuth({ logout: async () => {}, key: "two", getToken: async () => "two" }));
    await tick();
    expect(app.getSnapshot().client).toBeUndefined();
    expect(f.events).not.toContain("flush:one");
    consumer.acknowledge(ready);
    await tick();
    expect(f.events).not.toContain("flush:one");
    consumer.acknowledge(app.getSnapshot());
    await tick();
    expect(app.getSnapshot().status).toBe("ready");
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
  it("keeps credentials after flush failure and retries logout before revocation", async () => {
    const f = await setup();
    const app = createJazzAppOwner(
      {
        auth: jwtAuth({
          key: "one",
          getToken: async () => "one",
          logout: () => {
            f.events.push("revoke");
          },
        }),
      },
      async () => f.session,
    );
    await tick();
    f.failShutdown(true);
    await expect(app.logout()).rejects.toThrow();
    expect(app.getSnapshot().status).toBe("error");
    expect(app.getSnapshot().client).toBeUndefined();
    expect(f.events).not.toContain("revoke");
    f.failShutdown(false);
    await app.retry();
    expect(f.events.slice(-3)).toEqual(["flush:one", "logout", "revoke"]);
    app.updateAuth(jwtAuth({ logout: async () => {}, key: null, getToken: async () => "none" }));
    await tick();
    expect(app.getSnapshot().status).toBe("signed-out");
    await app.dispose();
  });
  it("retries admission and fences ABA token promises", async () => {
    const f = await setup();
    const token = deferred<string>();
    const app = createJazzAppOwner(
      { auth: jwtAuth({ logout: async () => {}, key: "one", getToken: () => token.promise }) },
      async () => f.session,
    );
    await tick();
    app.updateAuth(jwtAuth({ logout: async () => {}, key: "two", getToken: async () => "two" }));
    app.updateAuth(
      jwtAuth({ logout: async () => {}, key: "one", getToken: async () => "fresh-one" }),
    );
    token.resolve("stale-one");
    await tick();
    expect(f.events).not.toContain("admit:stale-one");
    expect(f.events).toContain("open:fresh-one");
    expect(app.getSnapshot().status).toBe("ready");
    f.failAdmission(true);
    app.updateAuth(
      jwtAuth({ logout: async () => {}, key: "three", getToken: async () => "three" }),
    );
    await tick();
    expect(app.getSnapshot().status).toBe("error");
    expect(app.getSnapshot().client).toBeUndefined();
    f.failAdmission(false);
    await app.retry();
    expect(app.getSnapshot().status).toBe("ready");
    expect(f.events).toContain("open:three");
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
    const linking = app.sessionActions.linkJWT({ getToken: async () => "linked" });
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
    await expect(app.sessionActions.linkJWT({ getToken: async () => "linked" })).rejects.toThrow(
      "link unavailable",
    );
    expect(app.getSnapshot()).toMatchObject({ status: "error", recovery: "action" });
    f.failLink(false);
    await Promise.all([app.retry(), app.retry()]);
    expect(f.events.filter((event) => event === "link")).toHaveLength(2);
    f.failOpen(true);
    await expect(
      app.sessionActions.linkJWT({ getToken: async () => "linked-again" }),
    ).rejects.toThrow("open unavailable");
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
  it("attaches real Better Auth connector and coalesces descriptor rerenders", async () => {
    const f = await setup();
    let subscriptions = 0;
    const state = {
      data: { session: { id: "session-one" }, user: { id: "one" } },
      isPending: false,
    };
    const client: BetterAuthClient = {
      $store: {
        atoms: {
          session: {
            get: () => state,
            subscribe: () => {
              subscriptions++;
              return () => {
                subscriptions--;
              };
            },
          },
        },
      },
      $fetch: async () => ({ data: { token: "one" } }),
      signOut: async () => {
        f.events.push("revoke");
        return {};
      },
    };
    const app = createJazzAppOwner({ auth: betterAuth(client) }, async () => f.session);
    await tick();
    app.updateAuth(betterAuth(client));
    await tick();
    expect(subscriptions).toBe(1);
    expect(f.events).toEqual(["admit:one", "open:one"]);
    await expect(app.sessionActions.createLocalFirst()).rejects.toThrow("without managed auth");
    await app.logout();
    expect(f.events.slice(-3)).toEqual(["flush:one", "logout", "revoke"]);
    await app.dispose();
    expect(subscriptions).toBe(0);
  });
});
it("rejected overlapping action does not erase failed action retry", async () => {
  const f = await setup();
  const app = createJazzAppOwner({}, async () => f.session);
  await tick();
  await app.sessionActions.createLocalFirst();
  f.failLink(true);
  const first = app.sessionActions.linkJWT({ getToken: async () => "linked" });
  const second = app.sessionActions.linkJWT({ getToken: async () => "linked" });
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
  await expect(app.sessionActions.linkJWT({ getToken: async () => "linked" })).rejects.toThrow(
    "already pending",
  );
  await expect(logout).rejects.toThrow();
  f.failShutdown(false);
  await app.retry();
  expect(app.getSnapshot().status).toBe("signed-out");
  expect(f.events).not.toContain("link");
  await app.dispose();
});

it.each(["jwt", "better-auth"] as const)(
  "keeps %s callbacks out of structured-cloned host configuration",
  async (kind) => {
    const f = await setup();
    const client: BetterAuthClient = {
      $store: {
        atoms: {
          session: {
            get: () => ({
              data: { session: { id: "one" }, user: { id: "one" } },
              isPending: false,
            }),
            subscribe: () => () => {},
          },
        },
      },
      $fetch: async () => ({ data: { token: "one" } }),
      signOut: async () => ({}),
    };
    const auth =
      kind === "jwt"
        ? jwtAuth({ key: "one", getToken: async () => "one", logout: async () => {} })
        : betterAuth(client);
    const host = {
      appId: "app",
      serverUrl: "https://sync.example.test",
      initial: "local-first" as const,
      driver: { type: "memory" as const },
    };
    const config = { ...host, auth };
    let calls = 0;
    const app = createJazzAppOwner(
      config,
      async (received) => {
        calls++;
        // The runtime worker uses the platform structured-clone boundary. Permissive
        // factories miss this failure until the first authenticated client opens.
        expect(structuredClone(received)).toEqual(host);
        expect(received).not.toHaveProperty("auth");
        return f.session;
      },
      { start: false },
    );
    expect(calls).toBe(0);
    await app.start();
    await tick();
    expect(app.getSnapshot().status).toBe("ready");
    expect(f.events).toEqual(["admit:one", "open:one"]);
    expect(config.auth).toBe(auth);
    app.updateAuth(auth);
    await tick();
    expect(calls).toBe(1);
    await app.dispose();
  },
);
