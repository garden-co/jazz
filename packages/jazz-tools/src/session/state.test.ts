import { describe, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import {
  attachJazzSessionConsumer,
  createJazzSessionOwner,
  replaceJazzSessionClient,
  switchJazzSessionUser,
} from "./state.js";
import type { JWTAuth } from "../accounts/enrollment.js";
import { GracefulShutdownSyncError } from "../runtime/graceful-shutdown-error.js";
import { markThinDb } from "../runtime/local-first-upgrade.js";
const handle = (id: string) =>
  Object.freeze({ id, identity: { issuer: "test", subject: id } }) as AccountHandle;
function deferred<T = void>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
async function setup() {
  const old = handle("old"),
    next = handle("next");
  const enrollment = {
    createLocalFirst: vi.fn(() => old),
    restoreLocalFirst: vi.fn(() => old),
    logout: vi.fn(),
    loginWithBackend: vi.fn(async () => next),
    linkWithBackend: vi.fn(async (_account: AccountHandle) => next),
  };
  const accounts = new AccountManager(enrollment, old);
  const clients: { account: AccountHandle; shutdown: ReturnType<typeof vi.fn> }[] = [];
  const openClient = vi.fn(async (account: AccountHandle) => {
    const client = { account, shutdown: vi.fn(async (_options?: { waitForSync?: boolean }) => {}) };
    clients.push(client);
    return client;
  });
  const session = await createJazzSessionOwner({ accounts, openClient });
  return { session, accounts, enrollment, clients, openClient, old, next };
}
const tick = async () => {
  for (let i = 0; i < 12; i++) await Promise.resolve();
};
describe("Jazz session lifecycle", () => {
  it("refuses a malformed secret to restore without touching the current client", async () => {
    const { session, enrollment, clients } = await setup();
    const before = session.getSnapshot();
    await expect(session.restoreLocalFirst(undefined as unknown as string)).rejects.toThrow(
      "jazz-auth-v1:",
    );
    await expect(session.restoreLocalFirst("not a secret")).rejects.toThrow();
    expect(session.getSnapshot()).toBe(before);
    expect(enrollment.restoreLocalFirst).not.toHaveBeenCalled();
    expect(clients[0]!.shutdown).not.toHaveBeenCalled();
  });

  it("does not open an unsaved selection and retries its failed synchronous persistence hook", async () => {
    const selected = handle("selected");
    const failure = new Error("cannot queue selection");
    let fail = true;
    const durable = deferred();
    const selectionChanged = vi.fn(() => {
      if (fail) throw failure;
    });
    const settle = vi.fn((retry: boolean) => (retry ? durable.promise : Promise.resolve()));
    const accounts = new AccountManager<JWTAuth>(
      {
        createLocalFirst: () => selected,
      },
      undefined,
      { selectionChanged, settle },
    );
    const openClient = vi.fn(async () => ({ shutdown: async () => {} }));
    const session = await createJazzSessionOwner({ accounts, openClient });
    settle.mockClear();

    await expect(session.createLocalFirst()).rejects.toBe(failure);
    expect(session.getSnapshot()).toMatchObject({
      status: "error",
      account: selected,
      error: failure,
    });
    expect(accounts.getSnapshot().error).toBe(failure);
    expect(openClient).not.toHaveBeenCalled();
    await expect(session.retry()).rejects.toBe(failure);
    expect(selectionChanged).toHaveBeenCalledTimes(2);
    expect(settle).not.toHaveBeenCalled();
    expect(openClient).not.toHaveBeenCalled();

    fail = false;
    const retry = session.retry();
    await tick();
    expect(selectionChanged).toHaveBeenNthCalledWith(3, selected);
    expect(settle).toHaveBeenCalledExactlyOnceWith(true);
    expect(openClient).not.toHaveBeenCalled();
    durable.resolve();
    await retry;
    expect(openClient).toHaveBeenCalledExactlyOnceWith(selected);
    expect(session.getSnapshot()).toMatchObject({ status: "ready", account: selected });
    expect(accounts.getSnapshot().error).toBeUndefined();
    await session.close();
  });

  it("backend sign-in waits for shutdown and uses the shared selection lifecycle", async () => {
    const { session, clients, enrollment, next } = await setup();
    const pending = deferred();
    clients[0]!.shutdown.mockReturnValueOnce(pending.promise);
    const command = session.loginWithBackend();
    await tick();
    expect(session.getSnapshot().pending).toBe("loginWithBackend");
    expect(enrollment.loginWithBackend).not.toHaveBeenCalled();
    pending.resolve();
    await command;
    expect(enrollment.loginWithBackend).toHaveBeenCalledOnce();
    expect(enrollment.linkWithBackend).not.toHaveBeenCalled();
    expect(session.getSnapshot()).toMatchObject({ status: "ready", account: next });
    await session.close();
  });

  it("waits for committed consumer detach before sync and enrollment", async () => {
    const { session, clients, enrollment, next } = await setup();
    const lease = attachJazzSessionConsumer(session),
      previous = session.getSnapshot();
    const command = session.linkWithBackend();
    await tick();
    expect(session.getSnapshot().status).toBe("transitioning");
    expect(session.getSnapshot().client).toBeUndefined();
    lease.acknowledge(previous);
    await tick();
    expect(clients[0]!.shutdown).not.toHaveBeenCalled();
    expect(enrollment.linkWithBackend).not.toHaveBeenCalled();
    lease.acknowledge(session.getSnapshot());
    await command;
    expect(clients[0]!.shutdown).toHaveBeenCalledWith({ waitForSync: true });
    expect(session.getSnapshot().client?.account).toBe(next);
    expect(previous.client?.account.id).toBe("old");
    lease.release();
    await session.close();
  });
  it("sync failure retains usable old client and prevents enrollment", async () => {
    const { session, clients, enrollment, old } = await setup();
    const failure = new GracefulShutdownSyncError(new Error("sync failed"));
    clients[0]!.shutdown.mockRejectedValueOnce(failure);
    await expect(session.loginWithBackend()).rejects.toBe(failure);
    expect(enrollment.loginWithBackend).not.toHaveBeenCalled();
    expect(session.getSnapshot()).toMatchObject({
      status: "ready",
      account: old,
      client: clients[0],
      error: failure,
    });
    await session.close();
  });
  it("signs out past writes the server refused for a sign-in it no longer accepts", async () => {
    const { session, clients } = await setup();
    const client = clients[0]! as (typeof clients)[0] & { db?: unknown };
    client.db = {
      getAuthState: () => ({ error: "invalid" }),
      getSyncStatus: () => ({ pendingUploads: 2 }),
      getConfig: () => ({ driver: { type: "persistent" } }),
    };
    const failure = new GracefulShutdownSyncError(new Error("session missing: not signed in"));
    client.shutdown.mockRejectedValueOnce(failure);
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    await session.logout();
    expect(session.getSnapshot().status).toBe("signed-out");
    expect(client.shutdown.mock.calls).toEqual([[{ waitForSync: true }], []]);
    expect(warned).toHaveBeenCalledWith(
      expect.stringMatching(/2 write\(s\) the server refused.*stay on this device/),
      failure.cause,
    );
    warned.mockRestore();
    await session.close();
  });
  // Another user signed in through the app (no sign-out first): the server
  // refuses the first user's socket, which carries the next user's cookie,
  // so their client can't finish syncing. The switch signs them out anyway.
  it("switches to the next user past a client the server refuses", async () => {
    const { session, clients, enrollment, old, next } = await setup();
    const client = clients[0]! as (typeof clients)[0] & { db?: unknown };
    client.db = {
      getAuthState: () => ({ error: "missing" }),
      getSyncStatus: () => ({ pendingUploads: 0 }),
      getConfig: () => ({ driver: { type: "persistent" } }),
    };
    client.shutdown.mockRejectedValueOnce(
      new GracefulShutdownSyncError(new Error("session missing: not signed in")),
    );
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    await expect(switchJazzSessionUser(session, old)).resolves.toBe(true);
    expect(enrollment.logout).toHaveBeenCalled();
    expect(session.getSnapshot()).toMatchObject({ status: "ready", account: next });
    expect(session.getSnapshot().error).toBeUndefined();
    expect(clients.at(-1)!.account).toBe(next);
    expect(warned).not.toHaveBeenCalled();
    warned.mockRestore();
    await session.close();
  });
  it("keeps a sign-out failing while its writes may still upload (an outage)", async () => {
    const { session, clients, old } = await setup();
    const client = clients[0]! as (typeof clients)[0] & { db?: unknown };
    client.db = { getAuthState: () => ({}) };
    const failure = new GracefulShutdownSyncError(new Error("websocket transport error"));
    client.shutdown.mockRejectedValueOnce(failure);
    await expect(session.logout()).rejects.toBe(failure);
    expect(session.getSnapshot()).toMatchObject({ status: "ready", account: old, error: failure });
    await session.close();
  });
  it("failed link reopens old selection and preserves primary error", async () => {
    const { session, clients, enrollment, old } = await setup();
    const failure = new Error("link failed");
    enrollment.linkWithBackend.mockRejectedValueOnce(failure);
    await expect(session.linkWithBackend()).rejects.toBe(failure);
    expect(session.getSnapshot()).toMatchObject({
      status: "ready",
      account: old,
      client: clients[1],
      error: failure,
    });
    expect(clients[0]).not.toBe(clients[1]);
    await session.close();
  });
  it("failed recovery reports startup error without hiding enrollment rejection", async () => {
    const { session, enrollment, openClient, old } = await setup();
    const primary = new Error("link failed"),
      startup = new Error("reopen failed");
    enrollment.linkWithBackend.mockRejectedValueOnce(primary);
    openClient.mockRejectedValueOnce(startup);
    await expect(session.linkWithBackend()).rejects.toBe(primary);
    expect(session.getSnapshot()).toMatchObject({ status: "error", account: old, error: startup });
    await session.retry();
    expect(session.getSnapshot().status).toBe("ready");
    await session.close();
  });
  it("successful registry action with failed open retains new selection for retry", async () => {
    const { session, enrollment, openClient, next } = await setup();
    openClient.mockRejectedValueOnce(new Error("open failed"));
    await expect(session.linkWithBackend()).rejects.toThrow("open failed");
    expect(session.getSnapshot()).toMatchObject({ status: "error", account: next });
    expect(session.getSnapshot().client).toBeUndefined();
    await session.retry();
    expect(enrollment.linkWithBackend).toHaveBeenCalledTimes(1);
    expect(session.getSnapshot().client?.account).toBe(next);
    await session.close();
  });
  it("rejects overlapping commands without queuing mutations", async () => {
    const { session, enrollment } = await setup();
    const first = session.loginWithBackend();
    await expect(session.createLocalFirst()).rejects.toThrow("already pending");
    await first;
    expect(enrollment.createLocalFirst).not.toHaveBeenCalled();
    await session.close();
  });
  it("logout fences pending enrollment and stays signed-out", async () => {
    const { session, enrollment, next, openClient, accounts } = await setup();
    const pending = deferred<AccountHandle>();
    enrollment.loginWithBackend.mockReturnValueOnce(pending.promise);
    const command = session.loginWithBackend(),
      rejected = expect(command).rejects.toThrow("superseded");
    await tick();
    const logout = session.logout();
    pending.resolve(next);
    await rejected;
    await logout;
    expect(session.getSnapshot().status).toBe("signed-out");
    expect(accounts.getLoggedIn()).toBeUndefined();
    expect(openClient).toHaveBeenCalledTimes(1);
    expect(enrollment.createLocalFirst).not.toHaveBeenCalled();
    await session.close();
  });
  it("close disposes late opened client without resurrection", async () => {
    const { session, openClient, next } = await setup();
    const pending = deferred<Awaited<ReturnType<typeof openClient>>>();
    openClient.mockReturnValueOnce(pending.promise);
    const command = session.loginWithBackend(),
      rejected = expect(command).rejects.toThrow("superseded");
    await tick();
    const close = session.close();
    const late = { account: next, shutdown: vi.fn(async () => {}) };
    pending.resolve(late);
    await rejected;
    await close;
    expect(late.shutdown).toHaveBeenCalledOnce();
    expect(session.getSnapshot()).toEqual({ status: "closed" });
    await expect(session.retry()).rejects.toThrow("closed");
  });
  it("release unblocks close while transition awaits detach", async () => {
    const { session, enrollment, clients } = await setup();
    const lease = attachJazzSessionConsumer(session);
    const command = session.loginWithBackend(),
      rejected = expect(command).rejects.toThrow("superseded");
    await tick();
    const close = session.close();
    lease.release();
    await rejected;
    await close;
    expect(enrollment.loginWithBackend).not.toHaveBeenCalled();
    expect(clients[0]!.shutdown).toHaveBeenCalledWith();
  });
  it("close before command starts never republishes a transition", async () => {
    const { session, enrollment } = await setup();
    const command = session.loginWithBackend(),
      rejected = expect(command).rejects.toThrow("superseded");
    await session.close();
    await rejected;
    expect(session.getSnapshot().status).toBe("closed");
    expect(enrollment.loginWithBackend).not.toHaveBeenCalled();
  });
  it("defaults signed-out and binds commands", async () => {
    const account = handle("local"),
      create = vi.fn(() => account);
    const accounts = new AccountManager<JWTAuth>({
      createLocalFirst: create,
    });
    const session = await createJazzSessionOwner({
      accounts,
      openClient: async () => ({ shutdown: async () => {} }),
    });
    const { createLocalFirst, getSnapshot, close } = session;
    expect(getSnapshot().status).toBe("signed-out");
    expect(create).not.toHaveBeenCalled();
    await createLocalFirst();
    expect(getSnapshot().status).toBe("ready");
    expect(Object.isFrozen(getSnapshot())).toBe(true);
    expect(getSnapshot()).toBe(getSnapshot());
    await close();
  });
  it("hides a client after cleanup failure and prevents registry mutation", async () => {
    const { session, clients, enrollment } = await setup();
    const failure = new Error("cleanup failed");
    clients[0]!.shutdown.mockRejectedValue(failure);
    await expect(session.linkWithBackend()).rejects.toBe(failure);
    expect(session.getSnapshot().status).toBe("error");
    expect(session.getSnapshot().client).toBeUndefined();
    await expect(session.retry()).rejects.toBe(failure);
    expect(enrollment.linkWithBackend).not.toHaveBeenCalled();
    await expect(session.close()).rejects.toBe(failure);
    expect(session.getSnapshot().status).toBe("closed");
  });
  it("reserves logout before notifying reentrant command subscribers", async () => {
    const { session, enrollment } = await setup();
    let attempted: Promise<void> | undefined;
    const stop = session.subscribe(() => {
      if (!attempted)
        attempted = expect(session.loginWithBackend()).rejects.toThrow("already pending");
    });
    await session.logout();
    await attempted;
    stop();
    expect(enrollment.loginWithBackend).not.toHaveBeenCalled();
    expect(session.getSnapshot().status).toBe("signed-out");
    await session.close();
  });
  it("reentrant close observes the same close promise", async () => {
    const { session, clients } = await setup();
    let reentrant: Promise<void> | undefined;
    session.subscribe(() => {
      reentrant = session.close();
    });
    const closing = session.close();
    expect(reentrant).toBe(closing);
    await closing;
    expect(clients[0]!.shutdown).toHaveBeenCalledOnce();
  });
  it("subscriber close fences a command before enrollment", async () => {
    const { session, enrollment } = await setup();
    let closing: Promise<void> | undefined;
    session.subscribe(() => {
      if (session.getSnapshot().status === "transitioning") closing = session.close();
    });
    await expect(session.loginWithBackend()).rejects.toThrow("superseded");
    await closing;
    expect(enrollment.loginWithBackend).not.toHaveBeenCalled();
    expect(session.getSnapshot().status).toBe("closed");
  });
});

it("clearOnSignOut wipes the signed-out account's storage instead of syncing it", async () => {
  const old = handle("old");
  const accounts = new AccountManager(
    {
      createLocalFirst: () => old,
      restoreLocalFirst: () => old,
      logout() {},
      loginWithBackend: async () => old,
    },
    old,
  );
  const calls: string[] = [];
  const openClient = async () => ({
    db: { deleteClientStorage: async () => void calls.push("wipe") },
    shutdown: async (options?: { waitForSync?: boolean }) =>
      void calls.push(options?.waitForSync ? "sync+shutdown" : "shutdown"),
  });
  const session = await createJazzSessionOwner({ accounts, openClient, clearOnSignOut: true });
  await session.logout();
  expect(calls).toEqual(["wipe", "shutdown"]);
  expect(session.getSnapshot().status).toBe("signed-out");
  await session.close();
});

it("clearOnSignOut says how many writes its wipe takes", async () => {
  const old = handle("old");
  const accounts = new AccountManager(
    {
      createLocalFirst: () => old,
      restoreLocalFirst: () => old,
      logout() {},
      loginWithBackend: async () => old,
    },
    old,
  );
  const openClient = async () => ({
    db: {
      // The server didn't take them within the wait (it refuses this user).
      waitForSync: async () => Promise.reject(new Error("timed out")),
      getSyncStatus: () => ({ pendingUploads: 3 }),
      deleteClientStorage: async () => {},
    },
    shutdown: async () => {},
  });
  const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
  const session = await createJazzSessionOwner({ accounts, openClient, clearOnSignOut: true });
  await session.logout();
  expect(session.getSnapshot().status).toBe("signed-out");
  expect(warned).toHaveBeenCalledWith(expect.stringMatching(/wiped 3 write\(s\)/));
  warned.mockRestore();
  await session.close();
});

it("clearOnSignOut leaves a thin client's writes to sync, as it stores nothing", async () => {
  const old = handle("old");
  const accounts = new AccountManager(
    {
      createLocalFirst: () => old,
      restoreLocalFirst: () => old,
      logout() {},
      loginWithBackend: async () => old,
    },
    old,
  );
  const calls: string[] = [];
  const openClient = async () => {
    const db = { deleteClientStorage: async () => void calls.push("wipe") };
    markThinDb(db as never);
    return {
      db,
      shutdown: async (options?: { waitForSync?: boolean }) =>
        void calls.push(options?.waitForSync ? "sync+shutdown" : "shutdown"),
    };
  };
  const session = await createJazzSessionOwner({ accounts, openClient, clearOnSignOut: true });
  await session.logout();
  expect(calls).toEqual(["sync+shutdown"]);
  await session.close();
});

describe("replacing the signed-in client", () => {
  it("publishes the new client, then shuts the old one down after its writes sync", async () => {
    const { session, clients, old } = await setup();
    const current = session.getSnapshot().client!;
    const replacement = { account: old, shutdown: vi.fn(async () => {}) };
    await replaceJazzSessionClient(session, async (client, account) => {
      expect(client).toBe(current);
      expect(account).toBe(old);
      return replacement;
    });
    expect(session.getSnapshot()).toMatchObject({ status: "ready", client: replacement });
    expect(clients[0]!.shutdown).toHaveBeenCalledWith({ waitForSync: true });
    // The session now owns the replacement: logging out shuts it down.
    await session.logout();
    expect(replacement.shutdown).toHaveBeenCalled();
  });

  it("completes the swap when the old client fails to shut down", async () => {
    const { session, clients } = await setup();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    clients[0]!.shutdown.mockRejectedValueOnce(new Error("sync failed"));
    const replacement = { shutdown: vi.fn(async () => {}) };
    await replaceJazzSessionClient(session, async () => replacement as never);
    expect(session.getSnapshot()).toMatchObject({ status: "ready", client: replacement });
    await vi.waitFor(() => expect(warn).toHaveBeenCalled());
    warn.mockRestore();
    await session.close();
  });

  it("drops the new client when the session logs out while it opens", async () => {
    const { session, clients } = await setup();
    const opening = deferred<{ shutdown: ReturnType<typeof vi.fn> }>();
    const replacing = replaceJazzSessionClient(session, () => opening.promise as never);
    await tick();
    const logout = session.logout();
    const replacement = { shutdown: vi.fn(async () => {}) };
    opening.resolve(replacement);
    await expect(replacing).rejects.toThrow("superseded");
    await logout;
    expect(replacement.shutdown).toHaveBeenCalled();
    expect(clients[0]!.shutdown).toHaveBeenCalled();
    expect(session.getSnapshot().status).toBe("signed-out");
  });

  it("refuses without a signed-in client", async () => {
    const { session } = await setup();
    await session.logout();
    await expect(
      replaceJazzSessionClient(session, async () => ({ shutdown: async () => {} })),
    ).rejects.toThrow("no signed-in client");
  });
});

describe("Jazz session: failures that leave a client usable (K2)", () => {
  async function wipingSetup() {
    const old = handle("old");
    const enrollment = {
      createLocalFirst: vi.fn(() => old),
      restoreLocalFirst: vi.fn(() => old),
      logout: vi.fn(),
      loginWithBackend: vi.fn(async () => handle("next")),
      linkWithBackend: vi.fn(async (_account: AccountHandle) => handle("next")),
    };
    const accounts = new AccountManager(enrollment, old);
    let usable = true;
    const db = {
      waitForSync: vi.fn(async () => {}),
      deleteClientStorage: vi.fn(async () => {}),
      getSyncStatus: () => ({ pendingUploads: 0 }),
      markReplaced: vi.fn(),
      isUsable: () => usable,
    };
    const client = {
      db,
      shutdown: vi.fn(async (_options?: { waitForSync?: boolean }) => {}),
    };
    const session = await createJazzSessionOwner({
      accounts,
      clearOnSignOut: true,
      openClient: async () => client,
    });
    return { session, client, db, enrollment, old, setUsable: (next: boolean) => (usable = next) };
  }

  it("stays ready with the client when an offline sign-out can't wipe storage", async () => {
    const { session, client, db, enrollment, old } = await wipingSetup();
    const failure = new Error("deleteClientStorage(): the browser worker is not connected");
    db.deleteClientStorage.mockRejectedValueOnce(failure);
    await expect(session.logout()).rejects.toBe(failure);
    expect(enrollment.logout).not.toHaveBeenCalled();
    expect(client.shutdown).not.toHaveBeenCalled();
    expect(session.getSnapshot()).toMatchObject({
      status: "ready",
      account: old,
      client,
      error: failure,
      recovery: "action",
    });
    // Tried again once the worker is back, it signs out.
    await session.logout();
    expect(session.getSnapshot().status).toBe("signed-out");
  });

  it("stays ready with the client when a sign-in's teardown fails before it began", async () => {
    const { session, client, enrollment, old } = await wipingSetup();
    // Not one of the known graceful errors, but the Db says it still works.
    const failure = new Error("flushLocal: worker unreachable");
    client.shutdown.mockRejectedValueOnce(failure);
    await expect(session.loginWithBackend()).rejects.toBe(failure);
    expect(enrollment.loginWithBackend).not.toHaveBeenCalled();
    expect(session.getSnapshot()).toMatchObject({
      status: "ready",
      account: old,
      client,
      error: failure,
      recovery: "action",
    });
  });

  it("ends a sign-in whose teardown began and failed in error, with no client", async () => {
    const { session, client, setUsable } = await wipingSetup();
    const failure = new Error("runtime shutdown failed");
    client.shutdown.mockImplementationOnce(async () => {
      setUsable(false);
      throw failure;
    });
    await expect(session.loginWithBackend()).rejects.toBe(failure);
    expect(session.getSnapshot()).toMatchObject({ status: "error", error: failure });
    expect(session.getSnapshot().client).toBeUndefined();
  });

  it("still signs out when the sign-out's teardown began and failed", async () => {
    const { session, client, db, enrollment, setUsable } = await wipingSetup();
    const failure = new Error("runtime shutdown failed");
    client.shutdown.mockImplementationOnce(async () => {
      setUsable(false);
      throw failure;
    });
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    await session.logout();
    expect(enrollment.logout).toHaveBeenCalledOnce();
    expect(db.markReplaced).toHaveBeenCalledWith("signed-out");
    expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
    expect(session.getSnapshot().error).toBeUndefined();
    expect(error).toHaveBeenCalledWith(
      "Jazz signed out, but closing the signed-out client failed.",
      failure,
    );
    error.mockRestore();
    expect(session.getSnapshot().client).toBeUndefined();
  });
});

describe("Jazz session: a clearOnSignOut wipe that didn't finish", () => {
  it("signs out and says the wipe may not have finished", async () => {
    const old = handle("old");
    const enrollment = {
      createLocalFirst: vi.fn(() => old),
      restoreLocalFirst: vi.fn(() => old),
      logout: vi.fn(),
      loginWithBackend: vi.fn(async () => handle("next")),
      linkWithBackend: vi.fn(async (_account: AccountHandle) => handle("next")),
    };
    let usable = true;
    const failure = new Error("IndexedDB deletion was blocked");
    const client = {
      db: {
        waitForSync: vi.fn(async () => {}),
        // The worker started the reset (the client is gone), then failed.
        deleteClientStorage: vi.fn(async () => {
          usable = false;
          throw failure;
        }),
        getSyncStatus: () => ({ pendingUploads: 0 }),
        markReplaced: vi.fn(),
        isUsable: () => usable,
      },
      shutdown: vi.fn(async () => {}),
    };
    const session = await createJazzSessionOwner({
      accounts: new AccountManager(enrollment, old),
      clearOnSignOut: true,
      openClient: async () => client,
    });
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    await session.logout();
    error.mockRestore();
    expect(enrollment.logout).toHaveBeenCalledOnce();
    const snapshot = session.getSnapshot();
    expect(snapshot.status).toBe("signed-out");
    expect(snapshot.error?.message).toMatch(/couldn't finish wiping this device's data/);
    expect(snapshot.error?.cause).toBe(failure);
  });
});
