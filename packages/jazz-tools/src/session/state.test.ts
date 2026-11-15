import { describe, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import {
  attachSessionCoreConsumer,
  createJazzSessionOwner,
  PENDING_WIPE_FAILURE_SPACING_MS,
  SESSION_VIEW_ACKNOWLEDGE_TIMEOUT_MS,
  replaceJazzSessionClient,
  switchJazzSessionUser,
} from "./state.js";
import {
  createAccountManagerWithRuntime,
  onAccountInvalidated,
  type JWTAuth,
} from "../accounts/enrollment.js";
import { GracefulShutdownSyncError } from "../runtime/graceful-shutdown-error.js";
import {
  isStoreHeldByAnotherTabError,
  StoreHeldByAnotherTabError,
} from "../runtime/store-held-by-another-tab-error.js";
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
    const lease = attachSessionCoreConsumer(session),
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
  it("moves on without a view that never acknowledges, names it, and stops waiting for it", async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const { session, clients, enrollment, next } = await setup();
      // A view suspended behind Suspense: attached, but it never acknowledges.
      const lease = attachSessionCoreConsumer(session, "React <JazzProvider>");
      const command = session.loginWithBackend();
      await vi.advanceTimersByTimeAsync(SESSION_VIEW_ACKNOWLEDGE_TIMEOUT_MS - 1);
      expect(session.getSnapshot().status).toBe("transitioning");
      expect(clients[0]!.shutdown).not.toHaveBeenCalled();
      expect(warn).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      await command;
      expect(enrollment.loginWithBackend).toHaveBeenCalledOnce();
      expect(session.getSnapshot()).toMatchObject({ status: "ready", account: next });
      expect(warn).toHaveBeenCalledOnce();
      expect(warn.mock.calls[0]![0]).toContain("Jazz moved on without React <JazzProvider>");
      // Stale: the next transition doesn't wait for it again.
      const logout = session.logout();
      await vi.advanceTimersByTimeAsync(0);
      await logout;
      expect(session.getSnapshot().status).toBe("signed-out");
      expect(warn).toHaveBeenCalledOnce();
      // Once it renders the session again, transitions wait for it again.
      lease.acknowledge(session.getSnapshot());
      const signIn = session.loginWithBackend();
      await vi.advanceTimersByTimeAsync(0);
      expect(session.getSnapshot().status).toBe("transitioning");
      lease.acknowledge(session.getSnapshot());
      await signIn;
      expect(session.getSnapshot().status).toBe("ready");
      expect(warn).toHaveBeenCalledOnce();
      lease.release();
      await session.close();
    } finally {
      warn.mockRestore();
      vi.useRealTimers();
    }
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
  // A new sign-in that isn't a sign-out (the core lost a refused user's
  // account, so the watch signs them in again) from a client the server
  // refuses: its writes can't upload, and the operation must not fail (and
  // be retried) for good while the refusal lasts.
  it("signs in again past a thin client the server refuses, dropping its pending write", async () => {
    const { session, clients, next } = await setup();
    const client = clients[0]! as (typeof clients)[0] & { db?: unknown };
    const db = {
      getAuthState: () => ({ error: "revoked" }),
      getSyncStatus: () => ({ pendingUploads: 1 }),
    };
    markThinDb(db as never);
    client.db = db;
    const failure = new GracefulShutdownSyncError(new Error("session revoked: revokeUser"));
    client.shutdown.mockRejectedValueOnce(failure);
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    await session.loginWithBackend();
    expect(session.getSnapshot()).toMatchObject({ status: "ready", account: next });
    expect(session.getSnapshot().error).toBeUndefined();
    // Its drain failed on the refusal, so it stops without waiting.
    expect(client.shutdown.mock.calls).toEqual([[{ waitForSync: true }], []]);
    expect(warned).toHaveBeenCalledWith(
      expect.stringMatching(
        /closed the previous client and dropped 1 write\(s\) the server refused/,
      ),
      failure.cause,
    );
    warned.mockRestore();
    await session.close();
  });
  it("closes a replaced client the server refuses instead of leaving it open", async () => {
    const { session, clients } = await setup();
    const client = clients[0]! as (typeof clients)[0] & { db?: unknown };
    client.db = {
      getAuthState: () => ({ error: "revoked" }),
      getSyncStatus: () => ({ pendingUploads: 0 }),
      getConfig: () => ({ driver: { type: "persistent" } }),
    };
    client.shutdown.mockRejectedValueOnce(
      new GracefulShutdownSyncError(new Error("session revoked: revokeUser")),
    );
    const replacement = { account: client.account, shutdown: vi.fn(async () => {}) };
    await replaceJazzSessionClient(session, async () => replacement as never);
    await tick();
    expect(session.getSnapshot().client).toBe(replacement);
    expect(client.shutdown.mock.calls).toEqual([[{ waitForSync: true }], []]);
    await session.close();
  });
  it("signs out offline, then uploads the signed-out user's writes once the server is back", async () => {
    vi.useFakeTimers();
    try {
      const { session, clients, enrollment, openClient } = await setup();
      const client = clients[0]! as (typeof clients)[0] & { db?: unknown };
      let onStatus: ((status: { link: { state: string } }) => void) | undefined;
      let replaced = false;
      client.db = {
        getAuthState: () => ({}),
        // As a Db reads: unusable once the session marked it replaced.
        isUsable: () => !replaced,
        markReplaced: vi.fn(() => (replaced = true)),
        onSyncStatusChange: (listener: typeof onStatus) => {
          onStatus = listener;
          return () => (onStatus = undefined);
        },
      };
      const failure = new GracefulShutdownSyncError(new Error("websocket transport error"));
      client.shutdown.mockRejectedValue(failure);
      // The local part is done: the sign-out resolves, and the session
      // shows that the server's part waits.
      await session.logout();
      expect(enrollment.logout).toHaveBeenCalled();
      expect(session.getSnapshot()).toMatchObject({
        status: "signing-out",
        error: failure,
        recovery: "action",
      });
      expect(session.getSnapshot().account).toBeUndefined();
      expect(session.getSnapshot().client).toBeUndefined();
      expect(
        (client.db as { markReplaced: ReturnType<typeof vi.fn> }).markReplaced,
      ).toHaveBeenCalledWith("signed-out");
      // Nothing signs in meanwhile, and signing out again joins it.
      await expect(session.createLocalFirst()).rejects.toMatchObject({
        name: "JazzSessionOperationError",
        reason: "signing-out",
      });
      await expect(session.loginWithBackend()).rejects.toMatchObject({ reason: "signing-out" });
      await session.logout();
      expect(openClient).toHaveBeenCalledTimes(1);
      // Still offline: the backoff tries again, and it keeps waiting.
      await vi.advanceTimersByTimeAsync(2_000);
      expect(client.shutdown).toHaveBeenCalledTimes(2);
      expect(session.getSnapshot().status).toBe("signing-out");
      // Back online: reconnecting uploads the writes and ends the sign-out.
      client.shutdown.mockResolvedValue(undefined);
      onStatus?.({ link: { state: "connected" } });
      await vi.advanceTimersByTimeAsync(0);
      expect(client.shutdown).toHaveBeenLastCalledWith({ waitForSync: true });
      expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
      expect(session.getSnapshot().error).toBeUndefined();
      expect(onStatus).toBeUndefined();
      await vi.advanceTimersByTimeAsync(120_000);
      expect(client.shutdown).toHaveBeenCalledTimes(3);
      await session.createLocalFirst();
      expect(session.getSnapshot().status).toBe("ready");
      await session.close();
    } finally {
      vi.useRealTimers();
    }
  });
  // As the hosts' account Db does (accounts/context.ts): when its account's
  // credential is revoked, it stops waiting for sync and tears itself down.
  async function realEnrollmentSetup(
    hostOptions: {
      deleteSignedOutStore?: (
        account: string,
        finishing?: { wanted(): boolean; forget(): Promise<void> },
      ) => Promise<"deleted" | "held" | "cancelled">;
      pendingDiscards?: {
        record(account: string): void;
        clear(account: string): void;
        list(): string[];
      };
    } = {},
  ) {
    // The account store's saves, which a test can make fail.
    const save = { fail: false };
    let created = 0;
    const accounts = createAccountManagerWithRuntime({
      selectionDurability: {
        selectionChanged() {},
        settle: async () => {
          if (save.fail) throw new Error("storage full");
        },
      },
      registry: "https://core.example/apps/test/accounts",
      localFirst: {
        create: () => ({
          // A new device account each time.
          accountId: `00000000-0000-4000-8000-00000000000${++created}`,
          identity: { issuer: "urn:jazz:local-first", subject: "local" },
          auth: "e30.e30.signature",
        }),
      },
    });
    const net = { online: false };
    const opened: {
      torn: boolean;
      invalidated: boolean;
      uploaded: boolean;
      deleted: boolean;
      shutdown: ReturnType<typeof vi.fn>;
      db: Record<string, unknown>;
    }[] = [];
    const session = await createJazzSessionOwner({
      accounts,
      ...hostOptions,
      openClient: async (account) => {
        const client = {
          torn: false,
          invalidated: false,
          uploaded: false,
          deleted: false,
          db: {
            getConfig: (): { driver?: { type?: string } } => ({ driver: { type: "persistent" } }),
            getAuthState: () => ({}),
            getSyncStatus: () => ({ pendingUploads: client.uploaded ? 0 : 1 }),
            deleteClientStorage: vi.fn(async () => {
              client.deleted = true;
            }),
            abortGracefulShutdown: () => {},
          },
          shutdown: vi.fn(async (options?: { waitForSync?: boolean }) => {
            // A torn-down client uploads nothing more.
            if (client.torn) return;
            if (options?.waitForSync) {
              if (!net.online)
                throw new GracefulShutdownSyncError(new Error("websocket transport error"));
              client.uploaded = true;
            }
            client.torn = true;
          }),
        };
        onAccountInvalidated(account, () => {
          client.invalidated = true;
          void client.shutdown();
        });
        opened.push(client);
        return client;
      },
    });
    return { session, net, opened, save };
  }
  it("keeps a signing-out client's credential until its writes uploaded", async () => {
    const { session, net, opened } = await realEnrollmentSetup();
    await session.createLocalFirst();
    const client = opened.at(-1)!;
    await session.logout();
    // Signed out, but the revocation waits for the upload: the client is
    // still open, not torn down by its account's invalidation.
    expect(session.getSnapshot().status).toBe("signing-out");
    expect(client.invalidated).toBe(false);
    expect(client.torn).toBe(false);
    await expect(session.retry()).rejects.toBeInstanceOf(GracefulShutdownSyncError);
    expect(session.getSnapshot().status).toBe("signing-out");
    net.online = true;
    await session.retry();
    expect(client.uploaded).toBe(true);
    expect(session.getSnapshot().status).toBe("signed-out");
    // Revoked once the upload ended.
    expect(client.invalidated).toBe(true);
    await session.close();
  });
  it("discards a signing-out user's unsynced writes and signs out, so another can sign in", async () => {
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const { session, opened } = await realEnrollmentSetup();
      await session.createLocalFirst();
      const client = opened.at(-1)!;
      await session.logout();
      expect(session.getSnapshot().status).toBe("signing-out");
      await session.logout({ discardUnsynced: true });
      expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
      expect(session.getSnapshot().error).toBeUndefined();
      expect(client.uploaded).toBe(false);
      expect(client.deleted).toBe(true);
      expect(client.torn).toBe(true);
      expect(client.invalidated).toBe(true);
      expect(warned).toHaveBeenCalledWith(
        expect.stringMatching(/wiped 1 write\(s\).*discardUnsynced/),
      );
      // Someone else signs in, still offline.
      await session.createLocalFirst();
      expect(session.getSnapshot().status).toBe("ready");
      expect(session.getSnapshot().client).toBe(opened.at(-1));
      expect(opened.at(-1)).not.toBe(client);
      await session.close();
    } finally {
      warned.mockRestore();
    }
  });
  it("discards unsynced writes on sign-out without waiting for the server", async () => {
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const { session, opened } = await realEnrollmentSetup();
      await session.createLocalFirst();
      const client = opened.at(-1)!;
      await session.logout({ discardUnsynced: true });
      expect(session.getSnapshot().status).toBe("signed-out");
      expect(client.shutdown).not.toHaveBeenCalledWith({ waitForSync: true });
      expect(client.deleted).toBe(true);
      expect(client.torn).toBe(true);
      await session.close();
    } finally {
      warned.mockRestore();
    }
  });
  it("discards a memory client's writes without a wipe, from ready and from signing-out", async () => {
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      for (const from of ["ready", "signing-out"] as const) {
        const { session, net, opened } = await realEnrollmentSetup();
        await session.createLocalFirst();
        const client = opened.at(-1)!;
        // As Node and React Native's memory driver: nothing to delete, and
        // deleting throws.
        client.db.getConfig = () => ({ driver: { type: "memory" } });
        client.db.deleteClientStorage = vi.fn(async () => {
          throw new Error("deleteClientStorage() is only available when driver.type='persistent'.");
        });
        net.online = false;
        if (from === "signing-out") {
          await session.logout();
          expect(session.getSnapshot().status).toBe("signing-out");
        }
        await session.logout({ discardUnsynced: true });
        expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
        expect(session.getSnapshot().error).toBeUndefined();
        expect(client.db.deleteClientStorage).not.toHaveBeenCalled();
        expect(client.torn).toBe(true);
        await session.close();
      }
    } finally {
      warned.mockRestore();
    }
  });
  it("wipes by account after the client closed, even when the upload finished as discard was asked", async () => {
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const deleteSignedOutStore = vi.fn(async (_account: string) => "deleted" as const);
      const { session, net, opened } = await realEnrollmentSetup({ deleteSignedOutStore });
      await session.createLocalFirst();
      const client = opened.at(-1)!;
      const account = session.getSnapshot().account!;
      await session.logout();
      // The next upload goes through, and finishes just as the app discards.
      const upload = deferred();
      client.shutdown.mockImplementationOnce(async (options?: { waitForSync?: boolean }) => {
        await upload.promise;
        client.uploaded = !!options?.waitForSync;
        client.torn = true;
      });
      net.online = true;
      const retrying = session.retry();
      const discarding = session.logout({ discardUnsynced: true });
      upload.resolve();
      await retrying.catch(() => {});
      await discarding;
      expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
      expect(session.getSnapshot().error).toBeUndefined();
      // Through the host, by account, once the client was down: no
      // delete through a client that may be shutting down.
      expect(deleteSignedOutStore).toHaveBeenCalledWith(account.id);
      expect(client.db.deleteClientStorage).not.toHaveBeenCalled();
      await session.close();
    } finally {
      warned.mockRestore();
    }
  });
  it("tells the app when another tab holds off a discard's wipe, and retry() finishes it", async () => {
    let holder: "held" | "deleted" = "held";
    const deleteSignedOutStore = vi.fn(async (_account: string) => holder);
    const { session, opened } = await realEnrollmentSetup({ deleteSignedOutStore });
    await session.createLocalFirst();
    const a = session.getSnapshot().account!;
    const client = opened.at(-1)!;
    await session.logout({ discardUnsynced: true });
    // Signed out, but the data stays while that tab has it: the app is told.
    expect(session.getSnapshot()).toMatchObject({
      status: "signed-out",
      recovery: "action",
      error: { message: expect.stringContaining("another tab still has this account open") },
    });
    expect(client.torn).toBe(true);
    await expect(session.retry()).rejects.toThrow("another tab still has this account open");
    expect(session.getSnapshot().error).toBeDefined();
    // That tab closed: retrying wipes it.
    holder = "deleted";
    await session.retry();
    expect(deleteSignedOutStore).toHaveBeenLastCalledWith(a.id, expect.anything());
    expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
    expect(session.getSnapshot().error).toBeUndefined();
    await session.close();
  });
  it("doesn't bring a held-off discard back after another account's session", async () => {
    const deleteSignedOutStore = vi.fn(async (_account: string) => "held" as const);
    const pending = new Set<string>();
    const pendingDiscards = {
      record: (account: string) => void pending.add(account),
      clear: (account: string) => void pending.delete(account),
      list: () => [...pending],
    };
    const { session, net } = await realEnrollmentSetup({ deleteSignedOutStore, pendingDiscards });
    net.online = true;
    await session.createLocalFirst();
    const a = session.getSnapshot().account!;
    await session.logout({ discardUnsynced: true });
    expect(session.getSnapshot().error?.message).toContain(
      "another tab still has this account open",
    );
    expect([...pending]).toEqual([a.id]);
    // B signs in and out while the other tab still has A open.
    await session.createLocalFirst();
    expect(session.getSnapshot().account?.id).not.toBe(a.id);
    await session.logout();
    expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
    // A's error stays gone: finishing A is the pending entry's, which only
    // wipes once no tab holds A's store.
    expect(session.getSnapshot().error).toBeUndefined();
    expect([...pending]).toEqual([a.id]);
    for (const [account] of deleteSignedOutStore.mock.calls.slice(1)) expect(account).toBe(a.id);
    await session.retry();
    expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
    expect(session.getSnapshot().error).toBeUndefined();
    await session.close();
  });
  it("doesn't wipe a pending account someone signed in as while a pass was busy with another", async () => {
    // Two discards earlier pages left pending, on a page that starts signed out.
    const pending = new Set(["discarded-1", "discarded-2"]);
    const pendingDiscards = {
      record: (account: string) => void pending.add(account),
      clear: (account: string) => void pending.delete(account),
      list: () => [...pending],
    };
    const first = deferred();
    const deleted: string[] = [];
    const forgotten: string[] = [];
    const deleteSignedOutStore = vi.fn(
      async (account: string, finishing?: { wanted(): boolean; forget(): Promise<void> }) => {
        if (account === "discarded-1") await first.promise;
        // As the browser host does, inside the store's exclusive lock.
        if (finishing && !finishing.wanted()) return "cancelled" as const;
        deleted.push(account);
        await finishing?.forget();
        forgotten.push(account);
        return "deleted" as const;
      },
    );
    const { session } = await realEnrollmentSetup({ deleteSignedOutStore, pendingDiscards });
    await tick();
    expect(deleteSignedOutStore).toHaveBeenCalledWith("discarded-1", expect.anything());
    // Meanwhile another tab signs in as the second account (clearing its
    // entry), and lets go of its store again.
    pendingDiscards.clear("discarded-2");
    first.resolve();
    await tick();
    await tick();
    expect(deleted).toEqual(["discarded-1"]);
    expect(forgotten).toEqual(["discarded-1"]);
    expect(pending.size).toBe(0);
    await session.close();
  });
  it("keeps a discard whose wipe fails pending and visible until retry() wipes it", async () => {
    const errors = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      let broken = true;
      const forgotten: string[] = [];
      const deleteSignedOutStore = vi.fn(
        async (account: string, finishing?: { wanted(): boolean; forget(): Promise<void> }) => {
          if (broken) throw new Error("IndexedDB unavailable");
          if (finishing && !finishing.wanted()) return "cancelled" as const;
          await finishing?.forget();
          forgotten.push(account);
          return "deleted" as const;
        },
      );
      const pending = new Set<string>();
      const pendingDiscards = {
        record: (account: string) => void pending.add(account),
        clear: (account: string) => void pending.delete(account),
        list: () => [...pending],
      };
      const { session } = await realEnrollmentSetup({ deleteSignedOutStore, pendingDiscards });
      await session.createLocalFirst();
      const a = session.getSnapshot().account!;
      await session.logout({ discardUnsynced: true });
      // Signed out, but the data is still here: the app sees it, and it stays pending.
      expect(session.getSnapshot()).toMatchObject({
        status: "signed-out",
        recovery: "action",
        error: { message: expect.stringContaining("couldn't wipe the signed-out account's data") },
      });
      expect([...pending]).toEqual([a.id]);
      await expect(session.retry()).rejects.toThrow("couldn't wipe");
      expect([...pending]).toEqual([a.id]);
      expect(session.getSnapshot().error).toBeDefined();
      broken = false;
      await session.retry();
      expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
      expect(session.getSnapshot().error).toBeUndefined();
      expect(pending.size).toBe(0);
      expect(forgotten).toEqual([a.id]);
      await session.close();
    } finally {
      errors.mockRestore();
    }
  });
  it("stops retrying a failing wipe on its own, but never drops it", async () => {
    const errors = vi.spyOn(console, "error").mockImplementation(() => {});
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      // A discard an earlier page left, whose wipe keeps failing.
      const pending = new Set(["discarded"]);
      const pendingDiscards = {
        record: (account: string) => void pending.add(account),
        clear: (account: string) => void pending.delete(account),
        list: () => [...pending],
      };
      const deleteSignedOutStore = vi.fn(async (_account: string) => {
        throw new Error("IndexedDB unavailable");
      });
      let now = 1_000_000;
      const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
      const { session, net } = await realEnrollmentSetup({ deleteSignedOutStore, pendingDiscards });
      net.online = true;
      await tick();
      // Each settled operation runs another pass. Quick passes count once:
      // a failure that passes can't use the attempts up in seconds.
      for (let i = 0; i < 3; i++) {
        await session.createLocalFirst();
        await tick();
        await session.logout();
        await tick();
      }
      expect(deleteSignedOutStore).toHaveBeenCalledTimes(7);
      expect(session.getSnapshot().error).toBeUndefined();
      // Failures spread over time (at least 10 minutes in all) give up.
      for (let i = 0; i < 4; i++) {
        now += PENDING_WIPE_FAILURE_SPACING_MS;
        await session.createLocalFirst();
        await tick();
        await session.logout();
        await tick();
      }
      clock.mockRestore();
      const calls = deleteSignedOutStore.mock.calls.length;
      // Still pending, and the signed-out session says so.
      expect([...pending]).toEqual(["discarded"]);
      expect(session.getSnapshot()).toMatchObject({
        status: "signed-out",
        error: { message: expect.stringContaining("couldn't wipe the signed-out account's data") },
      });
      await expect(session.retry()).rejects.toThrow("couldn't wipe");
      expect(deleteSignedOutStore).toHaveBeenCalledTimes(calls + 1);
      expect([...pending]).toEqual(["discarded"]);
      await session.close();
    } finally {
      errors.mockRestore();
      warned.mockRestore();
    }
  });
  it("saves a selection that failed during an offline sign-out on retry, back to signing-out", async () => {
    const { session, save } = await realEnrollmentSetup();
    await session.createLocalFirst();
    save.fail = true;
    await expect(session.logout()).rejects.toThrow("storage full");
    expect(session.getSnapshot()).toMatchObject({ status: "error", recovery: "session" });
    save.fail = false;
    // Saved now; the upload, still offline, keeps waiting.
    await expect(session.retry()).rejects.toBeInstanceOf(GracefulShutdownSyncError);
    expect(session.getSnapshot()).toMatchObject({ status: "signing-out" });
    await session.close();
  });
  it("discards while signing out even when the selection won't save", async () => {
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const { session, save, opened } = await realEnrollmentSetup();
      await session.createLocalFirst();
      const client = opened.at(-1)!;
      save.fail = true;
      await expect(session.logout()).rejects.toThrow("storage full");
      // The escape hatch still drops the writes and closes the client.
      await expect(session.logout({ discardUnsynced: true })).rejects.toThrow("storage full");
      expect(client.deleted).toBe(true);
      expect(client.torn).toBe(true);
      expect(session.getSnapshot()).toMatchObject({ status: "error", recovery: "session" });
      save.fail = false;
      await session.retry();
      expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
      await session.createLocalFirst();
      expect(session.getSnapshot().status).toBe("ready");
      await session.close();
    } finally {
      warned.mockRestore();
    }
  });
  it("tries a pending sign-out's upload on retry, and stops it on close", async () => {
    const { session, clients } = await setup();
    const client = clients[0]! as (typeof clients)[0] & { db?: unknown };
    const abortGracefulShutdown = vi.fn();
    client.db = { getAuthState: () => ({}), abortGracefulShutdown };
    const failure = new GracefulShutdownSyncError(new Error("websocket transport error"));
    client.shutdown.mockRejectedValue(failure);
    await session.logout();
    await expect(session.retry()).rejects.toBe(failure);
    expect(session.getSnapshot().status).toBe("signing-out");
    client.shutdown.mockResolvedValue(undefined);
    await session.close();
    expect(abortGracefulShutdown).toHaveBeenCalled();
    // Closing shuts it down without waiting for the server.
    expect(client.shutdown).toHaveBeenLastCalledWith();
    expect(session.getSnapshot().status).toBe("closed");
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
    const lease = attachSessionCoreConsumer(session);
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
        attempted = expect(session.loginWithBackend()).rejects.toMatchObject({
          reason: "signing-out",
        });
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

  // #5139: another tab holds the store as the guest this user signed up from,
  // and didn't let go within the patience. The sign-out reports that, rather
  // than a configuration error, and works once the other tab is gone.
  it("stays ready with the client when another tab holds the store past the wipe's patience", async () => {
    const { session, client, db, enrollment, old } = await wipingSetup();
    const failure = new StoreHeldByAnotherTabError();
    db.deleteClientStorage.mockRejectedValueOnce(failure);
    await expect(session.logout()).rejects.toSatisfy(isStoreHeldByAnotherTabError);
    expect(enrollment.logout).not.toHaveBeenCalled();
    expect(client.shutdown).not.toHaveBeenCalled();
    expect(session.getSnapshot()).toMatchObject({
      status: "ready",
      account: old,
      client,
      error: failure,
      recovery: "action",
    });
    await session.logout();
    expect(db.deleteClientStorage).toHaveBeenCalledTimes(2);
    expect(session.getSnapshot().status).toBe("signed-out");
  });

  it("hands a clearOnSignOut sign-out to the host's wipe, which closes the client itself", async () => {
    const old = handle("old");
    const accounts = new AccountManager(
      {
        createLocalFirst: vi.fn(() => old),
        restoreLocalFirst: vi.fn(() => old),
        logout: vi.fn(),
        loginWithBackend: vi.fn(async () => handle("next")),
        linkWithBackend: vi.fn(async (_account: AccountHandle) => handle("next")),
      },
      old,
    );
    const db = { deleteClientStorage: vi.fn(async () => {}), markReplaced: vi.fn() };
    const client = { db, shutdown: vi.fn(async () => {}) };
    const wipeSignedOut = vi.fn(async (_client: typeof client, _account: AccountHandle) => {});
    const session = await createJazzSessionOwner({
      accounts,
      clearOnSignOut: true,
      openClient: async () => client,
      wipeSignedOut,
    });
    await session.logout();
    expect(wipeSignedOut).toHaveBeenCalledWith(client, old);
    expect(db.deleteClientStorage).not.toHaveBeenCalled();
    expect(db.markReplaced).toHaveBeenCalledWith("signed-out");
    expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
    expect(session.getSnapshot().error).toBeUndefined();
  });

  it("signs out with the wipe's failure as its error when the host's wipe fails", async () => {
    const old = handle("old");
    const accounts = new AccountManager(
      {
        createLocalFirst: vi.fn(() => old),
        restoreLocalFirst: vi.fn(() => old),
        logout: vi.fn(),
        loginWithBackend: vi.fn(async () => handle("next")),
        linkWithBackend: vi.fn(async (_account: AccountHandle) => handle("next")),
      },
      old,
    );
    // The client is closed by then: the failure is the wipe's, after teardown.
    const db = { isUsable: () => false, markReplaced: vi.fn() };
    const client = { db, shutdown: vi.fn(async () => {}) };
    const failure = new Error("the wipe's open failed");
    const session = await createJazzSessionOwner({
      accounts,
      clearOnSignOut: true,
      openClient: async () => client,
      wipeSignedOut: async () => {
        throw failure;
      },
    });
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    await session.logout();
    error.mockRestore();
    expect(session.getSnapshot()).toMatchObject({ status: "signed-out" });
    expect(session.getSnapshot().error?.message).toContain("couldn't finish wiping");
    expect(session.getSnapshot().error?.cause).toBe(failure);
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
