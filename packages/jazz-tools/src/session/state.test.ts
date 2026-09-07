import { describe, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { attachJazzSessionConsumer, createJazzSessionOwner } from "./state.js";
import type { JWTAuth } from "../accounts/enrollment.js";
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
    registerJWT: vi.fn(async (_auth: JWTAuth) => next),
    loginJWT: vi.fn(async (_auth: JWTAuth) => next),
    linkJWT: vi.fn(async (_account: AccountHandle, _auth: JWTAuth) => next),
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
  it("waits for committed consumer detach before sync and enrollment", async () => {
    const { session, clients, enrollment, next } = await setup();
    const lease = attachJazzSessionConsumer(session),
      previous = session.getSnapshot();
    const command = session.linkJWT("token");
    await tick();
    expect(session.getSnapshot().status).toBe("transitioning");
    expect(session.getSnapshot().client).toBeUndefined();
    lease.acknowledge(previous);
    await tick();
    expect(clients[0]!.shutdown).not.toHaveBeenCalled();
    expect(enrollment.linkJWT).not.toHaveBeenCalled();
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
    const failure = new Error("sync failed");
    clients[0]!.shutdown.mockRejectedValueOnce(failure);
    await expect(session.registerJWT("token")).rejects.toBe(failure);
    expect(enrollment.registerJWT).not.toHaveBeenCalled();
    expect(session.getSnapshot()).toMatchObject({
      status: "ready",
      account: old,
      client: clients[0],
      error: failure,
    });
    await session.close();
  });
  it("failed link reopens old selection and preserves primary error", async () => {
    const { session, clients, enrollment, old } = await setup();
    const failure = new Error("link failed");
    enrollment.linkJWT.mockRejectedValueOnce(failure);
    await expect(session.linkJWT("token")).rejects.toBe(failure);
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
    enrollment.linkJWT.mockRejectedValueOnce(primary);
    openClient.mockRejectedValueOnce(startup);
    await expect(session.linkJWT("token")).rejects.toBe(primary);
    expect(session.getSnapshot()).toMatchObject({ status: "error", account: old, error: startup });
    await session.retry();
    expect(session.getSnapshot().status).toBe("ready");
    await session.close();
  });
  it("successful registry action with failed open retains new selection for retry", async () => {
    const { session, enrollment, openClient, next } = await setup();
    openClient.mockRejectedValueOnce(new Error("open failed"));
    await expect(session.linkJWT("token")).rejects.toThrow("open failed");
    expect(session.getSnapshot()).toMatchObject({ status: "error", account: next });
    expect(session.getSnapshot().client).toBeUndefined();
    await session.retry();
    expect(enrollment.linkJWT).toHaveBeenCalledTimes(1);
    expect(session.getSnapshot().client?.account).toBe(next);
    await session.close();
  });
  it("rejects overlapping commands without queuing mutations", async () => {
    const { session, enrollment } = await setup();
    const first = session.loginJWT("token");
    await expect(session.createLocalFirst()).rejects.toThrow("already pending");
    await first;
    expect(enrollment.createLocalFirst).not.toHaveBeenCalled();
    await session.close();
  });
  it("logout fences pending enrollment and stays signed-out", async () => {
    const { session, enrollment, next, openClient, accounts } = await setup();
    const pending = deferred<AccountHandle>();
    enrollment.loginJWT.mockReturnValueOnce(pending.promise);
    const command = session.loginJWT("token"),
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
    const command = session.loginJWT("token"),
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
    const command = session.loginJWT("token"),
      rejected = expect(command).rejects.toThrow("superseded");
    await tick();
    const close = session.close();
    lease.release();
    await rejected;
    await close;
    expect(enrollment.loginJWT).not.toHaveBeenCalled();
    expect(clients[0]!.shutdown).toHaveBeenCalledWith();
  });
  it("close before command starts never republishes a transition", async () => {
    const { session, enrollment } = await setup();
    const command = session.loginJWT("token"),
      rejected = expect(command).rejects.toThrow("superseded");
    await session.close();
    await rejected;
    expect(session.getSnapshot().status).toBe("closed");
    expect(enrollment.loginJWT).not.toHaveBeenCalled();
  });
  it("defaults signed-out and binds commands", async () => {
    const account = handle("local"),
      create = vi.fn(() => account);
    const accounts = new AccountManager<JWTAuth>({
      createLocalFirst: create,
      registerJWT: async () => account,
      loginJWT: async () => account,
      linkJWT: async () => account,
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
});
