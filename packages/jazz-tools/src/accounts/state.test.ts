import { describe, expect, it, vi } from "vitest";
import { AccountManager, AccountOperationSuperseded, type AccountHandle } from "./state.js";

// Enrollment is deliberately a controlled boundary here: these tests exercise
// UI scheduling, not credential verification or registry acceptance.
function handle(id: string): AccountHandle {
  return Object.freeze({
    id,
    identity: { issuer: "https://issuer.example", subject: id },
  }) as AccountHandle;
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
function setup() {
  const pending = deferred<AccountHandle>();
  const local = handle("local");
  const manager = new AccountManager({
    createLocalFirst: () => local,
    registerJWT: () => pending.promise,
    loginJWT: () => pending.promise,
    loginOrRegisterJWT: () => pending.promise,
    linkJWT: () => pending.promise,
  });
  return { manager, pending, local };
}

describe("account selection", () => {
  it("queues durability once per selection before notifying observers", async () => {
    const initial = handle("initial");
    const local = handle("local");
    const external = handle("external");
    const enrollment = deferred<AccountHandle>();
    const selectionChanged = vi.fn();
    const settled = deferred<void>();
    const settle = vi.fn(() => settled.promise);
    const manager = new AccountManager(
      {
        createLocalFirst: () => local,
        registerJWT: () => enrollment.promise,
        loginJWT: () => enrollment.promise,
        loginOrRegisterJWT: () => enrollment.promise,
        linkJWT: () => enrollment.promise,
        revalidateJWT: async (account) => account,
      },
      initial,
      { selectionChanged, settle },
    );
    expect(selectionChanged).not.toHaveBeenCalled();
    const observed: {
      account: AccountHandle | undefined;
      persisted: AccountHandle | undefined;
    }[] = [];
    manager.subscribe(() => {
      observed.push({
        account: manager.getLoggedIn(),
        persisted: selectionChanged.mock.lastCall?.[0],
      });
    });

    manager.createLocalFirst();
    manager.createLocalFirst(); // Publishing the same handle is not a transition.
    manager.reportPersistenceError(new Error("storage full"));
    await manager.loginJWT("same identity"); // Revalidation retains the handle.
    const pending = manager.registerJWT("new identity");
    expect(selectionChanged.mock.calls).toEqual([[local]]);
    enrollment.resolve(external);
    await pending;
    manager.logout();
    manager.logout();
    expect(selectionChanged.mock.calls).toEqual([[local], [external], [undefined]]);
    expect(observed).toContainEqual({ account: local, persisted: local });
    expect(observed).toContainEqual({ account: external, persisted: external });
    expect(observed.at(-1)).toEqual({ account: undefined, persisted: undefined });
    for (const { account, persisted } of observed) expect(persisted).toBe(account);

    expect(manager.settleSelection()).toBe(settled.promise);
    expect(manager.settleSelection(true)).toBe(settled.promise);
    expect(settle.mock.calls).toEqual([[false], [true]]);
    settled.resolve();
    await manager.settleSelection();
  });

  it("settles an in-memory selection without a host persistence capability", async () => {
    const { manager } = setup();
    manager.createLocalFirst();
    await expect(manager.settleSelection()).resolves.toBeUndefined();
    await expect(manager.settleSelection(true)).resolves.toBeUndefined();
  });

  it("does not restore a login after logout even if enrollment committed remotely", async () => {
    const { manager, pending } = setup();
    manager.createLocalFirst();
    const operation = manager.loginJWT("token");
    manager.logout();
    pending.resolve(handle("external"));
    await expect(operation).rejects.toBeInstanceOf(AccountOperationSuperseded);
    expect(manager.getLoggedIn()).toBeUndefined();
    expect(manager.getSnapshot().pending).toBeUndefined();
  });

  it("preserves a working selection while linking and after a failed link", async () => {
    const { manager, pending, local } = setup();
    manager.createLocalFirst();
    const operation = manager.linkJWT("token");
    expect(manager.getLoggedIn()).toBe(local);
    expect(manager.getSnapshot().pending).toBe("linkJWT");
    const error = new Error("already assigned");
    pending.reject(error);
    await expect(operation).rejects.toBe(error);
    expect(manager.getLoggedIn()).toBe(local);
    expect(manager.getSnapshot().error).toBe(error);
  });

  it("does not replace a newer offline selection with an older async response", async () => {
    const { manager, pending, local } = setup();
    const operation = manager.registerJWT("token");
    manager.createLocalFirst();
    pending.resolve(handle("external"));
    await expect(operation).rejects.toBeInstanceOf(AccountOperationSuperseded);
    expect(manager.getLoggedIn()).toBe(local);
  });
});

it("does not start remote linking when a pending-state subscriber logs out", async () => {
  let calls = 0;
  const local = handle("local");
  const manager = new AccountManager({
    createLocalFirst: () => local,
    registerJWT: async () => local,
    loginJWT: async () => local,
    loginOrRegisterJWT: async () => local,
    linkJWT: async () => {
      calls++;
      return local;
    },
  });
  manager.createLocalFirst();
  manager.subscribe(() => {
    if (manager.getSnapshot().pending === "linkJWT") manager.logout();
  });
  await expect(manager.linkJWT("token")).rejects.toBeInstanceOf(AccountOperationSuperseded);
  expect(calls).toBe(0);
  expect(manager.getLoggedIn()).toBeUndefined();
});
