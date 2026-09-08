import { describe, expect, it } from "vitest";
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
