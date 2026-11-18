import type { AccountStore } from "./persistence.js";

// Managers on a host without browser storage that were given no store.
const processOnlyManagers = new WeakSet<object>();

/**
 * @internal The error for `operation` (`"createDeviceAccount()"`,
 * `'initial: "device"'`, …) on a manager that can't keep a device account's
 * key: a host without browser storage whose app passed no `store`. Undefined
 * where device accounts are kept.
 */
export function deviceAccountStoreMissing(accounts: object, operation: string): Error | undefined {
  return processOnlyManagers.has(accounts)
    ? new DeviceAccountNeedsStoreError(operation)
    : undefined;
}

/**
 * A device account outside a browser with no `store`: its key would live
 * only in this process's memory, so the account and everything it wrote
 * would be lost on exit.
 */
export class DeviceAccountNeedsStoreError extends Error {
  override readonly name = "DeviceAccountNeedsStoreError";
  constructor(operation: string) {
    super(
      `${operation} needs a place to keep the device account's key, and outside a browser Jazz has none by default: pass createJazzClient a \`store\` (an AccountStore: { read, update }) backed by a file or the OS keychain. Without \`store\`, a client on this host signs in only as the user your backend names (bearer).`,
    );
  }
}

/**
 * The selection a host without browser storage keeps when the app passes no
 * store: in memory, for this process. It never holds a device key (see
 * `deviceAccountStoreMissing`); a write that would is refused, so no path
 * keeps a key that the next run silently loses.
 */
export function processAccountStore(): AccountStore {
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      const next = transform(stored);
      if ((JSON.parse(next) as { roots?: unknown[] }).roots?.length)
        throw new DeviceAccountNeedsStoreError("A device account");
      stored = next;
    },
  };
}

/** @internal Record that `accounts` keeps its selection in `processAccountStore`. */
export function markProcessOnly(accounts: object): void {
  processOnlyManagers.add(accounts);
}
