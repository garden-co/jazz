import { createAccountManagerWithRuntime } from "./enrollment.js";
import { localFirstFactory } from "./local-first.js";
import { parseAuthSecret } from "../runtime/auth-secret-codec.js";

/** Platform storage: use browser storage or an OS-protected store on native hosts. */
export interface AccountStore {
  read(): Promise<string | null>;
  /** Atomically read/transform/write across every manager sharing this store.
   * The callback is synchronous and may be retried by a transactional host.
   * Resolve only after the replacement is durable; leave the old value on failure.
   */
  update(transform: (current: string | null) => string): Promise<void>;
}

// Local helper preferences, not a database/wire codec. Keep every local root:
// logout clears selection but must never destroy the only key to offline data.
interface StoredAccounts {
  format: "jazz-account-selection-v1";
  roots: string[];
  selected: number | null;
}
function decode(value: string | null): StoredAccounts {
  if (value === null) return { format: "jazz-account-selection-v1", roots: [], selected: null };
  const parsed = JSON.parse(value) as StoredAccounts;
  if (
    parsed?.format !== "jazz-account-selection-v1" ||
    !Array.isArray(parsed.roots) ||
    parsed.roots.some((root) => typeof root !== "string") ||
    (parsed.selected !== null &&
      (!Number.isInteger(parsed.selected) ||
        parsed.selected < 0 ||
        parsed.selected >= parsed.roots.length))
  ) {
    throw new Error("Invalid persisted account selection");
  }
  for (const root of parsed.roots) parseAuthSecret(root);
  return { format: parsed.format, roots: [...parsed.roots], selected: parsed.selected };
}

/** @internal Hosts load native crypto first; all selection semantics stay shared. */
export async function prepareAccountManager(options: {
  appId: string;
  registry: string;
  store: AccountStore;
  mintToken(secret: string, audience: string): string;
  generateSecret?(): string;
  fetch?: typeof fetch;
}) {
  const stored = decode(await options.store.read());
  let writes = Promise.resolve();
  const save = () => {
    const roots = [...stored.roots];
    const selected = stored.selected === null ? null : stored.roots[stored.selected]!;
    // Serialize snapshots even for an asynchronous native secure store. A
    // rejected save does not prevent a later explicit selection from retrying.
    writes = writes
      .catch(() => {})
      .then(() =>
        options.store.update((current) => {
          const latest = decode(current);
          // Independent managers may have discovered roots since we loaded. Never
          // replace their key inventory with this manager's older snapshot.
          for (const root of roots) if (!latest.roots.includes(root)) latest.roots.push(root);
          latest.selected = selected === null ? null : latest.roots.indexOf(selected);
          return JSON.stringify(latest);
        }),
      );
    void writes.catch(() => {});
    return writes;
  };
  const manager = createAccountManagerWithRuntime({
    registry: options.registry,
    fetch: options.fetch,
    restoredLocalFirstSecret: stored.selected === null ? undefined : stored.roots[stored.selected],
    localFirst: localFirstFactory({
      appId: options.appId,
      mintToken: options.mintToken,
      generateSecret: options.generateSecret,
      retainSecret(secret) {
        let index = stored.roots.indexOf(secret);
        if (index < 0) index = stored.roots.push(secret) - 1;
        stored.selected = index;
        return save();
      },
    }),
  });
  let selection = manager.getLoggedIn();
  manager.subscribe(() => {
    const next = manager.getLoggedIn();
    if (next === selection) return;
    selection = next;
    // Creating/restoring a local handle already queued its root and selection.
    // External credentials are owned by the provider and never serialized here.
    if (next?.identity.issuer === "urn:jazz:local-first") return;
    stored.selected = null;
    void save().catch((error) => manager.reportPersistenceError(error));
  });
  await writes;
  return manager;
}
