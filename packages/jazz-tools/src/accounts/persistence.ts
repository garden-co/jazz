import { createAccountManagerWithRuntime } from "./enrollment.js";
import { localFirstFactory } from "./local-first.js";
import { parseAuthSecret } from "../runtime/auth-secret-codec.js";

/** Platform storage: use browser storage or an OS-protected store on native hosts. */
export interface AccountStore {
  read(): Promise<string | null>;
  write(value: string): Promise<void>;
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
  fetch?: typeof fetch;
}) {
  const stored = decode(await options.store.read());
  let writes = Promise.resolve();
  const save = () => {
    const value = JSON.stringify(stored);
    // Serialize snapshots even for an asynchronous native secure store. A
    // rejected save does not prevent a later explicit selection from retrying.
    writes = writes.catch(() => {}).then(() => options.store.write(value));
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
