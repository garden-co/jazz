import { setAccountSelectionBarrier } from "./selection-durability.js";
import {
  createAccountManagerWithRuntime,
  isRetainedAccountAssignment,
  retainedAccountAssignment,
  type BackendAccountHost,
  type JWTAuth,
  type RetainedAccountAssignment,
} from "./enrollment.js";
import type { AccountHandle, AccountManager } from "./state.js";
import { localFirstFactory } from "./local-first.js";
import { generateAuthSecret } from "../runtime/auth-secret-store.js";
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
  /**
   * The selected external account's non-secret registry assignment, when no
   * local root is selected. It lets the next start open that account's local
   * data immediately; credentials still come only from the provider.
   */
  assignment?: RetainedAccountAssignment | null;
}
function decode(value: string | null): StoredAccounts {
  if (value === null)
    return { format: "jazz-account-selection-v1", roots: [], selected: null, assignment: null };
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
  // Older writers omit the assignment; an invalid one is ignored, never trusted.
  const assignment =
    parsed.selected === null && isRetainedAccountAssignment(parsed.assignment)
      ? {
          account: parsed.assignment.account,
          issuer: parsed.assignment.issuer,
          subject: parsed.assignment.subject,
        }
      : null;
  return { format: parsed.format, roots: [...parsed.roots], selected: parsed.selected, assignment };
}

const automaticInitializers = new WeakMap<AccountManager<JWTAuth>, () => Promise<AccountHandle>>();

/** @internal Select and adopt one durable root for automatic first startup. */
export async function ensureAutomaticLocalFirst(
  accounts: AccountManager<JWTAuth>,
): Promise<AccountHandle> {
  const selected = accounts.getLoggedIn();
  if (selected) return selected;
  return automaticInitializers.get(accounts)?.() ?? accounts.createLocalFirst();
}

/** @internal Hosts load native crypto first; all selection semantics stay shared. */
export async function prepareAccountManager(options: {
  appId: string;
  registry: string;
  store: AccountStore;
  mintToken(secret: string, audience: string): string;
  generateSecret?(): string;
  fetch?: typeof fetch;
  backend?: BackendAccountHost;
  /**
   * Retain the selected external account's assignment so the next start
   * reopens it before the provider answers. Hosts opt in once their runtime
   * can open an account context before its first credential.
   */
  retainAccountAssignment?: boolean;
}) {
  const stored = decode(await options.store.read());
  if (!options.retainAccountAssignment) stored.assignment = null;
  let writes = Promise.resolve();
  let adoptingSecret: string | undefined;
  const save = () => {
    const roots = [...stored.roots];
    const selected = stored.selected === null ? null : stored.roots[stored.selected]!;
    const assignment = selected === null ? (stored.assignment ?? null) : null;
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
          latest.assignment = assignment;
          return JSON.stringify(latest);
        }),
      );
    void writes.catch(() => {});
    return writes;
  };
  const manager = createAccountManagerWithRuntime({
    registry: options.registry,
    fetch: options.fetch,
    backend: options.backend,
    restoredLocalFirstSecret: stored.selected === null ? undefined : stored.roots[stored.selected],
    restoredAccount: stored.selected === null ? (stored.assignment ?? undefined) : undefined,
    localFirst: localFirstFactory({
      appId: options.appId,
      mintToken: options.mintToken,
      generateSecret: options.generateSecret,
      isSecretRetained: async (secret) => decode(await options.store.read()).roots.includes(secret),
      retainSecret(secret) {
        stored.assignment = null;
        if (adoptingSecret === secret) {
          // Suppress only the one retention callback caused by adoption. Any
          // re-entrant explicit selection must retain and persist normally.
          adoptingSecret = undefined;
          let index = stored.roots.indexOf(secret);
          if (index < 0) index = stored.roots.push(secret) - 1;
          stored.selected = index;
          return;
        }
        let index = stored.roots.indexOf(secret);
        if (index < 0) index = stored.roots.push(secret) - 1;
        stored.selected = index;
        return save();
      },
    }),
  });
  automaticInitializers.set(manager, async () => {
    let changed = false;
    let previous = manager.getSnapshot();
    const unsubscribe = manager.subscribe(() => {
      const next = manager.getSnapshot();
      // An error-only publish (e.g. reportPersistenceError) leaves the selection
      // intact; every other publish (select, logout, operation) supersedes.
      const errorOnly =
        next.error !== undefined &&
        next.account === previous.account &&
        next.pending === previous.pending;
      previous = next;
      if (!errorOnly) changed = true;
    });
    try {
      // Generate outside the transform because transactional hosts may retry it.
      const candidate = options.generateSecret?.() ?? generateAuthSecret();
      parseAuthSecret(candidate);
      let winner: string | undefined;
      writes = writes
        .catch(() => {})
        .then(() =>
          options.store.update((current) => {
            const latest = decode(current);
            const selected = latest.selected === null ? undefined : latest.roots[latest.selected];
            if (selected === undefined) {
              const index = latest.roots.indexOf(candidate);
              latest.selected = index >= 0 ? index : latest.roots.push(candidate) - 1;
              latest.assignment = null;
              winner = candidate;
            } else {
              winner = selected;
            }
            return JSON.stringify(latest);
          }),
        );
      await writes;
      if (changed) {
        const current = manager.getLoggedIn();
        if (current) return current;
        stored.selected = null;
        await save();
        throw new Error("Automatic local-first startup was superseded");
      }
      if (winner === undefined)
        throw new Error("Automatic local-first selection was not committed");
      // Adoption only hydrates the manager; suppress its one retention callback.
      adoptingSecret = winner;
      try {
        manager.restoreLocalFirst(winner);
      } finally {
        if (adoptingSecret === winner) adoptingSecret = undefined;
      }
      const current = manager.getLoggedIn();
      if (current) return current;
      stored.selected = null;
      await save();
      throw new Error("Automatic local-first startup was superseded");
    } finally {
      unsubscribe();
    }
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
    // Keep only the assignment of a provider-admitted account, never its token.
    stored.assignment =
      next && options.retainAccountAssignment ? (retainedAccountAssignment(next) ?? null) : null;
    void save().catch((error) => manager.reportPersistenceError(error));
  });
  setAccountSelectionBarrier(manager, (retry) => (retry ? writes.catch(() => save()) : writes));
  await writes;
  return manager;
}
