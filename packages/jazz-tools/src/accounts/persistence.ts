import {
  createAccountManagerWithRuntime,
  rememberBackendUser,
  type AppBearer,
  type BackendAccountHost,
  type JWTAuth,
  type RememberedBackendUser,
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
//
// `backendUser` (optional, versioned on its own) remembers the user the app's
// backend signed in, when that is the selection: their account, identity and
// claims, never a credential. At most one of `selected` and `backendUser` is
// set. Older clients ignore the field and drop it on their next write, which
// only costs that device its remembered user.
interface StoredAccounts {
  format: "jazz-account-selection-v1";
  roots: string[];
  selected: number | null;
  backendUser?: StoredBackendUser;
}
const BACKEND_USER_FORMAT = "jazz-backend-user-v1";
type StoredBackendUser = RememberedBackendUser & { format: typeof BACKEND_USER_FORMAT };
function decodeBackendUser(value: unknown): StoredBackendUser | undefined {
  if (value === undefined || value === null) return undefined;
  const user = value as Partial<StoredBackendUser>;
  // Another version's record (a newer bundle in another tab, or one this
  // device rolled back from): the remembered user is a cache, so it is absent.
  if (typeof user.format === "string" && user.format !== BACKEND_USER_FORMAT) return undefined;
  if (
    user.format !== BACKEND_USER_FORMAT ||
    typeof user.account !== "string" ||
    typeof user.identity?.issuer !== "string" ||
    typeof user.identity.subject !== "string" ||
    !user.claims ||
    typeof user.claims !== "object" ||
    Array.isArray(user.claims)
  )
    throw new Error("Invalid persisted account selection");
  return {
    format: BACKEND_USER_FORMAT,
    account: user.account,
    identity: { issuer: user.identity.issuer, subject: user.identity.subject },
    claims: { ...user.claims },
  };
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
  const backendUser = decodeBackendUser(parsed.backendUser);
  return {
    format: parsed.format,
    roots: [...parsed.roots],
    // A local selection wins over a remembered user; writers never keep both.
    selected: parsed.selected,
    ...(backendUser && parsed.selected === null ? { backendUser } : {}),
  };
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
  bearer?: AppBearer;
  backend?: BackendAccountHost;
  /**
   * Remember the user the backend signed in, so a reload reopens them before
   * the backend answers (default). Off for `clearOnSignOut`: on a shared
   * device the next person must not open the previous user's account while
   * the backend is unreachable. A record already stored is then dropped.
   */
  rememberBackendUser?: boolean;
}) {
  const remembers = options.rememberBackendUser ?? true;
  const stored = decode(await options.store.read());
  if (!remembers) delete stored.backendUser;
  let writes = Promise.resolve();
  let adoptingSecret: string | undefined;
  const save = () => {
    const roots = [...stored.roots];
    const selected = stored.selected === null ? null : stored.roots[stored.selected]!;
    const backendUser = stored.backendUser;
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
          if (backendUser && selected === null) latest.backendUser = backendUser;
          else delete latest.backendUser;
          // Older versions kept an external account's assignment here.
          delete (latest as { assignment?: unknown }).assignment;
          return JSON.stringify(latest);
        }),
      );
    void writes.catch(() => {});
    return writes;
  };
  const manager = createAccountManagerWithRuntime({
    registry: options.registry,
    fetch: options.fetch,
    bearer: options.bearer,
    backend: options.backend,
    restoredLocalFirstSecret: stored.selected === null ? undefined : stored.roots[stored.selected],
    restoredBackendUser: stored.backendUser,
    selectionDurability: {
      selectionChanged(next) {
        // Creating/restoring a local handle already queued its root and selection.
        if (next?.identity.issuer === "urn:jazz:local-first") {
          delete stored.backendUser;
          return;
        }
        stored.selected = null;
        // A user the backend signed in is remembered without any credential,
        // so a reload reopens their account. Other accounts (the Node host's
        // own) are never serialized here.
        const remembered = remembers && next ? rememberBackendUser(next) : undefined;
        if (remembered) stored.backendUser = { format: BACKEND_USER_FORMAT, ...remembered };
        else delete stored.backendUser;
        // Construction does not publish a selection, and this promise callback runs
        // only after the factory has returned and assigned manager.
        void save().catch((error) => manager.reportPersistenceError(error));
      },
      settle: (retry) => (retry ? writes.catch(() => save()) : writes),
    },
    localFirst: localFirstFactory({
      appId: options.appId,
      mintToken: options.mintToken,
      generateSecret: options.generateSecret,
      isSecretRetained: async (secret) => decode(await options.store.read()).roots.includes(secret),
      retainSecret(secret) {
        let index = stored.roots.indexOf(secret);
        if (index < 0) index = stored.roots.push(secret) - 1;
        stored.selected = index;
        delete stored.backendUser;
        // Suppress only the one retention callback caused by adoption. Any
        // re-entrant explicit selection must retain and persist normally.
        if (adoptingSecret !== secret) return save();
        adoptingSecret = undefined;
      },
    }),
  });
  const superseded = async (): Promise<never> => {
    stored.selected = null;
    await save();
    throw new Error("Automatic local-first startup was superseded");
  };
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
              winner = candidate;
            } else {
              winner = selected;
            }
            return JSON.stringify(latest);
          }),
        );
      await writes;
      if (changed) return manager.getLoggedIn() ?? (await superseded());
      if (winner === undefined)
        throw new Error("Automatic local-first selection was not committed");
      // Adoption only hydrates the manager; suppress its one retention callback.
      adoptingSecret = winner;
      try {
        manager.restoreLocalFirst(winner);
      } finally {
        if (adoptingSecret === winner) adoptingSecret = undefined;
      }
      return manager.getLoggedIn() ?? (await superseded());
    } finally {
      unsubscribe();
    }
  });
  await writes;
  return manager;
}
