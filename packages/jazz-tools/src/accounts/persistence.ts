import {
  createAccountManagerWithRuntime,
  rememberBackendUser,
  type AppBearer,
  type BackendAccountHost,
  type JWTAuth,
  type RememberedBackendUser,
} from "./enrollment.js";
import type { AccountHandle, AccountManager } from "./state.js";
import { localFirstAccountId, localFirstFactory } from "./local-first.js";
import { parseJwtPayload } from "../runtime/client-session.js";
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
// The one exception is a manager with `rememberBackendUser: false` (as
// `clearOnSignOut` sets): a guest root linked to a backend user is no one's
// only key (the backend signs them in), and leaving it would let the next
// person on the device reopen that user.
//
// `forgottenAccounts` (optional) is the tombstone for those roots: the
// accounts they founded, never the roots themselves. Every manager honours
// it, including those that remember their user: each drops a root whose
// account is listed on load and on each write, so another tab that loaded the
// root before the link cannot write it back. It holds the last
// FORGOTTEN_ACCOUNTS_LIMIT entries: one only matters while a snapshot older
// than the link can still exist. An explicit restore of the root takes its
// account back out.
//
// `signedOut` (optional) records an explicit sign-out (E7): until someone
// signs in again, a reload stays signed out, and `initial: "device"` never
// mints a new device account over it. Kept device keys stay (logout never
// destroys the only key to offline data), and `reopenDeviceAccount()` resumes
// the most recent, `recentDevice` (the account it founded, not the key).
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
  forgottenAccounts?: string[];
  signedOut?: true;
  recentDevice?: string;
}
// About 45 bytes an entry: room for every link a long-open tab may outlive.
const FORGOTTEN_ACCOUNTS_LIMIT = 1024;
const BACKEND_USER_FORMAT = "jazz-backend-user-v1";
type StoredBackendUser = RememberedBackendUser & {
  format: typeof BACKEND_USER_FORMAT;
};
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
/** @internal Whether `value` is a selection this client can read. */
export function readableSelection(value: string): boolean {
  try {
    decode(value);
    return true;
  } catch {
    return false;
  }
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
  const forgottenAccounts = parsed.forgottenAccounts;
  if (
    forgottenAccounts !== undefined &&
    (!Array.isArray(forgottenAccounts) ||
      forgottenAccounts.some((account) => typeof account !== "string"))
  )
    throw new Error("Invalid persisted account selection");
  const backendUser = decodeBackendUser(parsed.backendUser);
  if (
    (parsed.signedOut !== undefined && parsed.signedOut !== true) ||
    (parsed.recentDevice !== undefined && typeof parsed.recentDevice !== "string")
  )
    throw new Error("Invalid persisted account selection");
  return {
    format: parsed.format,
    roots: [...parsed.roots],
    // A local selection wins over a remembered user; writers never keep both.
    selected: parsed.selected,
    ...(backendUser && parsed.selected === null ? { backendUser } : {}),
    ...(forgottenAccounts?.length ? { forgottenAccounts: [...forgottenAccounts] } : {}),
    ...(parsed.signedOut && parsed.selected === null ? { signedOut: true as const } : {}),
    ...(parsed.recentDevice ? { recentDevice: parsed.recentDevice } : {}),
  };
}

const automaticInitializers = new WeakMap<
  AccountManager<JWTAuth>,
  () => Promise<AccountHandle | undefined>
>();
const keptKeyReaders = new WeakMap<
  AccountManager<JWTAuth>,
  { latest(): Promise<string | undefined>; known(): string | undefined }
>();

/**
 * @internal The key of the device account this device keeps that signed in
 * most recently, from the store as it is now (`latest`), or as this manager
 * last saw it (`known`, for a synchronous export). `undefined` when the
 * device keeps none (`clearOnSignOut` forgets a signed-out one).
 */
export function keptDeviceKey(accounts: AccountManager<JWTAuth>) {
  const reader = keptKeyReaders.get(accounts);
  if (!reader) throw new Error("Jazz can't read this device's kept device accounts");
  return reader;
}
const forgottenReaders = new WeakMap<
  AccountManager<JWTAuth>,
  (account: string) => Promise<boolean>
>();

/**
 * @internal Whether the store `accounts` keeps tombstones `account` (see
 * `forgottenAccounts`): a manager that doesn't remember its backend user
 * (`clearOnSignOut`) linked this guest to a user, so the account is theirs
 * now. A guest another tab still holds in memory is then that user's
 * account, and signs out as they do.
 */
export async function accountForgotten(
  accounts: AccountManager<JWTAuth>,
  account: string,
): Promise<boolean> {
  return (await forgottenReaders.get(accounts)?.(account)) ?? false;
}

const keepNextSignedOutKey = new WeakSet<AccountManager<JWTAuth>>();

/**
 * @internal The next sign-out of `accounts` keeps the device account's key,
 * even with `clearOnSignOut`: another tab still has its store open (its
 * wipe was held off), so that tab, the last out, decides. Its own sign-out
 * forgets the key; if it closes instead, the key and the data stay.
 */
export function keepDeviceKeyOnSignOut(accounts: AccountManager<JWTAuth>): void {
  keepNextSignedOutKey.add(accounts);
}

const keptAccountReaders = new WeakMap<
  AccountManager<JWTAuth>,
  (account: string) => Promise<boolean>
>();

/**
 * @internal Whether the store `accounts` keeps still holds the key of the
 * device account `account` (as stored now, not as this manager last saw it):
 * while it does, that account can be opened again, so its local data is kept.
 */
export async function storedKeepsDeviceAccount(
  accounts: AccountManager<JWTAuth>,
  account: string,
): Promise<boolean> {
  const read = keptAccountReaders.get(accounts);
  if (!read) throw new Error("Jazz can't read this device's account store");
  return read(account);
}

/**
 * @internal Select and adopt one durable root for automatic first startup
 * (`initial: "device"`). A new device account is minted only when the device
 * keeps no device key and no one explicitly signed out; otherwise the session
 * starts signed out (`undefined`), and `reopenDeviceAccount()` resumes a kept one.
 */
export async function ensureAutomaticLocalFirst(
  accounts: AccountManager<JWTAuth>,
): Promise<AccountHandle | undefined> {
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
  /** The account a local-first root founds, as `localFirstFactory` derives it. */
  const rootAccounts = new Map<string, string | undefined>();
  const rootAccount = (root: string): string | undefined => {
    if (rootAccounts.has(root)) return rootAccounts.get(root);
    const subject = parseJwtPayload(options.mintToken(root, options.appId))?.sub;
    const account =
      typeof subject === "string" ? localFirstAccountId(options.appId, subject) : undefined;
    rootAccounts.set(root, account);
    return account;
  };
  /** Drop every root a tombstone names, keeping the selection only if it survives. */
  const dropForgotten = (accounts: StoredAccounts) => {
    const tombstone = new Set(accounts.forgottenAccounts ?? []);
    if (tombstone.size === 0) return;
    const selected = accounts.selected === null ? null : accounts.roots[accounts.selected]!;
    accounts.roots = accounts.roots.filter((root) => {
      const account = rootAccount(root);
      return account === undefined || !tombstone.has(account);
    });
    const index = selected === null ? -1 : accounts.roots.indexOf(selected);
    accounts.selected = index < 0 ? null : index;
  };
  const stored = decode(await options.store.read());
  if (!remembers) delete stored.backendUser;
  // A root written back by an older snapshot (or an older bundle) is not reopened.
  dropForgotten(stored);
  // Tombstone changes this manager made, applied to the latest value on every
  // write until one lands. Only this manager's own changes: a tombstone loaded
  // from the store is never written back over another manager's restore.
  const forgetting = new Set<string>();
  const unforgetting = new Set<string>();
  let writes = Promise.resolve();
  let adoptingSecret: string | undefined;
  // The selection restored at construction came from the filtered store, like
  // an adoption: retaining it must not take its account off the tombstone.
  const startupSecret = stored.selected === null ? undefined : stored.roots[stored.selected];
  let restoringStartup = startupSecret !== undefined;
  const save = () => {
    const roots = [...stored.roots];
    const selected = stored.selected === null ? null : stored.roots[stored.selected]!;
    const backendUser = stored.backendUser;
    const signedOut = stored.signedOut;
    const recentDevice = stored.recentDevice;
    const forgets = [...forgetting];
    const unforgets = [...unforgetting];
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
          const tombstone = (latest.forgottenAccounts ?? []).filter(
            (account) => !forgets.includes(account) && !unforgets.includes(account),
          );
          tombstone.push(...forgets);
          latest.forgottenAccounts = tombstone.slice(-FORGOTTEN_ACCOUNTS_LIMIT);
          dropForgotten(latest);
          if (latest.forgottenAccounts.length === 0) delete latest.forgottenAccounts;
          if (backendUser && selected === null) latest.backendUser = backendUser;
          else delete latest.backendUser;
          if (signedOut && selected === null && !backendUser) latest.signedOut = true;
          else delete latest.signedOut;
          if (recentDevice) latest.recentDevice = recentDevice;
          else delete latest.recentDevice;
          // Older versions kept an external account's assignment here.
          delete (latest as { assignment?: unknown }).assignment;
          return JSON.stringify(latest);
        }),
      )
      .then(() => {
        // Landed: later writes need not repeat these, and must not re-apply
        // them over another manager's newer change.
        for (const account of forgets) forgetting.delete(account);
        for (const account of unforgets) unforgetting.delete(account);
      });
    void writes.catch(() => {});
    return writes;
  };
  const manager = createAccountManagerWithRuntime({
    registry: options.registry,
    fetch: options.fetch,
    bearer: options.bearer,
    backend: options.backend,
    restoredLocalFirstSecret: startupSecret,
    restoredBackendUser: stored.backendUser,
    selectionDurability: {
      selectionChanged(next) {
        // Creating/restoring a local handle already queued its root and selection.
        if (next?.identity.issuer === "urn:jazz:local-first") {
          keepNextSignedOutKey.delete(manager);
          delete stored.backendUser;
          delete stored.signedOut;
          return;
        }
        if (next === undefined) {
          // A sign-out: it lasts across reloads until someone signs in.
          stored.signedOut = true;
          // `clearOnSignOut` forgets the device account's key with it, so
          // nothing on this device reopens it (its local data is wiped too).
          const signedOutRoot =
            stored.selected === null ? undefined : stored.roots[stored.selected];
          const account = signedOutRoot === undefined ? undefined : rootAccount(signedOutRoot);
          // Unless another tab still has the account's store open: the last
          // tab out decides (`keepDeviceKeyOnSignOut`).
          const keep = keepNextSignedOutKey.has(manager);
          keepNextSignedOutKey.delete(manager);
          if (!remembers && !keep && account !== undefined) {
            unforgetting.delete(account);
            forgetting.add(account);
            stored.roots.splice(stored.selected!, 1);
            if (stored.recentDevice === account) delete stored.recentDevice;
          }
          stored.selected = null;
          delete stored.backendUser;
          void save().catch((error) => manager.reportPersistenceError(error));
          return;
        }
        keepNextSignedOutKey.delete(manager);
        delete stored.signedOut;
        // A guest linked to this user (their sign-up): with no remembered
        // user, nothing on the device may reopen them, so forget the root.
        const guest = stored.selected === null ? undefined : stored.roots[stored.selected];
        if (!remembers && next && guest !== undefined && rootAccount(guest) === next.id) {
          unforgetting.delete(next.id);
          forgetting.add(next.id);
          stored.roots.splice(stored.selected!, 1);
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
      isSecretRetained: async (secret) => {
        const latest = decode(await options.store.read());
        dropForgotten(latest);
        return latest.roots.includes(secret);
      },
      retainSecret(secret) {
        // Selected again (the app restores it): kept like any other root.
        // An adopted or startup root came from the filtered store, so it was
        // not forgotten; only an explicit restore takes an account back out.
        const fromStore =
          adoptingSecret === secret || (restoringStartup && startupSecret === secret);
        restoringStartup = false;
        const account = fromStore ? undefined : rootAccount(secret);
        if (account !== undefined) {
          forgetting.delete(account);
          unforgetting.add(account);
        }
        let index = stored.roots.indexOf(secret);
        if (index < 0) index = stored.roots.push(secret) - 1;
        stored.selected = index;
        delete stored.backendUser;
        delete stored.signedOut;
        const founded = rootAccount(secret);
        if (founded !== undefined) stored.recentDevice = founded;
        // Suppress only the one retention callback caused by adoption. Any
        // re-entrant explicit selection must retain and persist normally.
        if (adoptingSecret !== secret) return save();
        adoptingSecret = undefined;
      },
    }),
  });
  restoringStartup = false;
  forgottenReaders.set(manager, async (account) =>
    (decode(await options.store.read()).forgottenAccounts ?? []).includes(account),
  );
  const keptKey = (accounts: StoredAccounts): string | undefined => {
    const kept = { ...accounts, roots: [...accounts.roots] };
    dropForgotten(kept);
    return kept.roots.find((root) => rootAccount(root) === kept.recentDevice) ?? kept.roots.at(-1);
  };
  keptKeyReaders.set(manager, {
    latest: async () => keptKey(decode(await options.store.read())),
    known: () => keptKey(stored),
  });
  keptAccountReaders.set(manager, async (account) => {
    const latest = decode(await options.store.read());
    dropForgotten(latest);
    return latest.roots.some((root) => rootAccount(root) === account);
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
            dropForgotten(latest);
            const selected = latest.selected === null ? undefined : latest.roots[latest.selected];
            if (selected !== undefined) {
              winner = selected;
            } else if (!latest.signedOut && latest.roots.length === 0) {
              // A device no one used yet: start it with a new device account.
              latest.selected = latest.roots.push(candidate) - 1;
              winner = candidate;
            } else {
              // Signed out on purpose, or it keeps a device account to
              // reopen (`reopenDeviceAccount()`): start signed out.
              winner = undefined;
            }
            return JSON.stringify(latest);
          }),
        );
      await writes;
      if (changed) return manager.getLoggedIn() ?? (await superseded());
      if (winner === undefined) return undefined;
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
