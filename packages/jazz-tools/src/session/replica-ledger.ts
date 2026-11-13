/**
 * The lifecycle of this browser's per-account local-first stores under
 * `clearOnSignOut` (#5250): which accounts' data may be on the device, who
 * holds it open, and who may wipe it. Everything about wiping in the
 * browser lives here; the session only calls `hold`, `signedOut` and
 * `reconcile`.
 *
 * A store is in one of four states, derived from two facts and nothing
 * else: the **ledger** (localStorage, below), and the per-account **Web
 * Lock** every tab with the store open holds shared (the browser releases
 * it when a tab closes, crashes or is discarded).
 *
 * - **Absent**: no ledger entry; nothing of the account is on the device.
 * - **Held(n)**: n ≥ 1 tabs hold the lock. Nothing wipes it.
 * - **Orphaned**: an entry, no tab holds the lock, no wipe began. A tab
 *   closed while signed in, or a sign-out happened where no page saw it.
 *   It is wiped only with proof that no one can open it again: a backend
 *   user the backend no longer names (twice), or a device account whose
 *   key the device no longer keeps.
 * - **Wiping**: the entry is marked `wiping` (written before the delete
 *   starts). A crash mid-delete leaves it so; it is finished before the
 *   account opens again, without further proof: the data is already partly
 *   gone.
 *
 * One owner per transition:
 * - opening (`hold`): the opening tab records the entry, then takes the
 *   lock, then records it again (a wipe may have forgotten it meanwhile);
 * - the last tab out signing out (`signedOut`): that tab wipes at once, with
 *   no further proof. A tab that signs out while another holds the store
 *   does nothing: the last holder decides, by signing out too, or by
 *   closing, which leaves the store Orphaned;
 * - Orphaned and Wiping stores (`reconcile`): any page, on load and on the
 *   triggers the session wires up. There is no polling.
 *
 * `wipeOne` is the only code that deletes, always under the exclusive lock
 * and only while no tab holds the store.
 */

/** What the ledger keeps of an account: enough to find its store, no claims, never a credential. */
export type ReplicaEntry = {
  account: string;
  /** A user the backend signed in, or a device account (a guest). */
  kind: "backend" | "device";
  identity: { issuer: string; subject: string };
  /** Set before a delete begins and kept until it finished. */
  wiping?: true;
};

/** Each account's entry is its own key, so tabs writing at once never overwrite each other's. */
const ledgerPrefix = (registry: string) => `jazz-replica-v2:${encodeURIComponent(registry)}:`;
/** alpha.61's record of backend users only; read alongside, until the next breaking alpha. */
const legacyKey = (registry: string) => `jazz-clear-on-sign-out-v1:${encodeURIComponent(registry)}`;

/**
 * The durable list of accounts whose store may be on this device: one
 * localStorage key per account. An entry is written before its store opens
 * and dropped only once it is wiped.
 */
export function replicaLedger(registry: string) {
  const prefix = ledgerPrefix(registry);
  const legacy = legacyKey(registry);
  const storage = (): Storage | undefined => {
    try {
      return globalThis.localStorage;
    } catch {
      return undefined;
    }
  };
  const parseEntry = (account: string): ReplicaEntry | undefined => {
    try {
      const value: unknown = JSON.parse(storage()?.getItem(prefix + account) ?? "null");
      return isEntry(value) && value.account === account ? value : undefined;
    } catch {
      return undefined;
    }
  };
  const legacyOwners = (): unknown[] => {
    try {
      const value: unknown = JSON.parse(storage()?.getItem(legacy) ?? "[]");
      return Array.isArray(value) ? value : [];
    } catch {
      return [];
    }
  };
  const legacyEntry = (account: string): ReplicaEntry | undefined => {
    const owner = legacyOwners().find(
      (candidate): candidate is Pick<ReplicaEntry, "account" | "identity"> =>
        isIdentified(candidate) && candidate.account === account,
    );
    return owner && { account, kind: "backend", identity: owner.identity };
  };
  const put = (entry: ReplicaEntry) => {
    try {
      storage()?.setItem(prefix + entry.account, JSON.stringify(entry));
    } catch {
      // Without storage there is no later page to wipe on either.
    }
  };
  const get = (account: string) => parseEntry(account) ?? legacyEntry(account);
  return {
    /** The keys this ledger's entries live under start with it (for `storage` events). */
    prefix,
    legacyKey: legacy,
    /** Whether entries persist here: without storage (disabled, or not a browser) nothing is recorded. */
    durable(): boolean {
      try {
        return typeof storage()?.getItem === "function";
      } catch {
        return false;
      }
    },
    read(): ReplicaEntry[] {
      const accounts = new Set<string>();
      const store = storage();
      try {
        for (let i = 0; i < (store?.length ?? 0); i++) {
          const key = store!.key(i);
          if (key?.startsWith(prefix)) accounts.add(key.slice(prefix.length));
        }
      } catch {
        // Unreadable storage records nothing.
      }
      for (const owner of legacyOwners()) if (isIdentified(owner)) accounts.add(owner.account);
      return [...accounts].map(get).filter((entry): entry is ReplicaEntry => entry !== undefined);
    },
    get,
    /** `entry`'s account is about to keep a store here. A wipe already marked stays marked. */
    record(entry: Omit<ReplicaEntry, "wiping">) {
      const { account, kind, identity } = entry;
      const recorded = get(account);
      const next: ReplicaEntry = {
        account,
        kind,
        identity: { issuer: identity.issuer, subject: identity.subject },
        ...(recorded?.wiping ? { wiping: true as const } : {}),
      };
      if (JSON.stringify(parseEntry(account)) !== JSON.stringify(next)) put(next);
    },
    markWiping(account: string, wiping: boolean) {
      const recorded = get(account);
      if (!recorded) return;
      const { wiping: _wiping, ...entry } = recorded;
      put(wiping ? { ...entry, wiping } : entry);
    },
    forget(account: string) {
      try {
        storage()?.removeItem(prefix + account);
        const owners = legacyOwners();
        const kept = owners.filter((owner) => !(isIdentified(owner) && owner.account === account));
        if (kept.length === owners.length) return;
        if (kept.length === 0) storage()?.removeItem(legacy);
        else storage()?.setItem(legacy, JSON.stringify(kept));
      } catch {
        // Nothing recorded, nothing to forget.
      }
    },
  };
}

export type ReplicaLedger = ReturnType<typeof replicaLedger>;

function isIdentified(value: unknown): value is Pick<ReplicaEntry, "account" | "identity"> {
  const entry = value as Partial<ReplicaEntry> | null;
  return (
    typeof entry?.account === "string" &&
    typeof entry.identity?.issuer === "string" &&
    typeof entry.identity.subject === "string"
  );
}

function isEntry(value: unknown): value is ReplicaEntry {
  const entry = value as Partial<ReplicaEntry> | null;
  return (
    isIdentified(value) &&
    (entry!.kind === "backend" || entry!.kind === "device") &&
    (entry!.wiping === undefined || entry!.wiping === true)
  );
}

export type Locks = {
  query?(): Promise<{
    held?: { name?: string; mode?: string }[];
    pending?: { name?: string; mode?: string }[];
  }>;
  request(
    name: string,
    options: { mode: "shared" | "exclusive"; ifAvailable?: boolean },
    callback: (lock: unknown) => Promise<unknown>,
  ): Promise<unknown>;
};
const webLocks = (): Locks | undefined =>
  (globalThis.navigator as { locks?: Locks } | undefined)?.locks;
/** Unchanged since alpha.61, so tabs of either build see each other's holds. */
const lockName = (registry: string, account: string) => `jazz-replica-v1:${registry}:${account}`;

/**
 * Hold `account`'s store open in this tab until the returned release is
 * called, so no tab wipes it meanwhile. Every browser tab with a
 * local-first client holds it, with `clearOnSignOut` or without: a store
 * delete (`delete-store`) relies on it to know no tab is joining. Without Web Locks there is nothing to
 * hold, and none is needed: persistent browser storage requires them.
 */
/**
 * Hold `account`'s store open, as a page without `clearOnSignOut` (or a
 * client opened directly) does: nothing is recorded, but a wipe a crash left
 * unfinished is finished first (through `deleteStore`), so the store is
 * never opened half-deleted.
 */
export function holdReplicaStore(
  registry: string,
  account: string,
  deleteStore: () => Promise<"deleted" | "held">,
): Promise<() => Promise<void>> {
  return replicaLifecycle({
    registry,
    ledger: replicaLedger(registry),
    heldHere: () => false,
    deleteStore,
    backendUser: async () => {
      throw new Error("not asked: this page only holds the store");
    },
    deviceKeyKept: async () => true,
  }).hold({ account });
}

async function holdLock(
  locks: Locks | undefined,
  registry: string,
  account: string,
): Promise<() => Promise<void>> {
  if (!locks) return async () => {};
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  let held!: Promise<unknown>;
  await new Promise<void>((acquired) => {
    held = locks.request(lockName(registry, account), { mode: "shared" }, async () => {
      acquired();
      await released;
    });
  });
  // Resolves once the lock is gone, not merely asked to go: a wipe that
  // checks right after must not find this tab still holding it.
  return async () => {
    release();
    await held.catch(() => {});
  };
}

/** Whether a tab holds `account`'s store or waits to: a look, taking no lock. */
async function lockHeld(
  locks: Locks | undefined,
  registry: string,
  account: string,
): Promise<boolean> {
  const state = await locks?.query?.();
  if (!state) return false;
  const name = lockName(registry, account);
  return [...(state.held ?? []), ...(state.pending ?? [])].some((lock) => lock.name === name);
}

/**
 * Whether only an exclusive hold (another page's wipe or check, which ends
 * soon) keeps `account`'s lock busy: no tab holds or waits to hold it open.
 */
async function onlyBeingChecked(
  locks: Locks | undefined,
  registry: string,
  account: string,
): Promise<boolean> {
  const state = await locks?.query?.();
  if (!state) return false;
  const name = lockName(registry, account);
  const locksOf = [...(state.held ?? []), ...(state.pending ?? [])].filter(
    (lock) => lock.name === name,
  );
  return locksOf.length > 0 && locksOf.every((lock) => lock.mode === "exclusive");
}

/** Run `run` under the exclusive lock, or resolve `undefined` without running it while any tab holds it. */
async function whileNoTabHolds<T>(
  locks: Locks | undefined,
  registry: string,
  account: string,
  run: () => Promise<T>,
): Promise<T | undefined> {
  if (!locks) return run();
  let result: T | undefined;
  await locks.request(
    lockName(registry, account),
    { mode: "exclusive", ifAvailable: true },
    async (lock) => {
      if (lock) result = await run();
    },
  );
  return result;
}

/** As the backend watch confirms a signed-out answer before it signs anyone out. */
export const CONFIRM_SIGNED_OUT_MS = 2_000;

/** How long an open waits for an unfinished wipe it can't finish yet: at most ~10 s. */
const FINISH_WIPE_RETRY_MS = 100;
const FINISH_WIPE_RETRIES = 100;
/** How long the last tab out waits for another page's wipe or check to end: at most ~10 s. */
const SIGNED_OUT_RETRY_MS = 50;
const SIGNED_OUT_RETRIES = 200;

export type WipeOutcome = "wiped" | "held" | "skipped";

export function replicaLifecycle(options: {
  registry: string;
  ledger: ReplicaLedger;
  /** Whether this page has `account` open, or is opening it. */
  heldHere(account: string): boolean;
  /** Whether this page has closed its session: it reconciles nothing then. */
  stopped?(): boolean;
  /**
   * Delete `account`'s store. `"held"` without deleting anything while a tab
   * (of any build) still has it open.
   */
  deleteStore(account: string): Promise<"deleted" | "held">;
  /** Who the backend signs in now. Throws when it can't be asked. */
  backendUser(): Promise<{ issuer: string; subject: string } | null>;
  /** Whether this device's account store still keeps device account `account`'s key. */
  deviceKeyKept(account: string): Promise<boolean>;
  confirmMs?: number;
  /** The browser's Web Locks, by default. */
  locks?: Locks;
}) {
  const { registry, ledger } = options;
  const locks = "locks" in options ? options.locks : webLocks();
  const confirmMs = options.confirmMs ?? CONFIRM_SIGNED_OUT_MS;

  /**
   * The only wiper. Under the exclusive lock (so only while no tab holds
   * the store), and only while the entry is still there and this page isn't
   * on the account: marks it `wiping`, deletes, forgets it.
   */
  const wipeOne = async (
    account: string,
    { pendingUploads = 0, byThisPage = false } = {},
  ): Promise<WipeOutcome> => {
    const outcome = await whileNoTabHolds(
      locks,
      registry,
      account,
      async (): Promise<WipeOutcome> => {
        // Another tab wiped it first, or this page is on the account again
        // (unless this page wipes it on purpose: its own sign-out, which still
        // shows the account until it settles, or its own open finishing a wipe).
        // Without durable storage nothing was recorded, and a page's own
        // sign-out wipes anyway: clearOnSignOut never depends on the ledger.
        const recorded = ledger.get(account) !== undefined || (byThisPage && !ledger.durable());
        if (!recorded || (!byThisPage && options.heldHere(account))) return "skipped";
        // A closed page wipes nothing it didn't sign out itself.
        if (!byThisPage && options.stopped?.()) return "skipped";
        // A mark an earlier, crashed delete left must survive this attempt.
        const crashed = ledger.get(account)?.wiping === true;
        ledger.markWiping(account, true);
        // A delete that fails leaves the mark: whether any of it went is
        // unknown, so the account's next open or a later reconcile finishes it.
        const deleted = await options.deleteStore(account);
        if (deleted === "held") {
          // Nothing was deleted now: a tab (of any build) still has it open,
          // or its worker is closing. Unmarked, unless an earlier delete crashed.
          ledger.markWiping(account, crashed);
          return "held";
        }
        ledger.forget(account);
        if (pendingUploads > 0)
          console.warn(
            `Jazz signed out and wiped ${pendingUploads} write(s) the server hadn't confirmed, as clearOnSignOut asks. They are gone from this device; any the server did receive stay there.`,
          );
        return "wiped";
      },
    );
    return outcome ?? "held";
  };

  /**
   * Proof that no one is signed in as these backend users: two answers,
   * `confirmMs` apart, neither naming them (so one failed session refresh
   * in the app's `authenticate` wipes nothing). Asked once for all of them.
   */
  const backendSignedOut = async (entries: ReplicaEntry[]): Promise<Set<string>> => {
    const out = new Set<string>();
    if (entries.length === 0) return out;
    const names = (user: { issuer: string; subject: string } | null, entry: ReplicaEntry) =>
      user?.issuer === entry.identity.issuer && user.subject === entry.identity.subject;
    try {
      const first = await options.backendUser();
      const candidates = entries.filter((entry) => !names(first, entry));
      if (candidates.length === 0) return out;
      await new Promise((resolve) => setTimeout(resolve, confirmMs));
      const second = await options.backendUser();
      for (const entry of candidates) if (!names(second, entry)) out.add(entry.account);
    } catch {
      // A backend that can't be asked proves nothing.
    }
    return out;
  };

  let reconciling: Promise<void> | undefined;
  let again = false;
  const reconcileOnce = async () => {
    if (options.stopped?.()) return;
    const orphans: ReplicaEntry[] = [];
    for (const entry of ledger.read()) {
      if (options.heldHere(entry.account)) continue;
      if (await lockHeld(locks, registry, entry.account)) continue;
      orphans.push(entry);
    }
    if (orphans.length === 0) return;
    // A device account whose key the device keeps can be opened again (it
    // signed out, or another user signed in over it): its data stays. Only
    // once the key is gone is its data no one's.
    const keyGone = new Set<string>();
    for (const entry of orphans) {
      if (entry.kind !== "device" || entry.wiping) continue;
      const kept = await options.deviceKeyKept(entry.account).catch(() => true);
      if (!kept) keyGone.add(entry.account);
    }
    const signedOut = await backendSignedOut(
      orphans.filter((entry) => entry.kind === "backend" && !entry.wiping),
    );
    for (const entry of orphans) {
      const proven =
        entry.wiping ||
        (entry.kind === "device" ? keyGone.has(entry.account) : signedOut.has(entry.account));
      if (!proven) continue;
      try {
        await wipeOne(entry.account);
      } catch (error) {
        console.warn(
          "Jazz could not wipe the local data a signed-out user left on this device (clearOnSignOut); it tries again later.",
          error,
        );
      }
    }
  };

  return {
    ledger,
    /**
     * Record `entry` and hold its store open in this tab, until the returned
     * release is called. A wipe a crash left unfinished is finished first:
     * a store is never opened half-deleted.
     */
    async hold(
      entry: Omit<ReplicaEntry, "wiping"> | { account: string },
    ): Promise<() => Promise<void>> {
      const { account } = entry;
      // Only a clearOnSignOut page records what it opens; any page finishes
      // a wipe it finds unfinished, and holds the store.
      const record = () => {
        if ("kind" in entry) ledger.record(entry);
      };
      for (let attempt = 0; ; attempt++) {
        // A store marked `wiping` (a crashed delete) is finished first: it is
        // never opened half-deleted. A failing delete fails the open.
        if (ledger.get(account)?.wiping) await wipeOne(account, { byThisPage: true });
        if (ledger.get(account)?.wiping) {
          // "held": a tab, or a closing worker, still has it. Wait for it.
          if (attempt >= FINISH_WIPE_RETRIES)
            throw new Error(
              "Jazz can't open this account's local data yet: a wipe of it (clearOnSignOut) is unfinished while another tab still has it open. Close the account's other tabs and try again.",
            );
          await new Promise((resolve) => setTimeout(resolve, FINISH_WIPE_RETRY_MS));
          continue;
        }
        record();
        const release = await holdLock(locks, registry, account);
        // Again once held: another tab's wipe may have forgotten it before.
        record();
        if (!ledger.get(account)?.wiping) return release;
        // Another tab's delete failed between the check and the lock.
        await release();
      }
    },
    /** Whether a tab holds `account`'s store open (a look, taking no lock). */
    heldByATab(account: string): Promise<boolean> {
      return lockHeld(locks, registry, account);
    },
    /**
     * This tab signed `account` out and closed its client (letting go of its
     * hold). As the last tab out it wipes now; while another tab holds the
     * store it does nothing, and that tab decides. `pendingUploads` is what
     * the store held unconfirmed, for the warning.
     */
    async signedOut(account: string, pendingUploads: number): Promise<WipeOutcome> {
      for (let attempt = 0; ; attempt++) {
        const outcome = await wipeOne(account, { pendingUploads, byThisPage: true });
        // Held only by another page's wipe or check (both tabs signing out
        // at once): that ends in moments, and the last tab out must not
        // leave its store for a later page to find. Held by a tab: it decides.
        if (
          outcome !== "held" ||
          attempt >= SIGNED_OUT_RETRIES ||
          !(await onlyBeingChecked(locks, registry, account))
        )
          return outcome;
        await new Promise((resolve) => setTimeout(resolve, SIGNED_OUT_RETRY_MS));
      }
    },
    /**
     * Wipe every Orphaned store no one can open again (a backend user the
     * backend no longer names, twice; a device account whose key the device
     * no longer keeps) and finish every Wiping one. Single-flight: a
     * call while one runs runs once more after it.
     */
    reconcile(): Promise<void> {
      if (reconciling) {
        again = true;
        return reconciling;
      }
      reconciling = (async () => {
        try {
          do {
            again = false;
            await reconcileOnce();
          } while (again);
        } finally {
          reconciling = undefined;
        }
      })();
      return reconciling;
    },
  };
}

export type ReplicaLifecycle = ReturnType<typeof replicaLifecycle>;
