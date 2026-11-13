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
 *   It is wiped only with proof that its owner is not signed in.
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

const ledgerKey = (registry: string) => `jazz-replicas-v2:${encodeURIComponent(registry)}`;
/** alpha.61's record of backend users only; read alongside, until the next breaking alpha. */
const legacyKey = (registry: string) => `jazz-clear-on-sign-out-v1:${encodeURIComponent(registry)}`;

/**
 * The durable list of accounts whose store may be on this device. An entry
 * is written before its store opens and dropped only once it is wiped.
 */
export function replicaLedger(registry: string) {
  const key = ledgerKey(registry);
  const legacy = legacyKey(registry);
  const parse = (name: string): unknown[] => {
    try {
      const value = JSON.parse(globalThis.localStorage?.getItem(name) ?? "[]");
      return Array.isArray(value) ? value : [];
    } catch {
      return [];
    }
  };
  const store = (name: string, entries: unknown[]) => {
    try {
      if (entries.length === 0) globalThis.localStorage?.removeItem(name);
      else globalThis.localStorage?.setItem(name, JSON.stringify(entries));
    } catch {
      // Without storage there is no later page to wipe on either.
    }
  };
  const read = (): ReplicaEntry[] => {
    const entries = parse(key).filter(isEntry);
    for (const owner of parse(legacy)) {
      if (!isIdentified(owner) || entries.some((entry) => entry.account === owner.account))
        continue;
      const { account, identity } = owner;
      entries.push({ account, kind: "backend", identity });
    }
    return entries;
  };
  const write = (entries: ReplicaEntry[]) => {
    store(key, entries);
    // What an alpha.61 page reads: keep it to the accounts still listed.
    const legacyOwners = parse(legacy).filter(
      (owner) => isIdentified(owner) && entries.some((entry) => entry.account === owner.account),
    );
    store(legacy, legacyOwners);
  };
  const get = (account: string) => read().find((entry) => entry.account === account);
  const update = (account: string, change: (entry: ReplicaEntry) => ReplicaEntry) =>
    write(read().map((entry) => (entry.account === account ? change(entry) : entry)));
  return {
    key,
    legacyKey: legacy,
    read,
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
      if (!recorded) write([...read(), next]);
      else if (JSON.stringify(recorded) !== JSON.stringify(next)) update(account, () => next);
    },
    markWiping(account: string, wiping: boolean) {
      update(account, ({ wiping: _wiping, ...entry }) => (wiping ? { ...entry, wiping } : entry));
    },
    forget(account: string) {
      write(read().filter((entry) => entry.account !== account));
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
 * called, so no tab wipes it meanwhile. Without Web Locks there is nothing to
 * hold, and none is needed: persistent browser storage requires them.
 */
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

/** The account store couldn't be read: no device account counts as signed out. */
const UNKNOWN = Symbol("unknown");

/** How long the last tab out waits for another page's wipe or check to end: at most ~10 s. */
const SIGNED_OUT_RETRY_MS = 50;
const SIGNED_OUT_RETRIES = 200;

export type WipeOutcome = "wiped" | "held" | "skipped";

export function replicaLifecycle(options: {
  registry: string;
  ledger: ReplicaLedger;
  /** Whether this page has `account` open, or is opening it. */
  heldHere(account: string): boolean;
  /**
   * Delete `account`'s store. `"held"` without deleting anything while a tab
   * (of any build) still has it open.
   */
  deleteStore(account: string): Promise<"deleted" | "held">;
  /** Who the backend signs in now. Throws when it can't be asked. */
  backendUser(): Promise<{ issuer: string; subject: string } | null>;
  /** The device account this device's account store selects, which its tabs open. */
  selectedDeviceAccount(): Promise<string | undefined>;
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
        if (!ledger.get(account) || (!byThisPage && options.heldHere(account))) return "skipped";
        ledger.markWiping(account, true);
        // A delete that fails leaves the mark: whether any of it went is
        // unknown, so the account's next open or a later reconcile finishes it.
        const deleted = await options.deleteStore(account);
        if (deleted === "held") {
          // A tab of another build still has it open, and nothing was deleted.
          ledger.markWiping(account, false);
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
    const orphans: ReplicaEntry[] = [];
    for (const entry of ledger.read()) {
      if (options.heldHere(entry.account)) continue;
      if (await lockHeld(locks, registry, entry.account)) continue;
      orphans.push(entry);
    }
    if (orphans.length === 0) return;
    const selected = orphans.some((entry) => entry.kind === "device" && !entry.wiping)
      ? await options.selectedDeviceAccount().catch(() => UNKNOWN)
      : undefined;
    const signedOut = await backendSignedOut(
      orphans.filter((entry) => entry.kind === "backend" && !entry.wiping),
    );
    for (const entry of orphans) {
      const proven =
        entry.wiping ||
        (entry.kind === "device"
          ? selected !== UNKNOWN && selected !== entry.account
          : signedOut.has(entry.account));
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
    async hold(entry: Omit<ReplicaEntry, "wiping">): Promise<() => Promise<void>> {
      const { account } = entry;
      // A store marked `wiping` is finished first. "held" here means a tab of
      // an older build has it open anyway: open alongside it.
      if (ledger.get(account)?.wiping) await wipeOne(account, { byThisPage: true });
      ledger.record(entry);
      let release = await holdLock(locks, registry, account);
      // Again once held: another tab's wipe may have forgotten it before.
      ledger.record(entry);
      if (ledger.get(account)?.wiping) {
        // Another tab's wipe failed between the check and the lock: try it
        // once more here. If it fails again, so does the open.
        await release();
        await wipeOne(account, { byThisPage: true });
        ledger.record(entry);
        release = await holdLock(locks, registry, account);
        ledger.record(entry);
      }
      return release;
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
     * Wipe every Orphaned store whose owner is proven signed out (a backend
     * user the backend no longer names, twice; a device account the account
     * store no longer selects) and finish every Wiping one. Single-flight: a
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
