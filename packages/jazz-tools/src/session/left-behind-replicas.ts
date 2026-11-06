import type { RememberedBackendUser } from "../accounts/enrollment.js";
import type { AccountHandle } from "../accounts/state.js";

/** What the record keeps of a user: enough to find their replica, no claims. */
export type ReplicaOwner = Pick<RememberedBackendUser, "account" | "identity">;

/**
 * With `clearOnSignOut`, the backend users whose local-first replica this
 * browser may hold. A sign-out Jazz sees in the page wipes the replica, but
 * one the page doesn't see (a server-side logout followed by a reload, or
 * another user signing in through the app before a reload) leaves it
 * behind: `clearOnSignOut` never reopens the previous user, so nothing would
 * wipe it. This record is what a later page load wipes. An entry is written
 * before its replica opens and dropped only once it is wiped. It keeps the
 * account and identity a replica is stored under, never claims or a
 * credential.
 */
export function leftBehindReplicas(registry: string) {
  const key = `jazz-clear-on-sign-out-v1:${encodeURIComponent(registry)}`;
  const read = (): ReplicaOwner[] => {
    try {
      const value = JSON.parse(globalThis.localStorage?.getItem(key) ?? "[]");
      return Array.isArray(value) ? value.filter(isOwner) : [];
    } catch {
      return [];
    }
  };
  const write = (owners: ReplicaOwner[]) => {
    try {
      if (owners.length === 0) globalThis.localStorage?.removeItem(key);
      else globalThis.localStorage?.setItem(key, JSON.stringify(owners));
    } catch {
      // Without storage there is no later load to wipe on either.
    }
  };
  return {
    read,
    /** `account` is about to keep a local-first replica on this device. */
    remember(account: AccountHandle) {
      if (read().some((owner) => owner.account === account.id)) return;
      const { issuer, subject } = account.identity;
      write([...read(), { account: account.id, identity: { issuer, subject } }]);
    },
    forget(account: string) {
      write(read().filter((owner) => owner.account !== account));
    },
  };
}

function isOwner(value: unknown): value is ReplicaOwner {
  const owner = value as Partial<ReplicaOwner> | null;
  return (
    typeof owner?.account === "string" &&
    typeof owner.identity?.issuer === "string" &&
    typeof owner.identity.subject === "string"
  );
}

type Locks = {
  request(
    name: string,
    options: { mode: "shared" | "exclusive"; ifAvailable?: boolean },
    callback: (lock: unknown) => Promise<unknown>,
  ): Promise<unknown>;
};
const locks = (): Locks | undefined =>
  (globalThis.navigator as { locks?: Locks } | undefined)?.locks;
const lockName = (registry: string, account: string) => `jazz-replica-v1:${registry}:${account}`;

/**
 * Hold `account`'s replica open in this tab until the returned release is
 * called, so no other tab wipes it meanwhile (a Web Lock). Without Web
 * Locks there is nothing to hold, and none is needed: persistent browser
 * storage requires them, so such a browser keeps no replica to protect.
 */
export async function holdReplica(registry: string, account: string): Promise<() => void> {
  const available = locks();
  if (!available) return () => {};
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  await new Promise<void>((acquired) => {
    void available.request(lockName(registry, account), { mode: "shared" }, async () => {
      acquired();
      await released;
    });
  });
  return release;
}

/**
 * Run `wipe` while no tab holds `account`'s replica open. Resolves false,
 * without running it, when one does.
 */
export async function whileNoTabHolds(
  registry: string,
  account: string,
  wipe: () => Promise<void>,
): Promise<boolean> {
  const available = locks();
  if (!available) {
    await wipe();
    return true;
  }
  let ran = false;
  await available.request(
    lockName(registry, account),
    { mode: "exclusive", ifAvailable: true },
    async (lock) => {
      if (!lock) return;
      ran = true;
      await wipe();
    },
  );
  return ran;
}

/** A wipe that hangs (storage another tab is still closing) waits for a later load. */
const WIPE_TIMEOUT_MS = 30_000;

/**
 * Wipe each replica `record` holds whose user the session doesn't hold now,
 * once the session has followed the backend's first answer: someone else
 * signed in, or no one. A "no one" answer is confirmed by a second one
 * (`confirmSignedOut`), so one failed session refresh in the app's
 * `authenticate` wipes nothing. Before each wipe `stillLeftBehind` is asked
 * again, so a user who signs back in meanwhile keeps their replica. Anything
 * not wiped stays recorded for a later load.
 */
export async function wipeLeftBehindReplicas(options: {
  record: ReturnType<typeof leftBehindReplicas>;
  /** The account the session holds now, once it settled. */
  current(): string | undefined;
  /** Whether that account is a user the backend signed in (not none, not a guest). */
  signedIn(): boolean;
  /** Whether the backend still says no one is signed in. */
  confirmSignedOut(): Promise<boolean>;
  stillLeftBehind(account: string): boolean;
  /**
   * Wipe `owner`'s replica and forget it in the record, in one step, so an
   * open of that account waiting for the wipe records its new replica after.
   * Resolves false when it left the replica (another tab holds it open).
   * Once `signal` aborts (the wipe took too long) before its delete began,
   * it lets go of the replica at once, without forgetting it; a delete that
   * began holds the replica until it settles.
   */
  wipe(owner: ReplicaOwner, signal: AbortSignal): Promise<boolean>;
}): Promise<void> {
  const owners = options.record.read().filter((owner) => owner.account !== options.current());
  if (owners.length === 0) return;
  if (!options.signedIn()) {
    let confirmed = false;
    try {
      confirmed = await options.confirmSignedOut();
    } catch {
      // A backend that can't be asked wipes nothing.
    }
    if (!confirmed) return;
  }
  for (const owner of owners) {
    if (!options.stillLeftBehind(owner.account)) continue;
    const timeout = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        options.wipe(owner, timeout.signal),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            const error = new Error("the wipe took too long");
            timeout.abort(error);
            reject(error);
          }, WIPE_TIMEOUT_MS);
        }),
      ]);
    } catch (error) {
      console.warn(
        "Jazz could not wipe the local data a signed-out user left on this device (clearOnSignOut); a later load tries again.",
        error,
      );
    } finally {
      clearTimeout(timer);
    }
  }
}
