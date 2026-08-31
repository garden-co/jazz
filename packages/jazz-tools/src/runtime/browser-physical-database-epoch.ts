/**
 * Origin-wide exclusion for a physical browser persistence root.
 *
 * SharedWorker names deliberately include the deployed worker/WASM asset
 * scope. During a rolling deployment that can leave two differently-named
 * realms alive for the same IndexedDB database. IndexedDB transactions alone
 * cannot tell a newly started realm whether an `active` foreground lease is
 * genuinely abandoned or merely owned by the other realm. A Web Lock does:
 * it is held by the agent that owns the root and is released by the browser
 * when that agent dies.
 *
 * The worker also records the opaque epoch in IndexedDB. The durable token is
 * a fence for clean release and diagnostics; the Web Lock is the liveness
 * proof that permits a successor to replace an epoch left by a crashed realm.
 */

export class BrowserPhysicalDatabaseBusyError extends Error {
  constructor(readonly databaseName: string) {
    super(
      `IndexedDB database ${databaseName} is active in another Jazz SharedWorker realm; wait for that worker to close before retrying`,
    );
    this.name = "BrowserPhysicalDatabaseBusyError";
  }
}

export type BrowserPhysicalDatabaseEpoch = {
  readonly id: string;
  release(): void;
};

type WebLock = object;
type LockManagerLike = {
  request<T>(
    name: string,
    options: { mode: "exclusive"; ifAvailable: true },
    callback: (lock: WebLock | null) => Promise<T> | T,
  ): Promise<T>;
};

/** Acquire the liveness fence for exactly one physical IndexedDB database. */
export async function acquireBrowserPhysicalDatabaseEpoch(
  databaseName: string,
  lockManager: LockManagerLike | undefined = globalThis.navigator?.locks as
    | LockManagerLike
    | undefined,
): Promise<BrowserPhysicalDatabaseEpoch> {
  if (!lockManager) {
    // Failing closed matters more than silently reintroducing a multi-realm
    // foreground-identity alias. SharedWorker persistence already requires
    // a modern browser platform; Web Locks supplies its missing liveness bit.
    throw new Error("Persistent browser storage requires the Web Locks API");
  }

  let resolveEpoch!: (epoch: BrowserPhysicalDatabaseEpoch) => void;
  let rejectEpoch!: (error: Error) => void;
  const epoch = new Promise<BrowserPhysicalDatabaseEpoch>((resolve, reject) => {
    resolveEpoch = resolve;
    rejectEpoch = reject;
  });
  let release!: () => void;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  const id = crypto.randomUUID();

  void lockManager
    .request(
      `jazz:browser-physical-database:${databaseName}`,
      { mode: "exclusive", ifAvailable: true },
      async (lock) => {
        if (!lock) {
          rejectEpoch(new BrowserPhysicalDatabaseBusyError(databaseName));
          return;
        }
        resolveEpoch({ id, release });
        await released;
      },
    )
    .catch((error: unknown) => {
      rejectEpoch(error instanceof Error ? error : new Error(String(error)));
    });
  return await epoch;
}
