/**
 * Framework-agnostic, refcounted client registry. Callers resolving to the same
 * `key` share one client, so a page with several providers for one identity runs
 * ONE runtime; distinct keys keep their own. Deferred release survives
 * remount/HMR/StrictMode. See the OOB issue spec for why coexisting same-heap
 * runtimes are a hazard: wasm-memory-access-oob-multi-client-teardown.md.
 */

export interface RegisteredClient {
  shutdown(): Promise<void>;
}

interface Entry {
  promise: Promise<RegisteredClient>;
  holders: Set<object>;
  releaseTimer: ReturnType<typeof setTimeout> | null;
  /** Resolver for the in-flight `releaseClient` promise, if a teardown is scheduled. */
  pendingRelease: (() => void) | null;
  /**
   * Set once teardown has started. A later acquire must wait for this before
   * constructing a replacement, otherwise two browser workers can open the
   * same persistent storage at once.
   */
  closing: Promise<void> | null;
}

const registry = new Map<string, Entry>();
const pendingReleases = new Set<Promise<void>>();

export function acquireClient<T extends RegisteredClient>(
  key: string,
  create: () => Promise<T>,
  holder: object,
): Promise<T> {
  let entry = registry.get(key);
  if (entry?.closing) {
    const previous = entry;
    const previousClosing = previous.closing!;
    const created: Entry = {
      promise: previousClosing.then(() => create()),
      holders: new Set(),
      releaseTimer: null,
      pendingRelease: null,
      closing: null,
    };
    created.promise.catch(() => {
      if (registry.get(key) === created) {
        registry.delete(key);
      }
    });
    registry.set(key, created);
    entry = created;
  }
  if (!entry) {
    const created: Entry = {
      promise: create(),
      holders: new Set(),
      releaseTimer: null,
      pendingRelease: null,
      closing: null,
    };
    // Evict on failure so the next acquire re-creates instead of re-rejecting.
    created.promise.catch(() => {
      if (registry.get(key) === created) {
        registry.delete(key);
      }
    });
    registry.set(key, created);
    entry = created;
  }

  entry.holders.add(holder);

  // A holder re-appeared inside the deferred-release window: cancel the teardown
  // and resolve the pending release promise without shutting anything down.
  if (entry.releaseTimer !== null) {
    clearTimeout(entry.releaseTimer);
    entry.releaseTimer = null;
    const resolve = entry.pendingRelease;
    entry.pendingRelease = null;
    resolve?.();
  }

  return entry.promise as Promise<T>;
}

/**
 * Release `holder`'s claim. The last release tears the client down on a deferred
 * tick (so a same-tick re-acquire keeps it alive); the promise resolves once
 * teardown has run, or immediately if other holders remain.
 */
export function releaseClient(key: string, holder: object): Promise<void> {
  const entry = registry.get(key);
  if (!entry) return Promise.resolve();

  entry.holders.delete(holder);
  if (entry.holders.size > 0) return Promise.resolve();
  if (entry.releaseTimer !== null) return Promise.resolve();

  const release = new Promise<void>((resolve) => {
    entry.pendingRelease = resolve;
    entry.releaseTimer = setTimeout(() => {
      entry.releaseTimer = null;
      entry.pendingRelease = null;
      if (entry.holders.size > 0) {
        resolve();
        return;
      }
      entry.closing = entry.promise
        .then((client) => client.shutdown())
        .catch(() => {})
        .finally(() => {
          if (registry.get(key) === entry) {
            registry.delete(key);
          }
          resolve();
        });
    }, 0);
  });
  pendingReleases.add(release);
  void release.then(() => pendingReleases.delete(release));
  return release;
}

/** Test-only: wait for every deferred client teardown that has been scheduled. */
export async function waitForClientRegistryIdleForTest(): Promise<void> {
  while (pendingReleases.size > 0) {
    await Promise.all(pendingReleases);
  }
}

/** Test-only: drop all entries without shutting them down. */
export function resetClientRegistryForTest(): void {
  for (const entry of registry.values()) {
    if (entry.releaseTimer !== null) clearTimeout(entry.releaseTimer);
    entry.pendingRelease?.();
  }
  registry.clear();
}
