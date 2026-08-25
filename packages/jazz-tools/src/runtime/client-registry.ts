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
  /** Shared in-flight release, so config replacement can await deferred teardown. */
  pendingRelease: { promise: Promise<void>; resolve(): void } | null;
  /**
   * Set once teardown has started. A later acquire must wait for this before
   * constructing a replacement, otherwise two browser workers can open the
   * same persistent storage at once. A failed teardown remains the barrier.
   */
  closing: Promise<void> | null;
  /** True only after creation produced a client and its shutdown was invoked. */
  shutdownStarted: boolean;
}

const registry = new Map<string, Entry>();

export function acquireClient<T extends RegisteredClient>(
  key: string,
  create: () => Promise<T>,
  holder: object,
): Promise<T> {
  let entry = registry.get(key);
  if (entry?.closing) {
    const previous = entry;
    const previousClosing = previous.closing;
    let teardownSucceeded = false;
    const created: Entry = {
      promise: previousClosing.then(() => {
        teardownSucceeded = true;
        return create();
      }),
      holders: new Set(),
      releaseTimer: null,
      pendingRelease: null,
      closing: null,
      shutdownStarted: false,
    };
    created.promise.catch(() => {
      if (registry.get(key) === created) {
        if (!teardownSucceeded && previous.shutdownStarted) {
          registry.set(key, previous);
        } else {
          registry.delete(key);
        }
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
      shutdownStarted: false,
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
    const pendingRelease = entry.pendingRelease;
    entry.pendingRelease = null;
    pendingRelease?.resolve();
  }

  return entry.promise as Promise<T>;
}

/**
 * Release `holder`'s claim. The last release tears the client down on a deferred
 * tick (so a same-tick re-acquire keeps it alive); the promise resolves once
 * teardown has settled, or immediately if other holders remain.
 */
export function releaseClient(key: string, holder: object): Promise<void> {
  const entry = registry.get(key);
  if (!entry) return Promise.resolve();

  entry.holders.delete(holder);
  if (entry.holders.size > 0) return Promise.resolve();
  if (entry.closing) return entry.closing.catch(() => {});
  if (entry.releaseTimer !== null) {
    return entry.pendingRelease?.promise ?? Promise.resolve();
  }

  let resolveRelease!: () => void;
  const releasePromise = new Promise<void>((resolve) => {
    resolveRelease = resolve;
  });
  entry.pendingRelease = { promise: releasePromise, resolve: resolveRelease };
  entry.releaseTimer = setTimeout(() => {
    entry.releaseTimer = null;
    entry.pendingRelease = null;
    if (entry.holders.size > 0) {
      resolveRelease();
      return;
    }
    entry.closing = entry.promise.then((client) => {
      entry.shutdownStarted = true;
      return client.shutdown();
    });
    entry.closing.then(
      () => {
        if (registry.get(key) === entry) {
          registry.delete(key);
        }
        resolveRelease();
      },
      () => resolveRelease(),
    );
  }, 0);
  return releasePromise;
}

/** Test-only: drop all entries without shutting them down. */
export function resetClientRegistryForTest(): void {
  for (const entry of registry.values()) {
    if (entry.releaseTimer !== null) clearTimeout(entry.releaseTimer);
  }
  registry.clear();
}
