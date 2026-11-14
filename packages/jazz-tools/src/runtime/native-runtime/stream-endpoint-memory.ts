/**
 * Endpoints whose createJazz backend said it can't upgrade sockets (426 +
 * `x-jazz-stream`), kept across page loads so that a new page, tab or
 * worker goes straight to the HTTP stream instead of failing a socket first
 * (which the browser logs as an error, #5187). In IndexedDB, as the sync
 * runtime lives in a (shared) worker, where Web Storage isn't available.
 *
 * Kept for {@link STREAM_ENDPOINT_MEMORY_MS}, so that a backend that gains
 * socket upgrades (`ws` installed, `attach()` added) gets sockets again
 * within that time; forgotten at once when the stream itself is refused.
 * Where there is no IndexedDB (Node), nothing is kept.
 */
export const STREAM_ENDPOINT_MEMORY_MS = 60 * 60 * 1000;

/** How long a lookup may take before the socket is opened as usual. */
const RECALL_TIMEOUT_MS = 500;

const DB_NAME = "jazz-http-stream-endpoints";
const STORE = "endpoints";

/** Whether endpoints can be kept here at all (not in Node). */
export function streamEndpointMemoryAvailable(): boolean {
  return (globalThis as { indexedDB?: IDBFactory }).indexedDB !== undefined;
}

function database(): Promise<IDBDatabase> | undefined {
  const factory = (globalThis as { indexedDB?: IDBFactory }).indexedDB;
  if (!factory) return undefined;
  return new Promise((resolve, reject) => {
    const open = factory.open(DB_NAME, 1);
    open.onupgradeneeded = () => open.result.createObjectStore(STORE);
    open.onsuccess = () => resolve(open.result);
    open.onerror = () => reject(open.error);
    open.onblocked = () => reject(new Error("blocked"));
  });
}

async function withStore<T>(
  mode: IDBTransactionMode,
  use: (store: IDBObjectStore) => IDBRequest<T>,
): Promise<T | undefined> {
  const opened = database();
  if (!opened) return undefined;
  const db = await opened;
  try {
    return await new Promise<T>((resolve, reject) => {
      const request = use(db.transaction(STORE, mode).objectStore(STORE));
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  } finally {
    db.close();
  }
}

/**
 * A page's copy of this memory, in Web Storage, which it reads at once:
 * looking `url` up in IndexedDB before the socket opens costs a page
 * ~10-20 ms on every load. Workers have no Web Storage and use IndexedDB alone.
 */
const MIRROR_PREFIX = "jazz-stream-endpoint:";

function pageStorage(): Storage | undefined {
  if (typeof document === "undefined") return undefined;
  try {
    return (globalThis as { localStorage?: Storage }).localStorage ?? undefined;
  } catch {
    return undefined;
  }
}

/**
 * {@link recallStreamEndpoint} at once, from a page's copy: `undefined`
 * where there is none to ask (a worker, Node, storage refused). A page
 * that has none for `url` gets `false`. Only an endpoint a worker alone
 * learned (it writes IndexedDB only) is then missed, and the page tries
 * one socket before it learns the endpoint itself.
 */
export function recallStreamEndpointNow(url: string, now = Date.now()): boolean | undefined {
  const storage = pageStorage();
  if (!storage) return undefined;
  try {
    const until = storage.getItem(MIRROR_PREFIX + url);
    return until !== null && Number(until) > now;
  } catch {
    return undefined;
  }
}

function mirror(update: (storage: Storage) => void): void {
  const storage = pageStorage();
  if (!storage) return;
  try {
    update(storage);
  } catch {
    // Storage full or refused: IndexedDB still has it.
  }
}

/** Whether `url`'s backend said, within {@link STREAM_ENDPOINT_MEMORY_MS}, that it serves the stream instead. */
export async function recallStreamEndpoint(url: string, now = Date.now()): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const until = await Promise.race([
    withStore<unknown>("readonly", (store) => store.get(url)).catch(() => undefined),
    new Promise<undefined>((resolve) => {
      timer = setTimeout(() => resolve(undefined), RECALL_TIMEOUT_MS);
    }),
  ]).finally(() => clearTimeout(timer));
  return typeof until === "number" && until > now;
}

/** Remember that `url`'s backend serves the stream instead of sockets. */
export function rememberStreamEndpoint(url: string, now = Date.now()): Promise<void> {
  mirror((storage) =>
    storage.setItem(MIRROR_PREFIX + url, String(now + STREAM_ENDPOINT_MEMORY_MS)),
  );
  return withStore("readwrite", (store) => store.put(now + STREAM_ENDPOINT_MEMORY_MS, url)).then(
    () => undefined,
    () => undefined,
  );
}

/** Forget `url`: its stream was refused, so the next session tries a socket again. */
export function forgetStreamEndpoint(url: string): Promise<void> {
  mirror((storage) => storage.removeItem(MIRROR_PREFIX + url));
  return withStore("readwrite", (store) => store.delete(url)).then(
    () => undefined,
    () => undefined,
  );
}
