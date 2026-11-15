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
 * ~10-20 ms on every load. Workers have no Web Storage and use IndexedDB
 * alone. Per endpoint, the copy holds what IndexedDB said, `stream:<until>`
 * or `none:<until>`; an endpoint it holds nothing for (not asked yet, a
 * write that failed, storage full) is looked up in IndexedDB, as before.
 */
const COPY_PREFIX = "jazz-stream-endpoint:";

/**
 * How long a page trusts its copy's "no stream" before asking IndexedDB
 * again: a worker (which writes IndexedDB alone) may have learned meanwhile
 * that the endpoint serves the stream.
 */
export const NO_STREAM_COPY_MS = 10 * 60 * 1000;

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
 * where it has no answer (a worker, Node, storage refused, nothing kept for
 * `url` yet, or what it kept expired, which it then removes).
 */
export function recallStreamEndpointNow(url: string, now = Date.now()): boolean | undefined {
  const storage = pageStorage();
  if (!storage) return undefined;
  try {
    const kept = storage.getItem(COPY_PREFIX + url);
    if (kept === null) return undefined;
    const [kind, until] = kept.split(":");
    if ((kind !== "stream" && kind !== "none") || !(Number(until) > now)) {
      storage.removeItem(COPY_PREFIX + url);
      return undefined;
    }
    return kind === "stream";
  } catch {
    return undefined;
  }
}

/** Keep `kind` for `url` in the page's copy, until `until`. A write that
 * fails leaves nothing for `url`, so the page asks IndexedDB instead. */
function keepCopy(url: string, kind: "stream" | "none", until: number): void {
  const storage = pageStorage();
  if (!storage) return;
  try {
    storage.setItem(COPY_PREFIX + url, `${kind}:${until}`);
  } catch {
    try {
      storage.removeItem(COPY_PREFIX + url);
    } catch {
      // Storage refused altogether: `recallStreamEndpointNow` can't read it either.
    }
  }
}

/** Whether `url`'s backend said, within {@link STREAM_ENDPOINT_MEMORY_MS}, that it serves the stream instead. */
export async function recallStreamEndpoint(url: string, now = Date.now()): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const read = await Promise.race([
    withStore<unknown>("readonly", (store) => store.get(url)).then(
      (until) => ({ until }),
      () => undefined,
    ),
    new Promise<undefined>((resolve) => {
      timer = setTimeout(() => resolve(undefined), RECALL_TIMEOUT_MS);
    }),
  ]).finally(() => clearTimeout(timer));
  // Timed out or failed: no answer to keep.
  if (!read) return false;
  const stream = typeof read.until === "number" && read.until > now;
  if (stream) keepCopy(url, "stream", read.until as number);
  else keepCopy(url, "none", now + NO_STREAM_COPY_MS);
  return stream;
}

/** Remember that `url`'s backend serves the stream instead of sockets. */
export function rememberStreamEndpoint(url: string, now = Date.now()): Promise<void> {
  keepCopy(url, "stream", now + STREAM_ENDPOINT_MEMORY_MS);
  return withStore("readwrite", (store) => store.put(now + STREAM_ENDPOINT_MEMORY_MS, url)).then(
    () => undefined,
    () => undefined,
  );
}

/** Forget `url`: its stream was refused, so the next session tries a socket again. */
export function forgetStreamEndpoint(url: string): Promise<void> {
  keepCopy(url, "none", Date.now() + NO_STREAM_COPY_MS);
  return withStore("readwrite", (store) => store.delete(url)).then(
    () => undefined,
    () => undefined,
  );
}
