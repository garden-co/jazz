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
  return withStore("readwrite", (store) => store.put(now + STREAM_ENDPOINT_MEMORY_MS, url)).then(
    () => undefined,
    () => undefined,
  );
}

/** Forget `url`: its stream was refused, so the next session tries a socket again. */
export function forgetStreamEndpoint(url: string): Promise<void> {
  return withStore("readwrite", (store) => store.delete(url)).then(
    () => undefined,
    () => undefined,
  );
}
