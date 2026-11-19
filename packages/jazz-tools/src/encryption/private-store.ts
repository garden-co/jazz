/**
 * The device's private store for end-to-end encryption: where its keys and
 * the keys it holds for each space live. The app supplies it (Node has no
 * default), because protecting it is a platform decision: keep the files in
 * a directory and the key that protects them in the operating system's key
 * store (see the docs). The shape follows the runtime's store contract:
 * documents with revisions and a compare-and-swap save, a scope with an
 * incarnation that a wipe changes, and `protect` / `unprotect` for secrets
 * at rest.
 */

/** A saved document and its revision. */
export interface StoredDocument {
  bytes: Uint8Array;
  revision: bigint;
}

/** The save found another revision than `expected`: read again and merge. */
export class PrivateStoreConflictError extends Error {
  constructor(message = "the private store changed since it was read") {
    super(message);
    this.name = "PrivateStoreConflictError";
  }
}

/** The scope was deleted (and perhaps made again) since it was opened: nothing is written. */
export class PrivateStoreWipedError extends Error {
  constructor(message = "the private store was wiped since it was opened") {
    super(message);
    this.name = "PrivateStoreWipedError";
  }
}

export interface PrivateStore {
  /** The document `doc` of `scope`, or `null`. */
  load(scope: string, doc: string): Promise<StoredDocument | null>;
  /** The incarnation of `scope`, a number fixed when the scope is made (never 0). */
  openScope(scope: string): Promise<bigint>;
  /**
   * Replaces `doc` with `bytes` if the scope still has `incarnation` and the
   * document's revision is `expected` (`null`: it must not exist), atomically,
   * and returns the new revision. Otherwise throws
   * {@link PrivateStoreWipedError} or {@link PrivateStoreConflictError} and
   * writes nothing. A save never creates a scope.
   */
  save(
    scope: string,
    incarnation: bigint,
    doc: string,
    bytes: Uint8Array,
    expected: bigint | null,
  ): Promise<bigint>;
  /** The names of the documents of `scope`. */
  list(scope: string): Promise<string[]>;
  /**
   * Removes every document of `scope`. Delete order is part of the contract:
   * remove the scope's incarnation first, then its documents, so a delete
   * that stops half way leaves documents without an incarnation. A store
   * that finds a scope in that state treats it as wiped: `openScope` removes
   * what is left and makes a new scope with a new incarnation, and nothing
   * left over is ever read or adopted again (a wiped device's keys must not
   * come back).
   */
  delete(scope: string): Promise<void>;
  /** Protects a secret at rest (the device's private keys, its approval nonce). */
  protect(secret: Uint8Array): Promise<Uint8Array>;
  /** Recovers a secret `protect` protected. */
  unprotect(blob: Uint8Array): Promise<Uint8Array>;
}

/**
 * A store in memory, for tests and sessions that keep nothing. It protects
 * nothing: its secrets are as they are.
 */
export function memoryPrivateStore(): PrivateStore {
  const scopes = new Map<string, { incarnation: bigint; docs: Map<string, StoredDocument> }>();
  let next = 1n;
  return {
    async load(scope, doc) {
      return scopes.get(scope)?.docs.get(doc) ?? null;
    },
    async openScope(scope) {
      let found = scopes.get(scope);
      if (!found) {
        found = { incarnation: next++, docs: new Map() };
        scopes.set(scope, found);
      }
      return found.incarnation;
    },
    async save(scope, incarnation, doc, bytes, expected) {
      const found = scopes.get(scope);
      if (!found || found.incarnation !== incarnation) throw new PrivateStoreWipedError();
      const current = found.docs.get(doc);
      if ((current?.revision ?? null) !== expected) throw new PrivateStoreConflictError();
      const revision = (current?.revision ?? 0n) + 1n;
      found.docs.set(doc, { bytes: bytes.slice(), revision });
      return revision;
    },
    async list(scope) {
      return [...(scopes.get(scope)?.docs.keys() ?? [])];
    },
    async delete(scope) {
      scopes.delete(scope);
    },
    async protect(secret) {
      return secret.slice();
    },
    async unprotect(blob) {
      return blob.slice();
    },
  };
}

/** The native side: the store's requests and where its answers go. */
export interface StoreTarget {
  takeStoreRequests(): string[];
  /** `false`: no such request is outstanding, and the answer was dropped. */
  replyStore(id: string, ok: boolean, payload: string): boolean;
}

// Without Buffer: this runs in browsers too.
const encode = (bytes: Uint8Array) => {
  let binary = "";
  for (let at = 0; at < bytes.length; at += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(at, at + 0x8000));
  }
  return btoa(binary);
};
const decode = (text: string) => Uint8Array.from(atob(text), (char) => char.charCodeAt(0));

type StoreRequest = Record<string, string | null> & { id: string; op: string };

async function answer(store: PrivateStore, request: StoreRequest): Promise<unknown> {
  const text = (name: string) => request[name] as string;
  switch (request.op) {
    case "load": {
      const found = await store.load(text("scope"), text("doc"));
      return found && { bytes: encode(found.bytes), revision: found.revision.toString() };
    }
    case "openScope":
      return (await store.openScope(text("scope"))).toString();
    case "save":
      return (
        await store.save(
          text("scope"),
          BigInt(text("incarnation")),
          text("doc"),
          decode(text("bytes")),
          request.expected === null ? null : BigInt(text("expected")),
        )
      ).toString();
    case "list":
      return store.list(text("scope"));
    case "delete":
      await store.delete(text("scope"));
      return null;
    case "protect":
      return encode(await store.protect(decode(text("secret"))));
    case "unprotect":
      return encode(await store.unprotect(decode(text("blob"))));
    default:
      throw new Error(`unknown private store request ${request.op}`);
  }
}

/** The runtime had no such request outstanding (it timed out or was cancelled): its answer was dropped. */
function refused(request: StoreRequest) {
  console.warn(
    `Jazz dropped the private store's answer to \`${request.op}\` (request ${request.id}): the runtime was no longer waiting for it.`,
  );
}

/**
 * Runs the app's store for the native runtime: takes its requests as they
 * arise, runs each against `store` (one at a time, in order) and answers.
 * `stop()` stops polling. A host closing its runtime can also cancel answers
 * already in flight. The timer never keeps the process alive.
 */
export function servePrivateStore(
  target: StoreTarget,
  store: PrivateStore,
  intervalMs = 2,
): { stop(options?: { cancelPending?: boolean }): void; drain(): Promise<void> } {
  const queue: StoreRequest[] = [];
  const stop = new AbortController();
  const { signal } = stop;
  let running: Promise<void> | undefined;
  const drain = async () => {
    for (let request = queue.shift(); request; request = queue.shift()) {
      try {
        // A store that never answers must not hold the others back: the
        // runtime fails that call after the same time, and its late answer
        // is refused.
        const payload = JSON.stringify(await withinAnswerTime(answer(store, request), signal));
        if (signal.aborted) return;
        if (!target.replyStore(request.id, true, payload)) refused(request);
      } catch (error) {
        if (signal.aborted) return;
        if (error instanceof StoreAnswerTimeout) continue;
        const reason =
          error instanceof PrivateStoreConflictError
            ? "conflict"
            : error instanceof PrivateStoreWipedError
              ? "wiped"
              : error instanceof Error
                ? error.message
                : String(error);
        if (!target.replyStore(request.id, false, reason)) refused(request);
      }
    }
  };
  const tick = () => {
    if (signal.aborted) return Promise.resolve();
    // Taking the requests is also how the runtime's clock runs (a request
    // nobody answers times out there), so it never waits for the store.
    for (const text of target.takeStoreRequests()) queue.push(JSON.parse(text) as StoreRequest);
    if (queue.length > 0)
      running ??= drain().finally(() => {
        running = undefined;
      });
    return running ?? Promise.resolve();
  };
  const timer = setInterval(() => void tick(), intervalMs);
  (timer as { unref?: () => void }).unref?.();
  return {
    stop({ cancelPending = false } = {}) {
      clearInterval(timer);
      if (cancelPending) {
        queue.length = 0;
        stop.abort();
      }
    },
    drain: tick,
  };
}

/** How long the app's store may take to answer a request (the runtime gives up at the same time). */
const STORE_ANSWER_MS = 30_000;

class StoreAnswerTimeout extends Error {}

function withinAnswerTime<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const expire = () => {
      cleanup();
      reject(new StoreAnswerTimeout());
    };
    const timer = setTimeout(expire, STORE_ANSWER_MS);
    (timer as { unref?: () => void }).unref?.();
    const cleanup = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", expire);
    };
    signal.addEventListener("abort", expire);
    if (signal.aborted) expire();
    work.then(
      (value) => {
        cleanup();
        resolve(value);
      },
      (error) => {
        cleanup();
        reject(error);
      },
    );
  });
}
