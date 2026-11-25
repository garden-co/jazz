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

// The errors live on their own so a store (the browser's) can throw them
// without bringing this module's code into the thin client's first load.
import { PrivateStoreConflictError, PrivateStoreWipedError } from "./private-store-errors.js";

export { PrivateStoreConflictError, PrivateStoreWipedError };

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
 * A store in memory, **for tests only** (and sessions that keep nothing). It
 * protects nothing: `protect` returns the secret as it is, and the device's
 * keys are lost when the app restarts. Never ship it in an app: on React
 * Native and Expo, supply a store whose protecting key lives in the Keychain or
 * the Android Keystore (see the docs' Private store page).
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
  /** Wakes the store now, replaces its deadline, or cancels the deadline. */
  setStoreWake?(callback: ((wake: string) => void) | null): void;
  /** False when the host can signal store work only. */
  readonly supportsEncryptionStateWake?: boolean;
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
type QueuedRequest = {
  request: StoreRequest;
  timer: ReturnType<typeof setTimeout>;
  expired: boolean;
  expire?: () => void;
};

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
 * `stop()` stops taking new requests. A host closing its runtime can also cancel answers
 * already in flight. Timers do not keep the process alive.
 */
export function servePrivateStore(
  target: StoreTarget,
  store: PrivateStore,
  intervalMs = 2,
): {
  stop(options?: { cancelPending?: boolean }): void;
  drain(): Promise<void>;
  onStateChange?: (listener: () => void) => () => void;
} {
  const queue: QueuedRequest[] = [];
  const stop = new AbortController();
  const { signal } = stop;
  let running: Promise<void> | undefined;
  let servicing = true;
  let scheduled = false;
  let stateChanged: (() => void) | undefined;
  let clock: ReturnType<typeof setTimeout> | undefined;
  const drain = async () => {
    for (let entry = queue.shift(); entry; entry = queue.shift()) {
      const { request, timer } = entry;
      if (entry.expired) continue;
      try {
        const payload = JSON.stringify(await untilExpired(answer(store, request), signal, entry));
        if (signal.aborted) return;
        if (!entry.expired && !target.replyStore(request.id, true, payload)) refused(request);
      } catch (error) {
        if (signal.aborted) return;
        if (entry.expired) continue;
        const reason =
          error instanceof PrivateStoreConflictError
            ? "conflict"
            : error instanceof PrivateStoreWipedError
              ? "wiped"
              : error instanceof Error
                ? error.message
                : String(error);
        if (!target.replyStore(request.id, false, reason)) refused(request);
      } finally {
        clearTimeout(timer);
      }
    }
  };
  const tick = () => {
    if (signal.aborted) return Promise.resolve();
    for (const text of target.takeStoreRequests()) {
      const request = JSON.parse(text) as StoreRequest;
      const entry = {
        request,
        expired: false,
        timer: setTimeout(function expireStoreRequest() {
          entry.expired = true;
          try {
            target.replyStore(
              request.id,
              false,
              `the private store did not answer \`${request.op}\` in time`,
            );
          } finally {
            entry.expire?.();
          }
        }, STORE_ANSWER_MS),
      } as QueuedRequest;
      (entry.timer as { unref?: () => void }).unref?.();
      queue.push(entry);
    }
    if (queue.length > 0)
      running ??= drain().finally(() => {
        running = undefined;
        if (queue.length > 0) void tick();
      });
    return running ?? Promise.resolve();
  };
  const wake = (urgency: string) => {
    if (!servicing) return;
    if (urgency === "encryption-state") {
      // The binding can signal in the middle of polling a Rust future.
      queueMicrotask(() => {
        if (servicing) stateChanged?.();
      });
      return;
    }
    if (urgency === "immediate") {
      if (scheduled) return;
      scheduled = true;
      // Rust may call this in the middle of polling a future.
      queueMicrotask(() => {
        scheduled = false;
        if (servicing) void tick();
      });
      return;
    }
    clearTimeout(clock);
    if (urgency === "cancel") return;
    const delay = Number(urgency.slice("after:".length));
    const deadline = performance.now() + delay;
    const schedule = (wait: number) => {
      clock = setTimeout(function wakeAtDeadline() {
        // Rust still considers the deadline armed when a timer fires early.
        const remaining = deadline - performance.now();
        if (remaining > 0) schedule(Math.ceil(remaining));
        else void tick();
      }, wait);
      (clock as { unref?: () => void }).unref?.();
    };
    schedule(delay);
  };
  let timer: ReturnType<typeof setInterval> | undefined;
  if (target.setStoreWake) {
    target.setStoreWake(wake);
    wake("immediate");
  } else {
    // Older bindings and custom store targets have no wake export.
    timer = setInterval(() => void tick(), intervalMs);
    (timer as { unref?: () => void }).unref?.();
  }
  return {
    onStateChange:
      target.setStoreWake && target.supportsEncryptionStateWake !== false
        ? (listener) => {
            stateChanged = listener;
            return () => {
              stateChanged = undefined;
            };
          }
        : undefined,
    stop({ cancelPending = false } = {}) {
      if (servicing) {
        servicing = false;
        target.setStoreWake?.(null);
      }
      clearInterval(timer);
      clearTimeout(clock);
      if (cancelPending) {
        for (const entry of queue) clearTimeout(entry.timer);
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

function untilExpired<T>(work: Promise<T>, signal: AbortSignal, entry: QueuedRequest): Promise<T> {
  return new Promise((resolve, reject) => {
    const expire = () => {
      cleanup();
      reject(new StoreAnswerTimeout());
    };
    const cleanup = () => {
      entry.expire = undefined;
      signal.removeEventListener("abort", expire);
    };
    entry.expire = expire;
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
