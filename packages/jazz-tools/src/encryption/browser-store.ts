/**
 * The browser's built-in private store for end-to-end encryption: this
 * device's keys, in IndexedDB, protected by a WebCrypto key that scripts can
 * use but never export. Nothing to configure.
 *
 * Every tab of the app opens the same database, so the store is built to be
 * shared:
 *
 * - Each call that changes state (a save, making or deleting a scope) is one
 *   IndexedDB transaction, and the browser runs transactions that touch the
 *   same stores one after another. A save checks the document's revision and
 *   the scope's incarnation inside that transaction, so two tabs saving at
 *   once never overwrite each other: the later one gets a conflict, reads
 *   again and merges (the contract of {@link PrivateStore}).
 * - The one thing a transaction cannot cover is making the protecting key: it
 *   is created in a step of its own, and two tabs doing it at once would each
 *   protect secrets with a different key, one of them unreadable. That step
 *   runs under a Web Lock, so a second tab waits for the first tab's key and
 *   uses it. Scopes are deleted under the same kind of lock, so a tab that is
 *   making a scope waits for a delete in progress and never sees half of it.
 *   Where Web Locks do not exist the key is made with an add-only IndexedDB
 *   write, and the tab that loses uses the key that won.
 */
import {
  PrivateStoreConflictError,
  PrivateStoreWipedError,
  type PrivateStore,
  type StoredDocument,
} from "./private-store.js";

/** Where the store keeps its state; one database per origin unless a name is given. */
const DEFAULT_NAME = "jazz-private-store";
/** The format of a protected secret: this byte, the nonce, then the sealed bytes. */
const PROTECTED_FORMAT = 1;
const NONCE_BYTES = 12;

export interface BrowserPrivateStoreOptions {
  /** The IndexedDB database's name (default `jazz-private-store`). */
  name?: string;
  /** For tests: an IndexedDB factory other than the page's. */
  indexedDB?: IDBFactory;
  /** For tests: a lock manager other than the page's; `null` for none. */
  locks?: Pick<LockManager, "request"> | null;
  /** For tests: a WebCrypto other than the page's. */
  crypto?: Crypto;
}

type ScopeRow = { name: string; incarnation: string };
type DocRow = { key: string; scope: string; doc: string; bytes: Uint8Array; revision: string };

const SCOPES = "scopes";
const DOCS = "docs";
const KEYS = "keys";
const WRAP_KEY = "wrap";

const docKey = (scope: string, doc: string) => `${scope}\u0000${doc}`;

function wait<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function finished(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error);
    transaction.onabort = () => reject(transaction.error ?? new Error("transaction aborted"));
  });
}

/** A random incarnation: 52 bits, never 0, so it fits a JavaScript number as well as a `u64`. */
function newIncarnation(crypto: Crypto): bigint {
  const bytes = crypto.getRandomValues(new Uint8Array(6));
  let value = 0n;
  for (const byte of bytes) value = (value << 8n) | BigInt(byte);
  return value === 0n ? 1n : value;
}

/** Creates the store; see the module description. */
export function browserPrivateStore(options: BrowserPrivateStoreOptions = {}): PrivateStore {
  const name = options.name ?? DEFAULT_NAME;
  const factory = options.indexedDB ?? globalThis.indexedDB;
  const crypto = options.crypto ?? globalThis.crypto;
  const locks = options.locks === undefined ? (globalThis.navigator?.locks ?? null) : options.locks;
  if (!factory) throw new Error("this environment has no IndexedDB for the private store");

  let opened: Promise<IDBDatabase> | undefined;
  const database = () =>
    (opened ??= new Promise<IDBDatabase>((resolve, reject) => {
      const request = factory.open(name, 1);
      request.onupgradeneeded = () => {
        const db = request.result;
        db.createObjectStore(SCOPES, { keyPath: "name" });
        const docs = db.createObjectStore(DOCS, { keyPath: "key" });
        docs.createIndex("scope", "scope");
        db.createObjectStore(KEYS);
      };
      request.onsuccess = () => {
        const db = request.result;
        // Another tab upgrading the database, or a close from the browser:
        // start over on the next call.
        db.onversionchange = () => {
          db.close();
          opened = undefined;
        };
        db.onclose = () => {
          opened = undefined;
        };
        resolve(db);
      };
      request.onerror = () => {
        opened = undefined;
        reject(request.error);
      };
    }));

  const exclusive = async <T>(lock: string, work: () => Promise<T>): Promise<T> => {
    if (!locks) return work();
    return (await locks.request(`${name}:${lock}`, { mode: "exclusive" }, work)) as T;
  };

  /** The key that protects secrets: made once, by whichever tab gets there first. */
  let wrapKey: Promise<CryptoKey> | undefined;
  const protector = () =>
    (wrapKey ??= exclusive("wrap-key", async () => {
      const db = await database();
      const read = () => wait(db.transaction(KEYS).objectStore(KEYS).get(WRAP_KEY));
      const found = (await read()) as CryptoKey | undefined;
      if (found) return found;
      const made = await crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, false, [
        "encrypt",
        "decrypt",
      ]);
      const transaction = db.transaction(KEYS, "readwrite");
      try {
        // Add only: where there are no locks, the tab that lost the race
        // uses the key that won.
        await wait(transaction.objectStore(KEYS).add(made, WRAP_KEY));
        return made;
      } catch {
        return (await read()) as CryptoKey;
      }
    }).catch((error) => {
      wrapKey = undefined;
      throw error;
    }));

  return {
    async load(scope, doc): Promise<StoredDocument | null> {
      const db = await database();
      const row = (await wait(db.transaction(DOCS).objectStore(DOCS).get(docKey(scope, doc)))) as
        | DocRow
        | undefined;
      return row ? { bytes: new Uint8Array(row.bytes), revision: BigInt(row.revision) } : null;
    },

    async openScope(scope) {
      return exclusive(`scope:${scope}`, async () => {
        const db = await database();
        const transaction = db.transaction([SCOPES, DOCS], "readwrite");
        const store = transaction.objectStore(SCOPES);
        const found = (await wait(store.get(scope))) as ScopeRow | undefined;
        if (found) return BigInt(found.incarnation);
        // Documents a scope has without an incarnation are a wiped device's:
        // never read or adopted again (a delete here is one transaction, so
        // this holds the contract for state left by anything else).
        const docs = transaction.objectStore(DOCS);
        for (const key of (await wait(docs.index("scope").getAllKeys(scope))) as IDBValidKey[]) {
          docs.delete(key);
        }
        const incarnation = newIncarnation(crypto);
        store.put({ name: scope, incarnation: incarnation.toString() } satisfies ScopeRow);
        await finished(transaction);
        return incarnation;
      });
    },

    async save(scope, incarnation, doc, bytes, expected) {
      const db = await database();
      const transaction = db.transaction([SCOPES, DOCS], "readwrite");
      const settled = finished(transaction);
      // Every outcome below is decided inside this one transaction.
      const verdict = (async () => {
        const row = (await wait(transaction.objectStore(SCOPES).get(scope))) as
          | ScopeRow
          | undefined;
        if (!row || BigInt(row.incarnation) !== incarnation) return "wiped" as const;
        const docs = transaction.objectStore(DOCS);
        const current = (await wait(docs.get(docKey(scope, doc)))) as DocRow | undefined;
        const have = current ? BigInt(current.revision) : null;
        if (have !== expected) return "conflict" as const;
        const revision = (have ?? 0n) + 1n;
        docs.put({
          key: docKey(scope, doc),
          scope,
          doc,
          bytes: new Uint8Array(bytes),
          revision: revision.toString(),
        } satisfies DocRow);
        return revision;
      })();
      const result = await verdict.catch((error) => {
        transaction.abort();
        throw error;
      });
      if (result === "wiped") {
        transaction.abort();
        await settled.catch(() => {});
        throw new PrivateStoreWipedError();
      }
      if (result === "conflict") {
        transaction.abort();
        await settled.catch(() => {});
        throw new PrivateStoreConflictError();
      }
      await settled;
      return result;
    },

    async list(scope) {
      const db = await database();
      const rows = (await wait(
        db.transaction(DOCS).objectStore(DOCS).index("scope").getAll(scope),
      )) as DocRow[];
      return rows.map((row) => row.doc);
    },

    async delete(scope) {
      await exclusive(`scope:${scope}`, async () => {
        const db = await database();
        const transaction = db.transaction([SCOPES, DOCS], "readwrite");
        transaction.objectStore(SCOPES).delete(scope);
        const docs = transaction.objectStore(DOCS);
        const keys = (await wait(docs.index("scope").getAllKeys(scope))) as IDBValidKey[];
        for (const key of keys) docs.delete(key);
        await finished(transaction);
      });
    },

    async protect(secret) {
      const key = await protector();
      const nonce = crypto.getRandomValues(new Uint8Array(NONCE_BYTES));
      const sealed = new Uint8Array(
        await crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce }, key, secret as BufferSource),
      );
      const blob = new Uint8Array(1 + NONCE_BYTES + sealed.length);
      blob[0] = PROTECTED_FORMAT;
      blob.set(nonce, 1);
      blob.set(sealed, 1 + NONCE_BYTES);
      return blob;
    },

    async unprotect(blob) {
      if (blob.length <= 1 + NONCE_BYTES || blob[0] !== PROTECTED_FORMAT) {
        throw new Error("the protected secret has an unknown format");
      }
      const key = await protector();
      const nonce = blob.slice(1, 1 + NONCE_BYTES);
      return new Uint8Array(
        await crypto.subtle.decrypt(
          { name: "AES-GCM", iv: nonce },
          key,
          blob.slice(1 + NONCE_BYTES) as BufferSource,
        ),
      );
    },
  };
}
