// Page-addressed browser persistence for the async opfs-btree experiment.
// Neither implementation exposes ordered keys, cursors, or scans: callers can
// only load opaque pages by numeric page identity and atomically commit pages.

const META = "@metadata";
const pageKey = (id) => `p:${id}`;

function request(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error || new Error("IndexedDB request failed"));
  });
}
function txDone(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = resolve;
    tx.onabort = tx.onerror = () => reject(tx.error || new Error("IndexedDB transaction failed"));
  });
}
function tx(db, mode) {
  try {
    return db.transaction("pages", mode, { durability: "relaxed" });
  } catch {
    return db.transaction("pages", mode);
  }
}

export class IndexedDbPageStore {
  static async open(name) {
    const opening = indexedDB.open(name, 1);
    opening.onupgradeneeded = () => opening.result.createObjectStore("pages");
    return new IndexedDbPageStore(name, await request(opening));
  }
  static async destroy(name) {
    await new Promise((resolve, reject) => {
      const deletion = indexedDB.deleteDatabase(name);
      deletion.onsuccess = resolve;
      deletion.onerror = () => reject(deletion.error);
      deletion.onblocked = () => reject(new Error("IndexedDB page store delete blocked"));
    });
  }
  constructor(name, db) {
    this.name = name;
    this.db = db;
  }
  close() {
    this.db.close();
  }
  async metadata() {
    const t = tx(this.db, "readonly");
    const value = await request(t.objectStore("pages").get(META));
    await txDone(t);
    return value || null;
  }
  async readPages(ids) {
    const t = tx(this.db, "readonly");
    const store = t.objectStore("pages");
    const pages = await Promise.all(
      ids.map(async (pageId) => {
        const bytes = await request(store.get(pageKey(pageId)));
        if (!bytes) throw new Error(`missing page ${pageId}`);
        return { pageId, bytes };
      }),
    );
    await txDone(t);
    return pages;
  }
  async commit({ metadata, writes, deletedPageIds }) {
    const t = tx(this.db, "readwrite");
    const store = t.objectStore("pages");
    // One relaxed transaction is the visibility boundary for page data+length.
    store.put({ pageSize: metadata.pageSize, logicalLen: metadata.logicalLen }, META);
    for (const { pageId, bytes } of writes) store.put(bytes.slice(0), pageKey(pageId));
    for (const pageId of deletedPageIds) store.delete(pageKey(pageId));
    await txDone(t);
  }
}

export class OpfsPageStore {
  static async open(name, pageSize) {
    const root = await navigator.storage.getDirectory();
    const file = await root.getFileHandle(`jazz-async-page-store-${name}.db`, { create: true });
    return new OpfsPageStore(root, file, await file.createSyncAccessHandle(), pageSize);
  }
  static async destroy(name) {
    const root = await navigator.storage.getDirectory();
    await root.removeEntry(`jazz-async-page-store-${name}.db`);
  }
  constructor(root, file, handle, pageSize) {
    this.root = root;
    this.file = file;
    this.handle = handle;
    this.pageSize = pageSize;
  }
  close() {
    this.handle.close();
  }
  async metadata() {
    const logicalLen = this.handle.getSize();
    return logicalLen === 0 ? null : { pageSize: this.pageSize, logicalLen };
  }
  async readPages(ids) {
    if (!this.pageSize) throw new Error("OPFS page size must be set before reads");
    return ids.map((pageId) => {
      const bytes = new Uint8Array(this.pageSize);
      const offset = pageId * this.pageSize;
      if (this.handle.read(bytes, { at: offset }) !== this.pageSize)
        throw new Error(`missing page ${pageId}`);
      return { pageId, bytes };
    });
  }
  async commit({ metadata, writes, deletedPageIds: _deletedPageIds }) {
    this.pageSize = metadata.pageSize;
    for (const { pageId, bytes } of writes)
      this.handle.write(new Uint8Array(bytes), { at: pageId * metadata.pageSize });
    this.handle.truncate(metadata.logicalLen);
    // The async API has the same commit visibility boundary as IndexedDB. The
    // sync handle's flush supplies a relaxed-but-complete persistence boundary.
    this.handle.flush();
  }
}
