/**
 * Helpers for the IndexedDB quota tests. Each of those files caps storage
 * with a Chromium DevTools quota override, which reaches IndexedDB only if
 * nothing in this browser has used the origin's IndexedDB before (Chromium
 * keeps using the space it last learned of). So each file runs alone in a
 * browser of its own (pnpm test:browser:storage-quota), and the shared
 * browser run leaves them out.
 */

export const chromium = /Chrome\//.test(navigator.userAgent);

/** A title of about `bytes` that no codec shrinks. */
export function incompressible(bytes: number): string {
  const raw = new Uint8Array(Math.ceil((bytes * 3) / 4));
  for (let offset = 0; offset < raw.length; offset += 65536) {
    crypto.getRandomValues(raw.subarray(offset, offset + 65536));
  }
  let binary = "";
  for (const byte of raw) binary += String.fromCharCode(byte);
  return btoa(binary);
}

export async function usage(): Promise<number> {
  return (await navigator.storage.estimate()).usage ?? 0;
}

/**
 * The bytes of SQLite pages the IndexedDB VFS holds, read straight from its
 * object store (crates/jazz-wasm/src/idb_vfs.rs). The origin's usage can't
 * show that a write landed: Chromium compacts its IndexedDB files, and usage
 * can drop while new pages are written.
 */
export async function vfsBytes(): Promise<number> {
  const idb = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open("jazz-idb");
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
  try {
    const blocks = await new Promise<{ data: Uint8Array }[]>((resolve, reject) => {
      const request = idb.transaction("blocks").objectStore("blocks").getAll();
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    return blocks.reduce((total, block) => total + block.data.byteLength, 0);
  } finally {
    idb.close();
  }
}
