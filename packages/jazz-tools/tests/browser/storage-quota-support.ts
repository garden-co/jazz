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
 * The SQLite pages the IndexedDB VFS holds, by file and offset, read
 * straight from its object store (crates/jazz-wasm/src/idb_vfs.rs). The
 * origin's usage can't show that a write landed: Chromium compacts its
 * IndexedDB files, and usage can drop while new pages are written.
 */
export async function vfsPages(): Promise<Map<string, Uint8Array>> {
  const idb = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open("jazz-idb");
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
  try {
    const blocks = await new Promise<{ path: string; offset: number; data: Uint8Array }[]>(
      (resolve, reject) => {
        const request = idb.transaction("blocks").objectStore("blocks").getAll();
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      },
    );
    return new Map(blocks.map((block) => [`${block.path}@${block.offset}`, block.data]));
  } finally {
    idb.close();
  }
}

/** Whether two `vfsPages()` reads hold the same pages with the same bytes. */
export function samePages(a: Map<string, Uint8Array>, b: Map<string, Uint8Array>): boolean {
  if (a.size !== b.size) return false;
  for (const [key, page] of a) {
    const other = b.get(key);
    if (!other || other.byteLength !== page.byteLength) return false;
    for (let index = 0; index < page.byteLength; index++) {
      if (page[index] !== other[index]) return false;
    }
  }
  return true;
}
