// The local-first replica in IndexedDB, read as raw pages: whether a wipe
// really left nothing of a user behind.

/** The IndexedDB VFS's path for a browser store (see `StoreSpec` in jazz-wasm). */
export async function vfsPath(physicalName: string): Promise<string> {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode(physicalName)),
  );
  // An array of strings: `Uint8Array.map` would turn each back into a byte.
  const hex = Array.from(digest.slice(0, 16), (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
  return `/jazz-${hex}.sqlite`;
}

/** The pages of `path` in the IndexedDB VFS, and how many hold `needle`. */
export async function vfsPages(
  path: string,
  needle: string,
): Promise<{ pages: number; holding: number }> {
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const open = indexedDB.open("jazz-idb");
    open.onsuccess = () => resolve(open.result);
    open.onerror = () => reject(open.error);
  });
  try {
    if (!db.objectStoreNames.contains("blocks")) return { pages: 0, holding: 0 };
    const rows = await new Promise<Array<{ path: string; data: Uint8Array }>>((resolve, reject) => {
      const request = db.transaction("blocks").objectStore("blocks").getAll();
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    const decoder = new TextDecoder();
    const mine = rows.filter((row) => row.path === path);
    return {
      pages: mine.length,
      holding: mine.filter((row) => decoder.decode(row.data).includes(needle)).length,
    };
  } finally {
    db.close();
  }
}
