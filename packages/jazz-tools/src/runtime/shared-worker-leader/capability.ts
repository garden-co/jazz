/**
 * Capability probe: does this worker-style scope expose
 * FileSystemFileHandle.createSyncAccessHandle()? Chromium and Firefox return
 * false today inside SharedWorker (sync OPFS is dedicated-Worker-only).
 * Safari returns true.
 *
 * Must be cheap and self-cleaning — it runs once per SharedWorker boot.
 */
export async function detectSyncOpfsInWorkerScope(): Promise<boolean> {
  const nav = (globalThis as { navigator?: { storage?: { getDirectory?: unknown } } }).navigator;
  const getDirectory = nav?.storage?.getDirectory as
    | (() => Promise<FileSystemDirectoryHandle>)
    | undefined;
  if (typeof getDirectory !== "function") return false;

  const name = `__jazz_leader_probe_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  let root: FileSystemDirectoryHandle;
  try {
    root = await getDirectory.call(nav!.storage);
  } catch {
    return false;
  }

  let supported = false;
  try {
    const fileHandle = await root.getFileHandle(name, { create: true });
    // `FileSystemSyncAccessHandle` is a worker-only DOM type (TS `webworker`
    // lib). This package compiles against the `dom` lib, so reference a minimal
    // structural type instead — we only ever call `close()` on the handle.
    const createSync = (
      fileHandle as unknown as {
        createSyncAccessHandle?: () => Promise<{ close(): void }>;
      }
    ).createSyncAccessHandle;
    if (typeof createSync !== "function") {
      supported = false;
    } else {
      const sync = await createSync.call(fileHandle);
      supported = true;
      try {
        sync.close();
      } catch {
        // best-effort
      }
    }
  } catch {
    supported = false;
  }

  try {
    await root.removeEntry(name);
  } catch {
    // best-effort — sync handle may not be GC'd yet on some engines
  }
  return supported;
}
