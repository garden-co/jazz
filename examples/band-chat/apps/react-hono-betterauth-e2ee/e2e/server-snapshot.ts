import { cp, mkdtemp, readdir, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const APP_DIR = resolve(import.meta.dirname, "..");

/** Where the app server keeps its data (as server/jazz.ts reads it). */
function serverDataPath(): string {
  return resolve(APP_DIR, process.env.JAZZ_DATA_PATH || ".jazz");
}

/** Every byte the server has stored. */
export async function storedBytes(path = serverDataPath()): Promise<Buffer> {
  const info = await stat(path).catch(() => undefined);
  if (!info) return Buffer.alloc(0);
  if (!info.isDirectory()) {
    // SQLite keeps recent writes beside the file until a checkpoint.
    const parts = [await readFile(path)];
    for (const suffix of ["-wal", "-journal"])
      parts.push(await readFile(`${path}${suffix}`).catch(() => Buffer.alloc(0)));
    return Buffer.concat(parts);
  }
  const entries = await readdir(path);
  return Buffer.concat(await Promise.all(entries.map((entry) => storedBytes(join(path, entry)))));
}

type Backend = Awaited<ReturnType<typeof openSnapshot>>;

/**
 * The server's own read of its data, as BandChat's keyless backend reads it:
 * a copy of the server's store opened with the app's generated files. The
 * server keeps running; the copy is read and thrown away.
 */
export async function serverRead<R>(read: (backend: Backend) => Promise<R>): Promise<R> {
  const dir = await mkdtemp(join(tmpdir(), "band-chat-e2ee-snapshot-"));
  const source = serverDataPath();
  const copy = join(dir, "store");
  try {
    const info = await stat(source);
    if (info.isDirectory()) await cp(source, copy, { recursive: true });
    else
      for (const suffix of ["", "-wal", "-shm", "-journal"])
        await cp(`${source}${suffix}`, `${copy}${suffix}`).catch((error: NodeJS.ErrnoException) => {
          if (suffix === "" || error.code !== "ENOENT") throw error;
        });
    const backend = await openSnapshot(copy);
    try {
      return await read(backend);
    } finally {
      await backend.jazz.close();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

async function openSnapshot(path: string) {
  const { createJazz, sqlite } = await import("jazz-tools/backend");
  const { app } = await import("../jazz.generated");
  const permissions = (await import("../jazz.permissions.json", { with: { type: "json" } }))
    .default;
  const jazz = await createJazz({
    app,
    permissions,
    store: sqlite({ path }),
    authenticate: async () => null,
  }).ready();
  // Jazz's key-version table is not part of the app's typed bindings.
  const successors = (app as unknown as Record<string, Parameters<typeof jazz.db.all>[0]>)[
    "__e2ee_space_successors"
  ]!;
  return { jazz, app, successors };
}

/** The server's count of `reader`'s read markers in a room. */
export function serverMarkerCount(roomId: string, reader: string): Promise<number> {
  return serverRead(
    async ({ jazz, app }) =>
      (await jazz.db.all(app.readMarkers.where({ roomId, reader }).select("id"))).length,
  );
}

/** The server's rows of the key-version table for a room. */
export function serverSuccessors(roomId: string): Promise<Array<Record<string, unknown>>> {
  return serverRead(async ({ jazz, successors }) =>
    ((await jazz.db.all(successors)) as Array<Record<string, unknown>>).filter(
      (row) => row.identifier === roomId,
    ),
  );
}
