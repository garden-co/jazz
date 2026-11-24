import { readdir, readFile, stat } from "node:fs/promises";
import { join } from "node:path";

/** Reads a test store in full, including any SQLite WAL or journal beside a file store. */
export async function storedBytes(path: string): Promise<Buffer> {
  if ((await stat(path)).isDirectory()) return readDirectory(path);

  const parts = [await readFile(path)];
  for (const suffix of ["-wal", "-journal"]) {
    try {
      parts.push(await readFile(path + suffix));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
  return Buffer.concat(parts);
}

async function readDirectory(path: string): Promise<Buffer> {
  const entries = await readdir(path);
  const parts = await Promise.all(
    entries.map(async (entry) => {
      const child = join(path, entry);
      return (await stat(child)).isDirectory() ? readDirectory(child) : readFile(child);
    }),
  );
  return Buffer.concat(parts);
}
