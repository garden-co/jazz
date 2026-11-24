import { readdir, readFile, stat } from "node:fs/promises";
import { join } from "node:path";

/** Every byte under `path` (a file, or a directory of them). */
export async function storedBytes(path: string): Promise<Buffer> {
  if (!(await stat(path)).isDirectory()) return readFile(path);
  const entries = await readdir(path);
  return Buffer.concat(await Promise.all(entries.map((entry) => storedBytes(join(path, entry)))));
}
