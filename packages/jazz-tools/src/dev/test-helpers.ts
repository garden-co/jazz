import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { APP_FILE, PERMISSIONS_FILE } from "./app-files.js";
import { generateApp } from "./project.js";

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const tmpBase = join(packageRoot, ".test-tmp");

export function createTempRootTracker() {
  const roots: string[] = [];

  return {
    async create(prefix: string): Promise<string> {
      // Create inside the package so Node's ESM resolver walks up and
      // finds node_modules for bare-specifier imports (jazz-tools in jazz.generated.ts).
      await mkdir(tmpBase, { recursive: true });
      const rootPath = await mkdtemp(join(tmpBase, prefix));
      roots.push(rootPath);
      return rootPath;
    },
    async cleanup(): Promise<void> {
      await Promise.all(roots.splice(0).map((p) => rm(p, { recursive: true, force: true })));
    },
  };
}

export async function getAvailablePort(): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const srv = createServer();
    srv.once("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const address = srv.address();
      if (!address || typeof address === "string") {
        srv.close(() => reject(new Error("Failed to allocate port")));
        return;
      }
      const port = address.port;
      srv.close(() => resolve(port));
    });
  });
}

/** A todos app's `jazz.app.json`: `todos { title: string, done: boolean }`. */
export function todoSchema(): string {
  return `${JSON.stringify(
    { format: 1, tables: { todos: { columns: { title: "string", done: "boolean" } } } },
    null,
    2,
  )}\n`;
}

/** Its `jazz.permissions.json`: anyone may read and insert todos. */
export function todoPermissions(): string {
  return `${JSON.stringify(
    { format: 1, tables: { todos: { read: "true", insert: "true" } } },
    null,
    2,
  )}\n`;
}

/**
 * Write the todos app's files into `dir`: `jazz.app.json`,
 * `jazz.permissions.json`, and the `jazz.generated.ts` generate writes.
 */
export async function writeTodoSchema(dir: string): Promise<void> {
  await writeFile(join(dir, APP_FILE), todoSchema());
  await writeFile(join(dir, PERMISSIONS_FILE), todoPermissions());
  await generateApp({ cwd: dir });
}
