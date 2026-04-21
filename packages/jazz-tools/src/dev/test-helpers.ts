import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const indexPath = fileURLToPath(new URL("../index.ts", import.meta.url));
const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const tmpBase = join(packageRoot, ".test-tmp");

export function createTempRootTracker() {
  const roots: string[] = [];

  return {
    async create(prefix: string): Promise<string> {
      // Create inside the package so Node's ESM resolver walks up and
      // finds node_modules for bare-specifier imports in bundled schema files.
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

export function todoSchema(): string {
  return `
import { schema as s } from ${JSON.stringify(indexPath)};

const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`;
}

export async function writeTodoSchema(dir: string): Promise<void> {
  await writeFile(join(dir, "schema.ts"), todoSchema());
}
