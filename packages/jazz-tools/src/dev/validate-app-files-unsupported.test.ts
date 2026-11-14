import { existsSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, expect, it, vi } from "vitest";

// Every .62 app has a form in the files (SPEC 10.10): should the writer
// ever refuse one, validate fails loudly instead of skipping the files.
vi.mock("jazz-napi", async (original) => ({
  ...(await original<typeof import("jazz-napi")>()),
  writeAppArtifact: () => {
    throw new Error(
      "jazz.permissions.json: tables.notes.read: a relation join is not supported in jazz.permissions.json yet",
    );
  },
}));

const { validateProject } = await import("./catalogue-project.js");

const indexPath = fileURLToPath(new URL("../index.ts", import.meta.url));
const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

it("validate refuses, as a bug to report, an app the writer can't express", async () => {
  const root = await mkdtemp(join(tmpdir(), "jazz-validate-unsupported-"));
  roots.push(root);
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "schema.ts"),
    `import { schema as s } from ${JSON.stringify(indexPath)}; export const app = s.defineApp({notes:s.table({body:s.string()}, {})}, { migrations: "none" });`,
  );
  await writeFile(join(root, "permissions.ts"), `export default {};`);
  await expect(validateProject({ schemaDir: root })).rejects.toThrow(
    /not supported in jazz\.permissions\.json yet.*report it/,
  );
  expect(existsSync(join(root, "jazz.app.json"))).toBe(false);
  expect(existsSync(join(root, "jazz.permissions.json"))).toBe(false);
});
