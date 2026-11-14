import { existsSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, expect, it, vi } from "vitest";

// The format can't express every app yet (SPEC 10.10): validate warns and
// writes neither file, and the app stays as on .62.
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

it("validate warns and writes no files for an app the format can't express yet", async () => {
  const root = await mkdtemp(join(tmpdir(), "jazz-validate-unsupported-"));
  roots.push(root);
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "schema.ts"),
    `import { schema as s } from ${JSON.stringify(indexPath)}; export const app = s.defineApp({notes:s.table({body:s.string()}, {})}, { migrations: "none" });`,
  );
  await writeFile(join(root, "permissions.ts"), `export default {};`);
  const result = await validateProject({ schemaDir: root });
  expect(result.warnings.join("\n")).toMatch(
    /jazz\.app\.json and jazz\.permissions\.json were not written: .*not supported in jazz\.permissions\.json yet/,
  );
  expect(existsSync(join(root, "jazz.app.json"))).toBe(false);
  expect(existsSync(join(root, "jazz.permissions.json"))).toBe(false);
  // --check doesn't refuse an app it can't write files for.
  await expect(validateProject({ schemaDir: root, check: true })).resolves.toBeTruthy();
});
