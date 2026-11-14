import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { computeSchemaHash } from "../schema-hash.js";
import { buildAppFiles, checkAppFiles, unsupportedWarning } from "./app-files.js";

// validate's handling of an app the files can't express yet (a row
// column): the real writer refuses through jazz-napi
// with JAZZ_APP_FILES_UNSUPPORTED, validate turns that into a warning and
// writes neither file. The warning text is what validate prints.

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

it("validate warns, without failing, for an app the files can't express yet", async () => {
  const schema = {
    notes: {
      columns: [
        {
          name: "status",
          column_type: {
            type: "Row",
            columns: [{ name: "at", column_type: { type: "Timestamp" }, nullable: false }],
          },
          nullable: true,
        },
      ],
    },
  } as unknown as WasmSchema;
  const built = await buildAppFiles({
    current: await computeSchemaHash(schema),
    schemas: [schema],
    migrations: [],
    permissions: {},
    wasmSchema: schema,
  });
  expect("unsupported" in built).toBe(true);
  if (!("unsupported" in built)) return;
  expect(unsupportedWarning(built.unsupported)).toMatch(
    /jazz\.app\.json and jazz\.permissions\.json were not written: .*row column.*runs from its TS as before/,
  );
});

it("validate --check says what to do when a project commits neither file", async () => {
  const root = await mkdtemp(join(tmpdir(), "jazz-validate-check-absent-"));
  roots.push(root);
  await expect(
    checkAppFiles(root, join(root, "schema.ts"), { app: "{}\n", permissions: "{}\n" }),
  ).rejects.toThrow(
    "Neither jazz.app.json nor jazz.permissions.json is committed: run `jazz-tools validate` and commit jazz.app.json and jazz.permissions.json.",
  );
});
