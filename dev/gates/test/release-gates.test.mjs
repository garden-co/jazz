import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");

const CONVENTIONAL_PROVENANCE = new Set(["createdAt", "createdBy", "updatedAt", "updatedBy"]);

/**
 * Tables whose columns mirror an outside system's own model, which names its
 * own timestamps: Better Auth's (`better_auth_*`, written by its adapter).
 */
const EXTERNAL_MODEL_TABLE = /^better_auth_/;

/** The `table.column`s of `app` (a parsed jazz.app.json) that duplicate Jazz provenance. */
function conventionalProvenanceColumns(app) {
  return Object.entries(app.tables ?? {}).flatMap(([table, definition]) =>
    EXTERNAL_MODEL_TABLE.test(table)
      ? []
      : Object.keys(definition.columns ?? {})
          .filter((column) => CONVENTIONAL_PROVENANCE.has(column))
          .map((column) => `${table}.${column}`),
  );
}

test("provenance gate targets only app tables' Jazz provenance aliases", () => {
  assert.deepEqual(
    conventionalProvenanceColumns({
      tables: {
        posts: { columns: { createdAt: "timestamp", createdOn: "timestamp", title: "string" } },
        better_auth_user: { columns: { createdAt: "timestamp", updatedAt: "timestamp" } },
      },
    }),
    ["posts.createdAt"],
  );
});

test("official examples do not duplicate Jazz provenance", () => {
  const offenders = [];

  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        if (entry.name !== "node_modules") visit(file);
      } else if (entry.isFile() && entry.name === "jazz.app.json") {
        for (const column of conventionalProvenanceColumns(
          JSON.parse(fs.readFileSync(file, "utf8")),
        ))
          offenders.push(`${path.relative(root, file)}: ${column}`);
      }
    }
  }

  visit(path.join(root, "examples"));
  assert.deepEqual(
    offenders,
    [],
    "Examples must use Jazz's $createdAt, $createdBy, $updatedAt and $updatedBy provenance columns",
  );
});
