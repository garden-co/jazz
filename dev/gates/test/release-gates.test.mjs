import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");

const CONVENTIONAL_PROVENANCE = new Set(["createdAt", "createdBy", "updatedAt", "updatedBy"]);

/**
 * The exact `table.column`s that mirror an outside system's own model, which
 * names its own timestamps: Better Auth's tables, written by its adapter.
 * Every other conventional-provenance column, Better Auth tables included,
 * must use Jazz's `$` provenance instead.
 */
const EXTERNAL_PROVENANCE_COLUMNS = new Set([
  "better_auth_user.createdAt",
  "better_auth_user.updatedAt",
  "better_auth_session.createdAt",
  "better_auth_session.updatedAt",
  "better_auth_account.createdAt",
  "better_auth_account.updatedAt",
  "better_auth_verification.createdAt",
  "better_auth_verification.updatedAt",
  "better_auth_jwks.createdAt",
]);

/** The `table.column`s of `app` (a parsed jazz.app.json) that duplicate Jazz provenance. */
function conventionalProvenanceColumns(app) {
  return Object.entries(app.tables ?? {}).flatMap(([table, definition]) =>
    Object.keys(definition.columns ?? {})
      .filter((column) => CONVENTIONAL_PROVENANCE.has(column))
      .map((column) => `${table}.${column}`)
      .filter((column) => !EXTERNAL_PROVENANCE_COLUMNS.has(column)),
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

test("provenance gate exempts only the listed Better Auth columns", () => {
  assert.deepEqual(
    conventionalProvenanceColumns({
      tables: {
        better_auth_user: { columns: { createdAt: "timestamp", updatedBy: "string" } },
        better_auth_jwks: { columns: { createdAt: "timestamp", updatedAt: "timestamp" } },
        better_auth_passkey: { columns: { createdAt: "timestamp" } },
      },
    }),
    ["better_auth_user.updatedBy", "better_auth_jwks.updatedAt", "better_auth_passkey.createdAt"],
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
