#!/usr/bin/env node
// Generate schema/app.ts and schema/current.sql from schema/current.ts.
// Newer TS-only Jazz builds no longer ship the legacy app.ts generator, so the
// checked-in app.ts is preserved when that module is absent.
import { existsSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const schemaFile = resolve(here, "../schema/current.ts");
const appFile = resolve(here, "../schema/app.ts");
const sqlFile = resolve(here, "../schema/current.sql");

await import(schemaFile);

const { getCollectedSchema } = await import("jazz-tools");
const codegenPath = resolve(
  here,
  "../../../packages/jazz-tools/dist/codegen/index.js",
);
const sqlGenPath = resolve(here, "../../../packages/jazz-tools/dist/sql-gen.js");
const schemaRuntimePath = resolve(here, "../../../packages/jazz-tools/dist/schema.js");
const codegen = await optionalImport(codegenPath);
const sqlGen = await optionalImport(sqlGenPath);
const { sqlTypeToString } = await import(schemaRuntimePath);
const SQL_RESERVED = new Set([
  "CREATE",
  "TABLE",
  "SELECT",
  "INSERT",
  "UPDATE",
  "DELETE",
  "REFERENCES",
  "NOT",
  "NULL",
]);

const schema = getCollectedSchema();
if (typeof codegen?.generateClient === "function") {
  const generatedClient = codegen.generateClient(schema).replace(
    /^\/\/ AUTO-GENERATED FILE - DO NOT EDIT\r?\n/,
    "",
  ).trimEnd();

  const appParts = [
    "// AUTO-GENERATED FILE - DO NOT EDIT",
    "// Regenerate via: node scripts/generate-app.mjs",
    generatedClient,
  ];

  writeFileSync(appFile, `${appParts.join("\n\n")}\n`, "utf-8");
  console.log(`wrote ${appFile}`);
} else if (existsSync(appFile)) {
  console.log(`kept ${appFile} (legacy codegen module not present)`);
} else {
  throw new Error(`Cannot generate ${appFile}: legacy codegen module is missing`);
}

const schemaToSql = typeof sqlGen?.schemaToSql === "function" ? sqlGen.schemaToSql : localSchemaToSql;
writeFileSync(sqlFile, schemaToSql(schema), "utf-8");
console.log(`wrote ${sqlFile}`);

async function optionalImport(path) {
  try {
    return await import(path);
  } catch (error) {
    if (error?.code === "ERR_MODULE_NOT_FOUND") {
      return null;
    }
    throw error;
  }
}

function localSchemaToSql(inputSchema) {
  return `${inputSchema.tables.map(tableToSql).join("\n\n")}\n`;
}

function tableToSql(table) {
  const columnDefs = table.columns.map(columnToSql);
  return `CREATE TABLE ${sqlIdentifier(table.name)} (\n${columnDefs.join(",\n")}\n);`;
}

function columnToSql(column) {
  const ref = column.references ? ` REFERENCES ${sqlIdentifier(column.references)}` : "";
  const nullability = column.nullable ? "" : " NOT NULL";
  return `    ${sqlIdentifier(column.name)} ${sqlTypeToString(column.sqlType)}${ref}${nullability}`;
}

function sqlIdentifier(identifier) {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier) && !SQL_RESERVED.has(identifier.toUpperCase())) {
    return identifier;
  }
  return `"${identifier.replace(/"/g, '""')}"`;
}
