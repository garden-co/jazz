#!/usr/bin/env node
// Generate schema/app.ts and schema/current.sql from schema/current.ts by
// invoking the Jazz codegen modules directly.
import { writeFileSync } from "node:fs";
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
const { generateClient } = await import(codegenPath);
const { schemaToSql } = await import(sqlGenPath);

const schema = getCollectedSchema();
const generatedClient = generateClient(schema).replace(
  /^\/\/ AUTO-GENERATED FILE - DO NOT EDIT\r?\n/,
  "",
).trimEnd();

const appParts = [
  "// AUTO-GENERATED FILE - DO NOT EDIT",
  "// Regenerate via: node scripts/generate-app.mjs",
  generatedClient,
];

writeFileSync(appFile, `${appParts.join("\n\n")}\n`, "utf-8");
writeFileSync(sqlFile, schemaToSql(schema), "utf-8");
console.log(`wrote ${appFile}`);
console.log(`wrote ${sqlFile}`);
