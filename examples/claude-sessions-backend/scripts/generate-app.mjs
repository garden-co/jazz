#!/usr/bin/env node
// Generate schema/app.ts from schema/current.ts by invoking the jazz-tools
// codegen module directly (its CLI `build` subcommand is not currently wired
// up, but the underlying functions work).
import { writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const schemaFile = resolve(here, "../schema/current.ts");
const outFile = resolve(here, "../schema/app.ts");

// Importing schema/current.ts registers the tables via the dsl.
await import(schemaFile);

const { getCollectedSchema } = await import("jazz-tools");
const codegenPath = resolve(
  here,
  "../../../packages/jazz-tools/dist/codegen/index.js",
);
const { analyzeRelations, generateClient, generateWhereInputTypes, generateQueryBuilderClasses, generateAppExport } =
  await import(codegenPath);

const schema = getCollectedSchema();
const relations = analyzeRelations(schema);

const parts = [
  "// AUTO-GENERATED FILE - DO NOT EDIT",
  "// Regenerate via: node scripts/generate-app.mjs",
  generateClient(schema),
  generateWhereInputTypes(schema),
  generateQueryBuilderClasses(schema, relations),
  generateAppExport(schema),
];
writeFileSync(outFile, parts.join("\n\n") + "\n", "utf-8");
console.log(`wrote ${outFile}`);
