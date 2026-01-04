/**
 * Generate types from SQL fixtures before running tests.
 * Run with: npx tsx test/generate.ts
 */

import { generateFromSql } from "../src/from-sql.js";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

console.log("Generating types from SQL fixtures...\n");

generateFromSql(join(__dirname, "fixtures/notes-app.sql"), {
  output: join(__dirname, "generated"),
});
