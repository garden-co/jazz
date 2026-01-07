/**
 * Generate types from SQL fixtures before running tests.
 * Run with: npx tsx test/generate.ts
 */

import { generateFromSql } from "@jazz/schema";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

console.log("Generating types from SQL fixtures for @jazz/client tests...\n");

generateFromSql(join(__dirname, "fixtures/app.sql"), {
  output: join(__dirname, "generated"),
});
