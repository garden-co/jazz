import { it } from "vitest";
import { runEncryptedTableScenario } from "./fixtures/encrypted-table-scenarios.js";

it.each([
  "ordinary-insert",
  "ordinary-insert-error",
  "ordinary-update",
  "ordinary-upsert",
  "ordinary-restore",
  "byoc-buffer",
] as const)(
  "atomically creates a creator-owned space and encrypted data through ordinary mutations (%s)",
  runEncryptedTableScenario,
  120_000,
);
