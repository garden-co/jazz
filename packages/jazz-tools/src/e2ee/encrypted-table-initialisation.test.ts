import { it } from "vitest";
import { runEncryptedTableScenario } from "./fixtures/encrypted-table-scenarios.js";

it.each([
  "immediate",
  "after-yield",
  "plaintext-first",
  "transaction-read",
  "transaction-projection",
  "offline-at-submit",
  "ordinary-scope-upsert",
  "transaction-scope-upsert",
] as const)(
  "atomically creates a creator-owned space and encrypted data through ordinary mutations (%s)",
  runEncryptedTableScenario,
  120_000,
);
