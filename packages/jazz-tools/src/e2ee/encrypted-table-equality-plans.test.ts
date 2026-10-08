import { it } from "vitest";
import { runEncryptedTableScenario } from "./fixtures/encrypted-table-scenarios.js";

it.each([
  "equality-index",
  "equality-collision",
  "equality-owned-bytes",
  "equality-transaction-initial",
  "equality-transaction-existing",
  "equality-uuid-scope",
] as const)(
  "atomically creates a creator-owned space and encrypted data through ordinary mutations (%s)",
  runEncryptedTableScenario,
  120_000,
);
