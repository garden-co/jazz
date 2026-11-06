import { it } from "vitest";
import { runEncryptedTableScenario } from "./fixtures/encrypted-table-scenarios.js";

it(
  "decodes encrypted projections, nested reads and live updates, and sanitises subscription failures",
  () => runEncryptedTableScenario("projections"),
  120_000,
);
