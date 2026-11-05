import { it } from "vitest";
import { runOfflineFounderScenario } from "./fixtures/offline-founder-fixture.js";

it(
  "keeps automatic offline initialization fail-closed across unknown-recipient",
  () => runOfflineFounderScenario("unknown-recipient"),
  60_000,
);
