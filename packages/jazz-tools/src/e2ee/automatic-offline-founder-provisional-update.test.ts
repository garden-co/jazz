import { it } from "vitest";
import { runOfflineFounderScenario } from "./fixtures/offline-founder-fixture.js";

it(
  "keeps automatic offline initialization fail-closed across provisional-update",
  () => runOfflineFounderScenario("provisional-update"),
  60_000,
);
