import { it } from "vitest";
import { runOfflineFounderScenario } from "./fixtures/offline-founder-fixture.js";

it(
  "keeps automatic offline initialization fail-closed across device-only-missing-owner",
  () => runOfflineFounderScenario("device-only-missing-owner"),
  60_000,
);
