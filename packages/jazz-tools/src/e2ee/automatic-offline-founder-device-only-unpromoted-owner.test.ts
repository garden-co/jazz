import { it } from "vitest";
import { runOfflineFounderScenario } from "./fixtures/offline-founder-fixture.js";

it(
  "keeps automatic offline initialization fail-closed across device-only-unpromoted-owner",
  () => runOfflineFounderScenario("device-only-unpromoted-owner"),
  60_000,
);
