import type { ScenarioResult } from "./protocol";
import { NATIVE_RELAY_ABI_V1 } from "jazz-rn/native-relay-abi";

/**
 * The acceptance matrix requires two UI peers and one native durable relay.
 * Only a scenario with an observed implementation becomes `passed`; the
 * remaining matrix stays explicit debt and is never handed to the strict
 * device driver as a green receipt requirement.
 */
export const scenarioPlan: readonly ScenarioResult[] = [
  [
    "linked-abi-admission",
    `Installed relay admits an opaque scope and reports ABI V${NATIVE_RELAY_ABI_V1}`,
  ],
  [
    "foreground-byte-abi",
    "Installed JSI foreground executes ABI v1 Probe, Tick, Close, and revoke",
  ],
  [
    "foreground-write-transaction",
    "JSI foreground commits and rolls back native mergeable/exclusive transactions",
  ],
  [
    "local-write-subscription",
    "Public RN Db API inserts, queries, and publishes locally; two aliases in one JSI runtime observe a write through one relay",
  ],
  [
    "independent-jsi-runtime-subscription",
    "Two physical JSI runtimes observe one relay write (explicit installed-device gap; current ABI receipt covers aliases in one JSI runtime only)",
  ],
  ["reconnect", "UI-A reconnects without replacing the admitted relay scope"],
  [
    "reopen",
    "A fresh app process reopens scope A's durable relay store and reads the prior process's row through createJazzClient",
  ],
  ["scope-isolation", "Distinct app/storage/auth scopes cannot observe each other"],
  ["logout-revocation", "Trusted native code revokes old admission aliases before any replacement"],
  [
    "logout-auth-switch",
    "Trusted native code revokes scope A foreground and relay aliases before admitting scope B",
  ],
  ["backpressure", "Relay frame progress resumes after bounded backpressure"],
  ["corrupt-store", "Corrupt durable store fails closed with a structured diagnostic"],
].map(([scenario, detail]) => ({
  protocol: 1,
  scenario,
  state:
    scenario === "linked-abi-admission" ||
    scenario === "foreground-byte-abi" ||
    scenario === "foreground-write-transaction" ||
    scenario === "local-write-subscription" ||
    scenario === "scope-isolation" ||
    scenario === "logout-revocation" ||
    scenario === "logout-auth-switch" ||
    scenario === "reopen"
      ? "passed"
      : "todo",
  detail,
})) as readonly ScenarioResult[];

/** The seed launch proves the normal matrix; the fresh process only emits the
 * single durable-reopen claim. Keeping the receipts disjoint means neither
 * run can accidentally satisfy the other's evidence requirement. */
export function scenariosForAcceptancePhase(phase: "seed" | "verify") {
  return scenarioPlan.filter((scenario) =>
    phase === "verify" ? scenario.scenario === "reopen" : scenario.scenario !== "reopen",
  );
}
