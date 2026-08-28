import type { ScenarioResult } from "./protocol";

/**
 * The acceptance matrix requires two UI peers and one native durable relay.
 * Entries are deliberately TODO until an app build emits an observed receipt.
 */
export const scenarioPlan: readonly ScenarioResult[] = [
  ["local-write-subscription", "Two UI runtimes observe a write through one relay"],
  ["reconnect", "UI-A reconnects without replacing the admitted relay scope"],
  ["reopen", "Process/app relaunch reopens the durable relay store"],
  ["scope-isolation", "Distinct app/storage/auth scopes cannot observe each other"],
  ["logout-auth-switch", "Trusted native code revokes old admission before new scope admission"],
  ["backpressure", "Relay frame progress resumes after bounded backpressure"],
  ["corrupt-store", "Corrupt durable store fails closed with a structured diagnostic"],
].map(([scenario, detail]) => ({
  protocol: 1,
  scenario,
  state: "todo",
  detail,
})) as readonly ScenarioResult[];
