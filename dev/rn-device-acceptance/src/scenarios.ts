import type { ScenarioResult } from "./protocol";

/**
 * The acceptance matrix requires two UI peers and one native durable relay.
 * Only a scenario with an observed implementation becomes `passed`; the
 * remaining matrix stays explicit debt and is never handed to the strict
 * device driver as a green receipt requirement.
 */
export const scenarioPlan: readonly ScenarioResult[] = [
  ["linked-abi-admission", "Installed relay admits an opaque scope and reports ABI 3"],
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
  state: scenario === "linked-abi-admission" ? "passed" : "todo",
  detail,
})) as readonly ScenarioResult[];
