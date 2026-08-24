import { jazzTopologyBrowserCommands } from "./browser-commands.js";
import type { TopologyReporter } from "../topology/harness.js";
import { withTimeout } from "./support.js";
export * from "../topology/harness.js";

/** Reporter which mirrors browser lifecycle phases to the Node test process. */
export const browserTopologyReporter: TopologyReporter = {
  phase(status, label, elapsedMs) {
    console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
    void jazzTopologyBrowserCommands()
      .jazzBrowserTopologyLog(status, label, elapsedMs)
      .catch(() => undefined);
  },
};

/**
 * Compatibility boundary for the first browser topology receipt. New cases
 * should use `runTopologyScenario` directly so faults and cleanup are recorded
 * in the shared receipt; this keeps the pre-existing named phases observable
 * while those cases are migrated.
 */
export async function browserTopologyPhase<T>(
  label: string,
  operation: () => Promise<T>,
  timeoutMs = 10_000,
): Promise<T> {
  const started = performance.now();
  browserTopologyReporter.phase("start", label, 0);
  try {
    const result = await withTimeout(
      operation(),
      timeoutMs,
      `Browser topology phase timed out: ${label}`,
    );
    browserTopologyReporter.phase("complete", label, Math.round(performance.now() - started));
    return result;
  } catch (error) {
    browserTopologyReporter.phase("failed", label, Math.round(performance.now() - started));
    throw error;
  }
}
