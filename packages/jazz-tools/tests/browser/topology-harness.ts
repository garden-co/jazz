import { commands } from "vitest/browser";
import { withTopologyTimeout } from "../topology/harness.js";
import type { TopologyReporter } from "../topology/harness.js";
export * from "../topology/harness.js";

declare module "vitest/internal/browser" {
  interface BrowserCommands {
    jazzBrowserTopologyLog: (
      status: "start" | "complete" | "failed",
      label: string,
      elapsedMs: number,
    ) => Promise<void>;
  }
}

/** Reporter which mirrors browser lifecycle phases to the Node test process. */
export const browserTopologyReporter: TopologyReporter = {
  phase(status, label, elapsedMs) {
    console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
    void commands.jazzBrowserTopologyLog(status, label, elapsedMs).catch(() => undefined);
  },
};

/**
 * Bound and name browser-topology setup work which happens before a scenario
 * can be expressed through the shared topology harness.
 *
 * Browser runners evaluate a test module before Vitest can apply the test's
 * timeout, so this keeps setup failures visible and bounded.
 */
export async function browserTopologyPhase<T>(
  label: string,
  operation: () => Promise<T>,
  timeoutMs = 10_000,
): Promise<T> {
  const startedAt = performance.now();
  browserTopologyReporter.phase("start", label, 0);
  try {
    const result = await withTopologyTimeout(
      () => operation(),
      timeoutMs,
      `browser topology phase timed out: ${label}`,
    );
    browserTopologyReporter.phase("complete", label, performance.now() - startedAt);
    return result;
  } catch (error) {
    browserTopologyReporter.phase("failed", label, performance.now() - startedAt);
    throw error;
  }
}
