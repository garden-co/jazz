import { withTimeout } from "./support.js";
import { commands } from "vitest/browser";

declare module "vitest/internal/browser" {
  interface BrowserCommands {
    jazzBrowserTopologyLog: (
      status: "start" | "complete" | "failed",
      label: string,
      elapsedMs: number,
    ) => Promise<void>;
  }
}

/**
 * Bound and name browser-topology setup work.
 *
 * Browser runners evaluate a test module before Vitest can apply the test's
 * timeout. Keep adopter imports, server startup, and client creation inside
 * this helper so a stalled lifecycle boundary identifies itself in the test
 * receipt rather than appearing as an unlabelled browser bootstrap timeout.
 */
export async function browserTopologyPhase<T>(
  label: string,
  operation: () => Promise<T>,
  timeoutMs = 10_000,
): Promise<T> {
  const startedAt = performance.now();
  console.info(`[jazz-browser-topology] start ${label}`);
  reportTopologyPhase("start", label, 0);
  try {
    const result = await withTimeout(
      operation(),
      timeoutMs,
      `Jazz browser topology phase timed out: ${label}`,
    );
    console.info(
      `[jazz-browser-topology] complete ${label} in ${Math.round(performance.now() - startedAt)}ms`,
    );
    reportTopologyPhase("complete", label, performance.now() - startedAt);
    return result;
  } catch (error) {
    console.error(
      `[jazz-browser-topology] failed ${label} after ${Math.round(performance.now() - startedAt)}ms`,
      error,
    );
    reportTopologyPhase("failed", label, performance.now() - startedAt);
    throw error;
  }
}

function reportTopologyPhase(
  status: "start" | "complete" | "failed",
  label: string,
  elapsedMs: number,
): void {
  // Browser console output is not consistently surfaced by every Vitest
  // provider. Mirror phase markers through a Node-side command; deliberately
  // do not await it, because observability must not become another lifecycle
  // dependency.
  void commands.jazzBrowserTopologyLog(status, label, Math.round(elapsedMs)).catch(() => undefined);
}
