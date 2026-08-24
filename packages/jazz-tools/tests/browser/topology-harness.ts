import { commands } from "vitest/browser";
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
