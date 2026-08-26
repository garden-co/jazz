import { jazzTopologyBrowserCommands } from "./browser-commands.js";
import type { TopologyReporter } from "../topology/harness.js";
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
