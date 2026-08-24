import {
  jazzServerBrowserCommands,
  jazzTopologyBrowserCommands,
} from "../../../../packages/jazz-tools/tests/browser/browser-commands.js";

/**
 * Fail before a topology scenario allocates clients or deploys a schema when
 * its Vitest browser project has stale or incomplete Jazz command wiring.
 */
export function assertWorldTourTopologyContract(): void {
  jazzServerBrowserCommands();
  jazzTopologyBrowserCommands();
}
