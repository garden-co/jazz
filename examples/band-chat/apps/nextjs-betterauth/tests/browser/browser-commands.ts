import { commands } from "vitest/browser";

import { requireBandChatBrowserCommands } from "./browser-command-contract.js";

/** Read the app-specific commands configured by this browser-test project. */
export function bandChatBrowserCommands() {
  return requireBandChatBrowserCommands(commands);
}
