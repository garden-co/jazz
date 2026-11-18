// The transport-control browser commands, in one place with no runtime
// imports, so this file type-checks without build artifacts (see
// tsconfig.transport-control.json, run by `pnpm check`).

/** What a browser test calls to cut, block and restore a client's transport. */
export interface JazzServerTransportControlBrowserCommands {
  jazzServerTransportControlCreate(serverUrl: string): Promise<string>;
  jazzServerTransportControlBlock(url: string, direction: "both" | "inbound"): Promise<void>;
  jazzServerTransportControlUnblock(url: string): Promise<void>;
  jazzServerTransportControlDown(url: string): Promise<void>;
  jazzServerTransportControlUp(url: string, serverUrl?: string): Promise<void>;
  jazzServerTransportControlStop(proxyUrl: string): Promise<void>;
}

/** Every command name, for the browser-side check that a project registered them all. */
export const TRANSPORT_CONTROL_COMMAND_NAMES = [
  "jazzServerTransportControlCreate",
  "jazzServerTransportControlBlock",
  "jazzServerTransportControlUnblock",
  "jazzServerTransportControlDown",
  "jazzServerTransportControlUp",
  "jazzServerTransportControlStop",
] as const satisfies readonly (keyof JazzServerTransportControlBrowserCommands)[];

// Fails to type-check when the interface gains a command the list lacks.
type Missing = Exclude<
  keyof JazzServerTransportControlBrowserCommands,
  (typeof TRANSPORT_CONTROL_COMMAND_NAMES)[number]
>;
const allNamesListed: [Missing] extends [never] ? true : Missing = true;
void allNamesListed;

/**
 * The Vitest browser commands for a test project's `test.browser.commands`,
 * wrapping the node-side implementations. Spread the result there rather
 * than listing commands: the `satisfies` below fails to type-check when the
 * harness gains a command this set lacks, so projects can't drift from it.
 */
export function transportControlBrowserCommands(impl: JazzServerTransportControlBrowserCommands) {
  return {
    jazzServerTransportControlCreate: async (_context: unknown, serverUrl: string) =>
      impl.jazzServerTransportControlCreate(serverUrl),
    jazzServerTransportControlBlock: async (
      _context: unknown,
      url: string,
      direction: "both" | "inbound",
    ) => impl.jazzServerTransportControlBlock(url, direction),
    jazzServerTransportControlUnblock: async (_context: unknown, url: string) =>
      impl.jazzServerTransportControlUnblock(url),
    jazzServerTransportControlDown: async (_context: unknown, url: string) =>
      impl.jazzServerTransportControlDown(url),
    jazzServerTransportControlUp: async (_context: unknown, url: string, serverUrl?: string) =>
      impl.jazzServerTransportControlUp(url, serverUrl),
    jazzServerTransportControlStop: async (_context: unknown, url: string) =>
      impl.jazzServerTransportControlStop(url),
  } satisfies {
    [Name in keyof JazzServerTransportControlBrowserCommands]: (
      context: unknown,
      ...args: Parameters<JazzServerTransportControlBrowserCommands[Name]>
    ) => ReturnType<JazzServerTransportControlBrowserCommands[Name]>;
  };
}
