import { commands } from "vitest/browser";

declare module "vitest/internal/browser" {
  interface BrowserCommands {
    sessionTabOpen: (
      id: string,
      serverUrl: string,
      options?: { clearOnSignOut?: boolean },
    ) => Promise<void>;
    sessionTabState: (id: string) => Promise<SessionTabState>;
    sessionTabInsert: (id: string, title: string) => Promise<void>;
    sessionTabHoldBackend: (id: string, hold: boolean) => Promise<void>;
    sessionTabClose: (id: string) => Promise<void>;
    sessionTabCrash: (id: string) => Promise<void>;
  }
}

export interface SessionTabState {
  status: string;
  account?: string;
  subject?: string;
  link: unknown;
  titles: string[];
}

/**
 * Another tab of this page's origin with a Jazz session of its own (see
 * session-tab-harness.ts): a local-first guest at `serverUrl`, once its
 * local-first client is open.
 */
export async function openSessionTab(
  id: string,
  serverUrl: string,
  options: { clearOnSignOut?: boolean } = {},
) {
  await commands.sessionTabOpen(id, serverUrl, options);
  return {
    state: () => commands.sessionTabState(id),
    insert: (title: string) => commands.sessionTabInsert(id, title),
    /** Hold the tab's requests to the backend (`true`), or let them go on. */
    holdBackend: (hold: boolean) => commands.sessionTabHoldBackend(id, hold),
    close: () => commands.sessionTabClose(id),
    /** The tab's page goes away without closing its session (a crash, a discarded tab). */
    crash: () => commands.sessionTabCrash(id),
  };
}
