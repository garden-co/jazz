import { commands } from "vitest/browser";

declare module "vitest/internal/browser" {
  interface BrowserCommands {
    sessionTabOpen: (
      id: string,
      serverUrl: string,
      options?: { clearOnSignOut?: boolean; localFirst?: "now"; signedIn?: boolean },
    ) => Promise<void>;
    sessionTabRun: (
      id: string,
      action: "insertLocally" | "signOut" | "reconnect",
      title?: string,
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
  error?: string;
  /** Each status the tab's session published, and what it logged. */
  history: { statuses: string[]; warnings: string[]; errors: string[] };
}

/**
 * Another tab of this page's origin with a Jazz session of its own (see
 * session-tab-harness.ts): a local-first guest at `serverUrl`, once its
 * local-first client is open.
 */
export async function openSessionTab(
  id: string,
  serverUrl: string,
  options: { clearOnSignOut?: boolean; localFirst?: "now"; signedIn?: boolean } = {},
) {
  await commands.sessionTabOpen(id, serverUrl, options);
  return {
    state: () => commands.sessionTabState(id),
    insert: (title: string) => commands.sessionTabInsert(id, title),
    /** Insert, waiting only for this browser's store (not the server). */
    insertLocally: (title: string) => commands.sessionTabRun(id, "insertLocally", title),
    /** The app's sign-out button: `signOut()`, through Jazz only. */
    signOut: () => commands.sessionTabRun(id, "signOut"),
    /** `db.reconnect()` after a `db.disconnect()` in any tab. */
    reconnect: () => commands.sessionTabRun(id, "reconnect"),
    /** Hold the tab's requests to the backend (`true`), or let them go on. */
    holdBackend: (hold: boolean) => commands.sessionTabHoldBackend(id, hold),
    close: () => commands.sessionTabClose(id),
    /** The tab's page goes away without closing its session (a crash, a discarded tab). */
    crash: () => commands.sessionTabCrash(id),
  };
}
