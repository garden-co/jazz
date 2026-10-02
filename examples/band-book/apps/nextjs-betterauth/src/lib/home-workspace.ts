import type { Db } from "jazz-tools";
import { app } from "@/schema";
import { bootstrapWorkspace } from "./server-calls";

/**
 * The account's demo workspace is created once, by the server. The browser
 * remembers the id the server answered, so later loads render from local data
 * straight away and never wait on the server; the first load renders too,
 * while the request runs in the background. One request per account per page
 * load at most: StrictMode's second effect, a second tab component and a
 * retry all share it.
 *
 * A remembered id can go stale (the server's data was reset, the workspace
 * was deleted), so `confirmHomeWorkspace` asks the server whether it still
 * exists. That runs in the background too, and only a "no" from the server
 * makes the page forget the id and set the workspace up again.
 */
const storageKey = (account: string) => `band-book:home-workspace:${account}`;
const requests = new Map<string, Promise<string>>();

export function rememberedHomeWorkspace(account: string): string | null {
  try {
    return localStorage.getItem(storageKey(account));
  } catch {
    return null;
  }
}

function forgetHomeWorkspace(account: string): void {
  try {
    localStorage.removeItem(storageKey(account));
  } catch {
    // Nothing was remembered then either.
  }
}

/**
 * The account's home workspace. `principal` is the signed-in Jazz session's
 * subject, which the server call authenticates as.
 */
export function ensureHomeWorkspace(account: string, principal: string): Promise<string> {
  const remembered = rememberedHomeWorkspace(account);
  if (remembered) return Promise.resolve(remembered);
  let request = requests.get(account);
  if (!request) {
    request = (async () => {
      const response = await bootstrapWorkspace(principal);
      if (!response.ok) throw new Error(`The server answered ${response.status}.`);
      const { workspaceId } = (await response.json()) as { workspaceId: string };
      try {
        localStorage.setItem(storageKey(account), workspaceId);
      } catch {
        // Storage can be unavailable (private mode); the next load asks again.
      }
      return workspaceId;
    })();
    requests.set(account, request);
    // A failed request can be retried.
    request.catch(() => requests.delete(account));
  }
  return request;
}

/**
 * Resolves true while the server still has the remembered workspace. When it
 * does not, the id is forgotten, so the next `ensureHomeWorkspace` asks the
 * server to set the workspace up again, and this resolves false. The read is
 * server-confirmed, so it waits while offline instead of guessing.
 */
export async function confirmHomeWorkspace(
  db: Db,
  account: string,
  workspaceId: string,
): Promise<boolean> {
  const workspace = await db.one(app.workspaces.where({ id: workspaceId }), { tier: "remote" });
  if (workspace) return true;
  if (rememberedHomeWorkspace(account) === workspaceId) forgetHomeWorkspace(account);
  return false;
}
