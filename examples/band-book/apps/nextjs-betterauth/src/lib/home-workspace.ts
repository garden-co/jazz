import { bootstrapWorkspace } from "./server-calls";

/**
 * The account's demo workspace is created once, by the server. The browser
 * remembers the id the server answered, so later loads render from local data
 * straight away and never call the server again; the first load renders too,
 * while the request runs in the background. One request per account per page
 * load at most: StrictMode's second effect, a second tab component and a
 * retry all share it.
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

export function ensureHomeWorkspace(account: string): Promise<string> {
  const remembered = rememberedHomeWorkspace(account);
  if (remembered) return Promise.resolve(remembered);
  let request = requests.get(account);
  if (!request) {
    request = (async () => {
      const response = await bootstrapWorkspace();
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
