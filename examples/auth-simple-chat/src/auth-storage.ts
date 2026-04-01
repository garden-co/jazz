export interface StoredAuthSession {
  token: string;
  username: string;
}

function storageKey(appId: string): string {
  return `chat-client-external-auth-react:${appId}:auth`;
}

export function readStoredAuthSession(appId: string): StoredAuthSession | null {
  const raw = localStorage?.getItem(storageKey(appId));
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as Partial<StoredAuthSession>;
    if (typeof parsed.token !== "string" || typeof parsed.username !== "string") {
      return null;
    }

    return {
      token: parsed.token,
      username: parsed.username,
    };
  } catch {
    return null;
  }
}

export function writeStoredAuthSession(appId: string, session: StoredAuthSession): void {
  localStorage?.setItem(storageKey(appId), JSON.stringify(session));
}

export function clearStoredAuthSession(appId: string): void {
  localStorage?.removeItem(storageKey(appId));
}
