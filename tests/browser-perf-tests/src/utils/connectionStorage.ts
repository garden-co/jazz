const STORAGE_KEY = "stress-test-connections";
const MAX_HISTORY = 5;

export interface ConnectionHistory {
  current: string;
  history: string[];
}

const DEFAULT_SYNC_URL = "ws://localhost:4200";

export function getConnectionHistory(): ConnectionHistory {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      const parsed = JSON.parse(stored);
      return {
        current: parsed.current || DEFAULT_SYNC_URL,
        history: Array.isArray(parsed.history)
          ? parsed.history
          : [DEFAULT_SYNC_URL],
      };
    }
  } catch {
    // Ignore parse errors
  }
  return {
    current: DEFAULT_SYNC_URL,
    history: [DEFAULT_SYNC_URL],
  };
}

export function saveConnection(url: string): ConnectionHistory {
  const { history } = getConnectionHistory();

  // Remove the URL if it already exists in history
  const filteredHistory = history.filter((h) => h !== url);

  // Add the new URL at the beginning
  const newHistory = [url, ...filteredHistory].slice(0, MAX_HISTORY);

  const data: ConnectionHistory = {
    current: url,
    history: newHistory,
  };

  localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
  return data;
}

export function getCurrentSyncUrl(): string {
  return getConnectionHistory().current;
}
