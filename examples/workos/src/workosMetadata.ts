const STORAGE_KEY = "workos-user-metadata";

type MetadataMap = Record<string, Record<string, unknown>>;

function getAll(): MetadataMap {
  const raw = localStorage.getItem(STORAGE_KEY);
  return raw ? JSON.parse(raw) : {};
}

function setAll(all: MetadataMap) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(all));
}

export async function getUserMetadata(userId: string): Promise<Record<string, unknown>> {
  return getAll()[userId] || {};
}

export async function setUserMetadata(userId: string, metadata: Record<string, unknown>) {
  const all = getAll();
  all[userId] = metadata;
  setAll(all);
}