import { formatObjectId, objectIdKey } from "../object-id.js";

const objectIds = new Map<string, Promise<string>>();

export function stableObjectId(namespace: string, value: string) {
  const appId = import.meta.env.VITE_JAZZ_APP_ID ?? "bluesky-offline-react-v2";
  const key = objectIdKey(appId, namespace, value);
  const cached = objectIds.get(key);
  if (cached) return cached;
  const id = crypto.subtle.digest("SHA-256", new TextEncoder().encode(key))
    .then((digest) => formatObjectId(new Uint8Array(digest)));
  objectIds.set(key, id);
  return id;
}
