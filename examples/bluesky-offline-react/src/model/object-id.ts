import { formatObjectId, jazzAppId, objectIdKey } from "../../shared/identifiers.js";

const objectIds = new Map<string, Promise<string>>();

export function stableObjectId(namespace: string, value: string) {
  const key = objectIdKey(jazzAppId, namespace, value);
  const cached = objectIds.get(key);
  if (cached) return cached;
  const id = crypto.subtle
    .digest("SHA-256", new TextEncoder().encode(key))
    .then((digest) => formatObjectId(new Uint8Array(digest)));
  objectIds.set(key, id);
  return id;
}
