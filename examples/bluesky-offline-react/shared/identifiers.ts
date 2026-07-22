// The browser, BFF, and Jazz sync server use this ID to address the same Jazz application.
export const jazzAppId = "2b231499-5315-4cab-852f-bb3e29c72b95";

// Jazz retains the authoritative ATProto URI; reconciliation extracts its PDS record key.
export function parseAtRecordUri(uri: string | null | undefined) {
  const match = uri?.match(/^at:\/\/([^/]+)\/([^/]+)\/([^/]+)$/);
  return match ? { repo: match[1], collection: match[2], rkey: match[3] } : undefined;
}

export function objectIdKey(applicationId: string, namespace: string, value: string) {
  // Changing this projection version intentionally generates a new set of Jazz object IDs.
  return `${applicationId}:projection-v3:${namespace}:${value}`;
}

export function formatObjectId(digest: ArrayLike<number>) {
  const bytes = Uint8Array.from(digest).slice(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
