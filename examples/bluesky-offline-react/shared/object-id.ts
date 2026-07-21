export function objectIdKey(appId: string, namespace: string, value: string) {
  return `${appId}:projection-v3:${namespace}:${value}`;
}

export function formatObjectId(digest: ArrayLike<number>) {
  const bytes = Uint8Array.from(digest).slice(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
