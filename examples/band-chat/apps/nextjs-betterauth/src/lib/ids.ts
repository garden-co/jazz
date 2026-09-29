/**
 * Deterministic row ids (RFC 4122 version 5, name-based SHA-1).
 *
 * Each account has exactly one profile, addressed by an id derived from the
 * account. Two tabs or devices that finish first-run setup at the same time
 * therefore write the same row instead of creating two profiles.
 */
const BAND_CHAT_NAMESPACE = "3f0c9a4e-7b2d-4c61-9e8a-5d1f2b6c7a90";

export function profileId(author: string): Promise<string> {
  return uuidV5(`profile:${author}`);
}

export async function uuidV5(name: string, namespace = BAND_CHAT_NAMESPACE): Promise<string> {
  const ns = namespace.replaceAll("-", "");
  const input = new Uint8Array(16 + new TextEncoder().encode(name).length);
  for (let i = 0; i < 16; i++) input[i] = parseInt(ns.slice(i * 2, i * 2 + 2), 16);
  input.set(new TextEncoder().encode(name), 16);
  const bytes = new Uint8Array(await crypto.subtle.digest("SHA-1", input)).slice(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
