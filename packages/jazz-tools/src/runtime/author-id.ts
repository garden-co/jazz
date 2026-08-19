import { sha1 } from "@noble/hashes/legacy.js";

const URL_NAMESPACE = Uint8Array.from([
  0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

function uuidBytes(value: string): Uint8Array | null {
  if (
    !/^(?:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{32})$/.test(
      value,
    )
  ) {
    return null;
  }
  const hex = value.replaceAll("-", "");

  return Uint8Array.from({ length: 16 }, (_, index) =>
    Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
}

/**
 * Opaque provider subjects retain their exact spelling; only ASCII-blank values
 * are invalid. This enumerates exactly space, tab, LF, VT, FF, and CR; Unicode
 * whitespace remains an opaque, valid provider subject.
 */
export function isUsableSubject(subject: string): boolean {
  for (let index = 0; index < subject.length; index++) {
    const codePoint = subject.charCodeAt(index)!;
    if (codePoint !== 0x20 && (codePoint < 0x09 || codePoint > 0x0d)) return true;
  }
  return false;
}

export function authorBytesForSubject(subject: string): Uint8Array {
  const uuid = uuidBytes(subject);
  if (uuid) return uuid;

  const subjectBytes = new TextEncoder().encode(subject);
  const input = new Uint8Array(URL_NAMESPACE.length + subjectBytes.length);
  input.set(URL_NAMESPACE);
  input.set(subjectBytes, URL_NAMESPACE.length);
  const author = sha1(input).slice(0, 16);
  author[6] = (author[6]! & 0x0f) | 0x50;
  author[8] = (author[8]! & 0x3f) | 0x80;
  return author;
}
