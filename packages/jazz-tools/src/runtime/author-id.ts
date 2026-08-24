import type { Session } from "./context.js";

const canonicalAuthorDecoder = new TextDecoder("utf-8", { fatal: true });
const STORED_SCALAR_INLINE_TAG = 0;
const CANONICAL_AUTHOR_OPEN_BRACKET = 0x5b;

export type CanonicalAuthorSubject = {
  issuer: string;
  user_id: string;
  canonical: string;
};

/** Opaque provider identity components retain exact spelling; only ASCII blanks are invalid. */
export function isUsableSubject(subject: string): boolean {
  for (let index = 0; index < subject.length; index++) {
    const codePoint = subject.charCodeAt(index)!;
    if (codePoint !== 0x20 && (codePoint < 0x09 || codePoint > 0x0d)) return true;
  }
  return false;
}

/** Portable logical author identity. Rust interns this canonical string only internally. */
export function canonicalAuthorSubject(issuer: string, subject: string): string {
  if (!isUsableSubject(issuer) || !isUsableSubject(subject)) {
    throw new Error("Author issuer and subject must be nonempty");
  }
  return JSON.stringify([issuer, subject]);
}

export function parseCanonicalAuthorSubject(value: string): CanonicalAuthorSubject | null {
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!Array.isArray(parsed) || parsed.length !== 2) return null;
    const [issuer, userId] = parsed;
    if (typeof issuer !== "string" || typeof userId !== "string") return null;
    const canonical = canonicalAuthorSubject(issuer, userId);
    if (value !== canonical) return null;
    return { issuer, user_id: userId, canonical };
  } catch {
    return null;
  }
}

export function decodeCanonicalAuthorSubjectBytes(bytes: Uint8Array): string {
  const logical =
    bytes[0] === STORED_SCALAR_INLINE_TAG
      ? bytes[1] === CANONICAL_AUTHOR_OPEN_BRACKET
        ? bytes.subarray(1)
        : null
      : bytes[0] === CANONICAL_AUTHOR_OPEN_BRACKET
        ? bytes
        : null;
  if (!logical) {
    throw new Error("invalid canonical author subject bytes");
  }
  let value: string;
  try {
    value = canonicalAuthorDecoder.decode(logical);
  } catch {
    throw new Error("invalid canonical author subject UTF-8");
  }
  const parsed = parseCanonicalAuthorSubject(value);
  if (!parsed) {
    throw new Error("invalid canonical author subject");
  }
  return parsed.canonical;
}

export function authorBytesForSession(session: Pick<Session, "issuer" | "user_id">): Uint8Array {
  return new TextEncoder().encode(canonicalAuthorSubject(session.issuer, session.user_id));
}
