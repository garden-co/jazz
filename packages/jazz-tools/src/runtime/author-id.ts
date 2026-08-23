import type { Session } from "./context.js";

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

export function authorBytesForSession(session: Pick<Session, "issuer" | "user_id">): Uint8Array {
  return new TextEncoder().encode(canonicalAuthorSubject(session.issuer, session.user_id));
}
