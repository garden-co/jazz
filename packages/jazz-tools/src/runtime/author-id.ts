import type { PublicSession, Session } from "./context.js";

const canonicalAuthorDecoder = new TextDecoder("utf-8", { fatal: true });
const STORED_SCALAR_INLINE_TAG = 2;
const CANONICAL_AUTHOR_OPEN_BRACKET = 0x5b;
const publicSessions = new WeakMap<Session, PublicSession>();

function cloneAndFreezeClaim(value: unknown): unknown {
  if (Array.isArray(value)) {
    return Object.freeze(value.map(cloneAndFreezeClaim));
  }
  if (value !== null && typeof value === "object") {
    const cloned = Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, nested]) => [
        key,
        cloneAndFreezeClaim(nested),
      ]),
    );
    return Object.freeze(cloned);
  }
  return value;
}

export type CanonicalAuthorSubject = {
  issuer: string;
  user_id: string;
  canonical: string;
};

/**
 * Opaque provider identity components retain exact spelling, but must be
 * portable across Rust and JS: nonempty after ASCII-blank filtering and no
 * unpaired UTF-16 surrogates.
 */
export function isPortableAuthorComponent(component: string): boolean {
  let hasNonAsciiBlank = false;
  for (let index = 0; index < component.length; index++) {
    const codeUnit = component.charCodeAt(index)!;
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const next = component.charCodeAt(index + 1);
      if (Number.isNaN(next) || next < 0xdc00 || next > 0xdfff) return false;
      hasNonAsciiBlank = true;
      index += 1;
      continue;
    }
    if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) return false;
    if (codeUnit !== 0x20 && (codeUnit < 0x09 || codeUnit > 0x0d)) {
      hasNonAsciiBlank = true;
    }
  }
  return hasNonAsciiBlank;
}

/** Existing subject helper now shares the canonical author component predicate. */
export function isUsableSubject(subject: string): boolean {
  return isPortableAuthorComponent(subject);
}

/** Portable logical author identity. Rust interns this canonical string only internally. */
export function canonicalAuthorSubject(issuer: string, subject: string): string {
  if (!isPortableAuthorComponent(issuer) || !isPortableAuthorComponent(subject)) {
    throw new Error("Author issuer and subject must be portable and nonempty");
  }
  return JSON.stringify([issuer, subject]);
}

/**
 * Attach the canonical logical user identity to a session crossing a public
 * binding boundary. Never preserve a caller-provided `user`: credentials
 * control `iss`/`sub`, and the identity is derived from those exact values.
 *
 * @internal Public bindings expose the resulting `PublicSession`; applications
 * should read `session.user` instead of reproducing this encoding.
 */
export function withCanonicalUser(session: Session): PublicSession {
  const existing = publicSessions.get(session);
  if (existing) return existing;
  const user = canonicalAuthorSubject(session.issuer, session.user_id);
  const claims = cloneAndFreezeClaim({
    ...session.claims,
    iss: session.issuer,
    sub: session.user_id,
  }) as Readonly<Record<string, unknown>>;
  const published: PublicSession = Object.freeze({
    user,
    claims,
    authMode: session.authMode,
  });
  publicSessions.set(session, published);
  return published;
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
