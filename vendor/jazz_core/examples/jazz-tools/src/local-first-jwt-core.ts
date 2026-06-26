export const LOCAL_FIRST_JWT_ISSUER = "urn:jazz:local-first";
export const ANONYMOUS_JWT_ISSUER = "urn:jazz:anonymous";

export type LocalFirstJwtOptions = {
  appId: string;
  secret: string;
  subject: string;
  ttlSeconds?: number;
  now?: Date | number;
};

export type LocalFirstJwtSigner = (
  signingInput: Uint8Array,
  privateKeyDer: Uint8Array,
) => Uint8Array | Promise<Uint8Array>;

export function localFirstJwtPayload(options: LocalFirstJwtOptions): Record<string, unknown> {
  const nowSeconds = secondsSinceEpoch(options.now ?? Date.now());
  const ttlSeconds = options.ttlSeconds ?? 60 * 60;
  return {
    sub: options.subject,
    iss: LOCAL_FIRST_JWT_ISSUER,
    exp: nowSeconds + ttlSeconds,
    aud: options.appId,
    appId: options.appId,
  };
}

export function localFirstPrivateKeyDer(appId: string, secret: string): Uint8Array {
  const seed = deterministicBytes(`local-first-ed25519:${appId}:${secret}`, 32);
  return concatBytes([ed25519Pkcs8SeedPrefix(), seed]);
}

export function localFirstJwtSigningInput(payload: Record<string, unknown>): string {
  return `${encodeBase64UrlUtf8(JSON.stringify({ alg: "EdDSA", typ: "JWT" }))}.${encodeBase64UrlUtf8(JSON.stringify(payload))}`;
}

export function makeJwtFromSignature(signingInput: string, signature: Uint8Array): string {
  return `${signingInput}.${encodeBase64Url(signature)}`;
}

export function makeEd25519JwtSync(
  payload: Record<string, unknown>,
  privateKeyDer: Uint8Array,
  sign: LocalFirstJwtSigner,
): string {
  const signingInput = localFirstJwtSigningInput(payload);
  const signature = sign(new TextEncoder().encode(signingInput), privateKeyDer);
  if (signature instanceof Promise)
    throw new Error("Local-first JWT sync signer returned a Promise");
  return makeJwtFromSignature(signingInput, signature);
}

export async function makeEd25519JwtAsync(
  payload: Record<string, unknown>,
  privateKeyDer: Uint8Array,
  sign: LocalFirstJwtSigner,
): Promise<string> {
  const signingInput = localFirstJwtSigningInput(payload);
  const signature = await sign(new TextEncoder().encode(signingInput), privateKeyDer);
  return makeJwtFromSignature(signingInput, signature);
}

export function createLocalFirstJwtWithSigner(
  options: LocalFirstJwtOptions,
  sign: LocalFirstJwtSigner,
): string {
  return makeEd25519JwtSync(
    localFirstJwtPayload(options),
    localFirstPrivateKeyDer(options.appId, options.secret),
    sign,
  );
}

export function createLocalFirstJwtWithSignerAsync(
  options: LocalFirstJwtOptions,
  sign: LocalFirstJwtSigner,
): Promise<string> {
  return makeEd25519JwtAsync(
    localFirstJwtPayload(options),
    localFirstPrivateKeyDer(options.appId, options.secret),
    sign,
  );
}

export function encodeBase64UrlUtf8(value: string): string {
  return encodeBase64Url(new TextEncoder().encode(value));
}

export function encodeBase64Url(bytes: Uint8Array): string {
  const buffer = maybeBuffer();
  if (buffer) return buffer.from(bytes).toString("base64url");
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

export function decodeBase64UrlToUtf8(value: string): string | null {
  const buffer = maybeBuffer();
  if (buffer) {
    try {
      return buffer.from(value, "base64url").toString("utf8");
    } catch {
      return null;
    }
  }
  try {
    const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
    return new TextDecoder().decode(bytes);
  } catch {
    return null;
  }
}

export function concatBytes(values: readonly Uint8Array[]): Uint8Array {
  const length = values.reduce((sum, value) => sum + value.length, 0);
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const value of values) {
    bytes.set(value, offset);
    offset += value.length;
  }
  return bytes;
}

function maybeBuffer(): BufferConstructor | undefined {
  return globalThis.Buffer;
}

function ed25519Pkcs8SeedPrefix(): Uint8Array {
  return new Uint8Array([
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
  ]);
}

function secondsSinceEpoch(value: Date | number): number {
  const milliseconds = value instanceof Date ? value.getTime() : value;
  return Math.floor(milliseconds / 1000);
}

function deterministicBytes(seed: string, length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  let state = 0x811c9dc5;
  const input = new TextEncoder().encode(seed);
  for (let index = 0; index < length; index += 1) {
    for (const byte of input) {
      state ^= byte + index;
      state = Math.imul(state, 0x01000193) >>> 0;
    }
    state ^= length + index;
    state = Math.imul(state, 0x01000193) >>> 0;
    bytes[index] = state & 0xff;
  }
  return bytes;
}
