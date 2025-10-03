/* tslint:disable */
/* eslint-disable */
/**
 * WASM-exposed function for XSalsa20 encryption without authentication.
 * - `key`: 32-byte key for encryption
 * - `nonce_material`: Raw bytes used to generate a 24-byte nonce via BLAKE3
 * - `plaintext`: Raw bytes to encrypt
 * Returns the encrypted bytes or throws a JsError if encryption fails.
 * Note: This function does not provide authentication. Use encrypt_xsalsa20_poly1305 for authenticated encryption.
 */
export function encryptXsalsa20(key: Uint8Array, nonce_material: Uint8Array, plaintext: Uint8Array): Uint8Array;
/**
 * WASM-exposed function for XSalsa20 decryption without authentication.
 * - `key`: 32-byte key for decryption (must match encryption key)
 * - `nonce_material`: Raw bytes used to generate a 24-byte nonce (must match encryption)
 * - `ciphertext`: Encrypted bytes to decrypt
 * Returns the decrypted bytes or throws a JsError if decryption fails.
 * Note: This function does not provide authentication. Use decrypt_xsalsa20_poly1305 for authenticated decryption.
 */
export function decryptXsalsa20(key: Uint8Array, nonce_material: Uint8Array, ciphertext: Uint8Array): Uint8Array;
/**
 * Generate a new X25519 private key using secure random number generation.
 * Returns 32 bytes of raw key material suitable for use with other X25519 functions.
 * This key can be reused for multiple Diffie-Hellman exchanges.
 */
export function newX25519PrivateKey(): Uint8Array;
/**
 * WASM-exposed function to derive an X25519 public key from a private key.
 * - `private_key`: 32 bytes of private key material
 * Returns 32 bytes of public key material or throws JsError if key is invalid.
 */
export function x25519PublicKey(private_key: Uint8Array): Uint8Array;
/**
 * WASM-exposed function to perform X25519 Diffie-Hellman key exchange.
 * - `private_key`: 32 bytes of private key material
 * - `public_key`: 32 bytes of public key material
 * Returns 32 bytes of shared secret material or throws JsError if key exchange fails.
 */
export function x25519DiffieHellman(private_key: Uint8Array, public_key: Uint8Array): Uint8Array;
/**
 * WASM-exposed function to derive a sealer ID from a sealer secret.
 * - `secret`: Raw bytes of the sealer secret
 * Returns a base58-encoded sealer ID with "sealer_z" prefix or throws JsError if derivation fails.
 */
export function getSealerId(secret: Uint8Array): string;
/**
 * WASM-exposed function to encrypt bytes with a key secret and nonce material.
 * - `value`: The raw bytes to encrypt
 * - `key_secret`: A base58-encoded key secret with "keySecret_z" prefix
 * - `nonce_material`: Raw bytes used to generate the nonce
 * Returns the encrypted bytes or throws a JsError if encryption fails.
 */
export function encrypt(value: Uint8Array, key_secret: string, nonce_material: Uint8Array): Uint8Array;
/**
 * WASM-exposed function to decrypt bytes with a key secret and nonce material.
 * - `ciphertext`: The encrypted bytes to decrypt
 * - `key_secret`: A base58-encoded key secret with "keySecret_z" prefix
 * - `nonce_material`: Raw bytes used to generate the nonce (must match encryption)
 * Returns the decrypted bytes or throws a JsError if decryption fails.
 */
export function decrypt(ciphertext: Uint8Array, key_secret: string, nonce_material: Uint8Array): Uint8Array;
/**
 * WASM-exposed function for sealing a message using X25519 + XSalsa20-Poly1305.
 * Provides authenticated encryption with perfect forward secrecy.
 * - `message`: Raw bytes to seal
 * - `sender_secret`: Base58-encoded sender's private key with "sealerSecret_z" prefix
 * - `recipient_id`: Base58-encoded recipient's public key with "sealer_z" prefix
 * - `nonce_material`: Raw bytes used to generate the nonce
 * Returns sealed bytes or throws JsError if sealing fails.
 */
export function seal(message: Uint8Array, sender_secret: string, recipient_id: string, nonce_material: Uint8Array): Uint8Array;
/**
 * WASM-exposed function for unsealing a message using X25519 + XSalsa20-Poly1305.
 * Provides authenticated decryption with perfect forward secrecy.
 * - `sealed_message`: The sealed bytes to decrypt
 * - `recipient_secret`: Base58-encoded recipient's private key with "sealerSecret_z" prefix
 * - `sender_id`: Base58-encoded sender's public key with "sealer_z" prefix
 * - `nonce_material`: Raw bytes used to generate the nonce (must match sealing)
 * Returns unsealed bytes or throws JsError if unsealing fails.
 */
export function unseal(sealed_message: Uint8Array, recipient_secret: string, sender_id: string, nonce_material: Uint8Array): Uint8Array;
/**
 * Generate a 24-byte nonce from input material using BLAKE3.
 * - `nonce_material`: Raw bytes to derive the nonce from
 * Returns 24 bytes suitable for use as a nonce in cryptographic operations.
 * This function is deterministic - the same input will produce the same nonce.
 */
export function generateNonce(nonce_material: Uint8Array): Uint8Array;
/**
 * Hash data once using BLAKE3.
 * - `data`: Raw bytes to hash
 * Returns 32 bytes of hash output.
 * This is the simplest way to compute a BLAKE3 hash of a single piece of data.
 */
export function blake3HashOnce(data: Uint8Array): Uint8Array;
/**
 * Hash data once using BLAKE3 with a context prefix.
 * - `data`: Raw bytes to hash
 * - `context`: Context bytes to prefix to the data
 * Returns 32 bytes of hash output.
 * This is useful for domain separation - the same data hashed with different contexts will produce different outputs.
 */
export function blake3HashOnceWithContext(data: Uint8Array, context: Uint8Array): Uint8Array;
/**
 * WASM-exposed function to sign a message using Ed25519.
 * - `message`: Raw bytes to sign
 * - `secret`: Raw Ed25519 signing key bytes
 * Returns base58-encoded signature with "signature_z" prefix or throws JsError if signing fails.
 */
export function sign(message: Uint8Array, secret: Uint8Array): string;
/**
 * WASM-exposed function to verify an Ed25519 signature.
 * - `signature`: Raw signature bytes
 * - `message`: Raw bytes that were signed
 * - `id`: Raw Ed25519 verifying key bytes
 * Returns true if signature is valid, false otherwise, or throws JsError if verification fails.
 */
export function verify(signature: Uint8Array, message: Uint8Array, id: Uint8Array): boolean;
/**
 * WASM-exposed function to derive a signer ID from a signing key.
 * - `secret`: Raw Ed25519 signing key bytes
 * Returns base58-encoded verifying key with "signer_z" prefix or throws JsError if derivation fails.
 */
export function getSignerId(secret: Uint8Array): string;
/**
 * Generate a new Ed25519 signing key using secure random number generation.
 * Returns 32 bytes of raw key material suitable for use with other Ed25519 functions.
 */
export function newEd25519SigningKey(): Uint8Array;
/**
 * WASM-exposed function to derive an Ed25519 verifying key from a signing key.
 * - `signing_key`: 32 bytes of signing key material
 * Returns 32 bytes of verifying key material or throws JsError if key is invalid.
 */
export function ed25519VerifyingKey(signing_key: Uint8Array): Uint8Array;
/**
 * WASM-exposed function to sign a message using Ed25519.
 * - `signing_key`: 32 bytes of signing key material
 * - `message`: Raw bytes to sign
 * Returns 64 bytes of signature material or throws JsError if signing fails.
 */
export function ed25519Sign(signing_key: Uint8Array, message: Uint8Array): Uint8Array;
/**
 * WASM-exposed function to verify an Ed25519 signature.
 * - `verifying_key`: 32 bytes of verifying key material
 * - `message`: Raw bytes that were signed
 * - `signature`: 64 bytes of signature material
 * Returns true if signature is valid, false otherwise, or throws JsError if verification fails.
 */
export function ed25519Verify(verifying_key: Uint8Array, message: Uint8Array, signature: Uint8Array): boolean;
/**
 * WASM-exposed function to validate and copy Ed25519 signing key bytes.
 * - `bytes`: 32 bytes of signing key material to validate
 * Returns the same 32 bytes if valid or throws JsError if invalid.
 */
export function ed25519SigningKeyFromBytes(bytes: Uint8Array): Uint8Array;
/**
 * WASM-exposed function to derive the public key from an Ed25519 signing key.
 * - `signing_key`: 32 bytes of signing key material
 * Returns 32 bytes of public key material or throws JsError if key is invalid.
 */
export function ed25519SigningKeyToPublic(signing_key: Uint8Array): Uint8Array;
/**
 * WASM-exposed function to sign a message with an Ed25519 signing key.
 * - `signing_key`: 32 bytes of signing key material
 * - `message`: Raw bytes to sign
 * Returns 64 bytes of signature material or throws JsError if signing fails.
 */
export function ed25519SigningKeySign(signing_key: Uint8Array, message: Uint8Array): Uint8Array;
/**
 * WASM-exposed function to validate and copy Ed25519 verifying key bytes.
 * - `bytes`: 32 bytes of verifying key material to validate
 * Returns the same 32 bytes if valid or throws JsError if invalid.
 */
export function ed25519VerifyingKeyFromBytes(bytes: Uint8Array): Uint8Array;
/**
 * WASM-exposed function to validate and copy Ed25519 signature bytes.
 * - `bytes`: 64 bytes of signature material to validate
 * Returns the same 64 bytes if valid or throws JsError if invalid.
 */
export function ed25519SignatureFromBytes(bytes: Uint8Array): Uint8Array;
export class Blake3Hasher {
  free(): void;
  constructor();
  update(data: Uint8Array): void;
  finalize(): Uint8Array;
  clone(): Blake3Hasher;
}
export class SessionLog {
  free(): void;
  constructor(co_id: string, session_id: string, signer_id?: string | null);
  clone(): SessionLog;
  tryAdd(transactions_json: string[], new_signature_str: string, skip_verify: boolean): void;
  addNewPrivateTransaction(changes_json: string, signer_secret: string, encryption_key: string, key_id: string, made_at: number, meta?: string | null): string;
  addNewTrustingTransaction(changes_json: string, signer_secret: string, made_at: number, meta?: string | null): string;
  decryptNextTransactionChangesJson(tx_index: number, encryption_key: string): string;
  decryptNextTransactionMetaJson(tx_index: number, encryption_key: string): string | undefined;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly __wbg_sessionlog_free: (a: number, b: number) => void;
  readonly sessionlog_new: (a: number, b: number, c: number, d: number, e: number, f: number) => number;
  readonly sessionlog_clone: (a: number) => number;
  readonly sessionlog_tryAdd: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number];
  readonly sessionlog_addNewPrivateTransaction: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number, k: number, l: number) => [number, number, number, number];
  readonly sessionlog_addNewTrustingTransaction: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => [number, number, number, number];
  readonly sessionlog_decryptNextTransactionChangesJson: (a: number, b: number, c: number, d: number) => [number, number, number, number];
  readonly sessionlog_decryptNextTransactionMetaJson: (a: number, b: number, c: number, d: number) => [number, number, number, number];
  readonly encryptXsalsa20: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
  readonly decryptXsalsa20: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
  readonly newX25519PrivateKey: () => [number, number];
  readonly x25519PublicKey: (a: number, b: number) => [number, number, number, number];
  readonly x25519DiffieHellman: (a: number, b: number, c: number, d: number) => [number, number, number, number];
  readonly getSealerId: (a: number, b: number) => [number, number, number, number];
  readonly encrypt: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
  readonly decrypt: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
  readonly seal: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => [number, number, number, number];
  readonly unseal: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => [number, number, number, number];
  readonly generateNonce: (a: number, b: number) => [number, number];
  readonly blake3HashOnce: (a: number, b: number) => [number, number];
  readonly blake3HashOnceWithContext: (a: number, b: number, c: number, d: number) => [number, number];
  readonly __wbg_blake3hasher_free: (a: number, b: number) => void;
  readonly blake3hasher_new: () => number;
  readonly blake3hasher_update: (a: number, b: number, c: number) => void;
  readonly blake3hasher_finalize: (a: number) => [number, number];
  readonly blake3hasher_clone: (a: number) => number;
  readonly sign: (a: number, b: number, c: number, d: number) => [number, number, number, number];
  readonly verify: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number];
  readonly getSignerId: (a: number, b: number) => [number, number, number, number];
  readonly newEd25519SigningKey: () => [number, number];
  readonly ed25519VerifyingKey: (a: number, b: number) => [number, number, number, number];
  readonly ed25519Sign: (a: number, b: number, c: number, d: number) => [number, number, number, number];
  readonly ed25519Verify: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number];
  readonly ed25519SigningKeyFromBytes: (a: number, b: number) => [number, number, number, number];
  readonly ed25519SigningKeyToPublic: (a: number, b: number) => [number, number, number, number];
  readonly ed25519VerifyingKeyFromBytes: (a: number, b: number) => [number, number, number, number];
  readonly ed25519SignatureFromBytes: (a: number, b: number) => [number, number, number, number];
  readonly ed25519SigningKeySign: (a: number, b: number, c: number, d: number) => [number, number, number, number];
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __wbindgen_exn_store: (a: number) => void;
  readonly __externref_table_alloc: () => number;
  readonly __wbindgen_export_4: WebAssembly.Table;
  readonly __externref_table_dealloc: (a: number) => void;
  readonly __wbindgen_free: (a: number, b: number, c: number) => void;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
