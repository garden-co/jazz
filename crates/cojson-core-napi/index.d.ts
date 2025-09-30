export const isActive: boolean;
export declare class Blake3Hasher {
  constructor()
  update(data: Uint8Array): void
  finalize(): Uint8Array
  clone(): Blake3Hasher
}

export declare class SessionLog {
  constructor(coId: string, sessionId: string, signerId?: string | undefined | null)
  clone(): SessionLog
  tryAdd(transactionsJson: Array<string>, newSignatureStr: string, skipVerify: boolean): void
  addNewPrivateTransaction(changesJson: string, signerSecret: string, encryptionKey: string, keyId: string, madeAt: number, meta?: string | undefined | null): string
  addNewTrustingTransaction(changesJson: string, signerSecret: string, madeAt: number, meta?: string | undefined | null): string
  decryptNextTransactionChangesJson(txIndex: number, encryptionKey: string): string
  decryptNextTransactionMetaJson(txIndex: number, encryptionKey: string): string | null
}

/**
 * Hash data once using BLAKE3.
 * - `data`: Raw bytes to hash
 * Returns 32 bytes of hash output.
 * This is the simplest way to compute a BLAKE3 hash of a single piece of data.
 */
export declare function blake3HashOnce(data: Uint8Array): Uint8Array

/**
 * Hash data once using BLAKE3 with a context prefix.
 * - `data`: Raw bytes to hash
 * - `context`: Context bytes to prefix to the data
 * Returns 32 bytes of hash output.
 * This is useful for domain separation - the same data hashed with different contexts will produce different outputs.
 */
export declare function blake3HashOnceWithContext(data: Uint8Array, context: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to decrypt bytes with a key secret and nonce material.
 * - `ciphertext`: The encrypted bytes to decrypt
 * - `key_secret`: A base58-encoded key secret with "keySecret_z" prefix
 * - `nonce_material`: Raw bytes used to generate the nonce (must match encryption)
 * Returns the decrypted bytes or throws a JsError if decryption fails.
 */
export declare function decrypt(ciphertext: Uint8Array, keySecret: string, nonceMaterial: Uint8Array): Uint8Array

/**
 * NAPI-exposed function for XSalsa20 decryption without authentication.
 * - `key`: 32-byte key for decryption (must match encryption key)
 * - `nonce_material`: Raw bytes used to generate a 24-byte nonce (must match encryption)
 * - `ciphertext`: Encrypted bytes to decrypt
 * Returns the decrypted bytes or throws a JsError if decryption fails.
 * Note: This function does not provide authentication. Use decrypt_xsalsa20_poly1305 for authenticated decryption.
 */
export declare function decryptXsalsa20(key: Uint8Array, nonceMaterial: Uint8Array, ciphertext: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to sign a message using Ed25519.
 * - `signing_key`: 32 bytes of signing key material
 * - `message`: Raw bytes to sign
 * Returns 64 bytes of signature material or throws JsError if signing fails.
 */
export declare function ed25519Sign(signingKey: Uint8Array, message: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to validate and copy Ed25519 signature bytes.
 * - `bytes`: 64 bytes of signature material to validate
 * Returns the same 64 bytes if valid or throws JsError if invalid.
 */
export declare function ed25519SignatureFromBytes(bytes: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to validate and copy Ed25519 signing key bytes.
 * - `bytes`: 32 bytes of signing key material to validate
 * Returns the same 32 bytes if valid or throws JsError if invalid.
 */
export declare function ed25519SigningKeyFromBytes(bytes: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to sign a message with an Ed25519 signing key.
 * - `signing_key`: 32 bytes of signing key material
 * - `message`: Raw bytes to sign
 * Returns 64 bytes of signature material or throws JsError if signing fails.
 */
export declare function ed25519SigningKeySign(signingKey: Uint8Array, message: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to derive the public key from an Ed25519 signing key.
 * - `signing_key`: 32 bytes of signing key material
 * Returns 32 bytes of public key material or throws JsError if key is invalid.
 */
export declare function ed25519SigningKeyToPublic(signingKey: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to verify an Ed25519 signature.
 * - `verifying_key`: 32 bytes of verifying key material
 * - `message`: Raw bytes that were signed
 * - `signature`: 64 bytes of signature material
 * Returns true if signature is valid, false otherwise, or throws JsError if verification fails.
 */
export declare function ed25519Verify(verifyingKey: Uint8Array, message: Uint8Array, signature: Uint8Array): boolean

/**
 * NAPI-exposed function to derive an Ed25519 verifying key from a signing key.
 * - `signing_key`: 32 bytes of signing key material
 * Returns 32 bytes of verifying key material or throws JsError if key is invalid.
 */
export declare function ed25519VerifyingKey(signingKey: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to validate and copy Ed25519 verifying key bytes.
 * - `bytes`: 32 bytes of verifying key material to validate
 * Returns the same 32 bytes if valid or throws JsError if invalid.
 */
export declare function ed25519VerifyingKeyFromBytes(bytes: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to encrypt bytes with a key secret and nonce material.
 * - `value`: The raw bytes to encrypt
 * - `key_secret`: A base58-encoded key secret with "keySecret_z" prefix
 * - `nonce_material`: Raw bytes used to generate the nonce
 * Returns the encrypted bytes or throws a JsError if encryption fails.
 */
export declare function encrypt(value: Uint8Array, keySecret: string, nonceMaterial: Uint8Array): Uint8Array

/**
 * NAPI-exposed function for XSalsa20 encryption without authentication.
 * - `key`: 32-byte key for encryption
 * - `nonce_material`: Raw bytes used to generate a 24-byte nonce via BLAKE3
 * - `plaintext`: Raw bytes to encrypt
 * Returns the encrypted bytes or throws a JsError if encryption fails.
 * Note: This function does not provide authentication. Use encrypt_xsalsa20_poly1305 for authenticated encryption.
 */
export declare function encryptXsalsa20(key: Uint8Array, nonceMaterial: Uint8Array, plaintext: Uint8Array): Uint8Array

/**
 * Generate a 24-byte nonce from input material using BLAKE3.
 * - `nonce_material`: Raw bytes to derive the nonce from
 * Returns 24 bytes suitable for use as a nonce in cryptographic operations.
 * This function is deterministic - the same input will produce the same nonce.
 */
export declare function generateNonce(nonceMaterial: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to derive a sealer ID from a sealer secret.
 * - `secret`: Raw bytes of the sealer secret
 * Returns a base58-encoded sealer ID with "sealer_z" prefix or throws JsError if derivation fails.
 */
export declare function getSealerId(secret: Uint8Array): string

/**
 * NAPI-exposed function to derive a signer ID from a signing key.
 * - `secret`: Raw Ed25519 signing key bytes
 * Returns base58-encoded verifying key with "signer_z" prefix or throws JsError if derivation fails.
 */
export declare function getSignerId(secret: Uint8Array): string

/**
 * Generate a new Ed25519 signing key using secure random number generation.
 * Returns 32 bytes of raw key material suitable for use with other Ed25519 functions.
 */
export declare function newEd25519SigningKey(): Uint8Array

/**
 * Generate a new X25519 private key using secure random number generation.
 * Returns 32 bytes of raw key material suitable for use with other X25519 functions.
 * This key can be reused for multiple Diffie-Hellman exchanges.
 */
export declare function newX25519PrivateKey(): Uint8Array

/**
 * NAPI-exposed function for sealing a message using X25519 + XSalsa20-Poly1305.
 * Provides authenticated encryption with perfect forward secrecy.
 * - `message`: Raw bytes to seal
 * - `sender_secret`: Base58-encoded sender's private key with "sealerSecret_z" prefix
 * - `recipient_id`: Base58-encoded recipient's public key with "sealer_z" prefix
 * - `nonce_material`: Raw bytes used to generate the nonce
 * Returns sealed bytes or throws JsError if sealing fails.
 */
export declare function seal(message: Uint8Array, senderSecret: string, recipientId: string, nonceMaterial: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to sign a message using Ed25519.
 * - `message`: Raw bytes to sign
 * - `secret`: Raw Ed25519 signing key bytes
 * Returns base58-encoded signature with "signature_z" prefix or throws JsError if signing fails.
 */
export declare function sign(message: Uint8Array, secret: Uint8Array): string

/**
 * NAPI-exposed function for unsealing a message using X25519 + XSalsa20-Poly1305.
 * Provides authenticated decryption with perfect forward secrecy.
 * - `sealed_message`: The sealed bytes to decrypt
 * - `recipient_secret`: Base58-encoded recipient's private key with "sealerSecret_z" prefix
 * - `sender_id`: Base58-encoded sender's public key with "sealer_z" prefix
 * - `nonce_material`: Raw bytes used to generate the nonce (must match sealing)
 * Returns unsealed bytes or throws JsError if unsealing fails.
 */
export declare function unseal(sealedMessage: Uint8Array, recipientSecret: string, senderId: string, nonceMaterial: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to verify an Ed25519 signature.
 * - `signature`: Raw signature bytes
 * - `message`: Raw bytes that were signed
 * - `id`: Raw Ed25519 verifying key bytes
 * Returns true if signature is valid, false otherwise, or throws JsError if verification fails.
 */
export declare function verify(signature: Uint8Array, message: Uint8Array, id: Uint8Array): boolean

/**
 * NAPI-exposed function to perform X25519 Diffie-Hellman key exchange.
 * - `private_key`: 32 bytes of private key material
 * - `public_key`: 32 bytes of public key material
 * Returns 32 bytes of shared secret material or throws JsError if key exchange fails.
 */
export declare function x25519DiffieHellman(privateKey: Uint8Array, publicKey: Uint8Array): Uint8Array

/**
 * NAPI-exposed function to derive an X25519 public key from a private key.
 * - `private_key`: 32 bytes of private key material
 * Returns 32 bytes of public key material or throws JsError if key is invalid.
 */
export declare function x25519PublicKey(privateKey: Uint8Array): Uint8Array
