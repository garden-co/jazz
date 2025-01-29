// Import and initialize the appropriate build based on environment
const wasmModule = await (typeof window === "undefined"
  ? import("./dist/node/jazz_crypto_rs.js")
  : import("./dist/web/jazz_crypto_rs.js"));

// Initialize if needed (web environment)
// https://webassembly.org/getting-started/js-api/
if ("default" in wasmModule && typeof wasmModule.default === "function") {
  await wasmModule.default();
}

// Handle both CommonJS and ES module exports
interface WasmExports {
  generate_nonce: (nonce_material: Uint8Array) => Uint8Array;
  blake3_hash_once: (data: Uint8Array) => Uint8Array;
  blake3_hash_once_with_context: (
    data: Uint8Array,
    context: Uint8Array,
  ) => Uint8Array;
  blake3_empty_state: () => Uint8Array;
  blake3_update_state: (state: Uint8Array, data: Uint8Array) => Uint8Array;
  blake3_digest_for_state: (state: Uint8Array) => Uint8Array;

  // X25519 functions
  new_x25519_private_key: () => Uint8Array;
  x25519_public_key: (private_key: Uint8Array) => Uint8Array;
  x25519_diffie_hellman: (
    private_key: Uint8Array,
    public_key: Uint8Array,
  ) => Uint8Array;

  // XSalsa20 functions
  encrypt_xsalsa20: (
    key: Uint8Array,
    nonce_material: Uint8Array,
    plaintext: Uint8Array,
  ) => Uint8Array;
  decrypt_xsalsa20: (
    key: Uint8Array,
    nonce_material: Uint8Array,
    ciphertext: Uint8Array,
  ) => Uint8Array;

  // Ed25519 functions
  new_ed25519_signing_key: () => Uint8Array;
  ed25519_signing_key_from_bytes: (bytes: Uint8Array) => Uint8Array;
  ed25519_signing_key_to_public: (signing_key: Uint8Array) => Uint8Array;
  ed25519_signing_key_sign: (
    signing_key: Uint8Array,
    message: Uint8Array,
  ) => Uint8Array;
  ed25519_verifying_key_from_bytes: (bytes: Uint8Array) => Uint8Array;
  ed25519_signature_from_bytes: (bytes: Uint8Array) => Uint8Array;
  ed25519_verify: (
    verifying_key: Uint8Array,
    message: Uint8Array,
    signature: Uint8Array,
  ) => boolean;

  // High-level seal/unseal functions
  seal: (
    message: Uint8Array,
    sender_private_key: Uint8Array,
    recipient_public_key: Uint8Array,
    nonce_material: Uint8Array,
  ) => Uint8Array;
  unseal: (
    sealed_message: Uint8Array,
    recipient_private_key: Uint8Array,
    sender_public_key: Uint8Array,
    nonce_material: Uint8Array,
  ) => Uint8Array;
}

const moduleExports = ("default" in wasmModule
  ? wasmModule.default
  : wasmModule) as unknown as WasmExports;

export const {
  generate_nonce,
  blake3_hash_once,
  blake3_hash_once_with_context,
  blake3_empty_state,
  blake3_update_state,
  blake3_digest_for_state,
  new_x25519_private_key,
  x25519_public_key,
  x25519_diffie_hellman,
  encrypt_xsalsa20,
  decrypt_xsalsa20,
  new_ed25519_signing_key,
  ed25519_signing_key_from_bytes,
  ed25519_signing_key_to_public,
  ed25519_signing_key_sign,
  ed25519_verifying_key_from_bytes,
  ed25519_signature_from_bytes,
  ed25519_verify,
  seal,
  unseal,
} = moduleExports;

export class CryptoError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CryptoError";
  }
}
