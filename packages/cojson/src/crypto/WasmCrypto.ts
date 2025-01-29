import { base58 } from "@scure/base";
import {
  blake3_digest_for_state,
  blake3_empty_state,
  blake3_hash_once,
  blake3_hash_once_with_context,
  blake3_update_state,
  decrypt_xsalsa20,
  ed25519_signature_from_bytes,
  ed25519_signing_key_from_bytes,
  ed25519_signing_key_sign,
  ed25519_signing_key_to_public,
  ed25519_verify,
  ed25519_verifying_key_from_bytes,
  encrypt_xsalsa20,
  new_ed25519_signing_key,
  new_x25519_private_key,
  seal,
  unseal,
  x25519_public_key,
} from "jazz-crypto-rs";
import { base64URLtoBytes, bytesToBase64url } from "../base64url.js";
import { RawCoID, TransactionID } from "../ids.js";
import { Stringified, stableStringify } from "../jsonStringify.js";
import { JsonValue } from "../jsonValue.js";
import { logger } from "../logger.js";
import {
  CryptoProvider,
  Encrypted,
  KeySecret,
  Sealed,
  SealerID,
  SealerSecret,
  Signature,
  SignerID,
  SignerSecret,
  textDecoder,
  textEncoder,
} from "./crypto.js";

/**
 * WebAssembly implementation of the CryptoProvider interface using jazz-crypto-rs.
 * This provides the primary implementation using WebAssembly for optimal performance, offering:
 * - Signing/verifying (Ed25519)
 * - Encryption/decryption (XSalsa20)
 * - Sealing/unsealing (X25519 + XSalsa20-Poly1305)
 * - Hashing (BLAKE3)
 */
export class WasmCrypto extends CryptoProvider<Uint8Array> {
  private constructor() {
    super();
  }

  static async create(): Promise<WasmCrypto> {
    return Promise.all([
      new Promise<void>((resolve) => {
        if ("crypto" in globalThis) {
          resolve();
        } else {
          return import(/*webpackIgnore: true*/ "node:crypto").then(
            ({ webcrypto }) => {
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              (globalThis as any).crypto = webcrypto;
              resolve();
            },
          );
        }
      }),
    ]).then(() => new WasmCrypto());
  }

  emptyBlake3State(): Uint8Array {
    return blake3_empty_state();
  }

  cloneBlake3State(state: Uint8Array): Uint8Array {
    return new Uint8Array(state);
  }

  blake3HashOnce(data: Uint8Array) {
    return blake3_hash_once(data);
  }

  blake3HashOnceWithContext(
    data: Uint8Array,
    { context }: { context: Uint8Array },
  ) {
    return blake3_hash_once_with_context(data, context);
  }

  blake3IncrementalUpdate(state: Uint8Array, data: Uint8Array): Uint8Array {
    return blake3_update_state(state, data);
  }

  blake3DigestForState(state: Uint8Array): Uint8Array {
    return blake3_digest_for_state(state);
  }

  newEd25519SigningKey(): Uint8Array {
    return new_ed25519_signing_key();
  }

  getSignerID(secret: SignerSecret): SignerID {
    const signingKey = ed25519_signing_key_from_bytes(
      base58.decode(secret.substring("signerSecret_z".length)),
    );
    const publicKey = ed25519_signing_key_to_public(signingKey);
    return `signer_z${base58.encode(publicKey)}`;
  }

  sign(secret: SignerSecret, message: JsonValue): Signature {
    const signingKey = ed25519_signing_key_from_bytes(
      base58.decode(secret.substring("signerSecret_z".length)),
    );
    const signature = ed25519_signing_key_sign(
      signingKey,
      textEncoder.encode(stableStringify(message)),
    );
    return `signature_z${base58.encode(signature)}`;
  }

  verify(signature: Signature, message: JsonValue, id: SignerID): boolean {
    const verifyingKey = ed25519_verifying_key_from_bytes(
      base58.decode(id.substring("signer_z".length)),
    );
    const signatureBytes = ed25519_signature_from_bytes(
      base58.decode(signature.substring("signature_z".length)),
    );
    return ed25519_verify(
      verifyingKey,
      textEncoder.encode(stableStringify(message)),
      signatureBytes,
    );
  }

  newX25519StaticSecret(): Uint8Array {
    return new_x25519_private_key();
  }

  getSealerID(secret: SealerSecret): SealerID {
    const privateBytes = base58.decode(
      secret.substring("sealerSecret_z".length),
    );
    const publicBytes = x25519_public_key(privateBytes);
    return `sealer_z${base58.encode(publicBytes)}`;
  }

  encrypt<T extends JsonValue, N extends JsonValue>(
    value: T,
    keySecret: KeySecret,
    nOnceMaterial: N,
  ): Encrypted<T, N> {
    const keySecretBytes = base58.decode(
      keySecret.substring("keySecret_z".length),
    );
    const plaintext = textEncoder.encode(stableStringify(value));
    try {
      const ciphertext = encrypt_xsalsa20(
        keySecretBytes,
        textEncoder.encode(stableStringify(nOnceMaterial)),
        plaintext,
      );
      return `encrypted_U${bytesToBase64url(ciphertext)}` as Encrypted<T, N>;
    } catch (e) {
      logger.error("Failed to encrypt message: " + (e as Error)?.message);
      throw e;
    }
  }

  decryptRaw<T extends JsonValue, N extends JsonValue>(
    encrypted: Encrypted<T, N>,
    keySecret: KeySecret,
    nOnceMaterial: N,
  ): Stringified<T> {
    const keySecretBytes = base58.decode(
      keySecret.substring("keySecret_z".length),
    );

    const ciphertext = base64URLtoBytes(
      encrypted.substring("encrypted_U".length),
    );
    try {
      const plaintext = decrypt_xsalsa20(
        keySecretBytes,
        textEncoder.encode(stableStringify(nOnceMaterial)),
        ciphertext,
      );
      return textDecoder.decode(plaintext) as Stringified<T>;
    } catch (e) {
      logger.error("Failed to decrypt message: " + (e as Error)?.message);
      throw e;
    }
  }

  seal<T extends JsonValue>({
    message,
    from,
    to,
    nOnceMaterial,
  }: {
    message: T;
    from: SealerSecret;
    to: SealerID;
    nOnceMaterial: { in: RawCoID; tx: TransactionID };
  }): Sealed<T> {
    const sealerPub = base58.decode(to.substring("sealer_z".length));
    const senderPriv = base58.decode(from.substring("sealerSecret_z".length));
    const plaintext = textEncoder.encode(stableStringify(message));
    const nonceMaterial = textEncoder.encode(stableStringify(nOnceMaterial));

    const sealedBytes = seal(plaintext, senderPriv, sealerPub, nonceMaterial);

    return `sealed_U${bytesToBase64url(sealedBytes)}` as Sealed<T>;
  }

  unseal<T extends JsonValue>(
    sealed: Sealed<T>,
    sealer: SealerSecret,
    from: SealerID,
    nOnceMaterial: { in: RawCoID; tx: TransactionID },
  ): T | undefined {
    const sealerPriv = base58.decode(sealer.substring("sealerSecret_z".length));
    const senderPub = base58.decode(from.substring("sealer_z".length));
    const sealedBytes = base64URLtoBytes(sealed.substring("sealed_U".length));
    const nonceMaterial = textEncoder.encode(stableStringify(nOnceMaterial));

    try {
      const plaintext = unseal(
        sealedBytes,
        sealerPriv,
        senderPub,
        nonceMaterial,
      );
      try {
        return JSON.parse(textDecoder.decode(plaintext));
      } catch (e) {
        logger.error(
          "Failed to decrypt/parse sealed message: " + (e as Error)?.message,
        );
        return undefined;
      }
    } catch (e) {
      logger.error(
        "Failed to decrypt/parse sealed message: " + (e as Error)?.message,
      );
      throw e;
    }
  }
}
