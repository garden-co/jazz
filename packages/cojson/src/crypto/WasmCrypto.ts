import {
  Ed25519Signature,
  Ed25519SigningKey,
  Ed25519VerifyingKey,
  Memory,
  X25519PublicKey,
  X25519StaticSecret,
  initBundledOnce,
} from "@hazae41/berith";
import { xsalsa20, xsalsa20_poly1305 } from "@noble/ciphers/salsa";
import { base58 } from "@scure/base";
import {
  blake3_digest_for_state,
  blake3_empty_state,
  blake3_hash_once,
  blake3_hash_once_with_context,
  blake3_update_state,
  generate_nonce,
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
 * WebAssembly implementation of the CryptoProvider interface using jazz-crypto-rs and berith library.
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
      initBundledOnce(),
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

  generateNonce(input: Uint8Array): Uint8Array {
    return generate_nonce(input);
  }

  private generateJsonNonce(material: JsonValue): Uint8Array {
    return this.generateNonce(textEncoder.encode(stableStringify(material)));
  }

  newEd25519SigningKey(): Uint8Array {
    return new Ed25519SigningKey().to_bytes().copyAndDispose();
  }

  getSignerID(secret: SignerSecret): SignerID {
    return `signer_z${base58.encode(
      Ed25519SigningKey.from_bytes(
        new Memory(base58.decode(secret.substring("signerSecret_z".length))),
      )
        .public()
        .to_bytes()
        .copyAndDispose(),
    )}`;
  }

  sign(secret: SignerSecret, message: JsonValue): Signature {
    const signature = Ed25519SigningKey.from_bytes(
      new Memory(base58.decode(secret.substring("signerSecret_z".length))),
    )
      .sign(new Memory(textEncoder.encode(stableStringify(message))))
      .to_bytes()
      .copyAndDispose();
    return `signature_z${base58.encode(signature)}`;
  }

  verify(signature: Signature, message: JsonValue, id: SignerID): boolean {
    return new Ed25519VerifyingKey(
      new Memory(base58.decode(id.substring("signer_z".length))),
    ).verify(
      new Memory(textEncoder.encode(stableStringify(message))),
      new Ed25519Signature(
        new Memory(base58.decode(signature.substring("signature_z".length))),
      ),
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
    const nOnce = this.generateJsonNonce(nOnceMaterial);

    const plaintext = textEncoder.encode(stableStringify(value));
    const ciphertext = xsalsa20(keySecretBytes, nOnce, plaintext);
    return `encrypted_U${bytesToBase64url(ciphertext)}` as Encrypted<T, N>;
  }

  decryptRaw<T extends JsonValue, N extends JsonValue>(
    encrypted: Encrypted<T, N>,
    keySecret: KeySecret,
    nOnceMaterial: N,
  ): Stringified<T> {
    const keySecretBytes = base58.decode(
      keySecret.substring("keySecret_z".length),
    );
    const nOnce = this.generateJsonNonce(nOnceMaterial);

    const ciphertext = base64URLtoBytes(
      encrypted.substring("encrypted_U".length),
    );
    const plaintext = xsalsa20(keySecretBytes, nOnce, ciphertext);

    return textDecoder.decode(plaintext) as Stringified<T>;
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
