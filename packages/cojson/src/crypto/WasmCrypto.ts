import {
  Blake3Hasher,
  blake3_empty_state,
  blake3_hash_once,
  blake3_hash_once_with_context,
  decrypt,
  encrypt,
  get_sealer_id,
  get_signer_id,
  initialize,
  new_ed25519_signing_key,
  new_x25519_private_key,
  seal,
  sign,
  unseal,
  verify,
} from "jazz-crypto-rs";
import { base64URLtoBytes, bytesToBase64url } from "../base64url.js";
import { RawCoID, TransactionID } from "../ids.js";
import { Stringified, stableStringify } from "../jsonStringify.js";
import { JsonValue } from "../jsonValue.js";
import { logger } from "../logger.js";
import { PureJSCrypto } from "./PureJSCrypto.js";
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

type Blake3State = Blake3Hasher;

/**
 * WebAssembly implementation of the CryptoProvider interface using jazz-crypto-rs.
 * This provides the primary implementation using WebAssembly for optimal performance, offering:
 * - Signing/verifying (Ed25519)
 * - Encryption/decryption (XSalsa20)
 * - Sealing/unsealing (X25519 + XSalsa20-Poly1305)
 * - Hashing (BLAKE3)
 */
export class WasmCrypto extends CryptoProvider<Blake3State> {
  private constructor() {
    super();
  }

  static async create(): Promise<WasmCrypto | PureJSCrypto> {
    try {
      await initialize();
    } catch (e) {
      logger.warn(
        "Failed to initialize WasmCrypto, falling back to PureJSCrypto",
        { err: e },
      );
      return new PureJSCrypto();
    }

    return new WasmCrypto();
  }

  emptyBlake3State(): Blake3State {
    return blake3_empty_state();
  }

  cloneBlake3State(state: Blake3State): Blake3State {
    return state.clone();
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

  blake3IncrementalUpdate(state: Blake3State, data: Uint8Array): Blake3State {
    state.update(data);
    return state;
  }

  blake3DigestForState(state: Blake3State): Uint8Array {
    return state.finalize();
  }

  newEd25519SigningKey(): Uint8Array {
    return new_ed25519_signing_key();
  }

  getSignerID(secret: SignerSecret): SignerID {
    return get_signer_id(textEncoder.encode(secret)) as SignerID;
  }

  sign(secret: SignerSecret, message: JsonValue): Signature {
    return sign(
      textEncoder.encode(stableStringify(message)),
      textEncoder.encode(secret),
    ) as Signature;
  }

  verify(signature: Signature, message: JsonValue, id: SignerID): boolean {
    return verify(
      textEncoder.encode(signature),
      textEncoder.encode(stableStringify(message)),
      textEncoder.encode(id),
    );
  }

  newX25519StaticSecret(): Uint8Array {
    return new_x25519_private_key();
  }

  getSealerID(secret: SealerSecret): SealerID {
    return get_sealer_id(textEncoder.encode(secret)) as SealerID;
  }

  encrypt<T extends JsonValue, N extends JsonValue>(
    value: T,
    keySecret: KeySecret,
    nOnceMaterial: N,
  ): Encrypted<T, N> {
    return `encrypted_U${bytesToBase64url(
      encrypt(
        textEncoder.encode(stableStringify(value)),
        keySecret,
        textEncoder.encode(stableStringify(nOnceMaterial)),
      ),
    )}` as Encrypted<T, N>;
  }

  decryptRaw<T extends JsonValue, N extends JsonValue>(
    encrypted: Encrypted<T, N>,
    keySecret: KeySecret,
    nOnceMaterial: N,
  ): Stringified<T> {
    return textDecoder.decode(
      decrypt(
        base64URLtoBytes(encrypted.substring("encrypted_U".length)),
        keySecret,
        textEncoder.encode(stableStringify(nOnceMaterial)),
      ),
    ) as Stringified<T>;
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
    return `sealed_U${bytesToBase64url(
      seal(
        textEncoder.encode(stableStringify(message)),
        from,
        to,
        textEncoder.encode(stableStringify(nOnceMaterial)),
      ),
    )}` as Sealed<T>;
  }

  unseal<T extends JsonValue>(
    sealed: Sealed<T>,
    sealer: SealerSecret,
    from: SealerID,
    nOnceMaterial: { in: RawCoID; tx: TransactionID },
  ): T | undefined {
    const plaintext = textDecoder.decode(
      unseal(
        base64URLtoBytes(sealed.substring("sealed_U".length)),
        sealer,
        from,
        textEncoder.encode(stableStringify(nOnceMaterial)),
      ),
    );
    try {
      return JSON.parse(plaintext) as T;
    } catch (e) {
      logger.error("Failed to decrypt/parse sealed message", { err: e });
      return undefined;
    }
  }
}
