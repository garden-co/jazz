import { xsalsa20, xsalsa20poly1305 } from "@noble/ciphers/salsa";
import { ed25519, x25519 } from "@noble/curves/ed25519";
import { blake3 } from "@noble/hashes/blake3";
import { base58 } from "@scure/base";
import { base64URLtoBytes, bytesToBase64url } from "../base64url.js";
import { Transaction } from "../coValueCore/verifiedState.js";
import { RawCoID, SessionID, TransactionID } from "../ids.js";
import { Stringified, stableStringify } from "../jsonStringify.js";
import { JsonValue } from "../jsonValue.js";
import { logger } from "../logger.js";
import {
  CryptoProvider,
  Encrypted,
  KeyID,
  KeySecret,
  Sealed,
  SealerID,
  SealerSecret,
  SessionLogImpl,
  Signature,
  SignerID,
  SignerSecret,
  StreamingHash,
  textDecoder,
  textEncoder,
} from "./crypto.js";

type Blake3State = ReturnType<typeof blake3.create>;

/**
 * Pure JavaScript implementation of the CryptoProvider interface using noble-curves and noble-ciphers libraries.
 * This provides a fallback implementation that doesn't require WebAssembly, offering:
 * - Signing/verifying (Ed25519)
 * - Encryption/decryption (XSalsa20)
 * - Sealing/unsealing (X25519 + XSalsa20-Poly1305)
 * - Hashing (BLAKE3)
 */
export class PureJSCrypto extends CryptoProvider<Blake3State> {
  static async create(): Promise<PureJSCrypto> {
    return new PureJSCrypto();
  }

  emptyBlake3State(): Blake3State {
    return blake3.create({});
  }

  cloneBlake3State(state: Blake3State): Blake3State {
    return state.clone();
  }

  blake3HashOnce(data: Uint8Array) {
    return blake3(data);
  }

  blake3HashOnceWithContext(
    data: Uint8Array,
    { context }: { context: Uint8Array },
  ) {
    return blake3.create({}).update(context).update(data).digest();
  }

  blake3IncrementalUpdate(state: Blake3State, data: Uint8Array) {
    return state.update(data);
  }

  blake3DigestForState(state: Blake3State): Uint8Array {
    return state.clone().digest();
  }

  generateNonce(input: Uint8Array): Uint8Array {
    return this.blake3HashOnce(input).slice(0, 24);
  }

  generateJsonNonce(material: JsonValue): Uint8Array {
    return this.generateNonce(textEncoder.encode(stableStringify(material)));
  }

  newEd25519SigningKey(): Uint8Array {
    return ed25519.utils.randomPrivateKey();
  }

  getSignerID(secret: SignerSecret): SignerID {
    return `signer_z${base58.encode(
      ed25519.getPublicKey(
        base58.decode(secret.substring("signerSecret_z".length)),
      ),
    )}`;
  }

  sign(secret: SignerSecret, message: JsonValue): Signature {
    const signature = ed25519.sign(
      textEncoder.encode(stableStringify(message)),
      base58.decode(secret.substring("signerSecret_z".length)),
    );
    return `signature_z${base58.encode(signature)}`;
  }

  verify(signature: Signature, message: JsonValue, id: SignerID): boolean {
    return ed25519.verify(
      base58.decode(signature.substring("signature_z".length)),
      textEncoder.encode(stableStringify(message)),
      base58.decode(id.substring("signer_z".length)),
    );
  }

  newX25519StaticSecret(): Uint8Array {
    return x25519.utils.randomPrivateKey();
  }

  getSealerID(secret: SealerSecret): SealerID {
    return `sealer_z${base58.encode(
      x25519.getPublicKey(
        base58.decode(secret.substring("sealerSecret_z".length)),
      ),
    )}`;
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
    const nOnce = this.generateJsonNonce(nOnceMaterial);

    const sealerPub = base58.decode(to.substring("sealer_z".length));

    const senderPriv = base58.decode(from.substring("sealerSecret_z".length));

    const plaintext = textEncoder.encode(stableStringify(message));

    const sharedSecret = x25519.getSharedSecret(senderPriv, sealerPub);

    const sealedBytes = xsalsa20poly1305(sharedSecret, nOnce).encrypt(
      plaintext,
    );

    return `sealed_U${bytesToBase64url(sealedBytes)}` as Sealed<T>;
  }

  unseal<T extends JsonValue>(
    sealed: Sealed<T>,
    sealer: SealerSecret,
    from: SealerID,
    nOnceMaterial: { in: RawCoID; tx: TransactionID },
  ): T | undefined {
    const nOnce = this.generateJsonNonce(nOnceMaterial);

    const sealerPriv = base58.decode(sealer.substring("sealerSecret_z".length));

    const senderPub = base58.decode(from.substring("sealer_z".length));

    const sealedBytes = base64URLtoBytes(sealed.substring("sealed_U".length));

    const sharedSecret = x25519.getSharedSecret(sealerPriv, senderPub);

    const plaintext = xsalsa20poly1305(sharedSecret, nOnce).decrypt(
      sealedBytes,
    );

    try {
      return JSON.parse(textDecoder.decode(plaintext));
    } catch (e) {
      logger.error("Failed to decrypt/parse sealed message", { err: e });
      return undefined;
    }
  }

  createSessionLog(
    coID: RawCoID,
    sessionID: SessionID,
    signerID: SignerID,
  ): SessionLogImpl {
    return new PureJSSessionLog(coID, sessionID, signerID, this);
  }
}

export class PureJSSessionLog implements SessionLogImpl {
  transactions: string[] = [];
  lastSignature: Signature | undefined;
  streamingHash: Blake3State;

  constructor(
    private readonly coID: RawCoID,
    private readonly sessionID: SessionID,
    private readonly signerID: SignerID,
    private readonly crypto: PureJSCrypto,
  ) {
    this.streamingHash = this.crypto.emptyBlake3State();
  }

  clone(): SessionLogImpl {
    const newLog = new PureJSSessionLog(
      this.coID,
      this.sessionID,
      this.signerID,
      this.crypto,
    );
    newLog.transactions = this.transactions.slice();
    newLog.lastSignature = this.lastSignature;
    newLog.streamingHash = this.crypto.cloneBlake3State(this.streamingHash);
    return newLog;
  }

  tryAdd(
    transactionsJson: string[],
    newSignatureStr: string,
    skipVerify: boolean,
  ): string {
    if (!skipVerify) {
      const checkHasher = this.crypto.cloneBlake3State(this.streamingHash);
      for (const tx of transactionsJson) {
        checkHasher.update(textEncoder.encode(tx));
      }
      const newHash = checkHasher.digest();
      const newHashEncoded = `hash_z${base58.encode(newHash)}`;

      if (
        !this.crypto.verify(
          newSignatureStr as Signature,
          newHashEncoded,
          this.signerID,
        )
      ) {
        throw new Error("Signature verification failed");
      }
    }

    for (const tx of transactionsJson) {
      this.crypto.blake3IncrementalUpdate(
        this.streamingHash,
        textEncoder.encode(tx),
      );
      this.transactions.push(tx);
    }

    this.lastSignature = newSignatureStr as Signature;

    return newSignatureStr;
  }

  expectedHashAfter(transactionsJson: string[]): string {
    const hasher = this.crypto.cloneBlake3State(this.streamingHash);
    for (const tx of transactionsJson) {
      hasher.update(textEncoder.encode(tx));
    }
    const newHash = hasher.digest();
    return `hash_z${base58.encode(newHash)}`;
  }

  addNewPrivateTransaction(
    changesJson: string,
    signerSecret: string,
    encryptionKey: string,
    keyId: string,
    madeAt: number,
  ): string {
    const encryptedChanges = this.crypto.encrypt(
      JSON.parse(changesJson),
      encryptionKey as KeySecret,
      {
        in: this.coID,
        tx: { sessionID: this.sessionID, txIndex: this.transactions.length },
      },
    );
    const tx = {
      encryptedChanges: encryptedChanges,
      madeAt: madeAt,
      privacy: "private",
      keyUsed: keyId as KeyID,
    } satisfies Transaction;
    const txJson = stableStringify(tx);
    const hashAfter = this.expectedHashAfter([txJson]);
    const signature = this.crypto.sign(signerSecret as SignerSecret, hashAfter);
    const signatureStr = this.tryAdd([txJson], signature, false);
    return JSON.stringify({
      signature: signatureStr,
      transaction: tx,
      hash: hashAfter,
    });
  }

  addNewTrustingTransaction(
    changesJson: string,
    signerSecret: string,

    madeAt: number,
  ): string {
    const tx = {
      changes: changesJson as Stringified<JsonValue[]>,
      madeAt: madeAt,
      privacy: "trusting",
    } satisfies Transaction;
    const txJson = stableStringify(tx);
    const hashAfter = this.expectedHashAfter([txJson]);
    const signature = this.crypto.sign(signerSecret as SignerSecret, hashAfter);
    const signatureStr = this.tryAdd([txJson], signature, false);
    return JSON.stringify({
      signature: signatureStr,
      transaction: tx,
      hash: hashAfter,
    });
  }

  testExpectedHashAfter(transactionsJson: string[]): string {
    return this.expectedHashAfter(transactionsJson);
  }

  decryptNextTransactionChangesJson(
    txIndex: number,
    keySecret: Uint8Array,
  ): string {
    const txJson = this.transactions[txIndex];
    if (!txJson) {
      throw new Error("Transaction not found");
    }
    const tx = JSON.parse(txJson) as Transaction;
    if (tx.privacy === "private") {
      const nOnceMaterial = {
        in: this.coID,
        tx: { sessionID: this.sessionID, txIndex: txIndex },
      };

      const nOnce = this.crypto.generateJsonNonce(nOnceMaterial);

      const ciphertext = base64URLtoBytes(
        tx.encryptedChanges.substring("encrypted_U".length),
      );
      const plaintext = xsalsa20(keySecret, nOnce, ciphertext);

      return textDecoder.decode(plaintext);
    } else {
      return tx.changes;
    }
  }

  free(): void {
    // no-op
  }
}
