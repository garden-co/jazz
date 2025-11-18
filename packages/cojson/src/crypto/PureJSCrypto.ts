import { xsalsa20, xsalsa20poly1305 } from "@noble/ciphers/salsa";
import { ed25519, x25519 } from "@noble/curves/ed25519";
import { blake3 } from "@noble/hashes/blake3";
import { base58 } from "@scure/base";
import { base64URLtoBytes, bytesToBase64url } from "../base64url.js";
import {
  PrivateTransaction,
  Transaction,
  TrustingTransaction,
} from "../coValueCore/verifiedState.js";
import { RawCoID, SessionID, TransactionID } from "../ids.js";
import { Stringified, stableStringify } from "../jsonStringify.js";
import { JsonObject, JsonValue } from "../jsonValue.js";
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
  textDecoder,
  textEncoder,
} from "./crypto.js";
import { ControlledAccountOrAgent } from "../coValues/account.js";

type Blake3State = ReturnType<typeof blake3.create>;

const x25519SharedSecretCache = new Map<string, Uint8Array>();

function getx25519SharedSecret(
  privateKeyA: SealerSecret,
  publicKeyB: SealerID,
): Uint8Array {
  const cacheKey = `${privateKeyA}-${publicKeyB}`;
  let sharedSecret = x25519SharedSecretCache.get(cacheKey);

  if (!sharedSecret) {
    const privateKeyABytes = base58.decode(
      privateKeyA.substring("sealerSecret_z".length),
    );
    const publicKeyBBytes = base58.decode(
      publicKeyB.substring("sealer_z".length),
    );
    sharedSecret = x25519.getSharedSecret(privateKeyABytes, publicKeyBBytes);
    x25519SharedSecretCache.set(cacheKey, sharedSecret);
  }

  return sharedSecret;
}

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

  blake3HashOnce(data: Uint8Array) {
    return blake3(data);
  }

  blake3HashOnceWithContext(
    data: Uint8Array,
    { context }: { context: Uint8Array },
  ) {
    return blake3.create({}).update(context).update(data).digest();
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
    const sharedSecret = getx25519SharedSecret(from, to);
    const nOnce = this.generateJsonNonce(nOnceMaterial);
    const plaintext = textEncoder.encode(stableStringify(message));

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

    const sharedSecret = getx25519SharedSecret(sealer, from);
    const sealedBytes = base64URLtoBytes(sealed.substring("sealed_U".length));

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
    signerID?: SignerID,
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
    private readonly signerID: SignerID | undefined,
    private readonly crypto: PureJSCrypto,
  ) {
    this.streamingHash = blake3.create({});
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
    newLog.streamingHash = this.streamingHash.clone();
    return newLog;
  }

  tryAdd(
    transactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean,
  ): void {
    this.internalTryAdd(
      transactions.map((tx) => stableStringify(tx)),
      newSignature,
      skipVerify,
    );
  }

  internalTryAdd(
    transactions: string[],
    newSignature: Signature,
    skipVerify: boolean,
  ) {
    for (const tx of transactions) {
      this.streamingHash.update(textEncoder.encode(tx));
    }

    if (!skipVerify) {
      if (!this.signerID) {
        throw new Error("Tried to add transactions without signer ID");
      }

      const newHash = this.streamingHash.clone().digest();
      const newHashEncoded = `hash_z${base58.encode(newHash)}`;

      if (!this.crypto.verify(newSignature, newHashEncoded, this.signerID)) {
        // Rebuild the streaming hash to the original state
        this.streamingHash = blake3.create({});
        for (const tx of this.transactions) {
          this.streamingHash.update(textEncoder.encode(tx));
        }
        throw new Error("Signature verification failed");
      }
    }

    for (const tx of transactions) {
      this.transactions.push(tx);
    }

    this.lastSignature = newSignature;

    return newSignature;
  }

  internalAddNewTransaction(
    transaction: string,
    signerAgent: ControlledAccountOrAgent,
  ) {
    this.streamingHash.update(textEncoder.encode(transaction));
    const newHash = this.streamingHash.clone().digest();
    const newHashEncoded = `hash_z${base58.encode(newHash)}`;
    const signature = this.crypto.sign(
      signerAgent.currentSignerSecret(),
      newHashEncoded,
    );
    this.transactions.push(transaction);
    this.lastSignature = signature;

    return signature;
  }

  addNewPrivateTransaction(
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    keyID: KeyID,
    keySecret: KeySecret,
    madeAt: number,
    meta: JsonObject | undefined,
  ): { signature: Signature; transaction: PrivateTransaction } {
    const encryptedChanges = this.crypto.encrypt(changes, keySecret, {
      in: this.coID,
      tx: { sessionID: this.sessionID, txIndex: this.transactions.length },
    });

    const encryptedMeta = meta
      ? this.crypto.encrypt(meta, keySecret, {
          in: this.coID,
          tx: { sessionID: this.sessionID, txIndex: this.transactions.length },
        })
      : undefined;

    const tx = {
      encryptedChanges: encryptedChanges,
      madeAt: madeAt,
      privacy: "private",
      keyUsed: keyID,
      meta: encryptedMeta,
    } satisfies Transaction;
    const signature = this.internalAddNewTransaction(
      stableStringify(tx),
      signerAgent,
    );
    return {
      signature: signature as Signature,
      transaction: tx,
    };
  }

  addNewTrustingTransaction(
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    madeAt: number,
    meta: JsonObject | undefined,
  ): { signature: Signature; transaction: TrustingTransaction } {
    const tx = {
      changes: stableStringify(changes),
      madeAt: madeAt,
      privacy: "trusting",
      meta: meta ? stableStringify(meta) : undefined,
    } satisfies Transaction;
    const signature = this.internalAddNewTransaction(
      stableStringify(tx),
      signerAgent,
    );
    return {
      signature: signature as Signature,
      transaction: tx,
    };
  }

  decryptNextTransactionChangesJson(
    txIndex: number,
    keySecret: KeySecret,
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

      return this.crypto.decryptRaw(
        tx.encryptedChanges,
        keySecret,
        nOnceMaterial,
      );
    } else {
      return tx.changes;
    }
  }

  decryptNextTransactionMetaJson(
    txIndex: number,
    keySecret: KeySecret,
  ): string | undefined {
    const txJson = this.transactions[txIndex];
    if (!txJson) {
      throw new Error("Transaction not found");
    }
    const tx = JSON.parse(txJson) as Transaction;

    if (!tx.meta) {
      return undefined;
    }

    if (tx.privacy === "private") {
      const nOnceMaterial = {
        in: this.coID,
        tx: { sessionID: this.sessionID, txIndex: txIndex },
      };

      return this.crypto.decryptRaw(tx.meta, keySecret, nOnceMaterial);
    } else {
      return tx.meta;
    }
  }

  free(): void {
    // no-op
  }
}
