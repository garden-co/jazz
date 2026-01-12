import {
  JsonValue,
  RawCoID,
  SessionID,
  Stringified,
  base64URLtoBytes,
  bytesToBase64url,
} from "../exports.js";
import { CojsonInternalTypes } from "../exports.js";
import { TransactionID } from "../ids.js";
import { stableStringify } from "../jsonStringify.js";
import { JsonObject } from "../jsonValue.js";
import { logger } from "../logger.js";
import { toFfiTransactionObject } from "./ffiTransaction.js";
import { ControlledAccountOrAgent } from "../coValues/account.js";
import {
  PrivateTransaction,
  Transaction,
  TrustingTransaction,
} from "../coValueCore/verifiedState.js";
import {
  CryptoProvider,
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
import {
  blake3HashOnce,
  blake3HashOnceWithContext,
  verify,
  encrypt,
  decrypt,
  newEd25519SigningKey,
  newX25519PrivateKey,
  getSealerId,
  getSignerId,
  sign,
  seal,
  unseal,
  Blake3Hasher,
  SessionLog,
} from "cojson-core-rn";
import { WasmCrypto } from "./WasmCrypto.js";

type Blake3State = Blake3Hasher;

/**
 *
 * @param view - The Uint8Array to convert to an ArrayBuffer.
 * @returns The ArrayBuffer.
 */
function toArrayBuffer(view: Uint8Array): ArrayBuffer {
  if (
    view.byteOffset === 0 &&
    view.byteLength === view.buffer.byteLength &&
    view.buffer instanceof ArrayBuffer
  ) {
    return view.buffer;
  }
  const buffer = new ArrayBuffer(view.byteLength);
  new Uint8Array(buffer).set(view);
  return buffer;
}

export class RNCrypto extends CryptoProvider<Blake3State> {
  private constructor() {
    super();
  }

  getSignerID(secret: SignerSecret): SignerID {
    return getSignerId(secret) as SignerID;
  }
  newX25519StaticSecret(): Uint8Array {
    return new Uint8Array(newX25519PrivateKey());
  }
  getSealerID(secret: SealerSecret): SealerID {
    return getSealerId(secret) as SealerID;
  }
  blake3HashOnce(data: Uint8Array): Uint8Array {
    return new Uint8Array(blake3HashOnce(toArrayBuffer(data)));
  }
  blake3HashOnceWithContext(
    data: Uint8Array,
    { context }: { context: Uint8Array },
  ): Uint8Array {
    return new Uint8Array(
      blake3HashOnceWithContext(toArrayBuffer(data), toArrayBuffer(context)),
    );
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
    const messageBuffer = toArrayBuffer(
      textEncoder.encode(stableStringify(message)),
    );
    const nOnceBuffer = toArrayBuffer(
      textEncoder.encode(stableStringify(nOnceMaterial)),
    );

    return `sealed_U${bytesToBase64url(
      new Uint8Array(seal(messageBuffer, from, to, nOnceBuffer)),
    )}` as Sealed<T>;
  }
  unseal<T extends JsonValue>(
    sealed: Sealed<T>,
    sealer: SealerSecret,
    from: SealerID,
    nOnceMaterial: { in: RawCoID; tx: TransactionID },
  ): T | undefined {
    const sealedBytes = base64URLtoBytes(sealed.substring("sealed_U".length));
    const nonceBuffer = toArrayBuffer(
      textEncoder.encode(stableStringify(nOnceMaterial)),
    );

    const plaintext = textDecoder.decode(
      unseal(toArrayBuffer(sealedBytes), sealer, from, nonceBuffer),
    );
    try {
      return JSON.parse(plaintext) as T;
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
    return new SessionLogAdapter(new SessionLog(coID, sessionID, signerID));
  }

  static async create(): Promise<RNCrypto> {
    return new RNCrypto();
  }

  newEd25519SigningKey(): Uint8Array {
    return new Uint8Array(newEd25519SigningKey());
  }

  sign(
    secret: CojsonInternalTypes.SignerSecret,
    message: JsonValue,
  ): CojsonInternalTypes.Signature {
    return sign(
      toArrayBuffer(textEncoder.encode(stableStringify(message))),
      secret,
    ) as CojsonInternalTypes.Signature;
  }

  verify(
    signature: CojsonInternalTypes.Signature,
    message: JsonValue,
    id: CojsonInternalTypes.SignerID,
  ): boolean {
    const result = verify(
      signature,
      toArrayBuffer(textEncoder.encode(stableStringify(message))),
      id,
    );

    return result;
  }

  encrypt<T extends JsonValue, N extends JsonValue>(
    value: T,
    keySecret: CojsonInternalTypes.KeySecret,
    nOnceMaterial: N,
  ): CojsonInternalTypes.Encrypted<T, N> {
    const valueBytes = toArrayBuffer(
      textEncoder.encode(stableStringify(value)),
    );
    const nOnceBytes = toArrayBuffer(
      textEncoder.encode(stableStringify(nOnceMaterial)),
    );

    const encrypted = `encrypted_U${bytesToBase64url(
      new Uint8Array(encrypt(valueBytes, keySecret, nOnceBytes)),
    )}` as CojsonInternalTypes.Encrypted<T, N>;
    return encrypted;
  }

  decryptRaw<T extends JsonValue, N extends JsonValue>(
    encrypted: CojsonInternalTypes.Encrypted<T, N>,
    keySecret: CojsonInternalTypes.KeySecret,
    nOnceMaterial: N,
  ): Stringified<T> {
    const buffer = base64URLtoBytes(encrypted.substring("encrypted_U".length));

    const decrypted = textDecoder.decode(
      decrypt(
        toArrayBuffer(buffer),
        keySecret,
        toArrayBuffer(textEncoder.encode(stableStringify(nOnceMaterial))),
      ),
    ) as Stringified<T>;

    return decrypted;
  }
}

class SessionLogAdapter implements SessionLogImpl {
  constructor(private readonly sessionLog: SessionLog) {}

  tryAdd(
    transactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean,
  ): void {
    const data = transactions.map(toFfiTransactionObject);

    (this.sessionLog as any).tryAddFfi(data, newSignature, skipVerify);
  }

  addNewPrivateTransaction(
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    keyID: KeyID,
    keySecret: KeySecret,
    madeAt: number,
    meta: JsonObject | undefined,
  ) {
    const output = this.sessionLog.addNewPrivateTransaction(
      // We can avoid stableStringify because it will be encrypted.
      JSON.stringify(changes),
      signerAgent.currentSignerSecret(),
      keySecret,
      keyID,
      madeAt,
      // We can avoid stableStringify because it will be encrypted.
      meta ? JSON.stringify(meta) : undefined,
    );
    const parsedOutput = JSON.parse(output);
    const transaction: PrivateTransaction = {
      privacy: "private",
      madeAt,
      encryptedChanges: parsedOutput.encrypted_changes,
      keyUsed: keyID,
      meta: parsedOutput.meta,
    };
    return { signature: parsedOutput.signature as Signature, transaction };
  }

  addNewTrustingTransaction(
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    madeAt: number,
    meta: JsonObject | undefined,
  ) {
    // We can avoid stableStringify because the changes will be in a string format already.
    const stringifiedChanges = JSON.stringify(changes);
    // We can avoid stableStringify because the meta will be in a string format already.
    const stringifiedMeta = meta ? JSON.stringify(meta) : undefined;
    const output = this.sessionLog.addNewTrustingTransaction(
      stringifiedChanges,
      signerAgent.currentSignerSecret(),
      madeAt,
      stringifiedMeta,
    );
    const transaction: TrustingTransaction = {
      privacy: "trusting",
      madeAt,
      changes: stringifiedChanges as Stringified<JsonValue[]>,
      meta: stringifiedMeta as Stringified<JsonObject> | undefined,
    };
    return { signature: output as Signature, transaction };
  }

  decryptNextTransactionChangesJson(
    txIndex: number,
    keySecret: KeySecret,
  ): string {
    return this.sessionLog.decryptNextTransactionChangesJson(
      txIndex,
      keySecret,
    );
  }

  decryptNextTransactionMetaJson(
    txIndex: number,
    keySecret: KeySecret,
  ): string | undefined {
    return this.sessionLog.decryptNextTransactionMetaJson(txIndex, keySecret);
  }

  free(): void {
    this.sessionLog.uniffiDestroy();
  }

  clone(): SessionLogImpl {
    return new SessionLogAdapter(
      this.sessionLog.cloneSessionLog() as SessionLog,
    );
  }
}
