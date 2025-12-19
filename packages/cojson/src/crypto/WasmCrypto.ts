import {
  SessionLog,
  initialize,
  initializeSync,
  Blake3Hasher,
  blake3HashOnce,
  blake3HashOnceWithContext,
  decrypt,
  encrypt,
  getSealerId,
  getSignerId,
  newEd25519SigningKey,
  newX25519PrivateKey,
  seal,
  sign,
  unseal,
  verify,
} from "cojson-core-wasm";
import { base64URLtoBytes, bytesToBase64url } from "../base64url.js";
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
  Signature,
  SignerID,
  SignerSecret,
  textDecoder,
  textEncoder,
} from "./crypto.js";
import { ControlledAccountOrAgent } from "../coValues/account.js";
import {
  PrivateTransaction,
  Transaction,
  TrustingTransaction,
} from "../coValueCore/verifiedState.js";
import { isCloudflare, isEvalAllowed } from "../platformUtils.js";

type Blake3State = Blake3Hasher;

let wasmInit = initialize;
let wasmInitSync = initializeSync;

const wasmCryptoErrorMessage = (
  e: unknown,
) => `Critical Error: Failed to load WASM module

${!isEvalAllowed() ? `You need to add \`import "jazz-tools/load-edge-wasm";\` on top of your entry module to make Jazz work with ${isCloudflare() ? "Cloudflare workers" : "this runtime"}` : (e as Error).message}

A native crypto module is required for Jazz to work. See https://jazz.tools/docs/react/reference/performance#use-the-best-crypto-implementation-for-your-platform for possible alternatives.`;

/**
 * Initializes the WasmCrypto module. This function can be used to initialize the WasmCrypto module in a worker or a browser.
 * if you are using SSR and you want to initialize WASM crypto asynchronously you can use this function.
 * @returns A promise that resolves when the WasmCrypto module is successfully initialized.
 */
export async function initWasmCrypto() {
  try {
    await wasmInit();
  } catch (e) {
    throw new Error(wasmCryptoErrorMessage(e), { cause: e });
  }
}

/**
 * WebAssembly implementation of the CryptoProvider interface using cojson-core-wasm.
 * This provides the primary implementation using WebAssembly for optimal performance, offering:
 * - Signing/verifying (Ed25519)
 * - Encryption/decryption (XSalsa20)
 * - Sealing/unsealing (X25519 + XSalsa20-Poly1305)
 * - Hashing (BLAKE3)
 */
export class WasmCrypto extends CryptoProvider<Blake3State> {
  protected constructor() {
    super();
  }

  static setInit(value: typeof initialize) {
    wasmInit = value;
  }

  static setInitSync(value: typeof initializeSync) {
    wasmInitSync = value;
  }

  static createSync(): WasmCrypto {
    try {
      wasmInitSync();
    } catch (e) {
      throw new Error(wasmCryptoErrorMessage(e), { cause: e });
    }
    return new WasmCrypto();
  }

  // TODO: Remove this method and use createSync instead, this is not necessary since we can use createSync in the browser and in the worker.
  // @deprecated
  static async create(): Promise<WasmCrypto> {
    await initWasmCrypto();
    return new WasmCrypto();
  }

  blake3HashOnce(data: Uint8Array) {
    return blake3HashOnce(data);
  }

  blake3HashOnceWithContext(
    data: Uint8Array,
    { context }: { context: Uint8Array },
  ) {
    return blake3HashOnceWithContext(data, context);
  }

  newEd25519SigningKey(): Uint8Array {
    return newEd25519SigningKey();
  }

  getSignerID(secret: SignerSecret): SignerID {
    return getSignerId(textEncoder.encode(secret)) as SignerID;
  }

  sign(secret: SignerSecret, message: JsonValue): Signature {
    return sign(
      textEncoder.encode(stableStringify(message)),
      textEncoder.encode(secret),
    ) as Signature;
  }

  verify(signature: Signature, message: JsonValue, id: SignerID): boolean {
    const result = verify(
      textEncoder.encode(signature),
      textEncoder.encode(stableStringify(message)),
      textEncoder.encode(id),
    );

    return result;
  }

  newX25519StaticSecret(): Uint8Array {
    return newX25519PrivateKey();
  }

  getSealerID(secret: SealerSecret): SealerID {
    return getSealerId(textEncoder.encode(secret)) as SealerID;
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

  createSessionLog(coID: RawCoID, sessionID: SessionID, signerID?: SignerID) {
    return new SessionLogAdapter(new SessionLog(coID, sessionID, signerID));
  }
}

class SessionLogAdapter {
  constructor(private readonly sessionLog: SessionLog) {}

  tryAdd(
    transactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean,
  ): void {
    this.sessionLog.tryAdd(
      // We can avoid stableStringify because in rust we will parse and stringify the transactions again.
      // And the changes are in a string format already.
      transactions.map((tx) => JSON.stringify(tx)),
      newSignature,
      skipVerify,
    );
  }

  addNewPrivateTransaction(
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    keyID: KeyID,
    keySecret: KeySecret,
    madeAt: number,
    meta: JsonObject | undefined,
  ): { signature: Signature; transaction: PrivateTransaction } {
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
    return { signature: parsedOutput.signature, transaction };
  }

  addNewTrustingTransaction(
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    madeAt: number,
    meta: JsonObject | undefined,
  ): { signature: Signature; transaction: TrustingTransaction } {
    const stringifiedChanges = stableStringify(changes);
    const stringifiedMeta = meta ? stableStringify(meta) : undefined;
    const output = this.sessionLog.addNewTrustingTransaction(
      stringifiedChanges,
      signerAgent.currentSignerSecret(),
      madeAt,
      stringifiedMeta,
    );
    const transaction: TrustingTransaction = {
      privacy: "trusting",
      madeAt,
      changes: stringifiedChanges,
      meta: stringifiedMeta,
    };
    return { signature: output as Signature, transaction };
  }

  decryptNextTransactionChangesJson(
    txIndex: number,
    keySecret: KeySecret,
  ): string {
    const output = this.sessionLog.decryptNextTransactionChangesJson(
      txIndex,
      keySecret,
    );
    return output;
  }

  decryptNextTransactionMetaJson(
    txIndex: number,
    keySecret: KeySecret,
  ): string | undefined {
    return this.sessionLog.decryptNextTransactionMetaJson(txIndex, keySecret);
  }

  free() {
    this.sessionLog.free();
  }

  clone(): SessionLogAdapter {
    return new SessionLogAdapter(this.sessionLog.clone());
  }
}
