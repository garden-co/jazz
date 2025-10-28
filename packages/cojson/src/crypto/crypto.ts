import { base58 } from "@scure/base";
import { ControlledAccountOrAgent, RawAccountID } from "../coValues/account.js";
import { AgentID, RawCoID, TransactionID } from "../ids.js";
import { SessionID } from "../ids.js";
import { Stringified, parseJSON, stableStringify } from "../jsonStringify.js";
import { JsonObject, JsonValue } from "../jsonValue.js";
import { logger } from "../logger.js";
import {
  PrivateTransaction,
  Transaction,
  TrustingTransaction,
} from "../coValueCore/verifiedState.js";

function randomBytes(bytesLength = 32): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(bytesLength));
}

export type SignerSecret = `signerSecret_z${string}`;
export type SignerID = `signer_z${string}`;
export type Signature = `signature_z${string}`;

export type SealerSecret = `sealerSecret_z${string}`;
export type SealerID = `sealer_z${string}`;
export type Sealed<T> = `sealed_U${string}` & { __type: T };

export type AgentSecret = `${SealerSecret}/${SignerSecret}`;

export const textEncoder = new TextEncoder();
export const textDecoder = new TextDecoder();

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export abstract class CryptoProvider<Blake3State = any> {
  randomBytes(length: number): Uint8Array {
    return randomBytes(length);
  }

  abstract newEd25519SigningKey(): Uint8Array;

  newRandomSigner(): SignerSecret {
    return `signerSecret_z${base58.encode(this.newEd25519SigningKey())}`;
  }

  abstract getSignerID(secret: SignerSecret): SignerID;

  abstract sign(secret: SignerSecret, message: JsonValue): Signature;

  abstract verify(
    signature: Signature,
    message: JsonValue,
    id: SignerID,
  ): boolean;

  abstract newX25519StaticSecret(): Uint8Array;

  newRandomSealer(): SealerSecret {
    return `sealerSecret_z${base58.encode(this.newX25519StaticSecret())}`;
  }

  abstract getSealerID(secret: SealerSecret): SealerID;

  newRandomAgentSecret(): AgentSecret {
    return `${this.newRandomSealer()}/${this.newRandomSigner()}`;
  }

  agentIdCache = new Map<string, AgentID>();
  getAgentID(secret: AgentSecret): AgentID {
    const cacheKey = secret;
    let agentId = this.agentIdCache.get(cacheKey);
    if (!agentId) {
      const [sealerSecret, signerSecret] = secret.split("/") as [
        SealerSecret,
        SignerSecret,
      ];
      agentId = `${this.getSealerID(sealerSecret)}/${this.getSignerID(signerSecret)}`;
      this.agentIdCache.set(cacheKey, agentId);
    }
    return agentId;
  }

  getAgentSignerID(agentId: AgentID): SignerID {
    return agentId.split("/")[1] as SignerID;
  }

  getAgentSignerSecret(agentSecret: AgentSecret): SignerSecret {
    return agentSecret.split("/")[1] as SignerSecret;
  }

  getAgentSealerID(agentId: AgentID): SealerID {
    return agentId.split("/")[0] as SealerID;
  }

  getAgentSealerSecret(agentSecret: AgentSecret): SealerSecret {
    return agentSecret.split("/")[0] as SealerSecret;
  }

  abstract blake3HashOnce(data: Uint8Array): Uint8Array;
  abstract blake3HashOnceWithContext(
    data: Uint8Array,
    { context }: { context: Uint8Array },
  ): Uint8Array;

  secureHash(value: JsonValue): Hash {
    return `hash_z${base58.encode(
      this.blake3HashOnce(textEncoder.encode(stableStringify(value))),
    )}`;
  }

  shortHash(value: JsonValue): ShortHash {
    return `shortHash_z${base58.encode(
      this.blake3HashOnce(textEncoder.encode(stableStringify(value))).slice(
        0,
        shortHashLength,
      ),
    )}`;
  }

  abstract encrypt<T extends JsonValue, N extends JsonValue>(
    value: T,
    keySecret: KeySecret,
    nOnceMaterial: N,
  ): Encrypted<T, N>;

  abstract decryptRaw<T extends JsonValue, N extends JsonValue>(
    encrypted: Encrypted<T, N>,
    keySecret: KeySecret,
    nOnceMaterial: N,
  ): Stringified<T>;

  decrypt<T extends JsonValue, N extends JsonValue>(
    encrypted: Encrypted<T, N>,
    keySecret: KeySecret,
    nOnceMaterial: N,
  ): T | undefined {
    try {
      return parseJSON(this.decryptRaw(encrypted, keySecret, nOnceMaterial));
    } catch (e) {
      logger.error("Decryption error", { err: e });
      return undefined;
    }
  }

  newRandomKeySecret(): { secret: KeySecret; id: KeyID } {
    return {
      secret: `keySecret_z${base58.encode(this.randomBytes(32))}`,
      id: `key_z${base58.encode(this.randomBytes(12))}`,
    };
  }

  encryptKeySecret(keys: {
    toEncrypt: { id: KeyID; secret: KeySecret };
    encrypting: { id: KeyID; secret: KeySecret };
  }): {
    encryptedID: KeyID;
    encryptingID: KeyID;
    encrypted: Encrypted<
      KeySecret,
      { encryptedID: KeyID; encryptingID: KeyID }
    >;
  } {
    const nOnceMaterial = {
      encryptedID: keys.toEncrypt.id,
      encryptingID: keys.encrypting.id,
    };

    return {
      encryptedID: keys.toEncrypt.id,
      encryptingID: keys.encrypting.id,
      encrypted: this.encrypt(
        keys.toEncrypt.secret,
        keys.encrypting.secret,
        nOnceMaterial,
      ),
    };
  }

  decryptKeySecret(
    encryptedInfo: {
      encryptedID: KeyID;
      encryptingID: KeyID;
      encrypted: Encrypted<
        KeySecret,
        { encryptedID: KeyID; encryptingID: KeyID }
      >;
    },
    sealingSecret: KeySecret,
  ): KeySecret | undefined {
    const nOnceMaterial = {
      encryptedID: encryptedInfo.encryptedID,
      encryptingID: encryptedInfo.encryptingID,
    };

    return this.decrypt(encryptedInfo.encrypted, sealingSecret, nOnceMaterial);
  }

  abstract seal<T extends JsonValue>({
    message,
    from,
    to,
    nOnceMaterial,
  }: {
    message: T;
    from: SealerSecret;
    to: SealerID;
    nOnceMaterial: { in: RawCoID; tx: TransactionID };
  }): Sealed<T>;

  abstract unseal<T extends JsonValue>(
    sealed: Sealed<T>,
    sealer: SealerSecret,
    from: SealerID,
    nOnceMaterial: { in: RawCoID; tx: TransactionID },
  ): T | undefined;

  uniquenessForHeader(): `z${string}` {
    return `z${base58.encode(this.randomBytes(12))}`;
  }

  createdNowUnique(): {
    createdAt: `2${string}`;
    uniqueness: `z${string}`;
  } {
    const createdAt = new Date().toISOString() as `2${string}`;
    return {
      createdAt,
      uniqueness: this.uniquenessForHeader(),
    };
  }

  newRandomSecretSeed(): Uint8Array {
    return this.randomBytes(secretSeedLength);
  }

  agentSecretFromSecretSeed(secretSeed: Uint8Array): AgentSecret {
    if (secretSeed.length !== secretSeedLength) {
      throw new Error(`Secret seed needs to be ${secretSeedLength} bytes long`);
    }

    return `sealerSecret_z${base58.encode(
      this.blake3HashOnceWithContext(secretSeed, {
        context: textEncoder.encode("seal"),
      }),
    )}/signerSecret_z${base58.encode(
      this.blake3HashOnceWithContext(secretSeed, {
        context: textEncoder.encode("sign"),
      }),
    )}`;
  }

  newRandomSessionID(accountID: RawAccountID | AgentID): SessionID {
    return `${accountID}_session_z${base58.encode(this.randomBytes(8))}`;
  }

  abstract createSessionLog(
    coID: RawCoID,
    sessionID: SessionID,
    signerID?: SignerID,
  ): SessionLogImpl;
}

export type Hash = `hash_z${string}`;
export type ShortHash = `shortHash_z${string}`;
export const shortHashLength = 19;

export type Encrypted<
  T extends JsonValue,
  N extends JsonValue,
> = `encrypted_U${string}` & { __type: T; __nOnceMaterial: N };

export type KeySecret = `keySecret_z${string}`;
export type KeyID = `key_z${string}`;

export const secretSeedLength = 32;

export interface SessionLogImpl {
  clone(): SessionLogImpl;
  tryAdd(
    transactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean,
  ): void;
  addNewPrivateTransaction(
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    keyID: KeyID,
    keySecret: KeySecret,
    madeAt: number,
    meta: JsonObject | undefined,
  ): { signature: Signature; transaction: PrivateTransaction };
  addNewTrustingTransaction(
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    madeAt: number,
    meta: JsonObject | undefined,
  ): { signature: Signature; transaction: TrustingTransaction };
  decryptNextTransactionChangesJson(
    tx_index: number,
    key_secret: KeySecret,
  ): string;
  free(): void;
  decryptNextTransactionMetaJson(
    tx_index: number,
    key_secret: KeySecret,
  ): string | undefined;
}
