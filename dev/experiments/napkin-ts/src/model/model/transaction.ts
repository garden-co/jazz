// Neutral transaction vocabulary shared by public bindings and core glue.
import { blake3 } from "@noble/hashes/blake3";
import { toHex, fromHex, equalBytes, type Bytes } from "../../support/bytes.js";
import { uuidV7 } from "../../support/hash.js";
import { nodeUuidBytes } from "../../_extern/model-core.js";
import type { TxId } from "../tx.js";

class Id16 {
  constructor(readonly bytes: Bytes) {}
  toString(): string {
    return toHex(this.bytes);
  }
  toJSON(): string {
    return this.toString();
  }
  equals(o: Id16): boolean {
    return equalBytes(this.bytes, o.bytes);
  }
  protected static parse(raw: string, kind: string): Bytes {
    let bytes: Bytes;
    try {
      bytes = fromHex(raw);
    } catch (err) {
      throw new Error(`invalid ${kind} hex: ${(err as Error).message}`);
    }
    if (bytes.length !== 16) throw new Error(`expected 16-byte ${kind}, got ${bytes.length}`);
    return bytes;
  }
}

/** Coordination-free identity for mutable, runtime-local work before commit. */
export class OpenTransactionId extends Id16 {
  static create(): OpenTransactionId {
    return new OpenTransactionId(uuidV7());
  }
  static fromStr(raw: string): OpenTransactionId {
    return new OpenTransactionId(Id16.parse(raw, "open transaction id"));
  }
  debug(): string {
    return `OpenTransactionId(${this})`;
  }
}

/** Identity of an immutable committed transaction. */
export class TransactionId extends Id16 {
  static fromStr(raw: string): TransactionId {
    return new TransactionId(Id16.parse(raw, "transaction id"));
  }
  debug(): string {
    return `TransactionId(${this})`;
  }
  /** Domain-separated digest of the core causal identity. */
  static fromCommittedTx(tx: TxId): TransactionId {
    const input = new Uint8Array(24);
    new DataView(input.buffer).setBigUint64(0, BigInt(tx.time), false);
    input.set(nodeUuidBytes(tx.node), 8);
    const digest = blake3(input, { context: "jazz committed batch id v1" } as any);
    return new TransactionId(digest.slice(0, 16));
  }
}
