import { uuidV7 } from "../support/hash.js";
import { concatBytes, utf8, fromUtf8 } from "../support/bytes.js";
import type { Reader, Writer } from "../support/postcard.js";
import { uuidBytes, uuidFromBytes, readUuid, writeUuid } from "./ids.js";

/** Object ids are canonical UUID strings; string equality replaces interning. */
export type ObjectId = string;
export const newObjectId = (): ObjectId => uuidFromBytes(uuidV7());
export const writeObjectId = writeUuid;
export const readObjectId = readUuid;

export type UnionArm = [number, string];

const u32be = (n: number): Uint8Array => {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, n);
  return b;
};

/** Stable identity of one rendered query-output occurrence. */
export class OutputOccurrenceId {
  constructor(
    readonly root: ObjectId,
    readonly joined: ObjectId[] = [],
    readonly unionArms: UnionArm[] = [],
  ) {}

  static singleSource(root: ObjectId): OutputOccurrenceId {
    return new OutputOccurrenceId(root);
  }

  static withUnionArms(
    root: ObjectId,
    joined: ObjectId[],
    unionArms: UnionArm[],
  ): OutputOccurrenceId | undefined {
    const arms = [...unionArms].sort((a, b) => a[0] - b[0]);
    const valid =
      arms.every(([pos, label]) => pos <= joined.length && label.length > 0) &&
      arms.every((a, i) => i === 0 || arms[i - 1][0] !== a[0]);
    return valid ? new OutputOccurrenceId(root, joined, arms) : undefined;
  }

  hasTypedDiscriminators(): boolean {
    return this.unionArms.length > 0;
  }

  /** Canonical positional bytes for terminal-state keys and consolidation. */
  canonicalBytes(): Uint8Array {
    if (this.unionArms.length) return this.typedCanonicalBytes();
    return concatBytes(uuidBytes(this.root), ...this.joined.map(uuidBytes));
  }

  typedCanonicalBytes(): Uint8Array {
    const parts: Uint8Array[] = [uuidBytes(this.root), u32be(this.joined.length)];
    for (const id of this.joined) parts.push(uuidBytes(id));
    parts.push(u32be(this.unionArms.length));
    for (const [pos, label] of this.unionArms) {
      const l = utf8(label);
      parts.push(u32be(pos), u32be(l.length), l);
    }
    return concatBytes(...parts);
  }

  key(): string {
    return JSON.stringify([this.root, this.joined, this.unionArms]);
  }

  equalsObjectId(id: ObjectId): boolean {
    return this.joined.length === 0 && this.unionArms.length === 0 && this.root === id;
  }

  static compare(a: OutputOccurrenceId, b: OutputOccurrenceId): number {
    const ka = a.key();
    const kb = b.key();
    return ka < kb ? -1 : ka > kb ? 1 : 0;
  }

  /** Wire form is exactly `(root, joined)`; union arms are skipped. */
  static write(w: Writer, o: OutputOccurrenceId) {
    writeObjectId(w, o.root);
    w.seq(o.joined, writeObjectId);
  }
  static read(r: Reader): OutputOccurrenceId {
    return new OutputOccurrenceId(readObjectId(r), r.seq(readObjectId));
  }
}

const RESULT_KEY_WIRE_VERSION = 1;

/** Stable, opaque identity of one query result. */
export class ResultKey {
  constructor(readonly occurrence: OutputOccurrenceId) {}

  static fromObjectId(id: ObjectId): ResultKey {
    return new ResultKey(OutputOccurrenceId.singleSource(id));
  }

  static fromUnionOccurrence(
    root: ObjectId,
    joined: ObjectId[],
    unionArms: UnionArm[],
  ): ResultKey | undefined {
    const o = OutputOccurrenceId.withUnionArms(root, joined, unionArms);
    return o && new ResultKey(o);
  }

  rowId(): ObjectId | undefined {
    return this.occurrence.joined.length === 0 ? this.occurrence.root : undefined;
  }

  static write(w: Writer, k: ResultKey) {
    w.bytes(
      concatBytes(Uint8Array.of(RESULT_KEY_WIRE_VERSION), k.occurrence.typedCanonicalBytes()),
    );
  }

  static read(r: Reader): ResultKey {
    const encoded = r.bytes();
    if (encoded.length === 0) throw new Error("unsupported ResultKey version");
    if (encoded[0] !== RESULT_KEY_WIRE_VERSION)
      throw new Error("unsupported or malformed ResultKey identity");
    const key = decodeTypedResultKey(encoded.subarray(1));
    if (!key) throw new Error("malformed ResultKey V1 identity");
    return key;
  }
}

function decodeTypedResultKey(identity: Uint8Array): ResultKey | undefined {
  const MAX_JOINED_COMPONENTS = 256;
  const MAX_DISCRIMINATOR_BYTES = 4 * 1024;
  const view = new DataView(identity.buffer, identity.byteOffset, identity.byteLength);
  let cursor = 0;
  const take = (len: number): Uint8Array | undefined => {
    if (cursor + len > identity.length) return undefined;
    const v = identity.subarray(cursor, cursor + len);
    cursor += len;
    return v;
  };
  const u32 = (): number | undefined => {
    if (cursor + 4 > identity.length) return undefined;
    const v = view.getUint32(cursor);
    cursor += 4;
    return v;
  };
  const objectId = (): ObjectId | undefined => {
    const b = take(16);
    return b && uuidFromBytes(b);
  };
  const root = objectId();
  if (root === undefined) return undefined;
  const joinedCount = u32();
  if (joinedCount === undefined) return undefined;
  if (joinedCount > MAX_JOINED_COMPONENTS || identity.length - cursor < joinedCount * 16 + 4)
    return undefined;
  const joined: ObjectId[] = [];
  for (let i = 0; i < joinedCount; i++) joined.push(objectId()!);
  const discriminatorCount = u32();
  if (discriminatorCount === undefined || discriminatorCount > joinedCount + 1) return undefined;
  const arms: UnionArm[] = [];
  let previous: number | undefined;
  for (let i = 0; i < discriminatorCount; i++) {
    const position = u32();
    const len = u32();
    if (position === undefined || len === undefined) return undefined;
    if (len === 0 || len > MAX_DISCRIMINATOR_BYTES || len > identity.length - cursor)
      return undefined;
    if (position > joinedCount || (previous !== undefined && position <= previous))
      return undefined;
    let label: string;
    try {
      label = fromUtf8(take(len)!);
    } catch {
      return undefined;
    }
    previous = position;
    arms.push([position, label]);
  }
  if (cursor !== identity.length) return undefined;
  return ResultKey.fromUnionOccurrence(root, joined, arms);
}

/** Branch names are plain strings (interning is free in JS). */
export type BranchName = string;
export const writeBranchName = (w: Writer, b: BranchName) => w.str(b);
export const readBranchName = (r: Reader): BranchName => r.str();
