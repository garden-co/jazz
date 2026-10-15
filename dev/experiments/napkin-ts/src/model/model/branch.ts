// Branch coordinates and snapshot references: canonical encodings of
// branch-column values and branch keys, and the dotted snapshot ref.
import { SortedMap, cmpStr } from "../../support/collections.js";
import { concatBytes, equalBytes, utf8, fromUtf8, type Bytes } from "../../support/bytes.js";
import {
  RecordDescriptor,
  valueTypeEquals,
  type GValue,
  type GValueType,
} from "../../_extern/model-core.js";
import type { NodeUuid, GlobalTime, TxTime } from "../../_extern/model-core.js";
import type { Snapshot, TxId } from "../tx.js";

export type BranchCodecErrorKind =
  | "InvalidEnvelope"
  | "UnsupportedType"
  | "TypeMismatch"
  | "Groove";
export class BranchCodecError extends Error {
  constructor(
    readonly kind: BranchCodecErrorKind,
    cause?: unknown,
  ) {
    super(
      kind === "InvalidEnvelope"
        ? "invalid branch codec envelope"
        : kind === "UnsupportedType"
          ? "unsupported branch column type"
          : kind === "TypeMismatch"
            ? "branch column encoding does not match its declared type"
            : `invalid Groove branch column payload: ${cause}`,
    );
  }
}
const invalid = () => new BranchCodecError("InvalidEnvelope");

const BRANCH_COLUMN_CODEC_VERSION = 1;
const BRANCH_COLUMN_SCALARS: GValueType[] = [
  { kind: "U8" },
  { kind: "U16" },
  { kind: "U32" },
  { kind: "U64" },
  { kind: "I32" },
  { kind: "I64" },
  { kind: "String" },
  { kind: "Uuid" },
];
const BRANCH_COLUMN_ENUM_TAG = 8;

function branchColumnTag(t: GValueType): number | undefined {
  if (t.kind === "EnumTag") return BRANCH_COLUMN_ENUM_TAG;
  const i = BRANCH_COLUMN_SCALARS.findIndex((s) => valueTypeEquals(s, t));
  return i < 0 ? undefined : i;
}

function grooveCall<T>(f: () => T): T {
  try {
    return f();
  } catch (e) {
    throw new BranchCodecError("Groove", e);
  }
}

/** Canonical wire/storage encoding of one branch-column value: [version, tag, ...payload]. */
export class BranchColumnValue {
  constructor(readonly bytes: Bytes) {}

  static fromValue(value: GValue): BranchColumnValue {
    switch (value.kind) {
      case "U8":
      case "U16":
      case "U32":
      case "U64":
      case "I32":
      case "I64":
      case "String":
      case "Uuid":
        return BranchColumnValue.encodeTyped(value, { kind: value.kind });
      case "EnumTag":
        return new BranchColumnValue(
          Uint8Array.of(BRANCH_COLUMN_CODEC_VERSION, BRANCH_COLUMN_ENUM_TAG, value.value),
        );
      default:
        return new BranchColumnValue(Uint8Array.of(BRANCH_COLUMN_CODEC_VERSION, 255));
    }
  }

  static encodeTyped(value: GValue, t: GValueType): BranchColumnValue {
    const tag = branchColumnTag(t);
    if (tag === undefined) throw new BranchCodecError("UnsupportedType");
    const d = new RecordDescriptor([["value", t]]);
    const payload: Bytes = grooveCall(() => d.create([value]));
    return new BranchColumnValue(
      concatBytes(Uint8Array.of(BRANCH_COLUMN_CODEC_VERSION, tag), payload),
    );
  }

  decode(): GValue {
    const [tag, payload] = this.envelope();
    if (tag === BRANCH_COLUMN_ENUM_TAG) {
      if (payload.length !== 1) throw invalid();
      return { kind: "EnumTag", value: payload[0] };
    }
    const t = BRANCH_COLUMN_SCALARS[tag];
    if (!t) throw invalid();
    return this.decodeAs(t);
  }

  decodeAs(t: GValueType): GValue {
    const [tag, payload] = this.envelope();
    if (branchColumnTag(t) !== tag) throw new BranchCodecError("TypeMismatch");
    const d = new RecordDescriptor([["value", t]]);
    const value = grooveCall(() => d.getIdx(payload, 0));
    if (
      !equalBytes(
        grooveCall(() => d.create([value])),
        payload,
      )
    )
      throw invalid();
    return value;
  }

  private envelope(): [number, Bytes] {
    if (this.bytes.length < 2 || this.bytes[0] !== BRANCH_COLUMN_CODEC_VERSION) throw invalid();
    return [this.bytes[1], this.bytes.subarray(2)];
  }
}

/** Canonical named values for one schema-wide branch selector. */
export class BranchSelector {
  readonly values = new SortedMap<string, BranchColumnValue>(cmpStr);
  constructor(values: Iterable<[string, GValue]> = []) {
    for (const [name, v] of values) this.values.set(name, BranchColumnValue.fromValue(v));
  }
}

const BRANCH_KEY_CODEC_VERSION = 1;

function putLen(out: number[], len: number) {
  if (len > 0xffffffff) throw new Error("branch codec component exceeds u32");
  out.push(len & 0xff, (len >>> 8) & 0xff, (len >>> 16) & 0xff, (len >>> 24) & 0xff);
}

/** Exact, table-projected branch coordinate carried by every row version. */
export class BranchKey {
  constructor(readonly values: [string, BranchColumnValue][] = []) {}

  tryCanonicalBytes(): Bytes {
    if (!this.isCanonical()) throw invalid();
    const out: number[] = [BRANCH_KEY_CODEC_VERSION];
    putLen(out, this.values.length);
    for (const [name, value] of this.values) {
      const n = utf8(name);
      putLen(out, n.length);
      out.push(...n);
      putLen(out, value.bytes.length);
      out.push(...value.bytes);
    }
    return Uint8Array.from(out);
  }

  canonicalBytes(): Bytes {
    return this.tryCanonicalBytes();
  }

  static fromCanonicalBytes(bytes: Bytes): BranchKey {
    if (bytes[0] !== BRANCH_KEY_CODEC_VERSION) throw invalid();
    let pos = 1;
    const take = (len: number) => {
      if (pos + len > bytes.length) throw invalid();
      const s = bytes.subarray(pos, pos + len);
      pos += len;
      return s;
    };
    const takeLen = () => new DataView(take(4).slice().buffer).getUint32(0, true);
    const count = takeLen();
    if (count > Math.floor((bytes.length - pos) / 8)) throw invalid();
    const values: [string, BranchColumnValue][] = [];
    for (let i = 0; i < count; i++) {
      let name: string;
      try {
        name = fromUtf8(take(takeLen()));
      } catch {
        throw invalid();
      }
      if (!name) throw invalid();
      const value = new BranchColumnValue(take(takeLen()).slice());
      value.decode();
      const prev = values[values.length - 1];
      if (prev && prev[0] >= name) throw invalid();
      values.push([name, value]);
    }
    if (pos !== bytes.length) throw invalid();
    return new BranchKey(values);
  }

  isCanonical(): boolean {
    for (let i = 1; i < this.values.length; i++)
      if (!(this.values[i - 1][0] < this.values[i][0])) return false;
    return this.values.every(([name, v]) => {
      if (!name) return false;
      try {
        v.decode();
        return true;
      } catch {
        return false;
      }
    });
  }
}

/** Dotted snapshot ref used by historic read views. */
export type SnapshotRef = {
  owner: NodeUuid;
  global_base: GlobalTime;
  local_base: TxTime;
  dots: TxId[];
};

export const snapshotRefFromSnapshot = (s: Snapshot): SnapshotRef => ({
  owner: s.owner,
  global_base: s.global_base,
  local_base: s.local_base,
  dots: s.dots,
});

/** Optional base composed underneath the live head of a branch view. */
export type BranchViewBase =
  | { kind: "Current"; branch: BranchSelector }
  | { kind: "Snapshot"; branch: BranchSelector; snapshot: SnapshotRef };
export const BranchViewBases = {
  current: (branch: BranchSelector): BranchViewBase => ({ kind: "Current", branch }),
  snapshot: (branch: BranchSelector, snapshot: SnapshotRef): BranchViewBase => ({
    kind: "Snapshot",
    branch,
    snapshot,
  }),
};
