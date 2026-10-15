// The durable, top-level storage compatibility boundary.
import { type Bytes, utf8, fromUtf8, equalBytes } from "../../support/bytes.js";
import { cmpStr } from "../../support/collections.js";
import { StorageError } from "./index.js";

export const STORAGE_EPOCH_1 = 1;
const MAGIC = utf8("JSM1");

export const GROOVE_EPOCH_1_CODECS = [
  "groove.large-value.v2",
  "groove.ordered-chunk-storage.v2",
  "groove.ordered-kv.v1",
];
export const GROOVE_DURABLE_INDEX_V2_CODEC = "groove.durable-index.v2";

const invalid = (message: string) => StorageError.invalidLayout(message);
const sortedUnique = (s: Iterable<string>) => [...new Set(s)].sort(cmpStr);

/** Closed, deterministic set of persistent payload codecs required by one root. */
export class StorageCodecProfile {
  private constructor(readonly requiredCodecs: string[]) {}

  static create(codecs: Iterable<string>): StorageCodecProfile {
    const set = new Set<string>();
    for (const codec of codecs) {
      validId("codec ID", codec);
      if (set.has(codec)) throw invalid("codec profile has duplicate codec ID");
      set.add(codec);
    }
    if (!set.size) throw invalid("codec profile is empty");
    requireGrooveEpoch1Codecs(set);
    return new StorageCodecProfile(sortedUnique(set));
  }
  static grooveEpoch1(): StorageCodecProfile {
    return StorageCodecProfile.create(GROOVE_EPOCH_1_CODECS);
  }
  withAdditionalCodecs(codecs: Iterable<string>): StorageCodecProfile {
    return StorageCodecProfile.create([...this.requiredCodecs, ...codecs]);
  }
  codecIds(): string[] {
    return this.requiredCodecs;
  }
}

export interface AdapterFormat {
  id: string;
  version: number;
}

export type ManifestOpenReceipt = "FreshEpoch1" | "ExistingEpoch1";

/** The complete decoding-relevant contract for one durable storage root. */
export class StorageEpochManifest {
  constructor(
    readonly epoch: number,
    readonly adapter: AdapterFormat,
    readonly requiredCodecs: string[],
    readonly parameters: Map<string, Bytes>,
  ) {}

  static epoch1(
    adapterId: string,
    adapterVersion: number,
    parameters: Map<string, Bytes>,
    profile = StorageCodecProfile.grooveEpoch1(),
  ) {
    const m = new StorageEpochManifest(
      STORAGE_EPOCH_1,
      { id: adapterId, version: adapterVersion },
      profile.requiredCodecs.slice(),
      parameters,
    );
    m.validate();
    return m;
  }

  encode(): Bytes {
    this.validate();
    return this.encodeStructure();
  }

  private encodeStructure(): Bytes {
    const out: number[] = [
      ...MAGIC,
      this.epoch >> 8,
      this.epoch & 0xff,
      this.adapter.version >> 8,
      this.adapter.version & 0xff,
    ];
    putString(out, this.adapter.id);
    putCount(out, this.requiredCodecs.length);
    for (const codec of this.requiredCodecs) putString(out, codec);
    putCount(out, this.parameters.size);
    for (const key of [...this.parameters.keys()].sort(cmpStr)) {
      const value = this.parameters.get(key)!;
      putString(out, key);
      if (value.length > 0xffff) throw invalid("parameter too long");
      out.push(value.length >> 8, value.length & 0xff, ...value);
    }
    return Uint8Array.from(out);
  }

  static decode(bytes: Bytes): StorageEpochManifest {
    const m = StorageEpochManifest.decodeStructure(bytes);
    requireGrooveEpoch1Codecs(new Set(m.requiredCodecs));
    return m;
  }

  private static decodeStructure(bytes: Bytes): StorageEpochManifest {
    const input = new ManifestReader(bytes);
    if (!equalBytes(input.take(4), MAGIC)) throw invalid("manifest magic is invalid");
    const epoch = input.u16();
    const version = input.u16();
    const id = input.string();
    const codecs = new Set<string>();
    for (let n = input.u8(); n > 0; n--) {
      const codec = input.string();
      if (codecs.has(codec)) throw invalid("manifest has duplicate codec ID");
      codecs.add(codec);
    }
    const parameters = new Map<string, Bytes>();
    for (let n = input.u8(); n > 0; n--) {
      const key = input.string();
      const value = input.take(input.u16()).slice();
      if (parameters.has(key)) throw invalid("manifest has duplicate parameter ID");
      parameters.set(key, value);
    }
    if (input.remaining) throw invalid("manifest has trailing bytes");
    const m = new StorageEpochManifest(epoch, { id, version }, sortedUnique(codecs), parameters);
    m.validateStructure();
    if (!equalBytes(m.encodeStructure(), bytes)) throw invalid("manifest is noncanonical");
    return m;
  }

  admitExisting(bytes: Bytes): ManifestOpenReceipt {
    const found = StorageEpochManifest.decodeStructure(bytes);
    if (found.epoch !== this.epoch) throw invalid("unsupported storage epoch");
    const mine = new Set(this.requiredCodecs);
    const theirs = new Set(found.requiredCodecs);
    const missing = this.requiredCodecs.filter((c) => !theirs.has(c));
    const unknown = found.requiredCodecs.filter((c) => !mine.has(c));
    if (missing.length || unknown.length)
      throw StorageError.unsupportedCodecs(found.epoch, missing, unknown);
    if (!equalBytes(found.encodeStructure(), this.encodeStructure()))
      throw invalid("storage manifest is inconsistent with this adapter");
    return "ExistingEpoch1";
  }

  admit(existing: Bytes | undefined): ManifestOpenReceipt {
    return existing ? this.admitExisting(existing) : "FreshEpoch1";
  }

  private validate() {
    this.validateStructure();
    requireGrooveEpoch1Codecs(new Set(this.requiredCodecs));
  }

  private validateStructure() {
    if (this.epoch !== STORAGE_EPOCH_1) throw invalid("unsupported storage epoch");
    validId("adapter ID", this.adapter.id);
    if (this.adapter.version === 0) throw invalid("adapter format version is zero");
    for (const codec of this.requiredCodecs) validId("codec ID", codec);
    if (!this.requiredCodecs.length) throw invalid("manifest codec profile is empty");
    for (const key of this.parameters.keys()) validId("parameter ID", key);
  }
}

function requireGrooveEpoch1Codecs(codecs: Set<string>) {
  for (const required of GROOVE_EPOCH_1_CODECS)
    if (!codecs.has(required))
      throw invalid("codec profile omits mandatory Groove epoch-one codec");
}

export interface StorageMigration {
  sourceEpoch(): number;
  targetEpoch(): number;
}
export interface MigrationJournal {
  sourceEpoch(): number;
  targetEpoch(): number;
}

export class MigrationRegistry {
  constructor(private readonly steps: StorageMigration[] = []) {
    const sources = new Set<number>();
    for (const step of steps) {
      if (sources.has(step.sourceEpoch()))
        throw invalid("multiple migrations have the same source epoch");
      sources.add(step.sourceEpoch());
      if (step.sourceEpoch() >= 0xffff) throw invalid("migration epoch overflow");
      if (step.targetEpoch() !== step.sourceEpoch() + 1) throw invalid("migration is not adjacent");
    }
  }
  adjacent(from: number): StorageMigration | undefined {
    return this.steps.find((s) => s.sourceEpoch() === from);
  }
}

function validId(kind: string, value: string) {
  if (!value.length || value.length > 255 || !/^[a-z0-9._-]+$/.test(value))
    throw invalid(`invalid ${kind}`);
}
function putCount(out: number[], count: number) {
  if (count > 255) throw invalid("too many manifest entries");
  out.push(count);
}
function putString(out: number[], value: string) {
  validId("manifest ID", value);
  out.push(value.length, ...utf8(value));
}

class ManifestReader {
  private pos = 0;
  constructor(private readonly buf: Bytes) {}
  get remaining() {
    return this.buf.length - this.pos;
  }
  take(n: number): Bytes {
    if (this.remaining < n) throw invalid("manifest is truncated");
    return this.buf.subarray(this.pos, (this.pos += n));
  }
  u8() {
    return this.take(1)[0];
  }
  u16() {
    const b = this.take(2);
    return (b[0] << 8) | b[1];
  }
  string() {
    const raw = this.take(this.u8());
    let value: string;
    try {
      value = fromUtf8(raw);
    } catch {
      throw invalid("manifest ID is not UTF-8");
    }
    validId("manifest ID", value);
    return value;
  }
}
