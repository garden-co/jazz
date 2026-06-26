export type ValueType = { tag: number; inner?: ValueType };
export type DescriptorField = { name?: string; valueType: ValueType };
export type AbiRow = { rowId: Uint8Array; deleted: boolean; raw: Uint8Array };
export type AbiRowBatch = { table: string; descriptor: DescriptorField[]; rows: AbiRow[] };

export function readAbiRowBatch(reader: PostcardReader): AbiRowBatch {
  return {
    table: reader.string(),
    descriptor: readDescriptor(reader),
    rows: reader.readVec((rowReader) => ({
      rowId: rowReader.bytes(),
      deleted: rowReader.bool(),
      raw: rowReader.bytes(),
    })),
  };
}

function readDescriptor(reader: PostcardReader): DescriptorField[] {
  return reader.readVec((fieldReader) => ({
    name: fieldReader.option((nameReader) => nameReader.string()),
    valueType: readValueType(fieldReader),
  }));
}

function readValueType(reader: PostcardReader): ValueType {
  const tag = reader.u64();
  if (tag === 12 || tag === 13) {
    return { tag, inner: readValueType(reader) };
  }
  if (tag === 10) {
    const members = reader.readVec(readValueType);
    return { tag, inner: members[0] };
  }
  return { tag };
}

export function openConfig(node: Uint8Array, author: Uint8Array, sourceId?: number, historyComplete = false): Uint8Array {
  const writer = new PostcardWriter();
  writer.bytes(node);
  writer.bytes(author);
  if (sourceId == null) {
    writer.none();
  } else {
    writer.some((value) => value.u64(sourceId));
  }
  writer.bool(historyComplete);
  return writer.finish();
}

export function queryFromTable(table: string): Uint8Array {
  const writer = new PostcardWriter();
  writer.string(table);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.none();
  writer.vec(() => undefined, 0);
  writer.none();
  writer.none();
  writer.u64(0);
  return writer.finish();
}

export class PostcardWriter {
  private chunks: number[] = [];

  finish(): Uint8Array {
    return Uint8Array.from(this.chunks);
  }

  u64(value: number): void {
    let remaining = value;
    do {
      let byte = remaining & 0x7f;
      remaining = Math.floor(remaining / 128);
      if (remaining !== 0) byte |= 0x80;
      this.chunks.push(byte);
    } while (remaining !== 0);
  }

  bool(value: boolean): void {
    this.chunks.push(value ? 1 : 0);
  }

  string(value: string): void {
    this.bytes(new TextEncoder().encode(value));
  }

  bytes(value: Uint8Array, withLength = true): void {
    if (withLength) this.u64(value.length);
    this.chunks.push(...value);
  }

  vec(writeItem: (writer: PostcardWriter, index: number) => void, length: number): void {
    this.u64(length);
    for (let index = 0; index < length; index += 1) {
      writeItem(this, index);
    }
  }

  none(): void {
    this.chunks.push(0);
  }

  some(writeValue: (writer: PostcardWriter) => void): void {
    this.chunks.push(1);
    writeValue(this);
  }

  enumUnit(index: number): void {
    this.u64(index);
  }
}

export class PostcardReader {
  private offset = 0;

  constructor(private readonly bytesValue: Uint8Array) {}

  u64(): number {
    let result = 0;
    let shift = 0;
    while (true) {
      const byte = this.readByte();
      result += (byte & 0x7f) * 2 ** shift;
      if ((byte & 0x80) === 0) return result;
      shift += 7;
    }
  }

  string(): string {
    return new TextDecoder().decode(this.bytes());
  }

  bool(): boolean {
    const tag = this.readByte();
    if (tag === 0) return false;
    if (tag === 1) return true;
    throw new Error(`invalid bool tag ${tag}`);
  }

  bytes(withLength = true): Uint8Array {
    const length = withLength ? this.u64() : 16;
    const end = this.offset + length;
    if (end > this.bytesValue.length) throw new Error("postcard bytes overflow");
    const value = this.bytesValue.subarray(this.offset, end);
    this.offset = end;
    return value;
  }

  option<T>(readValue: (reader: PostcardReader) => T): T | undefined {
    const tag = this.readByte();
    if (tag === 0) return undefined;
    if (tag !== 1) throw new Error(`invalid option tag ${tag}`);
    return readValue(this);
  }

  readVec<T>(readItem: (reader: PostcardReader) => T): T[] {
    const length = this.u64();
    return Array.from({ length }, () => readItem(this));
  }

  private readByte(): number {
    if (this.offset >= this.bytesValue.length) throw new Error("postcard eof");
    return this.bytesValue[this.offset++];
  }
}

export function assertBytes(value: unknown, label: string): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
  }
  if (Array.isArray(value) && value.every((byte) => Number.isInteger(byte) && byte >= 0 && byte <= 255)) {
    return Uint8Array.from(value);
  }
  throw new Error(`expected ${label} to be bytes`);
}
