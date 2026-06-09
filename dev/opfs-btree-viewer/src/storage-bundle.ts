export const STORAGE_BUNDLE_MAGIC = "JAZZOPFSBUNDLE1";
export const STORAGE_BUNDLE_VERSION = 1;
export const STORAGE_BUNDLE_MIME_TYPE = "application/vnd.jazz.opfs-btree-bundle";
export const STORAGE_BUNDLE_FILE_EXTENSION = ".jazz-opfs-bundle";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const magicBytes = textEncoder.encode(STORAGE_BUNDLE_MAGIC);

export interface StorageBundleFile {
  path: string;
  bytes: Uint8Array;
}

export interface StorageBundle {
  version: number;
  metadata: unknown;
  files: StorageBundleFile[];
}

export interface EncodeStorageBundleInput {
  metadata?: unknown;
  files: StorageBundleFile[];
}

class StorageBundleReader {
  private readonly bytes: Uint8Array;
  private readonly view: DataView;
  private offset = 0;

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  readBytes(length: number, label: string): Uint8Array {
    if (length < 0 || this.offset + length > this.bytes.byteLength) {
      throw new Error(`Invalid storage bundle: truncated ${label}`);
    }
    const out = this.bytes.slice(this.offset, this.offset + length);
    this.offset += length;
    return out;
  }

  readU32(label: string): number {
    if (this.offset + 4 > this.bytes.byteLength) {
      throw new Error(`Invalid storage bundle: truncated ${label}`);
    }
    const value = this.view.getUint32(this.offset, true);
    this.offset += 4;
    return value;
  }

  readU64(label: string): number {
    if (this.offset + 8 > this.bytes.byteLength) {
      throw new Error(`Invalid storage bundle: truncated ${label}`);
    }
    const value = this.view.getBigUint64(this.offset, true);
    this.offset += 8;
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error(`Invalid storage bundle: ${label} is too large`);
    }
    return Number(value);
  }

  assertDone(): void {
    if (this.offset !== this.bytes.byteLength) {
      throw new Error("Invalid storage bundle: trailing bytes");
    }
  }
}

export function decodeStorageBundle(bytes: Uint8Array): StorageBundle {
  const reader = new StorageBundleReader(bytes);
  const actualMagic = reader.readBytes(magicBytes.byteLength, "magic");
  if (!bytesEqual(actualMagic, magicBytes)) {
    throw new Error("Invalid storage bundle: bad magic");
  }

  const version = reader.readU32("version");
  if (version !== STORAGE_BUNDLE_VERSION) {
    throw new Error(`Unsupported storage bundle version: ${version}`);
  }

  const metadataLength = reader.readU32("metadata length");
  const metadataBytes = reader.readBytes(metadataLength, "metadata");
  const metadataText = textDecoder.decode(metadataBytes);
  const metadata = metadataText.length > 0 ? JSON.parse(metadataText) : null;
  const fileCount = reader.readU32("file count");
  const files: StorageBundleFile[] = [];

  for (let index = 0; index < fileCount; index++) {
    const pathLength = reader.readU32(`file ${index} path length`);
    const path = textDecoder.decode(reader.readBytes(pathLength, `file ${index} path`));
    const byteLength = reader.readU64(`file ${index} byte length`);
    const fileBytes = reader.readBytes(byteLength, `file ${index} bytes`);
    files.push({ path, bytes: fileBytes });
  }

  reader.assertDone();
  return { version, metadata, files };
}

export function encodeStorageBundle(input: EncodeStorageBundleInput): Uint8Array {
  const metadataBytes = textEncoder.encode(JSON.stringify(input.metadata ?? null));
  const chunks: Uint8Array[] = [
    magicBytes,
    u32(STORAGE_BUNDLE_VERSION),
    u32(metadataBytes.byteLength),
    metadataBytes,
    u32(input.files.length),
  ];

  for (const file of input.files) {
    const pathBytes = textEncoder.encode(file.path);
    chunks.push(u32(pathBytes.byteLength), pathBytes, u64(file.bytes.byteLength), file.bytes);
  }

  return concat(chunks);
}

function u32(value: number): Uint8Array {
  if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
    throw new Error(`Value does not fit in u32: ${value}`);
  }
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, value, true);
  return bytes;
}

function u64(value: number): Uint8Array {
  if (!Number.isInteger(value) || value < 0 || value > Number.MAX_SAFE_INTEGER) {
    throw new Error(`Value does not fit in safe u64: ${value}`);
  }
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
  return bytes;
}

function concat(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index++) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}
