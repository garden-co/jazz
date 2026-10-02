// Jazz Yjs Log v1: JYLG + uint32 BE version, then uint32 BE length + Yjs v1 update.
export const HEADER = new Uint8Array([0x4a, 0x59, 0x4c, 0x47, 0, 0, 0, 1]);

export function frame(update: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(4 + update.length);
  new DataView(bytes.buffer).setUint32(0, update.length);
  bytes.set(update, 4);
  return bytes;
}

export function updates(bytes: Uint8Array, initial: boolean): Uint8Array[] {
  if (initial && HEADER.some((byte, i) => bytes[i] !== byte)) {
    throw new Error("Unsupported Jazz Yjs log header");
  }
  const result: Uint8Array[] = [];
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = initial ? HEADER.length : 0;
  while (offset < bytes.length) {
    if (bytes.length - offset < 4) throw new Error("Truncated Yjs update length");
    const length = view.getUint32(offset);
    offset += 4;
    if (length === 0 || length > bytes.length - offset) {
      throw new Error("Invalid Yjs update length");
    }
    result.push(bytes.subarray(offset, offset + length));
    offset += length;
  }
  return result;
}
