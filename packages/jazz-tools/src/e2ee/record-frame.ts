/** Length-delimited lifecycle transcript fields, each prefixed with u32be. */
export function frameCryptoRecord(parts: Uint8Array[]): Uint8Array {
  const length = parts.reduce((size, part) => {
    if (part.length > 0xffffffff) throw new Error("Invalid E2EE record field length");
    return size + 4 + part.length;
  }, 0);
  const result = new Uint8Array(length);
  const view = new DataView(result.buffer);
  let offset = 0;
  for (const part of parts) {
    view.setUint32(offset, part.length, false);
    result.set(part, offset + 4);
    offset += part.length + 4;
  }
  return result;
}
