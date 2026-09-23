const byteHex = Array.from({ length: 256 }, (_, byte) => byte.toString(16).padStart(2, "0"));

export function parseUuid(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid ${value}`);
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

export function formatUuid(bytes: Uint8Array): string {
  return (
    byteHex[bytes[0]!] +
    byteHex[bytes[1]!] +
    byteHex[bytes[2]!] +
    byteHex[bytes[3]!] +
    "-" +
    byteHex[bytes[4]!] +
    byteHex[bytes[5]!] +
    "-" +
    byteHex[bytes[6]!] +
    byteHex[bytes[7]!] +
    "-" +
    byteHex[bytes[8]!] +
    byteHex[bytes[9]!] +
    "-" +
    byteHex[bytes[10]!] +
    byteHex[bytes[11]!] +
    byteHex[bytes[12]!] +
    byteHex[bytes[13]!] +
    byteHex[bytes[14]!] +
    byteHex[bytes[15]!]
  );
}
