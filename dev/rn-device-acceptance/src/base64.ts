const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/**
 * Small platform-neutral codec for the opaque native relay boundary. Hermes
 * does not promise browser `atob`/`btoa`, so acceptance must not depend on
 * web globals before it can report its first native receipt.
 */
export function decodeBase64(value: string): Uint8Array {
  if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value))
    throw new Error("native relay returned malformed base64");

  const output = new Uint8Array(
    (value.length / 4) * 3 - (value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0),
  );
  let outputOffset = 0;
  for (let offset = 0; offset < value.length; offset += 4) {
    const first = alphabet.indexOf(value[offset]!);
    const second = alphabet.indexOf(value[offset + 1]!);
    const third = value[offset + 2] === "=" ? 0 : alphabet.indexOf(value[offset + 2]!);
    const fourth = value[offset + 3] === "=" ? 0 : alphabet.indexOf(value[offset + 3]!);
    const word = (first << 18) | (second << 12) | (third << 6) | fourth;
    output[outputOffset++] = word >> 16;
    if (value[offset + 2] !== "=") output[outputOffset++] = (word >> 8) & 0xff;
    if (value[offset + 3] !== "=") output[outputOffset++] = word & 0xff;
  }
  return output;
}

export function encodeBase64(bytes: Uint8Array): string {
  let output = "";
  for (let offset = 0; offset < bytes.length; offset += 3) {
    const first = bytes[offset]!;
    const second = bytes[offset + 1];
    const third = bytes[offset + 2];
    output += alphabet[first >> 2]!;
    output += alphabet[((first & 0x03) << 4) | ((second ?? 0) >> 4)]!;
    output += second === undefined ? "=" : alphabet[((second & 0x0f) << 2) | ((third ?? 0) >> 6)]!;
    output += third === undefined ? "=" : alphabet[third & 0x3f]!;
  }
  return output;
}
