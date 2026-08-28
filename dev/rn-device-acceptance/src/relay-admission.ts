type NativeRelayCapability = Uint8Array;
type NativeRelayExecutor = { execute(commandBase64: string): Promise<string> };

/** Probe is public link diagnostics; a successful Open is the admission proof. */
export async function proveAdmittedRelay(
  executor: NativeRelayExecutor,
  capability: NativeRelayCapability,
): Promise<void> {
  const openedRelay = decodeOpened(await executor.execute(encodeOpen(capability)));
  const attachedClient = decodeAttached(await executor.execute(encodeAttach(openedRelay)));
  try {
    const response = Uint8Array.from(globalThis.atob(await executor.execute("AA==")), (byte) =>
      byte.charCodeAt(0),
    );
    if (response.length !== 2 || response[0] !== 0 || response[1] !== 3)
      throw new Error("installed Jazz relay returned an unexpected ABI probe response");
  } finally {
    if (!decodeClosed(await executor.execute(encodeCloseClient(attachedClient))))
      throw new Error("native relay did not close the admitted UI peer");
    if (!decodeClosed(await executor.execute(encodeCloseRelay(openedRelay))))
      throw new Error("native relay did not close the admitted scope");
  }
}

function encodeOpen(capability: Uint8Array): string {
  if (capability.byteLength !== 32)
    throw new Error("admission capability must be exactly 32 bytes");
  return encode([1, 3, 3, ...capability]); // postcard Open, ABI min/max, opaque capability
}
function encodeCloseRelay(relay: bigint): string {
  return encode([4, ...varint(relay)]);
}
function encodeAttach(relay: bigint): string {
  return encode([2, ...varint(relay)]);
}
function encodeCloseClient(client: bigint): string {
  return encode([3, ...varint(client)]);
}
function encode(bytes: number[]): string {
  return globalThis.btoa(String.fromCharCode(...bytes));
}
function varint(value: bigint): number[] {
  const bytes: number[] = [];
  while (value >= 0x80n) {
    bytes.push(Number((value & 0x7fn) | 0x80n));
    value >>= 7n;
  }
  bytes.push(Number(value));
  return bytes;
}
function decodeOpened(encoded: string): bigint {
  const bytes = bytesOf(encoded);
  if (bytes[0] !== 1) throw new Error("native relay did not open the admitted scope");
  const [relay, offset] = readVarint(bytes, 1);
  if (offset !== bytes.length) throw new Error("native relay returned malformed Open response");
  return relay;
}
function decodeClosed(encoded: string): boolean {
  const bytes = bytesOf(encoded);
  return bytes.length === 2 && bytes[0] === 3 && bytes[1] === 1;
}
function decodeAttached(encoded: string): bigint {
  const bytes = bytesOf(encoded);
  if (bytes[0] !== 2)
    throw new Error("native relay did not attach a UI peer to the admitted scope");
  const [client, offset] = readVarint(bytes, 1);
  if (offset !== bytes.length) throw new Error("native relay returned malformed Attach response");
  return client;
}
function bytesOf(encoded: string): Uint8Array {
  return Uint8Array.from(globalThis.atob(encoded), (byte) => byte.charCodeAt(0));
}
function readVarint(bytes: Uint8Array, start: number): [bigint, number] {
  let value = 0n,
    shift = 0n,
    offset = start;
  while (offset < bytes.length) {
    const byte = bytes[offset++]!;
    value |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return [value, offset];
    shift += 7n;
  }
  throw new Error("native relay returned malformed Open response");
}
