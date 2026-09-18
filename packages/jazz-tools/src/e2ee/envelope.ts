/** Stable identifier of an installed cryptographic adapter and its wire format. */
export type CryptoMechanism = Readonly<{ id: string; version: number }>;

function validateMechanism({ id, version }: CryptoMechanism): void {
  if (
    !/^[a-z0-9.-]{1,64}$/.test(id) ||
    !Number.isInteger(version) ||
    version < 1 ||
    version > 0xffffffff
  ) {
    throw new Error("Invalid E2EE mechanism");
  }
}

/** Encode the common header specified in E2EE_CRYPTO_FORMAT.md, not plaintext. */
export function encodeEnvelope(mechanism: CryptoMechanism, payload: Uint8Array): Uint8Array {
  validateMechanism(mechanism);
  const id = new TextEncoder().encode(mechanism.id);
  const result = new Uint8Array(10 + id.length + payload.length);
  result.set([74, 69, 50, 69, 1, id.length]);
  result.set(id, 6);
  new DataView(result.buffer).setUint32(6 + id.length, mechanism.version, false);
  result.set(payload, 10 + id.length);
  return result;
}

/** Validate routing metadata. The adapter must still authenticate the payload. */
export function decodeEnvelope(mechanism: CryptoMechanism, envelope: Uint8Array): Uint8Array {
  validateMechanism(mechanism);
  if (
    envelope.length < 11 ||
    envelope[0] !== 74 ||
    envelope[1] !== 69 ||
    envelope[2] !== 50 ||
    envelope[3] !== 69 ||
    envelope[4] !== 1
  ) {
    throw new Error("Invalid E2EE envelope");
  }
  const length = envelope[5]!;
  if (length < 1 || length > 64 || envelope.length < 10 + length) {
    throw new Error("Invalid E2EE envelope");
  }
  const id = String.fromCharCode(...envelope.subarray(6, 6 + length));
  const version = new DataView(envelope.buffer, envelope.byteOffset, envelope.byteLength).getUint32(
    6 + length,
    false,
  );
  if (id !== mechanism.id || version !== mechanism.version) {
    throw new Error("Unsupported E2EE mechanism");
  }
  // Buffer is a Uint8Array subtype, but its slice() shares the source storage.
  return Uint8Array.from(envelope.subarray(10 + length));
}
