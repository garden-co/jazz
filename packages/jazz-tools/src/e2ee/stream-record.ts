import { decodeEnvelopeView, encodeEnvelope, type CryptoMechanism } from "./envelope.js";
import type { LargeValueCipher } from "./types.js";
import { E2eeDataError } from "./data-error.js";

export const STREAM_RECORD = { id: "jazz.e2ee.stream-record", version: 1 } as const;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** Common framing owns the mechanism header even for adapters with unframed payloads. */
export async function* encodeStreamRecord(
  epoch: string,
  mechanism: CryptoMechanism,
  ciphertext: AsyncIterable<Uint8Array>,
): AsyncIterable<Uint8Array> {
  if (!UUID.test(epoch)) throw new Error("Invalid encrypted stream epoch");
  const header = encodeEnvelope(mechanism, new Uint8Array());
  const routing = new Uint8Array(36 + header.length);
  routing.set(new TextEncoder().encode(epoch));
  routing.set(header, 36);
  yield encodeEnvelope(STREAM_RECORD, routing);
  yield* ciphertext;
}

export function readStreamRecord(value: Uint8Array, mechanism: CryptoMechanism) {
  const payload = decodeEnvelopeView(STREAM_RECORD, value);
  const epoch = new TextDecoder().decode(payload.subarray(0, 36));
  if (!UUID.test(epoch)) throw new Error("Invalid encrypted stream epoch");

  return { epoch, ciphertext: decodeEnvelopeView(mechanism, payload.subarray(36)) };
}
/** Preserve producer failures without exposing potentially secret-bearing adapter diagnostics. */
export async function* encryptStreamRecord(
  cipher: LargeValueCipher,
  secret: Uint8Array,
  context: Uint8Array,
  epoch: string,
  source: AsyncIterable<Uint8Array>,
  options?: { signal?: AbortSignal },
): AsyncIterable<Uint8Array> {
  let sourceFailure: { error: unknown } | undefined;
  const trackedSource = (async function* () {
    try {
      yield* source;
    } catch (error) {
      sourceFailure = { error };
      throw error;
    }
  })();
  try {
    yield* encodeStreamRecord(
      epoch,
      cipher.mechanism,
      cipher.encrypt(secret, context, trackedSource, options),
    );
  } catch (error) {
    if (error instanceof E2eeDataError || (sourceFailure && sourceFailure.error === error))
      throw error;
    throw new E2eeDataError("encryption-failed");
  }
}

/** Ordinary row reads expose only complete authenticated values, never a verified prefix. */
export async function decryptStreamRecord(
  cipher: LargeValueCipher,
  secret: Uint8Array,
  context: Uint8Array,
  ciphertext: Uint8Array,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for await (const chunk of cipher.decrypt(
      secret,
      context,
      (async function* () {
        yield ciphertext;
      })(),
    )) {
      chunks.push(chunk);
      length += chunk.length;
    }
    const result = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }
    return result;
  } finally {
    for (const chunk of chunks) chunk.fill(0);
  }
}
