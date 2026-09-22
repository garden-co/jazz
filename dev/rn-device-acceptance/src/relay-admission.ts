import { decodeBase64, encodeBase64 } from "./base64.ts";
import { NATIVE_RELAY_ABI_V1 } from "jazz-rn/native-relay-abi";

type NativeRelayCapability = Uint8Array;
type NativeRelayExecutor = { execute(commandBase64: string): Promise<string> };

export type AdmittedRelay = {
  executor: NativeRelayExecutor;
  capability: NativeRelayCapability;
};

/** Fixed safe pre-receipt stages. They identify the failed native ABI
 * operation without allowing a command, capability, or native error to leave
 * the installed fixture. */
export type AdmittedRelayDiagnostic =
  | "relay-open-failed"
  | "relay-attach-failed"
  | "relay-probe-failed"
  | "relay-cleanup-failed";

/** Probe is public link diagnostics; a successful Open is the admission proof. */
export async function proveAdmittedRelay(
  executor: NativeRelayExecutor,
  capability: NativeRelayCapability,
  markFailure?: (stage: AdmittedRelayDiagnostic) => void,
): Promise<void> {
  let openedRelay: bigint | undefined;
  let attachedClient: bigint | undefined;
  let primaryFailure: unknown;
  try {
    markFailure?.("relay-open-failed");
    openedRelay = decodeOpened(await executor.execute(encodeOpen(capability)));
    markFailure?.("relay-attach-failed");
    attachedClient = decodeAttached(await executor.execute(encodeAttach(openedRelay)));
    markFailure?.("relay-probe-failed");
    const response = decodeBase64(await executor.execute("AA=="));
    const [abiVersion, abiOffset] = readVarint(response, 1);
    if (
      response[0] !== 0 ||
      abiOffset !== response.length ||
      abiVersion !== BigInt(NATIVE_RELAY_ABI_V1)
    )
      throw new Error("installed Jazz relay returned an unexpected ABI probe response");
  } catch (error) {
    primaryFailure = error;
  }
  let cleanupFailure: unknown;
  if (attachedClient !== undefined) {
    try {
      if (!primaryFailure) markFailure?.("relay-cleanup-failed");
      if (!decodeClosed(await executor.execute(encodeCloseClient(attachedClient))))
        cleanupFailure ??= new Error("native relay did not close the admitted UI peer");
    } catch (error) {
      cleanupFailure ??= error;
    }
  }
  if (openedRelay !== undefined) {
    try {
      if (!primaryFailure) markFailure?.("relay-cleanup-failed");
      if (!decodeClosed(await executor.execute(encodeCloseRelay(openedRelay))))
        cleanupFailure ??= new Error("native relay did not close the admitted scope");
    } catch (error) {
      cleanupFailure ??= error;
    }
  }
  if (primaryFailure) throw primaryFailure;
  if (cleanupFailure) throw cleanupFailure;
}

/**
 * A trusted logout must invalidate both the opaque capability and every
 * already-open relay/client alias derived from it. The replacement admission
 * is deliberately a new trusted-native call: JavaScript never supplies a
 * changed scope configuration through the generic command channel.
 */
export async function proveLogoutRevocation(
  admitted: AdmittedRelay,
  logout: () => Promise<void>,
  readmitted: () => Promise<AdmittedRelay>,
): Promise<void> {
  await proveRevocationAndReplacement(admitted, logout, readmitted, "logout");
}

/** A native auth switch must retire every old-scope alias before admitting B. */
export async function proveAuthScopeSwitch(
  admitted: AdmittedRelay,
  switchScope: () => Promise<AdmittedRelay>,
): Promise<AdmittedRelay> {
  let replacement: AdmittedRelay | undefined;
  await proveRevocationAndReplacement(
    admitted,
    async () => {
      replacement = await switchScope();
    },
    async () => {
      if (!replacement)
        throw new Error("trusted auth scope switch returned no replacement capability");
      return replacement;
    },
    "auth scope switch",
  );
  if (!replacement) throw new Error("trusted auth scope switch returned no replacement capability");
  return replacement;
}

async function proveRevocationAndReplacement(
  admitted: AdmittedRelay,
  revoke: () => Promise<void>,
  readmitted: () => Promise<AdmittedRelay>,
  operation: string,
): Promise<void> {
  const relay = decodeOpened(await admitted.executor.execute(encodeOpen(admitted.capability)));
  const client = decodeAttached(await admitted.executor.execute(encodeAttach(relay)));
  await revoke();

  await assertRejected(
    () => admitted.executor.execute(encodeOpen(admitted.capability)),
    `${operation} capability after revocation`,
  );
  await assertRejected(
    () => admitted.executor.execute(encodeAttach(relay)),
    `relay alias retained after ${operation}`,
  );
  if (decodeClosed(await admitted.executor.execute(encodeCloseClient(client))))
    throw new Error(`client alias remained live after ${operation}`);

  const replacement = await readmitted();
  if (replacement.capability.byteLength !== 32)
    throw new Error("replacement admission capability must be exactly 32 bytes");
  if (equalBytes(replacement.capability, admitted.capability))
    throw new Error(`trusted ${operation} reused a revoked admission capability`);
  await proveAdmittedRelay(replacement.executor, replacement.capability);
}

function encodeOpen(capability: Uint8Array): string {
  if (capability.byteLength !== 32)
    throw new Error("admission capability must be exactly 32 bytes");
  const abi = varint(BigInt(NATIVE_RELAY_ABI_V1));
  return encode([1, ...abi, ...abi, ...capability]);
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
  return encodeBase64(Uint8Array.from(bytes));
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
async function assertRejected(action: () => Promise<unknown>, expectation: string): Promise<void> {
  try {
    await action();
  } catch {
    return;
  }
  throw new Error(`native relay accepted ${expectation}`);
}
function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.byteLength === right.byteLength && left.every((byte, index) => byte === right[index]);
}
function bytesOf(encoded: string): Uint8Array {
  return decodeBase64(encoded);
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
