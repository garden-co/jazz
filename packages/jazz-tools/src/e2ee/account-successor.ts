import { encodeCryptoContext } from "./context.js";
import type { DeviceSigner } from "./types.js";

/** Independently identified immutable proposal; predecessor is signed explicitly. */
export type AccountSuccessor = {
  id: string;
  predecessor: string;
  accountId: string;
  epochId: string;
  signerId: string;
  removedDeviceId: string;
  membership: Uint8Array;
  revision: Uint8Array;
  verification: Uint8Array;
  history: Uint8Array;
  deliveries: Uint8Array;
  signature: Uint8Array;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

function canonical(value: unknown): Uint8Array {
  return encoder.encode(JSON.stringify(value));
}

function parse(bytes: Uint8Array): unknown {
  const value: unknown = JSON.parse(decoder.decode(bytes));
  const encoded = canonical(value);
  if (encoded.length !== bytes.length || !encoded.every((byte, i) => byte === bytes[i]))
    throw new Error("Non-canonical E2EE successor metadata");
  return value;
}

function checkIds(value: unknown, requireUuidV4 = true): asserts value is string[] {
  if (
    !Array.isArray(value) ||
    value.some(
      (id, i) =>
        typeof id !== "string" ||
        !id.length ||
        (requireUuidV4 && !uuid.test(id)) ||
        (i > 0 && value[i - 1] >= id),
    )
  )
    throw new Error("Invalid E2EE successor membership");
}

export function encodeEpochIds(ids: Iterable<string>): Uint8Array {
  const sorted = [...ids].sort();
  checkIds(sorted);
  return canonical(sorted);
}

export function decodeEpochIds(bytes: Uint8Array): string[] {
  const value = parse(bytes);
  checkIds(value);
  return value;
}

/** Raw candidate row IDs are not necessarily valid signed-statement UUIDv4 IDs. */
export function encodePublicApprovalRevision(ids: Iterable<string>): Uint8Array {
  const sorted = [...ids].sort();
  checkIds(sorted, false);
  return canonical(sorted);
}

export function encodeEpochDeliveries(deliveries: ReadonlyMap<string, Uint8Array>): Uint8Array {
  const entries = [...deliveries].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  const encoded = canonical(entries.map(([id, envelope]) => [id, Array.from(envelope)]));
  decodeEpochDeliveries(encoded);
  return encoded;
}

export function decodeEpochDeliveries(bytes: Uint8Array): Map<string, Uint8Array> {
  const value = parse(bytes);
  if (!Array.isArray(value)) throw new Error("Invalid E2EE successor deliveries");
  const ids: string[] = [];
  const result = new Map<string, Uint8Array>();
  for (const entry of value) {
    if (
      !Array.isArray(entry) ||
      entry.length !== 2 ||
      !Array.isArray(entry[1]) ||
      entry[1].length === 0 ||
      entry[1].length > 65536 ||
      entry[1].some(
        (byte: unknown) =>
          typeof byte !== "number" || !Number.isInteger(byte) || byte < 0 || byte > 255,
      )
    )
      throw new Error("Invalid E2EE successor delivery");
    ids.push(entry[0]);
    result.set(entry[0], Uint8Array.from(entry[1]));
  }
  checkIds(ids);
  return result;
}

export function successorContext(
  application: string,
  row: Pick<AccountSuccessor, "id" | "accountId" | "epochId">,
  column: string,
  recipient = "",
): Uint8Array {
  return encodeCryptoContext({
    application,
    policy: "jazz.e2ee.account-successor.v1",
    scope: "account",
    identifier: row.accountId,
    table: "__e2ee_account_successors",
    row: row.id,
    epoch: row.epochId,
    column,
    recipient,
  });
}

/** Length-prefixed fields prevent ambiguous concatenation; all routing is signed. */
export function successorSigningBytes(
  application: string,
  row: Omit<AccountSuccessor, "signature">,
): Uint8Array {
  if (
    ![row.id, row.predecessor, row.epochId, row.signerId, row.removedDeviceId].every((id) =>
      uuid.test(id),
    ) ||
    row.predecessor === row.epochId
  )
    throw new Error("Invalid E2EE successor identifiers");
  const members = decodeEpochIds(row.membership);
  decodeEpochIds(row.revision);
  const deliveries = decodeEpochDeliveries(row.deliveries);
  if (
    members.includes(row.removedDeviceId) ||
    members.length !== deliveries.size ||
    members.some((id) => !deliveries.has(id))
  )
    throw new Error("E2EE successor recipients do not match membership");
  const prefix = successorContext(application, row, "signature", row.signerId);
  const fields = [
    encoder.encode(row.predecessor),
    encoder.encode(row.removedDeviceId),
    row.membership,
    row.revision,
    row.verification,
    row.history,
    row.deliveries,
  ];
  return appendFields(prefix, fields);
}

export type PublicAccountSuccessor = Pick<
  AccountSuccessor,
  | "id"
  | "accountId"
  | "epochId"
  | "predecessor"
  | "signerId"
  | "removedDeviceId"
  | "membership"
  | "revision"
>;

/** Key-free projection; readers must validate complete accepted membership history. */
export function publicSuccessorSigningBytes(
  application: string,
  row: PublicAccountSuccessor,
): Uint8Array {
  if (
    ![row.id, row.predecessor, row.epochId, row.signerId, row.removedDeviceId].every(
      (id) => typeof id === "string" && uuid.test(id),
    ) ||
    row.predecessor === row.epochId
  )
    throw new Error("Invalid E2EE public successor identifiers");
  if (decodeEpochIds(row.membership).includes(row.removedDeviceId))
    throw new Error("Removed E2EE device remains in successor membership");
  checkIds(parse(row.revision), false);
  const prefix = encodeCryptoContext({
    application,
    policy: "jazz.e2ee.public-account-successor.v1",
    scope: "account",
    identifier: row.accountId,
    table: "__e2ee_public_account_successors",
    row: row.id,
    column: "signature",
    epoch: row.epochId,
    recipient: row.signerId,
  });
  return appendFields(prefix, [
    encoder.encode(row.predecessor),
    encoder.encode(row.removedDeviceId),
    row.membership,
    row.revision,
  ]);
}

function appendFields(prefix: Uint8Array, fields: Uint8Array[]): Uint8Array {
  if (fields.some((field) => field.length > 0xffffffff))
    throw new Error("E2EE successor field too large");
  const result = new Uint8Array(
    prefix.length + fields.reduce((n, field) => n + 4 + field.length, 0),
  );
  result.set(prefix);
  const view = new DataView(result.buffer);
  let offset = prefix.length;
  for (const field of fields) {
    view.setUint32(offset, field.length, false);
    result.set(field, offset + 4);
    offset += 4 + field.length;
  }
  return result;
}

export async function verifySuccessor(
  application: string,
  row: AccountSuccessor,
  signer: DeviceSigner,
  publicKey: Uint8Array,
): Promise<boolean> {
  return signer.verify(publicKey, successorSigningBytes(application, row), row.signature);
}
