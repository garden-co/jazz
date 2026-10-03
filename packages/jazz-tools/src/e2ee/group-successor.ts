import { encodeCryptoContext } from "./context.js";
import { frameCryptoRecord } from "./record-frame.js";
import { encodePublicApprovalRevision } from "./account-successor.js";
import type { GroupSuccessor } from "./groups.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export function encodeGroupMembership(members: ReadonlyMap<string, string>): Uint8Array {
  const entries = [...members].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  const bytes = encoder.encode(JSON.stringify(entries));
  decodeGroupMembership(bytes);
  return bytes;
}

export function decodeGroupMembership(bytes: Uint8Array): Map<string, string> {
  const entries: unknown = JSON.parse(decoder.decode(bytes));
  if (!Array.isArray(entries)) throw new Error("Invalid E2EE group successor membership");
  const members = new Map<string, string>();
  let previous: string | undefined;
  for (const entry of entries) {
    if (
      !Array.isArray(entry) ||
      entry.length !== 2 ||
      typeof entry[0] !== "string" ||
      !entry[0] ||
      typeof entry[1] !== "string" ||
      !uuid.test(entry[1]) ||
      (previous !== undefined && previous >= entry[0])
    )
      throw new Error("Invalid E2EE group successor membership");
    members.set(entry[0], entry[1]);
    previous = entry[0];
  }
  const canonical = encoder.encode(JSON.stringify(entries));
  if (canonical.length !== bytes.length || !canonical.every((byte, index) => byte === bytes[index]))
    throw new Error("Non-canonical E2EE group successor membership");
  return members;
}

export function groupSuccessorContext(
  application: string,
  record: Pick<GroupSuccessor, "id" | "groupId" | "epochId">,
  column: string,
  recipient = "",
): Uint8Array {
  return encodeCryptoContext({
    application,
    policy: "jazz.e2ee.group-successor.v1",
    scope: "group",
    identifier: record.groupId,
    table: "__e2ee_group_successors",
    row: record.id,
    column,
    epoch: record.epochId,
    recipient,
  });
}

export function groupSuccessorSigningBytes(
  application: string,
  record: Omit<GroupSuccessor, "signature">,
): Uint8Array {
  if (
    ![
      record.id,
      record.groupId,
      record.predecessor,
      record.epochId,
      record.authorDeviceId,
      record.authorEpochId,
    ].every((id) => typeof id === "string" && uuid.test(id)) ||
    record.predecessor === record.epochId ||
    typeof record.authorAccountId !== "string" ||
    !record.authorAccountId
  )
    throw new Error("Invalid E2EE group successor coordinates");
  const revision: unknown = JSON.parse(decoder.decode(record.revision));
  if (!Array.isArray(revision) || !revision.every((id): id is string => typeof id === "string"))
    throw new Error("Invalid E2EE group successor revision");
  const canonical = encodePublicApprovalRevision(revision);
  if (
    canonical.length !== record.revision.length ||
    !canonical.every((byte, index) => byte === record.revision[index])
  )
    throw new Error("Non-canonical E2EE group successor revision");
  decodeGroupMembership(record.membership);
  return frameCryptoRecord([
    groupSuccessorContext(
      application,
      record,
      "signature",
      JSON.stringify([record.authorAccountId, record.authorDeviceId, record.authorEpochId]),
    ),
    encoder.encode(record.predecessor),
    record.revision,
    record.membership,
    record.verification,
    record.history,
    record.authorEnvelope,
  ]);
}
