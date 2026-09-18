import { frameCryptoRecord } from "./record-frame.js";
import { encodePublicApprovalRevision } from "./account-successor.js";
import { decodeGroupMembership } from "./group-successor.js";
import { spaceContext } from "./space-format.js";
import type { SpaceRoot, SpaceSuccessor } from "./spaces.js";

type Root = Pick<SpaceRoot, "id" | "scopeId" | "identifier">;
type Coordinates = Pick<SpaceSuccessor, "id" | "spaceId" | "epochId">;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

export function spaceSuccessorContext(
  application: string,
  root: Root,
  record: Coordinates,
  role: string,
  recipient = "",
): Uint8Array {
  if (record.spaceId !== root.id) throw new Error("Invalid E2EE space successor lineage");
  return spaceContext(
    application,
    { ...root, epochId: record.epochId },
    JSON.stringify(["successor", record.id, role]),
    recipient,
  );
}

export function spaceSuccessorBytes(
  application: string,
  root: Root,
  record: Omit<SpaceSuccessor, "signature">,
): Uint8Array {
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  if (
    ![
      record.id,
      record.predecessor,
      record.epochId,
      record.authorDeviceId,
      record.authorEpochId,
    ].every((id) => typeof id === "string" && uuid.test(id)) ||
    record.predecessor === record.epochId ||
    typeof record.authorAccountId !== "string" ||
    !record.authorAccountId
  )
    throw new Error("Invalid E2EE space successor coordinates");
  const revision: unknown = JSON.parse(decoder.decode(record.revision));
  if (!Array.isArray(revision) || !revision.every((id): id is string => typeof id === "string"))
    throw new Error("Invalid E2EE space successor revision");
  const canonical = encodePublicApprovalRevision(revision);
  if (
    canonical.length !== record.revision.length ||
    !canonical.every((byte, i) => byte === record.revision[i])
  )
    throw new Error("Non-canonical E2EE space successor revision");
  decodeGroupMembership(record.membership);
  return frameCryptoRecord([
    spaceSuccessorContext(
      application,
      root,
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
