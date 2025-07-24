import { CoID } from "./coValue.js";
import { CoValueCore, ProcessedTransaction } from "./coValueCore/coValueCore.js";
import { Transaction } from "./coValueCore/verifiedState.js";
import { RawAccount, RawAccountID, RawProfile } from "./coValues/account.js";
import { MapOpPayload } from "./coValues/coMap.js";
import {
  EVERYONE,
  Everyone,
  ParentGroupReferenceRole,
  RawGroup,
  isInheritableRole,
} from "./coValues/group.js";
import { KeyID } from "./crypto/crypto.js";
import {
  AgentID,
  ParentGroupReference,
  RawCoID,
  SessionID,
  TransactionID,
  getParentGroupId,
} from "./ids.js";
import { parseJSON } from "./jsonStringify.js";
import { JsonValue } from "./jsonValue.js";
import { logger } from "./logger.js";
import { CoValueKnownState } from "./sync.js";
import { accountOrAgentIDfromSessionID } from "./typeUtils/accountOrAgentIDfromSessionID.js";
import { expectGroup } from "./typeUtils/expectGroup.js";

export type PermissionsDef =
  | { type: "group"; initialAdmin: RawAccountID | AgentID }
  | { type: "ownedByGroup"; group: RawCoID }
  | { type: "unsafeAllowAll" };

export type AccountRole = "reader" | "writer" | "admin" | "writeOnly";

export type Role =
  | AccountRole
  | "revoked"
  | "adminInvite"
  | "writerInvite"
  | "readerInvite"
  | "writeOnlyInvite";

export type MemberState = { [agent: RawAccountID | AgentID]: Role; [EVERYONE]?: Role };

let logPermissionErrors = true;

export function disablePermissionErrors() {
  logPermissionErrors = false;
}

function logPermissionError(
  message: string,
  attributes?: Record<string, JsonValue>,
) {
  if (logPermissionErrors === false) {
    return;
  }

  logger.debug("Permission error: " + message, attributes);
}

export function validationPass(coValue: CoValueCore): void {
  if (!coValue.isAvailable()) {
    throw new Error("determineValidTransactions CoValue is not available");
  }

  if (coValue.verified.header.ruleset.type === "group") {
    const initialAdmin = coValue.verified.header.ruleset.initialAdmin;
    if (!initialAdmin) {
      throw new Error("Group must have initialAdmin");
    }

    groupValidationPass(coValue, initialAdmin);
  } else if (coValue.verified.header.ruleset.type === "ownedByGroup") {
    const groupContent = expectGroup(
      coValue.node
        .expectCoValueLoaded(
          coValue.verified.header.ruleset.group,
          "Determining valid transaction in owned object but its group wasn't loaded",
        )
        .getCurrentContent(),
    );

    if (groupContent.type !== "comap") {
      throw new Error("Group must be a map");
    }

    ownedByGroupValidationPass.call(coValue, groupContent);
  } else if (coValue.verified.header.ruleset.type === "unsafeAllowAll") {
    for (const tx of coValue.processedSorted) {
      tx.valid = true;
    }
  } else {
    throw new Error(
      "Unknown ruleset type " +
        (coValue.verified.header.ruleset as { type: string }).type,
    );
  }
}

function ownedByGroupValidationPass(this: CoValueCore, groupContent: RawGroup) {
  for (let i = this.nValidated; i < this.processedSorted.length; i++) {
    const processed = this.processedSorted[i]!;
    const transactor = accountOrAgentIDfromSessionID(processed.txID.sessionID);

    if (processed.valid !== null) {
      continue;
    }

    const groupAtTime = groupContent.atTime(processed.madeAt);
    const effectiveTransactor = agentInAccountOrMemberInGroup(
      transactor,
      groupAtTime,
    );

    if (!effectiveTransactor) {
      processed.valid = false;
      continue;
    }

    const transactorRoleAtTxTime =
      groupAtTime.roleOfInternal(effectiveTransactor);

    if (
      transactorRoleAtTxTime !== "admin" &&
      transactorRoleAtTxTime !== "writer" &&
      transactorRoleAtTxTime !== "writeOnly"
    ) {
      processed.valid = false;
      continue;
    }

    processed.valid = true;
  }
}

function isHigherRole(a: Role, b: Role | undefined) {
  if (a === undefined || a === "revoked") return false;
  if (b === undefined || b === "revoked") return true;
  if (b === "admin") return false;
  if (a === "admin") return true;

  return a === "writer" && b === "reader";
}

function resolveMemberStateFromParentReference(
  coValue: CoValueCore,
  memberState: MemberState,
  parentReference: ParentGroupReference,
  roleMapping: ParentGroupReferenceRole,
  extendChain: Set<CoValueCore["id"]>,
) {
  const parentGroup = coValue.node.expectCoValueLoaded(
    getParentGroupId(parentReference),
    "Expected parent group to be loaded",
  );

  if (parentGroup.verified.header.ruleset.type !== "group") {
    return;
  }

  // Skip circular references
  if (extendChain.has(parentGroup.id)) {
    return;
  }

  const initialAdmin = parentGroup.verified.header.ruleset.initialAdmin;

  if (!initialAdmin) {
    throw new Error("Group must have initialAdmin");
  }

  extendChain.add(parentGroup.id);

  const { memberState: parentGroupMemberState } =
    groupValidationPass(parentGroup, initialAdmin, extendChain);

  for (const agent of Object.keys(parentGroupMemberState) as Array<
    keyof MemberState
  >) {
    const parentRole = parentGroupMemberState[agent];
    const currentRole = memberState[agent];

    if (isInheritableRole(parentRole)) {
      if (roleMapping !== "extend" && isHigherRole(roleMapping, currentRole)) {
        memberState[agent] = roleMapping;
      } else if (isHigherRole(parentRole, currentRole)) {
        memberState[agent] = parentRole;
      }
    }
  }
}

function groupValidationPass(
  coValue: CoValueCore,
  initialAdmin: RawAccountID | AgentID,
  extendChain?: Set<CoValueCore["id"]>,
): { memberState: MemberState } {
  const memberState: MemberState = {};
  const writeOnlyKeys: Record<RawAccountID | AgentID, KeyID> = {};

  const writeKeys = new Set<string>();

  // always process all transactions in groups so we rebuild the full member state
  // TODO: make this better
  for (const processed of coValue.processedSorted) {
    const transactor = accountOrAgentIDfromSessionID(processed.txID.sessionID);

    if (processed.tx.privacy === "private") {
      if (memberState[transactor] === "admin") {
        processed.valid = true;
        continue;
      } else {
        setInvalid(processed, "Only admins can make private transactions in groups");
        continue;
      }
    }

    let changes;

    changes = processed.changes;

    if (!changes) {
      setInvalid(processed, "Expected decrypted changes in transaction");
      continue;
    }

    const change = changes[0] as
      | MapOpPayload<RawAccountID | AgentID | Everyone, Role>
      | MapOpPayload<"readKey", JsonValue>
      | MapOpPayload<"profile", CoID<RawProfile>>
      | MapOpPayload<`parent_${CoID<RawGroup>}`, CoID<RawGroup>>
      | MapOpPayload<`child_${CoID<RawGroup>}`, CoID<RawGroup>>;

    if (changes.length !== 1) {
      setInvalid(processed, "Group transaction must have exactly one change");
      continue;
    }

    if (change.op !== "set") {
      setInvalid(processed, "Group transaction must set a role or readKey");
      continue;
    }

    if (change.key === "readKey") {
      if (memberState[transactor] !== "admin") {
        setInvalid(processed, "Only admins can set readKeys");
        continue;
      }

      processed.valid = true;
      continue;
    } else if (change.key === "profile") {
      if (memberState[transactor] !== "admin") {
        setInvalid(processed, "Only admins can set profile");
        continue;
      }

      processed.valid = true;
      continue;
    } else if (
      isKeyForKeyField(change.key) ||
      isKeyForAccountField(change.key)
    ) {
      if (
        memberState[transactor] !== "admin" &&
        memberState[transactor] !== "adminInvite" &&
        memberState[transactor] !== "writerInvite" &&
        memberState[transactor] !== "readerInvite" &&
        memberState[transactor] !== "writeOnlyInvite" &&
        !isOwnWriteKeyRevelation(change.key, transactor, writeOnlyKeys)
      ) {
        setInvalid(processed, "Only admins can reveal keys");
        continue;
      }

      // TODO: check validity of agents who the key is revealed to?
      processed.valid = true;
      continue;
    } else if (isParentExtension(change.key)) {
      if (memberState[transactor] !== "admin") {
        setInvalid(processed, "Only admins can set parent extensions");
        continue;
      }

      extendChain = extendChain ?? new Set([]);

      resolveMemberStateFromParentReference(
        coValue,
        memberState,
        change.key,
        change.value as ParentGroupReferenceRole,
        extendChain,
      );

      // Circular reference detected, drop all the transactions involved
      if (extendChain.has(coValue.id)) {
        setInvalid(processed, "Circular extend detected, dropping the transaction");
        continue;
      }

      processed.valid = true;
      continue;
    } else if (isChildExtension(change.key)) {
      processed.valid = true;
      continue;
    } else if (isWriteKeyForMember(change.key)) {
      const memberKey = getAccountOrAgentFromWriteKeyForMember(change.key);

      if (
        memberState[transactor] !== "admin" &&
        memberState[transactor] !== "writeOnlyInvite" &&
        memberKey !== transactor
      ) {
        setInvalid(processed, "Only admins can set writeKeys");
        continue;
      }

      writeOnlyKeys[memberKey] = change.value as KeyID;

      /**
       * writeOnlyInvite need to be able to set writeKeys because every new writeOnly
       * member comes with their own write key.
       *
       * We don't want to give the ability to invite members to override
       * write keys, otherwise they could hide a write key to other writeOnly users
       * blocking them from accessing the group.ß
       */
      if (writeKeys.has(change.key) && memberState[transactor] !== "admin") {
        setInvalid(processed, "Write key already exists and can't be overridden by invite");
        continue;
      }

      writeKeys.add(change.key);

      processed.valid = true;
      continue;
    }

    const affectedMember = change.key;
    const assignedRole = change.value;

    if (
      change.value !== "admin" &&
      change.value !== "writer" &&
      change.value !== "reader" &&
      change.value !== "writeOnly" &&
      change.value !== "revoked" &&
      change.value !== "adminInvite" &&
      change.value !== "writerInvite" &&
      change.value !== "readerInvite" &&
      change.value !== "writeOnlyInvite"
    ) {
      setInvalid(processed, "Group transaction must set a valid role");
      continue;
    }

    if (
      affectedMember === EVERYONE &&
      !(
        change.value === "reader" ||
        change.value === "writer" ||
        change.value === "writeOnly" ||
        change.value === "revoked"
      )
    ) {
      setInvalid(processed, "Everyone can only be set to reader, writer, writeOnly or revoked");
      continue;
    }

    const isFirstSelfAppointment =
      !memberState[transactor] &&
      transactor === initialAdmin &&
      change.op === "set" &&
      change.key === transactor &&
      change.value === "admin";

    const currentAccountId = coValue.node.getCurrentAccountOrAgentID();

    const isSelfRevoke =
      currentAccountId === change.key && change.value === "revoked";

    if (!isFirstSelfAppointment && !isSelfRevoke) {
      if (memberState[transactor] === "admin") {
        if (
          memberState[affectedMember] === "admin" &&
          affectedMember !== transactor &&
          assignedRole !== "admin"
        ) {
          setInvalid(processed, "Admins can only demote themselves.");
          continue;
        }
      } else if (memberState[transactor] === "adminInvite") {
        if (change.value !== "admin") {
          setInvalid(processed, "AdminInvites can only create admins.");
          continue;
        }
      } else if (memberState[transactor] === "writerInvite") {
        if (change.value !== "writer") {
          setInvalid(processed, "WriterInvites can only create writers.");
          continue;
        }
      } else if (memberState[transactor] === "readerInvite") {
        if (change.value !== "reader") {
          setInvalid(processed, "ReaderInvites can only create reader.");
          continue;
        }
      } else if (memberState[transactor] === "writeOnlyInvite") {
        if (change.value !== "writeOnly") {
          setInvalid(processed, "WriteOnlyInvites can only create writeOnly.");
          continue;
        }
      } else {
        setInvalid(processed, "Group transaction must be made by current admin or invite");
        continue;
      }
    }

    memberState[affectedMember] = change.value;
    processed.valid = true;
  }

  return { memberState };
}

function agentInAccountOrMemberInGroup(
  transactor: RawAccountID | AgentID,
  groupAtTime: RawGroup,
): RawAccountID | AgentID | undefined {
  if (transactor === groupAtTime.id && groupAtTime instanceof RawAccount) {
    return groupAtTime.currentAgentID();
  }
  return transactor;
}

export function isWriteKeyForMember(
  co: string,
): co is `writeKeyFor_${RawAccountID | AgentID}` {
  return co.startsWith("writeKeyFor_");
}

export function getAccountOrAgentFromWriteKeyForMember(
  co: `writeKeyFor_${RawAccountID | AgentID}`,
): RawAccountID | AgentID {
  return co.slice("writeKeyFor_".length) as RawAccountID | AgentID;
}

export function isKeyForKeyField(co: string): co is `${KeyID}_for_${KeyID}` {
  return co.startsWith("key_") && co.includes("_for_key");
}

export function isKeyForAccountField(
  co: string,
): co is `${KeyID}_for_${RawAccountID | AgentID}` {
  return (
    (co.startsWith("key_") &&
      (co.includes("_for_sealer") || co.includes("_for_co"))) ||
    co.includes("_for_everyone")
  );
}

function isParentExtension(key: string): key is `parent_${CoID<RawGroup>}` {
  return key.startsWith("parent_");
}

function isChildExtension(key: string): key is `child_${CoID<RawGroup>}` {
  return key.startsWith("child_");
}

function isOwnWriteKeyRevelation(
  key: `${KeyID}_for_${string}`,
  memberKey: RawAccountID | AgentID,
  writeOnlyKeys: Record<RawAccountID | AgentID, KeyID>,
): key is `${KeyID}_for_${RawAccountID | AgentID}` {
  if (Object.keys(writeOnlyKeys).length === 0) {
    return false;
  }

  const keyID = key.slice(0, key.indexOf("_for_"));

  return writeOnlyKeys[memberKey] === keyID;
}

function setInvalid(processed: ProcessedTransaction, reason: string) {
  processed.valid = false;
  processed.invalidReason = reason;
  logPermissionError(reason);
}