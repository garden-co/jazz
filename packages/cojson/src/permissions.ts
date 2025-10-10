import { CoID } from "./coValue.js";
import { CoValueCore } from "./coValueCore/coValueCore.js";
import { Transaction } from "./coValueCore/verifiedState.js";
import { RawAccount, RawAccountID, RawProfile } from "./coValues/account.js";
import { MapOpPayload, RawCoMap } from "./coValues/coMap.js";
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

export type AccountRole =
  /**
   * Can read the group's CoValues
   */
  | "reader"
  /**
   * Can read and write to the group's CoValues
   */
  | "writer"
  /**
   * Can read and write to the group, and change group member roles
   */
  | "admin"
  /**
   * Can only write to the group's CoValues and read their own changes
   */
  | "writeOnly";

export type Role =
  | AccountRole
  | "revoked"
  | "adminInvite"
  | "writerInvite"
  | "readerInvite"
  | "writeOnlyInvite";

export function isAccountRole(role?: Role): role is AccountRole {
  return (
    role === "admin" ||
    role === "writer" ||
    role === "reader" ||
    role === "writeOnly"
  );
}

type ValidTransactionsResult = { txID: TransactionID; tx: Transaction };
type MemberState = { [agent: RawAccountID | AgentID]: Role; [EVERYONE]?: Role };

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

export function determineValidTransactions(coValue: CoValueCore) {
  if (!coValue.isAvailable()) {
    throw new Error("determineValidTransactions CoValue is not available");
  }

  if (coValue.verified.header.ruleset.type === "group") {
    const initialAdmin = coValue.verified.header.ruleset.initialAdmin;
    if (!initialAdmin) {
      throw new Error("Group must have initialAdmin");
    }

    determineValidTransactionsForGroup(coValue, initialAdmin);
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

    for (const tx of coValue.verifiedTransactions) {
      if (tx.isValidated) {
        continue;
      }

      tx.isValidated = true;
      // We use the original made at to get the group at the original time when the transaction was made
      // madeAt might be changed by the meta field (e.g. merged transactions), and so can't be used for permissions checks
      const groupAtTime = groupContent.atTime(tx.originalMadeAt);
      const effectiveTransactor = agentInAccountOrMemberInGroup(
        tx.author,
        groupAtTime,
      );

      if (!effectiveTransactor) {
        tx.isValid = false;
        continue;
      }

      const transactorRoleAtTxTime =
        groupAtTime.roleOfInternal(effectiveTransactor);

      if (
        transactorRoleAtTxTime !== "admin" &&
        transactorRoleAtTxTime !== "writer" &&
        transactorRoleAtTxTime !== "writeOnly"
      ) {
        tx.isValid = false;
        continue;
      }

      tx.isValid = true;
    }
  } else if (coValue.verified.header.ruleset.type === "unsafeAllowAll") {
    for (const tx of coValue.verifiedTransactions) {
      tx.isValid = true;
      tx.isValidated = true;
    }
  } else {
    throw new Error(
      "Unknown ruleset type " +
        (coValue.verified.header.ruleset as { type: string }).type,
    );
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
    determineValidTransactionsForGroup(parentGroup, initialAdmin, extendChain);

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

function determineValidTransactionsForGroup(
  coValue: CoValueCore,
  initialAdmin: RawAccountID | AgentID,
  extendChain?: Set<CoValueCore["id"]>,
): { memberState: MemberState } {
  coValue.verifiedTransactions.sort(coValue.compareTransactions);

  const memberState: MemberState = {};
  const writeOnlyKeys: Record<RawAccountID | AgentID, KeyID> = {};

  const writeKeys = new Set<string>();

  for (const transaction of coValue.verifiedTransactions) {
    const transactor = transaction.author;

    transaction.isValidated = true;

    const tx = transaction.tx;

    if (tx.privacy === "private") {
      if (memberState[transactor] === "admin") {
        transaction.isValid = true;
        continue;
      } else {
        logPermissionError(
          "Only admins can make private transactions in groups",
        );
        continue;
      }
    }

    let changes = transaction.changes;

    if (!changes) {
      try {
        changes = parseJSON(tx.changes);
        transaction.changes = changes;
      } catch (e) {
        logPermissionError("Invalid JSON in transaction", {
          id: coValue.id,
          tx,
        });
        transaction.hasInvalidChanges = true;
        continue;
      }
    }

    const change = changes[0] as
      | MapOpPayload<RawAccountID | AgentID | Everyone, Role>
      | MapOpPayload<"readKey", JsonValue>
      | MapOpPayload<"profile", CoID<RawProfile>>
      | MapOpPayload<"root", CoID<RawCoMap>>
      | MapOpPayload<`parent_${CoID<RawGroup>}`, CoID<RawGroup>>
      | MapOpPayload<`child_${CoID<RawGroup>}`, CoID<RawGroup>>;

    if (changes.length !== 1) {
      logPermissionError("Group transaction must have exactly one change");
      transaction.isValid = false;
      continue;
    }

    if (change.op !== "set") {
      logPermissionError("Group transaction must set a role or readKey");
      transaction.isValid = false;
      continue;
    }

    if (change.key === "readKey") {
      if (memberState[transactor] !== "admin") {
        logPermissionError("Only admins can set readKeys");
        transaction.isValid = false;
        continue;
      }

      transaction.isValid = true;
      continue;
    } else if (change.key === "profile") {
      if (memberState[transactor] !== "admin") {
        logPermissionError("Only admins can set profile");
        transaction.isValid = false;
        continue;
      }

      transaction.isValid = true;
      continue;
    } else if (change.key === "root") {
      if (memberState[transactor] !== "admin") {
        logPermissionError("Only admins can set root");
        continue;
      }

      transaction.isValid = true;
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
        logPermissionError("Only admins can reveal keys");
        transaction.isValid = false;
        continue;
      }

      // TODO: check validity of agents who the key is revealed to?
      transaction.isValid = true;
      continue;
    } else if (isParentExtension(change.key)) {
      if (memberState[transactor] !== "admin") {
        logPermissionError("Only admins can set parent extensions");
        transaction.isValid = false;
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
        logPermissionError(
          "Circular extend detected, dropping the transaction",
        );
        transaction.isValid = false;
        continue;
      }

      transaction.isValid = true;
      continue;
    } else if (isChildExtension(change.key)) {
      transaction.isValid = true;
      continue;
    } else if (isWriteKeyForMember(change.key)) {
      const memberKey = getAccountOrAgentFromWriteKeyForMember(change.key);

      if (
        memberState[transactor] !== "admin" &&
        memberState[transactor] !== "writeOnlyInvite" &&
        memberKey !== transactor
      ) {
        logPermissionError("Only admins can set writeKeys");
        transaction.isValid = false;
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
        logPermissionError(
          "Write key already exists and can't be overridden by invite",
        );
        transaction.isValid = false;
        continue;
      }

      writeKeys.add(change.key);

      transaction.isValid = true;
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
      logPermissionError("Group transaction must set a valid role");
      transaction.isValid = false;
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
      logPermissionError(
        "Everyone can only be set to reader, writer, writeOnly or revoked",
      );
      transaction.isValid = false;
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
      currentAccountId === change.key &&
      transactor === currentAccountId &&
      change.value === "revoked";

    if (!isFirstSelfAppointment && !isSelfRevoke) {
      if (memberState[transactor] === "admin") {
        if (
          memberState[affectedMember] === "admin" &&
          affectedMember !== transactor &&
          assignedRole !== "admin"
        ) {
          logPermissionError("Admins can only demote themselves.");
          transaction.isValid = false;
          continue;
        }
      } else if (memberState[transactor] === "adminInvite") {
        if (change.value !== "admin") {
          logPermissionError("AdminInvites can only create admins.");
          transaction.isValid = false;
          continue;
        }
      } else if (memberState[transactor] === "writerInvite") {
        if (change.value !== "writer") {
          logPermissionError("WriterInvites can only create writers.");
          transaction.isValid = false;
          continue;
        }
      } else if (memberState[transactor] === "readerInvite") {
        if (change.value !== "reader") {
          logPermissionError("ReaderInvites can only create reader.");
          transaction.isValid = false;
          continue;
        }
      } else if (memberState[transactor] === "writeOnlyInvite") {
        if (change.value !== "writeOnly") {
          logPermissionError("WriteOnlyInvites can only create writeOnly.");
          transaction.isValid = false;
          continue;
        }
      } else {
        logPermissionError(
          "Group transaction must be made by current admin or invite",
        );
        transaction.isValid = false;
        continue;
      }
    }

    memberState[affectedMember] = change.value;
    transaction.isValid = true;
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
