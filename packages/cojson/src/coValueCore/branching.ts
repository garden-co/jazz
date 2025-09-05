import type { CoValueCore, JsonValue } from "../exports.js";
import type { RawCoID, SessionID, TransactionID } from "../ids.js";
import { type AvailableCoValueCore, idforHeader } from "./coValueCore.js";
import type { CoValueHeader } from "./verifiedState.js";
import type { CoValueKnownState } from "../sync.js";
import type { ListOpPayload, OpID } from "../coValues/coList.js";

export function getBranchHeader({
  type,
  branchName,
  ownerId,
  sourceId,
}: {
  type: CoValueHeader["type"];
  branchName: string;
  ownerId: RawCoID;
  sourceId: RawCoID;
}): CoValueHeader {
  return {
    type,
    // Branch name and source id are stored in the meta field
    // and used to generate the unique id for the branch
    meta: {
      branch: branchName,
      source: sourceId,
    },
    ruleset: {
      type: "ownedByGroup",
      // The owner is part of the id generation, making it possible to have multiple branches with the same name
      // but different owners
      group: ownerId,
    },
    // The meta is enough to have reproducible unique id for the branch
    uniqueness: "",
  };
}

/**
 * Given a coValue, a branch name and an owner id, returns the id for the branch
 */
export function getBranchId(
  coValue: CoValueCore,
  name: string,
  ownerId?: RawCoID,
): RawCoID {
  if (!coValue.verified) {
    throw new Error(
      "CoValueCore: getBranchId called on coValue without verified state",
    );
  }

  if (!ownerId) {
    const header = coValue.verified.header;

    // Group and account coValues can't have branches, so we return the source id
    if (header.ruleset.type !== "ownedByGroup") {
      return coValue.id;
    }

    ownerId = header.ruleset.group;
  }

  const header = getBranchHeader({
    type: coValue.verified.header.type,
    branchName: name,
    ownerId,
    sourceId: coValue.id,
  });

  return idforHeader(header, coValue.node.crypto);
}

export type BranchCommit = {
  branch: CoValueKnownState["sessions"];
};

/**
 * Given a coValue, a branch name and an owner id, creates a new branch CoValue
 */
export function createBranch(
  coValue: CoValueCore,
  name: string,
  ownerId?: RawCoID,
): CoValueCore {
  if (!coValue.verified) {
    throw new Error(
      "CoValueCore: createBranch called on coValue without verified state",
    );
  }

  if (!ownerId) {
    const header = coValue.verified.header;

    // Group and account coValues can't have branches, so we return the source coValue
    if (header.ruleset.type !== "ownedByGroup") {
      return coValue;
    }

    ownerId = header.ruleset.group;
  }

  const header = getBranchHeader({
    type: coValue.verified.header.type,
    branchName: name,
    ownerId,
    sourceId: coValue.id,
  });

  const value = coValue.node.createCoValue(header);

  // Create a branch commit to identify the starting point of the branch
  value.makeTransaction([], "private", {
    branch: coValue.knownState().sessions,
  } satisfies BranchCommit);

  return value;
}

/**
 * Given a branch coValue, returns the source coValue if available
 */
export function getBranchSource(
  coValue: CoValueCore,
): AvailableCoValueCore | undefined {
  if (!coValue.verified) {
    return undefined;
  }

  const sourceId = coValue.getCurrentBranchSourceId();

  if (!sourceId) {
    return undefined;
  }

  const source = coValue.node.getCoValue(sourceId as RawCoID);

  if (!source.isAvailable()) {
    return undefined;
  }

  return source;
}

export type MergeCommit = {
  // The point where the branch was merged
  merge: CoValueKnownState["sessions"];
  // The id of the branch that was merged
  id: RawCoID;
  // The number of transactions that were merged, will be used in the future to handle the edits history properly
  count: number;
};

/**
 * Given a branch coValue, merges the branch into the source coValue
 */
export function mergeBranch(branch: CoValueCore): CoValueCore {
  if (!branch.verified) {
    throw new Error(
      "CoValueCore: mergeBranch called on coValue without verified state",
    );
  }

  if (branch.verified.header.ruleset.type !== "ownedByGroup") {
    return branch;
  }

  const sourceId = branch.getCurrentBranchSourceId();

  if (!sourceId) {
    throw new Error("CoValueCore: mergeBranch called on a non-branch coValue");
  }

  const target = getBranchSource(branch);

  if (!target) {
    throw new Error("CoValueCore: unable to find source branch");
  }

  // Look for previous merge commits, to see which transactions needs to be merged
  // Done mostly for performance reasons, as we could merge all the transactions every time and nothing would change
  const mergedTransactions = target.mergeCommits.reduce(
    (acc, { commit }) => {
      if (commit.id !== branch.id) {
        return acc;
      }

      for (const [sessionID, count] of Object.entries(commit.merge) as [
        SessionID,
        number,
      ][]) {
        acc[sessionID] = Math.max(acc[sessionID] ?? 0, count);
      }

      return acc;
    },
    {} as CoValueKnownState["sessions"],
  );

  // Get the valid transactions from the branch, skipping the branch source and the previously merged transactions
  const branchValidTransactions = branch
    .getValidTransactions({
      from: mergedTransactions,
      ignorePrivateTransactions: false,
      skipBranchSource: true,
    })
    .filter((tx) => tx.changes.length > 0);

  // If there are no valid transactions to merge, we don't want to create a merge commit
  if (branchValidTransactions.length === 0) {
    return target;
  }

  // Create a merge commit to identify the merge point
  target.makeTransaction([], "private", {
    merge: { ...branch.knownState().sessions },
    id: branch.id,
    count: branchValidTransactions.length,
  } satisfies MergeCommit);

  const currentSessionID = target.node.currentSessionID;

  if (
    target.verified.header.type === "colist" ||
    target.verified.header.type === "coplaintext"
  ) {
    const mapping: Record<`${SessionID}:${number}`, number> = {};

    const session = target.verified.sessions.get(currentSessionID);
    let txIdx = session ? session.transactions.length : 0;

    // Create a mapping from the branch transactions to the target transactions
    for (const { txID } of branchValidTransactions) {
      mapping[`${txID.sessionID}:${txID.txIndex}`] = txIdx;
      txIdx++;
    }

    for (const { tx, changes } of branchValidTransactions) {
      target.makeTransaction(
        mapCoListChangesToTarget(
          changes as ListOpPayload<JsonValue>[],
          currentSessionID,
          mapping,
        ),
        tx.privacy,
      );
    }
  } else {
    for (const { tx, changes } of branchValidTransactions) {
      target.makeTransaction(changes, tx.privacy);
    }
  }

  return target;
}

/**
 * Given a list of changes, maps the opIDs to the target transactions
 */
function mapCoListChangesToTarget(
  changes: ListOpPayload<JsonValue>[],
  currentSessionID: SessionID,
  mapping: Record<`${SessionID}:${number}`, number>,
) {
  return changes.map((change) => {
    if (change.op === "app") {
      if (change.after === "start") {
        return change;
      }

      return {
        ...change,
        after: convertOpID(change.after, currentSessionID, mapping),
      };
    }

    if (change.op === "del") {
      return {
        ...change,
        insertion: convertOpID(change.insertion, currentSessionID, mapping),
      };
    }

    if (change.op === "pre") {
      if (change.before === "end") {
        return change;
      }

      return {
        ...change,
        before: convertOpID(change.before, currentSessionID, mapping),
      };
    }

    return change;
  });
}

function convertOpID(
  opID: OpID,
  sessionID: SessionID,
  mapping: Record<`${SessionID}:${number}`, number>,
) {
  // If the opID comes from the source branch, we don't need to map it
  if (!opID.branch) {
    return opID;
  }

  const mappedIndex = mapping[`${opID.sessionID}:${opID.txIndex}`];

  // If the opID doesn't exist in the mapping, we don't need to map it
  if (mappedIndex === undefined) {
    return opID;
  }

  return {
    sessionID: sessionID,
    txIndex: mappedIndex,
    changeIdx: opID.changeIdx,
  };
}
