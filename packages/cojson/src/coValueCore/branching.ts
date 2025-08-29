import { CoValueCore } from "../exports.js";
import { RawCoID, SessionID } from "../ids.js";
import { AvailableCoValueCore, idforHeader } from "./coValueCore.js";
import { CoValueHeader } from "./verifiedState.js";
import { CoValueKnownState } from "../sync.js";

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

  const sourceId = coValue.verified.header.meta?.source;

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

  const sourceId = branch.verified.header.meta?.source;

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

  for (const { tx, changes } of branchValidTransactions) {
    target.makeTransaction(changes, tx.privacy);
  }

  return target;
}
