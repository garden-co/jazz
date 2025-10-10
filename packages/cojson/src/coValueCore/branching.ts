import type { CoValueCore } from "../exports.js";
import type { RawCoID, SessionID } from "../ids.js";
import { type AvailableCoValueCore, idforHeader } from "./coValueCore.js";
import type { CoValueHeader } from "./verifiedState.js";
import type { CoValueKnownState } from "../sync.js";
import { combineKnownStateSessions } from "../knownState.js";

/**
 * Commit to identify the starting point of the branch
 *
 * In case of clonflicts, the first commit of this kind is considered the source of truth
 */
export type BranchStartCommit = {
  from: CoValueKnownState["sessions"];
};

/**
 * Commit that tracks a branch creation
 */
export type BranchPointerCommit = {
  branch: string;
  ownerId?: RawCoID;
};

/**
 * Meta information attached to each merged transaction to retrieve the original transaction ID
 */
export type MergedTransactionMetadata = {
  mi: number; // Transaction index and marker of a merge commit
  t?: number;
  s?: SessionID;
  b?: RawCoID;
};

/**
 * Merge commit located in a branch to track how many transactions have already been merged
 */
export type MergeCommit = {
  merged: CoValueKnownState["sessions"];
  branch: RawCoID;
};

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
 * Checks if the coValue can be branched
 */
export function canBeBranched(coValue: CoValueCore): boolean {
  return coValue.verified?.header.ruleset.type === "ownedByGroup";
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

  const currentOwnerId = ownerId ?? getBranchOwnerId(coValue);

  if (!currentOwnerId) {
    return coValue.id;
  }

  const header = getBranchHeader({
    type: coValue.verified.header.type,
    branchName: name,
    ownerId: currentOwnerId,
    sourceId: coValue.id,
  });

  return idforHeader(header, coValue.node.crypto);
}

export function getBranchOwnerId(coValue: CoValueCore) {
  if (!coValue.verified) {
    throw new Error(
      "CoValueCore: getBranchOwnerId called on coValue without verified state",
    );
  }

  const header = coValue.verified.header;

  if (header.ruleset.type !== "ownedByGroup") {
    return undefined;
  }

  return header.ruleset.group;
}

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

  const branchOwnerId = ownerId ?? getBranchOwnerId(coValue);

  if (!branchOwnerId) {
    return coValue;
  }

  const header = getBranchHeader({
    type: coValue.verified.header.type,
    branchName: name,
    ownerId: branchOwnerId,
    sourceId: coValue.id,
  });

  const branch = coValue.node.createCoValue(header);
  const sessions = { ...coValue.knownState().sessions };

  // Create a branch commit to identify the starting point of the branch
  branch.makeTransaction([], "private", {
    from: sessions,
  } satisfies BranchStartCommit);

  // Create a branch pointer, to identify that we created a branch
  coValue.makeTransaction([], "private", {
    branch: name,
    ownerId,
  } satisfies BranchPointerCommit);

  return branch;
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

/**
 * Given a branch coValue, merges the branch into the source coValue
 */
export function mergeBranch(branch: CoValueCore): CoValueCore {
  if (!branch.verified) {
    throw new Error(
      "CoValueCore: mergeBranch called on coValue without verified state",
    );
  }

  if (!canBeBranched(branch)) {
    return branch;
  }

  const target = getBranchSource(branch);

  if (!target) {
    throw new Error("CoValueCore: unable to find source branch");
  }

  // Look for previous merge commits, to see which transactions needs to be merged
  // Done mostly for performance reasons, as we could merge all the transactions every time and nothing would change
  let mergedTransactions = {} as CoValueKnownState["sessions"];
  for (const item of target.getMergeCommits()) {
    if (item.branch === branch.id) {
      mergedTransactions = combineKnownStateSessions(
        mergedTransactions,
        item.merged,
      );
    }
  }

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

  // We do track in the meta information the original txID to make sure that
  // the CoList opid still point to the correct transaction
  // To reduce the cost of the meta we skip the repeated information
  let lastSessionId: string | undefined = undefined;
  let lastBranchId: string | undefined = undefined;
  let lastOriginalMadeAt: number = 0;

  const now = Date.now();

  for (const tx of branchValidTransactions) {
    const mergeMeta: MergedTransactionMetadata = {
      mi: tx.txID.txIndex,
    };

    // We compress the original made at, to reduce the size of the meta information
    if (tx.madeAt !== lastOriginalMadeAt) {
      // Storing the diff with madeAt to consume less bytes in the meta information
      mergeMeta.t = now - tx.madeAt;
    }

    if (lastSessionId !== tx.txID.sessionID) {
      mergeMeta.s = tx.txID.sessionID;
    }

    if (lastBranchId !== tx.txID.branch) {
      mergeMeta.b = tx.txID.branch;
    }

    target.makeTransaction(tx.changes, tx.tx.privacy, mergeMeta, now);
    lastSessionId = tx.txID.sessionID;
    lastBranchId = tx.txID.branch;
    lastOriginalMadeAt = tx.madeAt;
  }

  // Track the merged transactions for the branch, so future merges will know which transactions have already been merged
  // Store only the diff of sessions between the branch and already merged transactions
  const currentSessions = branch.knownState().sessions;
  const prevMergedSessions = mergedTransactions;
  const diff = {} as CoValueKnownState["sessions"];

  for (const [sessionId, count] of Object.entries(currentSessions) as [
    SessionID,
    number,
  ][]) {
    const prevMergedSession = prevMergedSessions[sessionId] ?? 0;
    if (prevMergedSession < count) {
      diff[sessionId] = count;
    }
  }

  target.makeTransaction([], "private", {
    merged: diff,
    branch: branch.id,
  } satisfies MergeCommit);

  return target;
}
