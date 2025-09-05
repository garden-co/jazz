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
  from: CoValueKnownState["sessions"];
};

export type BranchPointerCommit = {
  branch: string;
  ownerId?: RawCoID;
};

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

  // Create a branch commit to identify the starting point of the branch
  branch.makeTransaction([], "private", {
    from: coValue.knownState().sessions,
  } satisfies BranchCommit);

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

export type MergedTransactionCommit = {
  i: number;
  s?: SessionID;
  b?: RawCoID;
  mergeEnd?: 1;
};

export type MergeStartCommit = {
  merge: CoValueKnownState["sessions"];
  b: RawCoID;
  s: SessionID;
  i: number;
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
    (acc, commit) => {
      if (commit.b !== branch.id) {
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

  // We do track in the meta information the original txID to make sure that
  // the CoList opid still point to the correct transaction
  // To reduce the cost of the meta we skip the repeated information
  let lastSessionId: string | undefined = undefined;
  let lastBranchId: string | undefined = undefined;
  branchValidTransactions.forEach((tx, i) => {
    const mergeMeta: MergedTransactionCommit & Partial<MergeStartCommit> = {
      i: tx.txID.txIndex,
    };

    if (i === 0) {
      mergeMeta.merge = branch.knownState().sessions;
      mergeMeta.b = branch.id;
      mergeMeta.s = tx.txID.sessionID;
    }

    if (i === branchValidTransactions.length - 1) {
      mergeMeta.mergeEnd = 1;
    }

    if (lastSessionId !== tx.txID.sessionID) {
      mergeMeta.s = tx.txID.sessionID;
    }

    if (lastBranchId !== tx.txID.branch) {
      mergeMeta.b = branch.id;
    }

    target.makeTransaction(tx.changes, tx.tx.privacy, mergeMeta, tx.madeAt);
    lastSessionId = tx.txID.sessionID;
    lastBranchId = tx.txID.branch;
  });

  return target;
}
