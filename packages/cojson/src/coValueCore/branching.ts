import { CoValueCore } from "../exports.js";
import { RawCoID } from "../ids.js";
import { AvailableCoValueCore, idforHeader } from "./coValueCore.js";
import { CoValueHeader } from "./verifiedState.js";

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
    meta: {
      branch: branchName,
      source: sourceId,
    },
    ruleset: {
      type: "ownedByGroup",
      group: ownerId,
    },
    uniqueness: "",
  };
}

export function getBranchId(
  coValue: CoValueCore,
  name: string,
  ownerId: RawCoID,
): RawCoID {
  if (!coValue.verified) {
    throw new Error(
      "CoValueCore: getBranchId called on coValue without verified state",
    );
  }

  const header = getBranchHeader({
    type: coValue.verified.header.type,
    branchName: name,
    ownerId,
    sourceId: coValue.id,
  });

  return idforHeader(header, coValue.node.crypto);
}

export function createBranch(
  coValue: CoValueCore,
  name: string,
  ownerId: RawCoID,
): CoValueCore {
  if (!coValue.verified) {
    throw new Error(
      "CoValueCore: createBranch called on coValue without verified state",
    );
  }

  const header = getBranchHeader({
    type: coValue.verified.header.type,
    branchName: name,
    ownerId,
    sourceId: coValue.id,
  });

  const value = coValue.node.createCoValue(header);

  value.makeTransaction([], "private", {
    branch: coValue.knownState().sessions,
  });

  return value;
}

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

export function mergeBranch(coValue: CoValueCore): CoValueCore {
  if (!coValue.verified) {
    throw new Error(
      "CoValueCore: mergeBranch called on coValue without verified state",
    );
  }

  const sourceId = coValue.verified.header.meta?.source;

  if (!sourceId) {
    throw new Error("CoValueCore: mergeBranch called on a non-branch coValue");
  }

  // TODO: Discover if someone has already merged this branch
  const source = getBranchSource(coValue);

  if (!source) {
    throw new Error("CoValueCore: unable to find source branch");
  }

  const branchValidTransactions = coValue.getValidTransactions();

  coValue.makeTransaction([], "private", {
    merge: coValue.knownState().sessions,
    id: coValue.id,
    count: branchValidTransactions.length,
  });

  // TODO: Copy the meta transactions, except the branch meta
  for (const tx of branchValidTransactions) {
    source.makeTransaction(tx.changes, tx.trusting ? "trusting" : "private");
  }

  return coValue;
}
