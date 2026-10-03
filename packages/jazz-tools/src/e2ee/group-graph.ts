import type { RowSettlement } from "../runtime/client.js";
import type { GroupRoot, GroupMembership, GroupSuccessor } from "./groups.js";
import { encodePublicApprovalRevision } from "./account-successor.js";
import { encodeGroupMembership } from "./group-successor.js";

type Settled<T> = { rows: T[]; settlements: RowSettlement[] };

export function validGroupGraph(
  graph: ReadonlyMap<string, { children: ReadonlySet<string> }>,
): boolean {
  const depth = (id: string, path: Set<string>): number => {
    if (path.has(id) || !graph.has(id)) return Infinity;
    const next = new Set(path).add(id);
    let longest = 0;
    for (const child of graph.get(id)!.children) {
      longest = Math.max(longest, 1 + depth(child, next));
      if (longest > 8) break;
    }
    return longest;
  };
  return [...graph.keys()].every((id) => depth(id, new Set()) <= 8);
}
type KeyRoot = Pick<
  GroupRoot,
  "id" | "accountId" | "epochId" | "mechanism" | "version" | "verification"
>;
type State = {
  root: GroupRoot;
  direct: Map<string, string>;
  children: Set<string>;
  members: Map<string, string>;
  keyRoot: KeyRoot;
  epochPosition: bigint;
  successor?: GroupSuccessor;
  usedEpochs: Set<string>;
  revisionIds: string[];
  sealed: boolean;
  rotationRequired: boolean;
};

/** Key-free replay; callers supply settled records and the existing device-authentication checks. */
export async function replayGroupGraph(
  snapshot: {
    roots: Settled<GroupRoot>;
    records: Settled<GroupMembership>;
    successors: Settled<GroupSuccessor>;
  },
  authority: {
    root(row: GroupRoot, position: bigint): Promise<boolean>;
    membership(root: GroupRoot, row: GroupMembership, position: bigint): Promise<boolean>;
    successor(root: GroupRoot, row: GroupSuccessor, position: bigint): Promise<boolean>;
    accountEpoch(accountId: string, before?: bigint): Promise<string>;
  },
  cutoff?: bigint,
) {
  const position = (settlements: RowSettlement[], id: string) => {
    const value = settlements.find((entry) => entry.rowId === id)?.position;
    if (value === undefined) throw new Error("Missing E2EE group authority coverage");
    return BigInt(value);
  };
  const events = [
    ...snapshot.roots.rows.map((row) => ({
      kind: "root" as const,
      row,
      at: position(snapshot.roots.settlements, row.id),
    })),
    ...snapshot.records.rows.map((row) => ({
      kind: "member" as const,
      row,
      at: position(snapshot.records.settlements, row.id),
    })),
    ...snapshot.successors.rows.map((row) => ({
      kind: "epoch" as const,
      row,
      at: position(snapshot.successors.settlements, row.id),
    })),
  ].sort((a, b) => {
    if (a.at !== b.at) return a.at < b.at ? -1 : 1;
    const rank = { root: 0, epoch: 1, member: 2 };
    return rank[a.kind] - rank[b.kind] || a.row.id.localeCompare(b.row.id);
  });
  const states = new Map<string, State>();
  const revision = (state: State): Uint8Array => {
    const ids = new Set(state.revisionIds);
    const seen = new Set([state.root.id]);
    const visit = (childId: string) => {
      if (seen.has(childId)) return;
      seen.add(childId);
      const child = states.get(childId)!;
      // Membership IDs retain the flat transcript; other tables need a namespace.
      ids.add(`__e2ee_groups:${child.root.id}`);
      for (const id of child.revisionIds) ids.add(id);
      if (child.successor) ids.add(`__e2ee_group_successors:${child.successor.id}`);
      for (const id of child.children) visit(id);
    };
    for (const id of state.children) visit(id);
    return encodePublicApprovalRevision([...ids]);
  };
  // ponytail: replay scans the accepted graph per transaction; incremental
  // ancestor invalidation can replace this if lifecycle-scale measurements require it.
  const reconcile = () => {
    const effective = new Map<string, Map<string, string>>();
    const collect = (state: State): Map<string, string> => {
      const prior = effective.get(state.root.id);
      if (prior) return prior;
      const members = state.sealed ? new Map<string, string>() : new Map(state.direct);
      effective.set(state.root.id, members);
      if (!state.sealed)
        for (const child of state.children)
          for (const [accountId, epoch] of collect(states.get(child)!))
            if (!members.has(accountId)) members.set(accountId, epoch);
      return members;
    };
    for (const state of states.values()) collect(state);
    for (const state of states.values()) {
      const next = effective.get(state.root.id)!;
      for (const accountId of state.members.keys())
        if (!next.has(accountId)) state.rotationRequired = true;
      for (const accountId of next.keys())
        if (state.members.has(accountId)) next.set(accountId, state.members.get(accountId)!);
      state.members = next;
      if (next.size === 0) state.sealed = true;
    }
  };
  let previousPosition: bigint | undefined;
  for (const event of events) {
    if (cutoff !== undefined && event.at >= cutoff) break;
    if (previousPosition !== undefined && event.at !== previousPosition) reconcile();
    previousPosition = event.at;
    if (event.kind === "root") {
      const root = event.row;
      if (states.has(root.id) || !(await authority.root(root, event.at))) continue;
      states.set(root.id, {
        root,
        direct: new Map([[root.accountId, root.accountEpochId]]),
        children: new Set(),
        members: new Map([[root.accountId, root.accountEpochId]]),
        keyRoot: root,
        epochPosition: event.at,
        usedEpochs: new Set([root.epochId]),
        revisionIds: [],
        sealed: false,
        rotationRequired: false,
      });
      continue;
    }
    const state = states.get(event.row.groupId);
    if (!state || state.sealed) continue;
    if (event.kind === "epoch") {
      const row = event.row;
      if (
        event.at <= state.epochPosition ||
        row.predecessor !== state.keyRoot.epochId ||
        state.usedEpochs.has(row.epochId) ||
        !state.members.has(row.authorAccountId)
      )
        continue;
      const next = new Map<string, string>();
      for (const accountId of state.members.keys())
        next.set(accountId, await authority.accountEpoch(accountId, event.at));
      const expectedRevision = revision(state);
      const expectedMembers = encodeGroupMembership(next);
      if (
        next.get(row.authorAccountId) !== row.authorEpochId ||
        expectedRevision.length !== row.revision.length ||
        !expectedRevision.every((byte, index) => byte === row.revision[index]) ||
        expectedMembers.length !== row.membership.length ||
        !expectedMembers.every((byte, index) => byte === row.membership[index]) ||
        !(await authority.successor(state.root, row, event.at))
      )
        continue;
      state.members = next;
      for (const accountId of state.direct.keys())
        state.direct.set(accountId, next.get(accountId)!);
      state.keyRoot = { ...state.keyRoot, epochId: row.epochId, verification: row.verification };
      state.epochPosition = event.at;
      state.successor = row;
      state.usedEpochs.add(row.epochId);
      state.rotationRequired = false;
      continue;
    }
    const row = event.row;
    state.revisionIds.push(row.id);
    if (
      event.at <= position(snapshot.roots.settlements, state.root.id) ||
      row.epochId !== state.keyRoot.epochId ||
      !(await authority.membership(state.root, row, event.at))
    )
      continue;
    if (row.memberKind === "account") {
      if (row.operation === "remove") state.direct.delete(row.memberId);
      else if (row.operation === "add" && !state.direct.has(row.memberId))
        state.direct.set(row.memberId, await authority.accountEpoch(row.memberId, event.at));
    } else if (row.memberKind === "group") {
      if (row.operation === "remove") state.children.delete(row.memberId);
      else if (row.operation === "add" && !state.children.has(row.memberId)) {
        const child = states.get(row.memberId);
        if (!child || child.sealed) continue;
        state.children.add(row.memberId);
        if (!validGroupGraph(states)) state.children.delete(row.memberId);
      }
    }
  }
  reconcile();
  return new Map(
    await Promise.all(
      [...states].map(async ([id, state]) => {
        for (const [accountId, epoch] of state.members)
          if ((await authority.accountEpoch(accountId, cutoff)) !== epoch)
            state.rotationRequired = true;
        return [
          id,
          {
            members: state.members,
            sealed: state.sealed,
            rotationRequired: state.rotationRequired,
            keyRoot: state.keyRoot,
            epochPosition: state.epochPosition,
            successor: state.successor,
            revision: revision(state),
            children: state.children,
          },
        ] as const;
      }),
    ),
  );
}
