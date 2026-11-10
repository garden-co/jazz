import type { GroupMembership, GroupRoot, GroupSuccessor } from "./groups.js";

export type GroupHistoryScope = { groupIds: readonly string[] } | { accountId: string };

export interface GroupHistoryReader {
  roots(selector: { id: { in: string[] } } | { accountId: string }): Promise<GroupRoot[]>;
  members(
    selector:
      | { groupId: { in: string[] } }
      | { memberKind: "group"; memberId: { in: string[] } }
      | { memberKind: "account"; memberId: string },
  ): Promise<GroupMembership[]>;
  successors(groupIds: string[]): Promise<GroupSuccessor[]>;
}

export type GroupHistory = {
  roots: GroupRoot[];
  records: GroupMembership[];
  successors: GroupSuccessor[];
};

// Keep UUID operands well below the 64 KiB shape-registration budget, leaving
// room for predicates and read-view metadata. Every chunk is read; this is not
// a limit on component size or traversal depth.
const idBatchSize = 128;

/** Collect raw historical dependencies; only authenticated replay grants membership. */
export async function collectGroupHistory(
  scope: GroupHistoryScope,
  reader: GroupHistoryReader,
): Promise<GroupHistory> {
  const roots = new Map<string, GroupRoot>();
  const records = new Map<string, GroupMembership>();
  const successors = new Map<string, GroupSuccessor>();
  const visited = new Set<string>();
  let pending = new Set<string>();
  const enqueue = (id: string) => {
    if (!visited.has(id)) pending.add(id);
  };
  const includeMembers = (rows: GroupMembership[]) => {
    for (const row of rows) {
      records.set(row.id, row);
      enqueue(row.groupId);
      if (row.memberKind === "group") enqueue(row.memberId);
    }
  };

  if ("groupIds" in scope) {
    for (const id of scope.groupIds) enqueue(id);
  } else {
    const [created, memberships] = await Promise.all([
      reader.roots({ accountId: scope.accountId }),
      reader.members({ memberKind: "account", memberId: scope.accountId }),
    ]);
    for (const root of created) {
      roots.set(root.id, root);
      enqueue(root.id);
    }
    includeMembers(memberships);
  }

  while (pending.size) {
    const frontier = [...pending];
    pending = new Set();
    for (const id of frontier) visited.add(id);
    for (let start = 0; start < frontier.length; start += idBatchSize) {
      const ids = frontier.slice(start, start + idBatchSize);
      // Empty incoming/outgoing predicates are part of the authoritative read
      // set too: a concurrent connecting edge must invalidate acceptance.
      const [outgoing, incoming] = await Promise.all([
        reader.members({ groupId: { in: ids } }),
        reader.members({ memberKind: "group", memberId: { in: ids } }),
      ]);
      // Removed and invalid candidates still connect historical dependencies.
      includeMembers(outgoing);
      includeMembers(incoming);
    }
  }

  // Roots and epochs cannot extend the raw membership component. Fetch them
  // once its complete ID set is known instead of reopening both per vertex.
  const groupIds = [...visited];
  for (let start = 0; start < groupIds.length; start += idBatchSize) {
    const ids = groupIds.slice(start, start + idBatchSize);
    const missingRoots = ids.filter((id) => !roots.has(id));
    const [found, epochs] = await Promise.all([
      missingRoots.length ? reader.roots({ id: { in: missingRoots } }) : [],
      reader.successors(ids),
    ]);
    for (const root of found) roots.set(root.id, root);
    for (const epoch of epochs) successors.set(epoch.id, epoch);
  }
  return {
    roots: [...roots.values()],
    records: [...records.values()],
    successors: [...successors.values()],
  };
}
